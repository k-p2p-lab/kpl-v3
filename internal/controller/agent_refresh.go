package controller

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const agentRefreshTimeout = 15 * time.Second
const agentDiscoveryLimit = 256

type agentRefreshFailure struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Error string `json:"error"`
}

type agentDiscoveryResult struct {
	Enabled   bool   `json:"enabled"`
	Addresses int    `json:"addresses"`
	Added     int    `json:"added"`
	Error     string `json:"error,omitempty"`
}

type agentRefreshResponse struct {
	Agents    []model.Agent         `json:"agents"`
	Requested int                   `json:"requested"`
	Refreshed int                   `json:"refreshed"`
	Failures  []agentRefreshFailure `json:"failures"`
	Discovery *agentDiscoveryResult `json:"discovery,omitempty"`
}

type agentRefreshTarget struct {
	ID, Name, URL string
	Discover      bool
}

func (s *Server) handleAgentRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	discover := false
	if raw := r.URL.Query().Get("discover"); raw != "" {
		var err error
		discover, err = strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "discover must be a boolean")
			return
		}
	}
	if !s.agentRefreshMu.TryLock() {
		writeError(w, http.StatusConflict, "Agent refresh is already in progress")
		return
	}
	defer s.agentRefreshMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), agentRefreshTimeout)
	defer cancel()
	writeJSON(w, http.StatusOK, s.refreshAgents(ctx, discover))
}

// Check offline Agents too, without restarting tasks or interrupting Peers.
// Retain unreachable records; normal heartbeat expiry determines offline state.
func (s *Server) refreshRegisteredAgents(ctx context.Context) agentRefreshResponse {
	return s.refreshAgents(ctx, false)
}

func (s *Server) discoverAgentTargets(ctx context.Context) ([]agentRefreshTarget, *agentDiscoveryResult) {
	host := strings.TrimSpace(s.config.AgentDiscoveryDNS)
	result := &agentDiscoveryResult{Enabled: host != ""}
	if host == "" {
		result.Error = "Agent discovery is not configured. Redeploy the Swarm stack with the updated configuration."
		return nil, result
	}
	// A DNS outage must leave time to refresh previously registered addresses.
	dnsCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	lookup := s.agentLookupIP
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	addresses, err := lookup(dnsCtx, host)
	if err != nil {
		result.Error = fmt.Sprintf("Could not discover Agent tasks: %v", err)
		return nil, result
	}
	urls := make(map[string]struct{})
	for _, address := range addresses {
		if address.IP.To16() == nil || address.IP.IsUnspecified() || address.IP.IsMulticast() || address.Zone != "" {
			continue
		}
		urls["http://"+net.JoinHostPort(address.IP.String(), "8090")] = struct{}{}
	}
	ordered := make([]string, 0, len(urls))
	for url := range urls {
		ordered = append(ordered, url)
	}
	sort.Strings(ordered)
	result.Addresses = len(ordered)
	if len(ordered) == 0 {
		result.Error = "No Agent task addresses were found. Check Swarm task placement and startup logs."
	}
	if len(ordered) > agentDiscoveryLimit {
		result.Error = fmt.Sprintf("Discovery is limited to %d of %d task addresses", agentDiscoveryLimit, len(ordered))
		ordered = ordered[:agentDiscoveryLimit]
	}
	targets := make([]agentRefreshTarget, 0, len(ordered))
	for _, url := range ordered {
		targets = append(targets, agentRefreshTarget{Name: url, URL: url, Discover: true})
	}
	return targets, result
}

func (s *Server) refreshAgentTarget(ctx context.Context, target agentRefreshTarget) (model.Agent, error) {
	var heartbeat model.AgentHeartbeat
	if err := s.callAgent(ctx, target.URL, http.MethodGet, "/api/v1/status", nil, &heartbeat); err != nil {
		return model.Agent{}, err
	}
	identity := heartbeat.Agent
	if identity.ID == "" || !target.Discover && target.ID != "" && identity.ID != target.ID {
		return model.Agent{}, fmt.Errorf("Agent identity mismatch: received %q", identity.ID)
	}
	registered, exists := s.agent(identity.ID)
	if target.Discover && (!exists || !identity.StartedAt.Equal(registered.StartedAt)) {
		// GET status alone cannot register an unknown Agent. Ask it to register
		// through its authenticated Controller connection, including settings
		// and history replay. The Agent never accepts a supplied Controller URL.
		if err := s.callAgent(ctx, target.URL, http.MethodPost, "/api/v1/registration/refresh", nil, &heartbeat); err != nil {
			return identity, fmt.Errorf("re-register Agent: %w", err)
		}
		if heartbeat.Agent.ID != identity.ID || !heartbeat.Agent.StartedAt.Equal(identity.StartedAt) {
			return identity, fmt.Errorf("Agent instance changed during discovery; retry")
		}
		registered, exists = s.agent(identity.ID)
		if !exists || !heartbeat.Agent.StartedAt.Equal(registered.StartedAt) {
			return identity, fmt.Errorf("Agent did not register with this Controller; check its Controller URL and credentials")
		}
	}
	return heartbeat.Agent, s.state.heartbeat(heartbeat)
}

func (s *Server) refreshAgents(ctx context.Context, discover bool) agentRefreshResponse {
	agents := s.state.agentInventory()
	initial := make(map[string]bool, len(agents))
	result := agentRefreshResponse{Failures: []agentRefreshFailure{}}
	var targets []agentRefreshTarget
	if discover {
		targets, result.Discovery = s.discoverAgentTargets(ctx)
	}
	// Try current task addresses before obsolete registered addresses. DNS can
	// return either overlay on multi-network tasks; final accounting uses IDs.
	byURL := make(map[string]int, len(targets)+len(agents))
	for i, target := range targets {
		byURL[target.URL] = i
	}
	for _, agent := range agents {
		initial[agent.ID] = true
		url := strings.TrimRight(agent.URL, "/")
		if i, ok := byURL[url]; ok && targets[i].Discover && targets[i].ID == "" {
			targets[i].ID, targets[i].Name = agent.ID, agent.Name
		} else {
			byURL[url] = len(targets)
			targets = append(targets, agentRefreshTarget{ID: agent.ID, Name: agent.Name, URL: url})
		}
	}
	failures := make([]error, len(targets))
	reported := make([]model.Agent, len(targets))
	jobs := make(chan int, len(targets))
	for i := range targets {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(4, len(targets)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				if failures[i] = ctx.Err(); failures[i] == nil {
					reported[i], failures[i] = s.refreshAgentTarget(ctx, targets[i])
				}
			}
		}()
	}
	workers.Wait()

	s.state.markStaleAgents(agentStaleAfter)
	success := make(map[string]bool)
	for i, err := range failures {
		if err == nil {
			success[reported[i].ID] = true
		}
	}
	failedIDs := make(map[string]bool)
	for i, err := range failures {
		if err == nil {
			continue
		}
		id, name := targets[i].ID, targets[i].Name
		if reported[i].ID != "" {
			id, name = reported[i].ID, firstNonEmpty(reported[i].Name, reported[i].ID)
		}
		if id != "" && (success[id] || failedIDs[id]) {
			continue
		}
		failedIDs[id] = true
		result.Failures = append(result.Failures, agentRefreshFailure{ID: id, Name: name, Error: err.Error()})
	}
	result.Refreshed = len(success)
	result.Requested = result.Refreshed + len(result.Failures)
	if result.Discovery != nil {
		for id := range success {
			if !initial[id] {
				result.Discovery.Added++
			}
		}
	}
	result.Agents = s.state.agentInventory()
	if result.Agents == nil {
		result.Agents = []model.Agent{}
	}
	return result
}
