package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type verifiedHostRebootProof struct {
	proof      model.HostRebootProof
	receivedAt time.Time
	identities map[string]*verifiedHostRebootProof
}

func dataForRebootRequest(request model.HostRebootRequest) []byte {
	request.AgentStartedAt = request.AgentStartedAt.UTC()
	data, _ := json.Marshal(request)
	return data
}

// The challenge is read-only. A service-key holder may relay it, but cannot
// forge a new kernel boot or replay a proof for a different random challenge.
func (s *Server) requestHostRebootProof(ctx context.Context, agent model.Agent) (*verifiedHostRebootProof, error) {
	if s.config.HostRebootToken == "" {
		return nil, errors.New("host-control authentication is unavailable")
	}
	nonce := rand.Text()
	data, _ := json.Marshal(map[string]string{"nonce": nonce})
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(agent.URL, "/")+"/api/v1/host/reboot-status", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+s.config.Token)
	client := *s.client
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(data) > 4096 || response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, errors.New("host boot proof is unavailable")
	}
	var proof model.HostRebootProof
	if json.Unmarshal(data, &proof) != nil || proof.Nonce != nonce || proof.AgentID != agent.ID || proof.NodeID != agentHostNodeID(agent) || !proof.AgentStartedAt.Equal(agent.StartedAt) || proof.BootID == "" {
		return nil, errors.New("host boot proof identity is invalid")
	}
	signature := proof.Signature
	proof.Signature = ""
	proof.AgentStartedAt = proof.AgentStartedAt.UTC()
	data, err = json.Marshal(proof)
	if err != nil || signature == "" || !auth.Equal(signature, auth.HostRebootStatusSignature(s.config.HostRebootToken, data)) {
		return nil, errors.New("host boot proof signature is invalid")
	}
	proof.Signature = signature
	return &verifiedHostRebootProof{proof: proof, receivedAt: time.Now().UTC()}, nil
}

func proofShowsNewBoot(proof *verifiedHostRebootProof, nodeID string, fence hostRebootFence) bool {
	return proof != nil && proof.proof.NodeID == nodeID && proof.proof.BootID != fence.BootID && proof.proof.StartupReconciled && !proof.proof.RebootPending && proof.proof.AgentStartedAt.After(fence.AgentStartedAt) && proof.receivedAt.After(fence.RequestedAt)
}

func (s *Server) rebootProofCandidates(host hostRebootHostStatus, target hostRebootTarget) []model.Agent {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	var agents []model.Agent
	for _, agent := range s.state.agents {
		if agent.Host != nil && agentHostNodeID(agent) == host.NodeID && agentIsOnline(agent, time.Now()) && agent.StartedAt.After(target.AgentStartedAt) && agent.Host.BootID != host.BootID {
			agents = append(agents, cloneAgentHost(agent))
		}
	}
	slices.SortFunc(agents, func(a, b model.Agent) int { return strings.Compare(a.ID, b.ID) })
	return agents
}

func (s *Server) confirmRebootProofs(ctx context.Context, status *hostRebootStatus) error {
	var workers sync.WaitGroup
	var failuresMu sync.Mutex
	var failures []error
	limit := make(chan struct{}, 4)
	for i, host := range status.Hosts {
		if host.State != "rejected" && ((host.State != "requested" && host.State != "unknown") || host.RequestedAt == nil) {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				return
			}
			requested := status.CreatedAt
			if host.RequestedAt != nil {
				requested = *host.RequestedAt
			}
			fence := hostRebootFence{BootID: host.BootID, AgentStartedAt: status.Targets[i].AgentStartedAt, RequestedAt: requested}
			candidates := s.rebootProofCandidates(host, status.Targets[i])
			if host.State == "rejected" {
				// A durably rejected request never committed a reboot. A fresh,
				// ready proof of the unchanged boot can therefore restore its hold.
				s.state.mu.RLock()
				for _, agent := range s.state.agents {
					if agentHostNodeID(agent) == host.NodeID && agentIsOnline(agent, time.Now()) && agent.Host != nil && agent.Host.BootID == host.BootID && !agent.StartedAt.Before(status.Targets[i].AgentStartedAt) {
						candidates = append(candidates, cloneAgentHost(agent))
					}
				}
				s.state.mu.RUnlock()
			}
			for _, agent := range candidates {
				proof, err := s.requestHostRebootProof(ctx, agent)
				readyRefusal := err == nil && host.State == "rejected" && proof.proof.BootID == host.BootID && proof.proof.StartupReconciled && !proof.proof.RebootPending
				if err == nil && (proofShowsNewBoot(proof, host.NodeID, fence) || readyRefusal) {
					err := s.verifyHostRebootParticipationIdentities(ctx, status.ID, host.NodeID, proof)
					if err == nil {
						err = s.restoreHostRebootParticipation(*status, i, agent, proof)
					}
					if err != nil {
						status.Hosts[i].Error = "Host return verified, but participation restoration failed: " + err.Error()
						failuresMu.Lock()
						failures = append(failures, err)
						failuresMu.Unlock()
						return
					}
					if host.State != "rejected" {
						status.Hosts[i].State, status.Hosts[i].Error = "rebooted", ""
					}
					return
				}
			}
		}()
	}
	workers.Wait()
	return errors.Join(failures...)
}
