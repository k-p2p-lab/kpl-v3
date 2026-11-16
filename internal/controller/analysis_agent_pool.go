package controller

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/contextlock"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type analysisWorkerTarget struct {
	url   string
	label string
	agent *model.Agent
}

// cancelMu protects host maintenance holds. A group's executor set is captured
// after Peer cleanup, so every host is used by at most one calculation.
func (s *Server) analysisWorkerTargetsLocked() []analysisWorkerTarget {
	if s.config.ControlNodeID == "" || s.config.AnalysisAgentToken == "" {
		return nil
	}
	agents := s.state.agentInventory()
	sort.Slice(agents, func(i, j int) bool {
		iHost, jHost := agentHostNodeID(agents[i]), agentHostNodeID(agents[j])
		if iHost != jHost {
			return iHost < jHost
		}
		return agents[i].ID < agents[j].ID
	})
	now := time.Now()
	seenHosts, seenURLs := make(map[string]bool), make(map[string]bool)
	var workers []analysisWorkerTarget
	for _, agent := range agents {
		host := agentHostNodeID(agent)
		if seenHosts[host] || seenURLs[agent.URL] || !s.analysisAgentEligibleLocked(agent, now) {
			continue
		}
		seenHosts[host], seenURLs[agent.URL] = true, true
		name := strings.TrimSpace(agent.Name)
		if name == "" {
			name = strings.TrimSpace(agent.Hostname)
		}
		if name == "" {
			name = agent.ID
		}
		workerAgent := cloneAgentHost(agent)
		workers = append(workers, analysisWorkerTarget{url: agent.URL, label: "Agent " + name, agent: &workerAgent})
	}
	return workers
}

// Caller holds cancelMu, which protects host maintenance holds.
func (s *Server) analysisAgentEligibleLocked(agent model.Agent, now time.Time) bool {
	host := agentHostNodeID(agent)
	if host == "" || host == s.config.ControlNodeID || !agent.AnalysisProcesses || agent.Disabled ||
		!agent.StartupReconciled || !agentIsOnline(agent, now) || agent.StartedAt.IsZero() ||
		agent.ActiveNodes != 0 || s.hostRebootActiveNodes[host] != "" || agent.Host != nil && agent.Host.RebootPending {
		return false
	}
	_, err := remoteAnalysisURL(agent.URL)
	return err == nil
}

// A frozen pool retains its hosts and instances, while eligibility can change
// before the next calculation starts. Never adopt a replacement endpoint here.
func (s *Server) analysisWorkerEligibleLocked(endpoint string, expected *model.Agent) bool {
	if expected == nil {
		return false
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	current, exists := s.state.agents[expected.ID]
	return exists && expected.URL == endpoint && current.URL == endpoint &&
		current.StartedAt.Equal(expected.StartedAt) && agentHostNodeID(current) == agentHostNodeID(*expected) &&
		s.analysisAgentEligibleLocked(current, time.Now())
}

func (s *Server) checkAnalysisWorkerAdmission(ctx context.Context, endpoint string, expected *model.Agent) error {
	if err := contextlock.Lock(ctx, &s.cancelMu); err != nil {
		return err
	}
	defer s.cancelMu.Unlock()
	if !s.analysisWorkerEligibleLocked(endpoint, expected) {
		return errors.New("assigned analysis Agent is unavailable or changed; refresh Agents and retry the analysis")
	}
	return nil
}
