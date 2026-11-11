package controller

import (
	"fmt"
	"sort"
	"strings"
	"time"

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
	if len(s.config.AnalysisWorkerURLs) != 0 {
		workers := make([]analysisWorkerTarget, 0, len(s.config.AnalysisWorkerURLs))
		for i, endpoint := range s.config.AnalysisWorkerURLs {
			workers = append(workers, analysisWorkerTarget{url: endpoint, label: fmt.Sprintf("Analysis worker %d", i+1)})
		}
		return workers
	}
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
		if host == "" || host == s.config.ControlNodeID || seenHosts[host] || seenURLs[agent.URL] ||
			!agent.AnalysisProcesses || agent.Disabled || !agent.StartupReconciled || !agentIsOnline(agent, now) ||
			agent.StartedAt.IsZero() || agent.ActiveNodes != 0 || s.hostRebootActiveNodes[host] != "" ||
			agent.Host != nil && agent.Host.RebootPending {
			continue
		}
		if _, err := remoteAnalysisURL(agent.URL); err != nil {
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

func (s *Server) analysisWorkerCredential(slot *workSlot) string {
	if slot.analysisAgent != nil {
		return s.config.AnalysisAgentToken
	}
	return s.config.AnalysisWorkerToken
}
