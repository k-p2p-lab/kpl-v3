package controller

import (
	"fmt"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// This is per-Peer historical evidence, not a completed Run cleanup proof.
// Keep active Runs and unconfirmed removal in the live control inventory.
func confirmedHistoricalNode(node model.Node) bool {
	return (node.State == model.NodeStopped || node.State == model.NodeFailed) && node.Metadata["cleanupComplete"] == "true"
}

func (s *state) keepRunHistoryHotLocked(id string) bool {
	run, exists := s.experiments[id]
	return exists && (run.State == "queued" || run.State == "running" || run.CleanupState == "running")
}

// Validate the complete batch before its atomic disk transaction. No network,
// disk transaction or fsync holds the main state or telemetry journal mutex.
// heartbeat rechecks registration after this returns, before publishing state.
func (s *state) prepareHistoricalHeartbeat(h model.AgentHeartbeat, retired map[string]bool) (map[string]model.Node, error) {
	if !s.journalOnly || s.terminalHistory == nil || len(h.Nodes) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	registered, exists := s.agents[h.Agent.ID]
	if !exists || !h.Agent.StartedAt.IsZero() && !h.Agent.StartedAt.Equal(registered.StartedAt) {
		s.mu.RUnlock()
		return nil, fmt.Errorf("Agent %q history does not match its registered instance", h.Agent.ID)
	}
	nodes := make([]model.Node, 0, len(h.Nodes))
	cold := make(map[string]bool)
	seen := make(map[string]bool, len(h.Nodes))
	now := time.Now().UTC()
	for _, node := range h.Nodes {
		old, known := s.nodes[node.ID]
		reserved := s.reservations[node.ID]
		if node.ID == "" || seen[node.ID] || known && old.AgentID != "" && old.AgentID != h.Agent.ID || known && old.RunID != "" && node.RunID != "" && old.RunID != node.RunID || reserved != "" && reserved != h.Agent.ID {
			s.mu.RUnlock()
			return nil, fmt.Errorf("Agent %q heartbeat contains an empty, duplicate, or foreign Peer %q", h.Agent.ID, node.ID)
		}
		seen[node.ID] = true
		node.AgentID = h.Agent.ID
		if node.RunID == "" && known {
			node.RunID = old.RunID
		}
		if node.LastSeen.IsZero() {
			node.LastSeen = now
		}
		if node.StartedAt.IsZero() {
			if known && !old.StartedAt.IsZero() {
				node.StartedAt = old.StartedAt
			} else {
				node.StartedAt = now
			}
		}
		nodes = append(nodes, node)
		_, uncertain := s.uncertainCreates[node.ID]
		if confirmedHistoricalNode(node) && validResultID(node.RunID) && !retired[node.RunID] && !s.keepRunHistoryHotLocked(node.RunID) && reserved == "" && !uncertain && (!known || confirmedHistoricalNode(old)) {
			cold[node.ID] = true
		}
	}
	s.mu.RUnlock()
	frozen, err := s.terminalHistory.reconcile(nodes, cold)
	if err != nil {
		return nil, fmt.Errorf("persist terminal Peer history: %w", err)
	}
	return frozen, nil
}
