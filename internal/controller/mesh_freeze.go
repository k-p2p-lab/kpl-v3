package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// Freeze every selected peer only after checking the entire target set. This
// prevents configuration errors from partially applying a phase; transport
// failures can still occur after other peers have acknowledged the command.
func (s *Server) runMeshFreeze(ctx context.Context, runID string, generation uint64, phase scenario.Phase) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timeout := 30 * time.Second
	if phase.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(phase.Timeout)
		if err != nil || timeout <= 0 {
			return fmt.Errorf("mesh-freeze requires a positive timeout")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	targets, err := s.meshFreezeTargets(runID, generation, phase)
	if err != nil {
		return err
	}
	var acknowledged atomic.Int64
	err = runOperations(ctx, len(targets), true, 8, nil, false, func(operationCtx context.Context, i int) error {
		target := targets[i]
		// Recheck after dispatch waiting: a background lifecycle phase or an
		// Agent restart may have invalidated the earlier selection snapshot.
		s.state.mu.RLock()
		current, exists := s.state.nodes[target.ID]
		agent := s.state.agents[target.AgentID]
		var currentErr error
		if !exists || current.RunID != runID || current.Generation != generation || current.PeerID != target.PeerID || current.AgentID != target.AgentID {
			currentErr = fmt.Errorf("node %q no longer matches the selected run generation and peer", target.ID)
		} else {
			currentErr = validateMeshFreezeNode(current, agent)
		}
		s.state.mu.RUnlock()
		if currentErr != nil {
			return currentErr
		}
		request := model.MeshFreezeRequest{RunID: runID, Generation: generation}
		var response model.MeshFreezeResponse
		path := "/api/v1/nodes/" + url.PathEscape(target.ID) + "/mesh-freeze"
		if err := s.callAgent(operationCtx, agent.URL, http.MethodPost, path, request, &response); err != nil {
			return fmt.Errorf("freeze node %q: %w", target.ID, err)
		}
		if !response.Frozen || response.NodeID != target.ID || response.PeerID != target.PeerID {
			return fmt.Errorf("node %q returned an invalid mesh-freeze acknowledgement", target.ID)
		}
		acknowledged.Add(1)
		return nil
	})
	if err != nil {
		return fmt.Errorf("mesh-freeze acknowledged by %d/%d peers; peers already frozen remain frozen: %w", acknowledged.Load(), len(targets), err)
	}
	return nil
}

func (s *Server) meshFreezeTargets(runID string, generation uint64, phase scenario.Phase) ([]model.Node, error) {
	if phase.Group == "" && len(phase.NodeIDs) == 0 && len(phase.PeerIDs) == 0 {
		return nil, fmt.Errorf("mesh-freeze requires group, nodeIds, or peerIds")
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	var targets []model.Node
	seenNodes := make(map[string]bool, len(phase.NodeIDs))
	seenPeers := make(map[string]bool, len(phase.PeerIDs))
	for _, id := range phase.NodeIDs {
		seenNodes[id] = false
	}
	for _, id := range phase.PeerIDs {
		seenPeers[id] = false
	}
	for _, node := range s.state.nodes {
		_, selectedNode := seenNodes[node.ID]
		_, selectedPeer := seenPeers[node.PeerID]
		if node.RunID != runID || node.Generation != generation ||
			phase.Group != "" && node.Group != phase.Group ||
			phase.Role != "" && node.Role != phase.Role ||
			phase.NodeType != "" && node.Type != phase.NodeType ||
			len(phase.NodeIDs) > 0 && !selectedNode ||
			len(phase.PeerIDs) > 0 && !selectedPeer {
			continue
		}
		// Group selectors refer to the live cohort. Explicit selectors below
		// must all resolve, including when a requested peer has already left.
		if node.State == model.NodeStopping || node.State == model.NodeStopped || node.State == model.NodeFailed {
			continue
		}
		if err := validateMeshFreezeNode(node, s.state.agents[node.AgentID]); err != nil {
			return nil, err
		}
		if selectedNode {
			seenNodes[node.ID] = true
		}
		if selectedPeer {
			seenPeers[node.PeerID] = true
		}
		// Only immutable identity fields leave the lock; metadata maps remain
		// owned by state and may be replaced by incoming Agent heartbeats.
		targets = append(targets, model.Node{ID: node.ID, PeerID: node.PeerID, AgentID: node.AgentID})
	}
	for _, id := range phase.NodeIDs {
		if !seenNodes[id] {
			return nil, fmt.Errorf("mesh-freeze node ID %q does not match an active peer in the selected run generation and filters", id)
		}
	}
	for _, id := range phase.PeerIDs {
		if !seenPeers[id] {
			return nil, fmt.Errorf("mesh-freeze peer ID %q does not match an active peer in the selected run generation and filters", id)
		}
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("mesh-freeze has no matching active peers")
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	return targets, nil
}

func validateMeshFreezeNode(node model.Node, agent model.Agent) error {
	if node.State != model.NodeReady || node.PeerID == "" {
		return fmt.Errorf("mesh-freeze node %q is not ready", node.ID)
	}
	if !agentIsOnline(agent, time.Now()) || agent.URL == "" {
		return fmt.Errorf("mesh-freeze agent %q for node %q is unavailable", node.AgentID, node.ID)
	}
	if node.Metadata["pubsubRouter"] != "gossipsub" || !nodeFeatureEnabled(node, "pubsubEnabled", false) || !nodeFeatureEnabled(node, "meshFreezeEnabled", false) {
		return fmt.Errorf("mesh-freeze node %q requires enabled GossipSub with meshFreeze: true", node.ID)
	}
	return nil
}
