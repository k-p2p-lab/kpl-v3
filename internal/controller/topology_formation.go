package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/topology"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// Generate one undirected mesh for a snapshot of the selected domain. Complete
// every connection preflight before installing any mesh. Application is atomic
// on each Peer, but a distributed failure can leave a partially frozen domain.
func (s *Server) runTopology(ctx context.Context, runID string, generation uint64, phase scenario.Phase, seed int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if phase.Topology == nil || phase.Topic == "" {
		return errors.New("topology requires a graph configuration and topic")
	}
	timeout := 2 * time.Minute
	if phase.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(phase.Timeout)
		if err != nil || timeout <= 0 {
			return errors.New("topology requires a positive timeout")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	targets, err := s.topologyTargets(runID, generation, phase)
	if err != nil {
		return fmt.Errorf("topology preflight: %w", err)
	}
	if phase.Topology.Seed != nil {
		seed = *phase.Topology.Seed
	}
	graph, err := topology.Generate(ctx, *phase.Topology, len(targets), seed)
	if err != nil {
		return fmt.Errorf("generate topology: %w", err)
	}
	// Include the incarnation and complete graph in its identity. Addresses can
	// change without changing an intended mesh; they are only connection hints.
	identity := struct {
		RunID      string
		Generation uint64
		Topic      string
		NodeIDs    []string
		PeerIDs    []string
		Neighbors  [][]int
	}{RunID: runID, Generation: generation, Topic: phase.Topic, Neighbors: graph.Neighbors}
	for _, target := range targets {
		identity.NodeIDs = append(identity.NodeIDs, target.ID)
		identity.PeerIDs = append(identity.PeerIDs, target.PeerID)
	}
	digest := sha256.New()
	if err := json.NewEncoder(digest).Encode(identity); err != nil {
		return err
	}
	planID := hex.EncodeToString(digest.Sum(nil))
	requests := make([]model.TopologyRequest, len(targets))
	edges := 0
	for i, neighbors := range graph.Neighbors {
		if err := ctx.Err(); err != nil {
			return err
		}
		request := model.TopologyRequest{RunID: runID, Generation: generation, TopologyID: planID, Stage: "prepare", Topic: phase.Topic,
			Neighbors: make([]model.TopologyPeer, 0, len(neighbors))}
		for _, neighbor := range neighbors {
			node := targets[neighbor]
			request.Neighbors = append(request.Neighbors, model.TopologyPeer{PeerID: node.PeerID, Addresses: node.Addresses})
		}
		if _, err := request.PeerInfos(); err != nil {
			return fmt.Errorf("topology request for node %q: %w", targets[i].ID, err)
		}
		data, err := json.Marshal(request)
		if err != nil || len(data) > model.MaxTopologyRequestBytes {
			return fmt.Errorf("topology request for node %q exceeds the supported body size", targets[i].ID)
		}
		requests[i] = request
		edges += len(neighbors)
	}
	edges /= 2
	if err := s.recordTopologyPlan(ctx, runID, phase, seed, planID, edges, targets, requests, graph.Positions); err != nil {
		return fmt.Errorf("record topology plan: %w", err)
	}
	for _, stage := range []string{"prepare", "apply"} {
		var acknowledged atomic.Int64
		err := runOperations(ctx, len(targets), true, 16, nil, false, func(operationCtx context.Context, i int) error {
			target := targets[i]
			agent, err := s.currentTopologyAgent(runID, generation, target)
			if err != nil {
				return err
			}
			request := requests[i]
			request.Stage = stage
			commandCtx, stop := context.WithTimeout(operationCtx, model.TopologyCommandTimeout)
			defer stop()
			var response model.TopologyResponse
			path := "/api/v1/nodes/" + url.PathEscape(target.ID) + "/topology"
			if err := s.callAgent(commandCtx, agent.URL, http.MethodPost, path, request, &response); err != nil {
				return fmt.Errorf("%s topology on node %q: %w", stage, target.ID, err)
			}
			if err := response.Validate(request, target.ID, target.PeerID); err != nil {
				return fmt.Errorf("node %q: %w", target.ID, err)
			}
			if _, err := s.currentTopologyAgent(runID, generation, target); err != nil {
				return err
			}
			acknowledged.Add(1)
			return nil
		})
		fields := map[string]any{"topologyId": planID, "phase": phase.Name, "stage": stage, "acknowledged": acknowledged.Load(), "targets": len(targets)}
		if err != nil {
			fields["error"] = err.Error()
		}
		persistErr := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: runID, Type: "topology_stage", Topic: phase.Topic, Fields: fields}}})
		if err != nil || persistErr != nil {
			return fmt.Errorf("topology %s acknowledged by %d/%d peers; applied meshes remain frozen and unacknowledged requests may have applied: %w", stage, acknowledged.Load(), len(targets), errors.Join(err, persistErr))
		}
	}
	return nil
}

func (s *Server) topologyTargets(runID string, generation uint64, phase scenario.Phase) ([]model.Node, error) {
	targets, err := s.meshFreezeTargets(runID, generation, phase)
	if err != nil {
		return nil, err
	}
	if phase.Count != 0 && phase.Count != len(targets) {
		return nil, fmt.Errorf("topology requires exactly %d selected peers, found %d", phase.Count, len(targets))
	}
	seen := make(map[peer.ID]bool, len(targets))
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	for i, target := range targets {
		current, exists := s.state.nodes[target.ID]
		if !exists || current.RunID != runID || current.Generation != generation || current.PeerID != target.PeerID || current.AgentID != target.AgentID {
			return nil, fmt.Errorf("topology node %q changed during selection", target.ID)
		}
		if err := validateMeshFreezeNode(current, s.state.agents[target.AgentID]); err != nil {
			return nil, err
		}
		id, err := peer.Decode(target.PeerID)
		if err != nil || seen[id] {
			return nil, fmt.Errorf("topology node %q has an invalid or duplicate Peer ID", target.ID)
		}
		seen[id] = true
		if len(current.Addresses) == 0 {
			return nil, fmt.Errorf("topology node %q has no connection addresses", target.ID)
		}
		for _, raw := range current.Addresses {
			if _, err := multiaddr.NewMultiaddr(raw); err != nil {
				return nil, fmt.Errorf("topology node %q has an invalid connection address: %w", target.ID, err)
			}
		}
		targets[i].Addresses = slices.Clone(current.Addresses)
	}
	return targets, nil
}

func (s *Server) currentTopologyAgent(runID string, generation uint64, target model.Node) (model.Agent, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	current, exists := s.state.nodes[target.ID]
	agent := s.state.agents[target.AgentID]
	if !exists || current.RunID != runID || current.Generation != generation || current.PeerID != target.PeerID || current.AgentID != target.AgentID {
		return model.Agent{}, fmt.Errorf("topology node %q no longer matches the selected incarnation", target.ID)
	}
	if err := validateMeshFreezeNode(current, agent); err != nil {
		return model.Agent{}, err
	}
	return agent, nil
}

func (s *Server) recordTopologyPlan(ctx context.Context, runID string, phase scenario.Phase, seed int64, planID string, edges int, targets []model.Node, requests []model.TopologyRequest, positions []topology.Position) error {
	summary := model.TraceEvent{RunID: runID, Type: "topology_generated", Topic: phase.Topic, Fields: map[string]any{
		"topologyId": planID, "phase": phase.Name, "group": phase.Group, "model": phase.Topology.Model,
		"seed": seed, "config": phase.Topology, "nodes": len(targets), "edges": edges, "generatorVersion": 1, "scope": "gossipsub-mesh",
	}}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summary}}); err != nil {
		return err
	}
	// Record the intended graph independently of later acknowledgements. Write
	// bounded batches to local run storage; do not retain adjacency in live SSE.
	for start := 0; start < len(targets); start += 32 {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch := model.EventBatch{}
		for i := start; i < min(start+32, len(targets)); i++ {
			neighbors := make([]string, len(requests[i].Neighbors))
			for j, neighbor := range requests[i].Neighbors {
				neighbors[j] = neighbor.PeerID
			}
			fields := map[string]any{"topologyId": planID, "index": i, "neighbors": neighbors, "evidence": "planned"}
			if len(positions) == len(targets) {
				fields["position"] = positions[i]
			}
			batch.Events = append(batch.Events, model.TraceEvent{RunID: runID, NodeID: targets[i].ID, PeerID: targets[i].PeerID,
				Type: "topology_assignment", Topic: phase.Topic, Fields: fields})
		}
		if err := s.state.appendEvents(batch); err != nil {
			return err
		}
	}
	return nil
}
