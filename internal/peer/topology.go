package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/sync/errgroup"
)

type topologyCommandState struct {
	gate     publicationGate
	lifetime context.Context
	prepared *preparedTopology
}

type preparedTopology struct {
	id        string
	topic     string
	neighbors []string
	applied   bool
}

func (p *preparedTopology) matches(request model.TopologyRequest, neighbors []string) bool {
	return p != nil && p.id == request.TopologyID && p.topic == request.Topic && slices.Equal(p.neighbors, neighbors)
}

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.config.Node.ID == "" || r.Header.Get("X-KPL-Node-ID") != s.config.Node.ID {
		http.Error(w, "peer identity does not match topology target", http.StatusConflict)
		return
	}
	var request model.TopologyRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, model.MaxTopologyRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid topology request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid topology request: expected a single JSON value", http.StatusBadRequest)
		return
	}
	infos, err := request.PeerInfos()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, info := range infos {
		if info.ID == s.host.ID() {
			http.Error(w, "peer cannot be its own topology neighbor", http.StatusBadRequest)
			return
		}
	}
	if request.RunID != s.config.Node.RunID || request.Generation != s.config.Node.Generation {
		http.Error(w, "run/generation does not match topology target", http.StatusConflict)
		return
	}
	config := s.config.NodeConfig.GossipSub
	if s.pubsub == nil || config.Router != "gossipsub" || config.MeshFreeze == nil || !*config.MeshFreeze {
		http.Error(w, "GossipSub meshFreeze is not enabled", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), model.TopologyCommandTimeout)
	defer cancel()
	// The ordinary Peer API has a shorter write timeout. Extend only this
	// command's response so subscription readiness can use its full budget.
	deadline, _ := ctx.Deadline()
	if err := http.NewResponseController(w).SetWriteDeadline(deadline.Add(time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		http.Error(w, "set topology response deadline: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	response, err := s.executeTopology(ctx, request, infos)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		} else if errors.Is(err, context.Canceled) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) executeTopology(ctx context.Context, request model.TopologyRequest, infos []peer.AddrInfo) (model.TopologyResponse, error) {
	response := model.TopologyResponse{NodeID: s.config.Node.ID, PeerID: s.host.ID().String(),
		TopologyID: request.TopologyID, Stage: request.Stage, Topic: request.Topic, Neighbors: make([]string, 0, len(infos))}
	ids := make([]peer.ID, 0, len(infos))
	for _, info := range infos {
		ids = append(ids, info.ID)
		response.Neighbors = append(response.Neighbors, info.ID.String())
	}
	slices.Sort(response.Neighbors)
	if err := s.topology.gate.acquire(ctx); err != nil {
		return response, err
	}
	defer s.topology.gate.release()
	previous := s.topology.prepared
	if previous != nil && !previous.matches(request, response.Neighbors) && (previous.id == request.TopologyID || previous.applied) {
		return response, fmt.Errorf("topology command conflicts with the existing plan")
	}
	if request.Stage == "apply" {
		if !previous.matches(request, response.Neighbors) {
			return response, fmt.Errorf("topology apply requires a successful prepare for the same plan")
		}
		if err := s.pubsub.SetMeshAndFreeze(ctx, request.Topic, ids); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				s.recoverTopologyApplication(request, previous, response.Neighbors)
			}
			return response, err
		}
		response.Frozen = true
		s.recordMeshFrozen()
		if !previous.applied {
			previous.applied = true
			s.recordTopologyApplied(request, response.Neighbors, false)
		}
		return response, nil
	}
	// Retain just one prepared plan. A failed new prepare cannot leave that
	// command eligible for apply, and an applied plan cannot be replaced.
	if previous == nil || !previous.applied {
		s.topology.prepared = nil
	}
	err := s.pubsub.ValidateMeshPlan(ctx, request.Topic, ids)
	if errors.Is(err, pubsub.ErrMeshPlanNotReady) {
		if err = s.connectTopologyPeers(ctx, infos); err != nil {
			return response, err
		}
		err = s.waitTopologyReady(ctx, request.Topic, ids)
	}
	if err != nil {
		return response, err
	}
	snapshot, err := s.pubsub.MeshFreezeSnapshot(ctx)
	if err != nil {
		return response, err
	}
	response.Frozen = snapshot.Frozen
	if previous == nil || !previous.applied {
		s.topology.prepared = &preparedTopology{id: request.TopologyID, topic: request.Topic, neighbors: slices.Clone(response.Neighbors)}
	}
	return response, nil
}

func (s *Server) connectTopologyPeers(ctx context.Context, infos []peer.AddrInfo) error {
	group, connectCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for _, info := range infos {
		if connectCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			return s.connectTopologyPeer(connectCtx, info)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

func (s *Server) waitTopologyReady(ctx context.Context, topic string, peers []peer.ID) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := s.pubsub.ValidateMeshPlan(ctx, topic, peers)
		if !errors.Is(err, pubsub.ErrMeshPlanNotReady) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: waiting for topology subscriptions: %v", ctx.Err(), err)
		case <-ticker.C:
		}
	}
}

// Cancellation may win the API acknowledgement after the PubSub loop committed
// the mesh. Confirm only the resulting state; retain the caller's original
// error. The Peer lifetime bounds this recovery even when the request is gone.
// The topology gate is held by the caller.
func (s *Server) recoverTopologyApplication(request model.TopologyRequest, plan *preparedTopology, neighbors []string) {
	if s.topology.lifetime == nil || plan.applied {
		return
	}
	ctx, cancel := context.WithTimeout(s.topology.lifetime, 500*time.Millisecond)
	defer cancel()
	snapshot, err := s.pubsub.MeshFreezeSnapshot(ctx)
	if err != nil || !snapshot.Frozen {
		return
	}
	pinned, exists := snapshot.Mesh[request.Topic]
	if !exists {
		return
	}
	actual := make([]string, 0, len(pinned))
	for _, id := range pinned {
		actual = append(actual, id.String())
	}
	slices.Sort(actual)
	if !slices.Equal(actual, neighbors) {
		return
	}
	plan.applied = true
	s.recordMeshFrozen()
	s.recordTopologyApplied(request, neighbors, true)
}

func (s *Server) recordTopologyApplied(request model.TopologyRequest, neighbors []string, recovered bool) {
	if s.telemetry == nil {
		return
	}
	neighbors = slices.Clone(neighbors)
	s.telemetry.emitObservedPriority(func(reading controllerClockReading) (model.TraceEvent, bool) {
		return model.TraceEvent{Type: "topology_applied", PeerID: s.host.ID().String(), Topic: request.Topic, Timestamp: reading.timestamp,
			Fields: map[string]any{"topologyId": request.TopologyID, "topic": request.Topic, "neighbors": neighbors, "frozen": true, "recoveredAfterCancellation": recovered}}, true
	}, true)
}
