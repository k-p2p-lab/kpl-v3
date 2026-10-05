package peer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func (s *Server) handleMeshFreeze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("X-KPL-Node-ID") != s.config.Node.ID || s.config.Node.ID == "" {
		http.Error(w, "peer identity does not match mesh-freeze target", http.StatusConflict)
		return
	}
	var request model.MeshFreezeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid request: expected a single JSON value", http.StatusBadRequest)
		return
	}
	if request.RunID == "" || request.RunID != s.config.Node.RunID || request.Generation != s.config.Node.Generation {
		http.Error(w, "run/generation does not match mesh-freeze target", http.StatusConflict)
		return
	}
	config := s.config.NodeConfig.GossipSub
	if s.pubsub == nil || config.Router != "gossipsub" || config.MeshFreeze == nil || !*config.MeshFreeze {
		http.Error(w, "GossipSub meshFreeze is not enabled", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.pubsub.FreezeMesh(ctx); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, pubsub.ErrMeshFreezeDisabled) || errors.Is(err, pubsub.ErrMeshFreezeUnsupported) {
			status = http.StatusConflict
		} else if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, err.Error(), status)
		return
	}
	s.recordMeshFrozen()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: s.config.Node.ID, PeerID: s.host.ID().String(), Frozen: true})
}

func (s *Server) recordMeshFrozen() {
	if s.meshFrozen.CompareAndSwap(false, true) && s.telemetry != nil {
		s.telemetry.emitObservedPriority(func(reading controllerClockReading) (model.TraceEvent, bool) {
			return model.TraceEvent{Type: "mesh_freeze", PeerID: s.host.ID().String(), Timestamp: reading.timestamp,
				Fields: map[string]any{"frozen": true, "scope": "peer", "membership": "pinned"}}, true
		}, true)
	}
}

func (s *Server) addMeshFreezeStatus(node *model.Node) {
	config := s.config.NodeConfig.GossipSub
	if config.MeshFreeze == nil || !*config.MeshFreeze {
		return
	}
	node.Metadata = maps.Clone(node.Metadata)
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}
	node.Metadata["meshFrozen"] = strconv.FormatBool(s.meshFrozen.Load())
}

// A frozen logical member may lose its transport or topic subscription. Use
// the router's current usable subset for the live overlay, including reconnects
// of the same pinned peer, without fabricating GRAFT/PRUNE observations.
func (s *Server) observeMeshFreezeStatus(ctx context.Context, node *model.Node) error {
	config := s.config.NodeConfig.GossipSub
	if config.MeshFreeze == nil || !*config.MeshFreeze || s.pubsub == nil {
		return nil
	}
	snapshot, err := s.pubsub.MeshFreezeSnapshot(ctx)
	if err != nil {
		return err
	}
	if snapshot.Frozen {
		s.recordMeshFrozen()
		node.MeshPeers = make(map[string][]string, len(snapshot.Active))
		for topic, peers := range snapshot.Active {
			ids := make([]string, 0, len(peers))
			for _, id := range peers {
				ids = append(ids, id.String())
			}
			sort.Strings(ids)
			node.MeshPeers[topic] = ids
		}
		node.OverlayObservedAt = snapshot.ObservedAt.UTC()
	}
	s.addMeshFreezeStatus(node)
	return nil
}
