package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/netem"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type profileUpdateState struct {
	gate     publicationGate
	prepared *preparedProfileUpdate
	last     *preparedProfileUpdate
	failed   bool
}

type preparedProfileUpdate struct {
	request model.ProfileUpdateRequest
	config  model.NodeConfig
}

type profileAppliedState struct {
	config    model.NodeConfig
	revision  uint64
	appliedAt time.Time
	json      string
}

func (s *Server) handleProfileUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.config.Node.ID == "" || r.Header.Get("X-KPL-Node-ID") != s.config.Node.ID {
		http.Error(w, "profile target identity mismatch", http.StatusConflict)
		return
	}
	var request model.ProfileUpdateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, model.MaxProfileUpdateBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid profile update: "+err.Error(), http.StatusBadRequest)
		return
	}
	if decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "expected one profile update", http.StatusBadRequest)
		return
	}
	if err := request.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if request.RunID != s.config.Node.RunID || request.Generation != s.config.Node.Generation {
		http.Error(w, "profile run/generation mismatch", http.StatusConflict)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	response, err := s.executeProfileUpdate(ctx, request, netem.Update)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, context.Canceled) {
			status = http.StatusServiceUnavailable
		}
		if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func sameProfileUpdate(a, b model.ProfileUpdateRequest) bool {
	a.Stage, b.Stage = "", ""
	return reflect.DeepEqual(a, b)
}

func (s *Server) executeProfileUpdate(ctx context.Context, request model.ProfileUpdateRequest, applyNetwork func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error) (model.ProfileUpdateResponse, error) {
	response := model.ProfileUpdateResponse{NodeID: s.config.Node.ID, PeerID: s.host.ID().String(), Revision: request.Revision, Stage: request.Stage}
	if err := request.Validate(); err != nil {
		return response, err
	}
	if err := s.profileUpdates.gate.acquire(ctx); err != nil {
		return response, err
	}
	defer s.profileUpdates.gate.release()
	state := &s.profileUpdates
	if state.failed {
		return response, fmt.Errorf("previous profile application failed; effective state may be incomplete")
	}
	current, revision := s.config.NodeConfig, s.config.ProfileRevision
	if applied := s.profileApplied.Load(); applied != nil {
		current, revision = applied.config, applied.revision
	}
	if request.Revision <= revision {
		if state.last != nil && sameProfileUpdate(state.last.request, request) {
			response.Effective = request.Set.Snapshot(current)
			return response, nil
		}
		return response, fmt.Errorf("stale or conflicting profile revision")
	}
	if request.Set.Network != nil && !s.config.NetworkMutable {
		return response, fmt.Errorf("Peer was not created with network update capability")
	}
	if request.Set.GossipSub != nil && s.pubsub == nil {
		return response, fmt.Errorf("GossipSub is not running")
	}
	updated, err := request.Set.Apply(current)
	if err != nil {
		return response, err
	}
	if request.Stage == "prepare" {
		if state.prepared != nil && request.Revision <= state.prepared.request.Revision && !sameProfileUpdate(state.prepared.request, request) {
			return response, fmt.Errorf("conflicting prepared profile revision")
		}
		state.prepared = &preparedProfileUpdate{request: request, config: updated}
		response.Effective = request.Set.Snapshot(updated)
		return response, nil
	}
	if state.prepared == nil || !sameProfileUpdate(state.prepared.request, request) {
		return response, fmt.Errorf("profile apply requires matching preparation")
	}
	updated = state.prepared.config
	if err := ctx.Err(); err != nil {
		return response, err
	}
	if request.Set.Network != nil {
		applyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = applyNetwork(applyCtx, current.Network.Initial(), updated.Network.Initial(), netem.DefaultP2PPort)
		cancel()
		if err == nil {
			raw, _ := json.Marshal(updated.Network.Initial())
			s.networkApplied.Store(&networkAppliedState{configJSON: string(raw), revision: int(request.Revision), appliedAt: time.Now().UTC()})
		}
	}
	if err == nil && request.Set.GossipSub != nil {
		p := updated.GossipSub.Params
		err = s.pubsub.SetGossipSubRuntimeParams(ctx, pubsub.GossipSubRuntimeParams{D: *p.D, Dlo: *p.DLow, Dhi: *p.DHigh, Dscore: *p.DScore, Dout: *p.DOut, Dlazy: *p.DLazy, GossipFactor: *p.GossipFactor, HopwaveFactor: *p.HopwaveFactor, HopwaveInterval: *p.HopwaveInterval})
	}
	if err != nil {
		state.failed = true
		if s.telemetry != nil {
			s.telemetry.emitObservedPriority(func(controllerClockReading) (model.TraceEvent, bool) {
				return model.TraceEvent{Type: "profile_config_failed", Fields: map[string]any{"revision": request.Revision, "error": err.Error(), "effectiveState": "incomplete"}}, true
			}, true)
		}
		return response, fmt.Errorf("profile apply may be partial: %w", err)
	}
	response.Effective = request.Set.Snapshot(updated)
	raw, _ := json.Marshal(s.profileStatusSnapshot(updated))
	s.profileApplied.Store(&profileAppliedState{config: updated, revision: request.Revision, appliedAt: time.Now().UTC(), json: string(raw)})
	state.last, state.prepared = state.prepared, nil
	if s.telemetry != nil {
		s.telemetry.emitObservedPriority(func(controllerClockReading) (model.TraceEvent, bool) {
			return model.TraceEvent{Type: "profile_config_applied", Fields: map[string]any{"revision": request.Revision, "set": request.Set, "effective": response.Effective}}, true
		}, true)
	}
	return response, nil
}

func (s *Server) addProfileStatus(node *model.Node) {
	applied := s.profileApplied.Load()
	if applied == nil {
		if s.config.ProfileRevision == 0 {
			return
		}
		raw, _ := json.Marshal(s.profileStatusSnapshot(s.config.NodeConfig))
		applied = &profileAppliedState{revision: s.config.ProfileRevision, appliedAt: s.startedAt, json: string(raw)}
	}
	node.Metadata = maps.Clone(node.Metadata)
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}
	node.Metadata["profileRevision"] = strconv.FormatUint(applied.revision, 10)
	node.Metadata["runtimeProfile"] = applied.json
	node.Metadata["profileAppliedAt"] = applied.appliedAt.Format(time.RFC3339Nano)
}

func (s *Server) profileStatusSnapshot(config model.NodeConfig) model.RuntimeProfilePatch {
	snapshot := model.RuntimeProfileSnapshot(config)
	if config.GossipSub.Enabled == nil || !*config.GossipSub.Enabled || config.GossipSub.Router != "gossipsub" {
		snapshot.GossipSub = nil
	}
	if !s.config.NetworkMutable || config.Network.Scheduled() {
		snapshot.Network = nil
	}
	return snapshot
}
