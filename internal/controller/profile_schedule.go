package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"golang.org/x/sync/semaphore"
)

const profileWriteWeight int64 = 1 << 30

type profileRuntimeKey struct{}
type profilePhaseStartKey struct{}
type profileOverride struct {
	phase scenario.Phase
	set   model.RuntimeProfilePatch
}

// Owned by one run's phase jobs, never by the shared parsed scenario. Admission
// readers cover only an actual create request, not capacity/retry waiting.
type runProfileState struct {
	gate      *semaphore.Weighted
	revision  uint64
	overrides []profileOverride
}

func newRunProfileState() *runProfileState {
	return &runProfileState{gate: semaphore.NewWeighted(profileWriteWeight)}
}

func profileMatchesNode(phase scenario.Phase, node model.Node) bool {
	if !phase.MatchesProfile(node.Group, node.Profile, node.Type, node.Role) {
		return false
	}
	return (len(phase.NodeIDs) == 0 || hasProfileID(phase.NodeIDs, node.ID)) && (len(phase.PeerIDs) == 0 || hasProfileID(phase.PeerIDs, node.PeerID))
}

func hasProfileID(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func profileCreateRequest(ctx context.Context, request model.CreateNodeRequest) (model.CreateNodeRequest, func(), error) {
	profiles, _ := ctx.Value(profileRuntimeKey{}).(*runProfileState)
	if profiles == nil {
		return request, func() {}, nil
	}
	if err := profiles.gate.Acquire(ctx, 1); err != nil {
		return request, func() {}, err
	}
	release := func() { profiles.gate.Release(1) }
	if err := ctx.Err(); err != nil {
		release()
		return request, func() {}, err
	}
	node := model.Node{ID: request.ID, Group: request.Group, Profile: request.Profile, Type: request.Type, Role: request.Role}
	for _, overlay := range profiles.overrides {
		if !profileMatchesNode(overlay.phase, node) {
			continue
		}
		config, err := overlay.set.Apply(request.Config)
		if err != nil {
			release()
			return request, func() {}, fmt.Errorf("scheduled profile for new node %q: %w", request.ID, err)
		}
		request.Config = config
	}
	request.ProfileRevision = profiles.revision
	return request, release, nil
}

func (s *Server) runProfileSchedule(ctx context.Context, runID string, generation uint64, phase scenario.Phase, profiles *runProfileState) error {
	if phase.Schedule == nil {
		return fmt.Errorf("missing profile schedule")
	}
	base := time.Now()
	if started, ok := ctx.Value(profilePhaseStartKey{}).(time.Time); ok {
		base = started
	}
	if phase.Schedule.Clock() == "experiment-start" {
		s.state.mu.RLock()
		started := s.state.experiments[runID].StartedAt
		s.state.mu.RUnlock()
		if started.IsZero() {
			return fmt.Errorf("schedule requires an experiment start time")
		}
		base = base.Add(started.Sub(base))
	}
	for index, change := range phase.Schedule.Changes {
		after, _ := time.ParseDuration(change.After)
		deadline := base.Add(after)
		if err := sleepContext(ctx, max(time.Duration(0), time.Until(deadline))); err != nil {
			return err
		}
		if err := s.applyProfileChange(ctx, runID, generation, phase, profiles, index, change, deadline); err != nil {
			return err
		}
	}
	return nil
}

type profileTarget struct {
	model.Node
	agentStarted time.Time
}

func (s *Server) profileTargets(runID string, generation uint64, phase scenario.Phase) ([]profileTarget, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	var targets []profileTarget
	seenNodes, seenPeers := make(map[string]bool), make(map[string]bool)
	for _, node := range s.state.nodes {
		if node.RunID != runID || node.Generation != generation || !profileMatchesNode(phase, node) || node.State == model.NodeStopping || node.State == model.NodeStopped || node.State == model.NodeFailed {
			continue
		}
		seenNodes[node.ID], seenPeers[node.PeerID] = true, true
		targets = append(targets, profileTarget{Node: model.Node{ID: node.ID, PeerID: node.PeerID, AgentID: node.AgentID}, agentStarted: s.state.agents[node.AgentID].StartedAt})
	}
	for _, id := range phase.NodeIDs {
		if !seenNodes[id] {
			return nil, fmt.Errorf("schedule node ID %q does not match an active peer in this generation", id)
		}
	}
	for _, id := range phase.PeerIDs {
		if !seenPeers[id] {
			return nil, fmt.Errorf("schedule peer ID %q does not match an active peer in this generation", id)
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	// Empty group/profile selections still update subsequent joins.
	return targets, nil
}

func (s *Server) currentProfileTarget(runID string, generation uint64, target profileTarget, patch model.RuntimeProfilePatch) (profileTarget, model.Agent, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	node, exists := s.state.nodes[target.ID]
	agent := s.state.agents[target.AgentID]
	if !exists || node.RunID != runID || node.Generation != generation || node.AgentID != target.AgentID || target.PeerID != "" && node.PeerID != target.PeerID || !agent.StartedAt.Equal(target.agentStarted) {
		return target, agent, fmt.Errorf("schedule node %q changed incarnation", target.ID)
	}
	if !agentIsOnline(agent, time.Now()) || agent.URL == "" {
		return target, agent, fmt.Errorf("schedule agent %q is unavailable", agent.ID)
	}
	if node.State == model.NodeStopping || node.State == model.NodeStopped || node.State == model.NodeFailed {
		return target, agent, fmt.Errorf("schedule node %q left before application", node.ID)
	}
	if patch.Network != nil && !nodeFeatureEnabled(node, "networkMutable", false) {
		return target, agent, fmt.Errorf("schedule node %q lacks runtime network capability", node.ID)
	}
	if patch.GossipSub != nil && (node.Metadata["pubsubRouter"] != "gossipsub" || !nodeFeatureEnabled(node, "pubsubEnabled", false)) {
		return target, agent, fmt.Errorf("schedule node %q requires enabled GossipSub", node.ID)
	}
	target.State, target.PeerID = node.State, node.PeerID
	return target, agent, nil
}

func (s *Server) applyProfileChange(parent context.Context, runID string, generation uint64, phase scenario.Phase, profiles *runProfileState, index int, change model.ProfileChange, deadline time.Time) error {
	timeout, _ := time.ParseDuration(phase.Timeout)
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := profiles.gate.Acquire(ctx, profileWriteWeight); err != nil {
		return err
	}
	defer profiles.gate.Release(profileWriteWeight)
	if err := ctx.Err(); err != nil {
		return err
	}
	targets, err := s.profileTargets(runID, generation, phase)
	if err != nil {
		return err
	}
	// Already-admitted containers may still be starting. Hold new admissions
	// while these finish so no old-profile Peer escapes the target cohort.
	for i := range targets {
		for {
			current, _, err := s.currentProfileTarget(runID, generation, targets[i], change.Set)
			if err != nil {
				return err
			}
			if current.State == model.NodeReady && current.PeerID != "" {
				targets[i] = current
				break
			}
			if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
				return fmt.Errorf("wait for schedule target %q: %w", targets[i].ID, err)
			}
		}
	}
	profiles.revision++
	request := model.ProfileUpdateRequest{RunID: runID, Generation: generation, Revision: profiles.revision, Set: change.Set}
	for _, stage := range []string{"prepare", "apply"} {
		request.Stage = stage
		var acknowledged atomic.Int64
		err := runOperations(ctx, len(targets), true, 16, nil, false, func(ctx context.Context, i int) error {
			target := targets[i]
			current, agent, err := s.currentProfileTarget(runID, generation, target, change.Set)
			if err != nil {
				return err
			}
			if current.State != model.NodeReady || current.PeerID != target.PeerID {
				return fmt.Errorf("schedule node %q is no longer ready", target.ID)
			}
			commandCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
			defer cancel()
			var response model.ProfileUpdateResponse
			if err := s.callAgent(commandCtx, agent.URL, http.MethodPost, "/api/v1/nodes/"+url.PathEscape(target.ID)+"/profile", request, &response); err != nil {
				return fmt.Errorf("%s profile on node %q: %w", stage, target.ID, err)
			}
			if response.NodeID != target.ID || response.PeerID != target.PeerID || response.Stage != stage || response.Revision != request.Revision {
				return fmt.Errorf("invalid profile acknowledgement from node %q", target.ID)
			}
			if err := model.ValidateProfileAcknowledgement(request, response); err != nil {
				return err
			}
			if _, _, err := s.currentProfileTarget(runID, generation, target, change.Set); err != nil {
				return err
			}
			acknowledged.Add(1)
			return ctx.Err()
		})
		fields := map[string]any{"phase": phase.Name, "stage": stage, "change": index + 1, "revision": request.Revision, "reference": phase.Schedule.Clock(), "after": change.After, "lateByMs": float64(max(time.Duration(0), time.Since(deadline))) / float64(time.Millisecond), "targets": len(targets), "acknowledged": acknowledged.Load(), "set": change.Set}
		if err != nil {
			fields["error"] = err.Error()
		}
		persistErr := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: runID, Type: "schedule_change", Fields: fields}}})
		if err != nil || persistErr != nil {
			if stage == "prepare" {
				return fmt.Errorf("schedule prepare acknowledged by %d/%d peers; apply was not started: %w", acknowledged.Load(), len(targets), errors.Join(err, persistErr))
			}
			return fmt.Errorf("schedule apply acknowledged by %d/%d peers; unacknowledged requests may have applied: %w", acknowledged.Load(), len(targets), errors.Join(err, persistErr))
		}
	}
	profiles.overrides = append(profiles.overrides, profileOverride{phase: phase, set: change.Set})
	return nil
}
