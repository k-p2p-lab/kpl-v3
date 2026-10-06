package controller

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func controllerProfilePhase(t *testing.T) scenario.Phase {
	t.Helper()
	var schedule model.ProfileSchedule
	if err := json.Unmarshal([]byte(`{"changes":[{"after":"0s","set":{"gossipsub":{"params":{"d":8}},"network":{"delay":"30ms"}}}]}`), &schedule); err != nil {
		t.Fatal(err)
	}
	return scenario.Phase{Action: "schedule", Name: "conditions", Group: "workers", Profile: "baseline", Timeout: "2s", Schedule: &schedule, Repeat: 1}
}

func profileControllerFixture(t *testing.T, handler http.HandlerFunc) *Server {
	t.Helper()
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	s := New(ServerConfig{DataDir: t.TempDir(), Token: "test"}, nil)
	s.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline, Capacity: 100}
	s.state.experiments["run"] = model.Experiment{ID: "run", StartedAt: time.Now().UTC()}
	return s
}

func acknowledgeProfile(t *testing.T, w http.ResponseWriter, r *http.Request, base model.NodeConfig) (string, model.ProfileUpdateRequest) {
	t.Helper()
	var request model.ProfileUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Error(err)
	}
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"), "/profile")
	updated, err := request.Set.Apply(base)
	if err != nil {
		t.Error(err)
	}
	_ = json.NewEncoder(w).Encode(model.ProfileUpdateResponse{NodeID: id, PeerID: "peer-" + id, Revision: request.Revision, Stage: request.Stage, Effective: request.Set.Snapshot(updated)})
	return id, request
}

func TestProfileScheduleChangesExistingAndFuturePeersWithinSelectedGroup(t *testing.T) {
	base, _ := model.BuiltInNodeConfig("full")
	var mu sync.Mutex
	var commands []string
	created := make(chan model.CreateNodeRequest, 2)
	s := profileControllerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/nodes" {
			var request model.CreateNodeRequest
			_ = json.NewDecoder(r.Body).Decode(&request)
			created <- request
			_ = json.NewEncoder(w).Encode(model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, Profile: request.Profile, Type: request.Type, Role: request.Role, AgentID: "agent", State: model.NodeReady, PeerID: "peer-" + request.ID})
			return
		}
		id, request := acknowledgeProfile(t, w, r, base)
		if request.RunID != "run" || request.Generation != 2 || r.Header.Get("Authorization") != "Bearer test" {
			t.Error("lost execution/authentication scope")
		}
		mu.Lock()
		commands = append(commands, id+":"+request.Stage)
		mu.Unlock()
	})
	for _, id := range []string{"selected", "other-group", "other-profile", "other-run", "stopped", "old"} {
		node := meshFreezeNode(id)
		node.Profile = "baseline"
		node.Metadata["networkMutable"] = "true"
		switch id {
		case "other-group":
			node.Group = "other"
		case "other-profile":
			node.Profile = "other"
		case "other-run":
			node.RunID = "other"
		case "stopped":
			node.State = model.NodeStopped
		case "old":
			node.Generation--
		}
		s.state.nodes[id] = node
	}
	jobs := newPhaseJobs()
	phase := controllerProfilePhase(t)
	if err := s.runPhase(t.Context(), "run", 2, phase, nil, jobs, time.Second); err != nil {
		t.Fatal(err)
	}
	if strings.Join(commands, ",") != "selected:prepare,selected:apply" {
		t.Fatalf("wrong target selection: %v", commands)
	}
	for _, group := range []string{"workers", "other"} {
		join := scenario.Phase{Action: "join", Group: group, Profile: "baseline", NodeType: "full", Role: "worker", Count: 1, Node: base, NetworkMutable: group == "workers"}
		if err := s.runPhase(t.Context(), "run", 2, join, rand.New(rand.NewSource(42)), jobs, time.Second); err != nil {
			t.Fatal(err)
		}
		req := <-created
		wantD, wantDelay := 6, ""
		if group == "workers" {
			wantD, wantDelay = 8, "30ms"
		}
		if *req.Config.GossipSub.Params.D != wantD || req.Config.Network.Delay != wantDelay || req.ProfileRevision != 1 || req.NetworkMutable != (group == "workers") {
			t.Fatalf("future peer got wrong profile: %+v", req)
		}
	}
	if *base.GossipSub.Params.D != 6 || base.Network.Delay != "" {
		t.Fatal("shared profile template mutated")
	}
}

func TestProfileSchedulePrepareFailureDoesNotApplyOrChangeFutureJoins(t *testing.T) {
	var apply atomic.Int32
	s := profileControllerFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var req model.ProfileUpdateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Stage == "apply" {
			apply.Add(1)
		}
		http.Error(w, "bad profile", 409)
	})
	node := meshFreezeNode("selected")
	node.Profile = "baseline"
	node.Metadata["networkMutable"] = "true"
	s.state.nodes[node.ID] = node
	state := newRunProfileState()
	err := s.runProfileSchedule(t.Context(), "run", 2, controllerProfilePhase(t), state)
	if err == nil || !strings.Contains(err.Error(), "apply was not started") || apply.Load() != 0 || len(state.overrides) != 0 {
		t.Fatalf("prepare failure leaked changes: %v", err)
	}
}

func TestProfileScheduleClockCancellationAndIndependentRunState(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.experiments["run"] = model.Experiment{ID: "run", StartedAt: time.Now().Add(-time.Hour)}
	phase := controllerProfilePhase(t)
	phase.Schedule.Changes[0].After = "20ms"
	profiles := newRunProfileState()
	start := time.Now()
	if err := s.runProfileSchedule(t.Context(), "run", 2, phase, profiles); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 20*time.Millisecond || len(profiles.overrides) != 1 {
		t.Fatal("phase-start deadline was skipped")
	}
	phase.Schedule.Reference = "experiment-start"
	phase.Schedule.Changes[0].After = "30m"
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := s.runProfileSchedule(ctx, "run", 2, phase, newRunProfileState()); err != nil {
		t.Fatalf("past experiment deadline should execute now: %v", err)
	}
	phase.Schedule.Reference = "phase-start"
	anchored := context.WithValue(ctx, profilePhaseStartKey{}, time.Now().Add(-time.Hour))
	if err := s.runProfileSchedule(anchored, "run", 2, phase, newRunProfileState()); err != nil {
		t.Fatalf("delayed job restarted the phase clock: %v", err)
	}
	phase.Schedule.Changes[0].After = "1h"
	ctx2, stop := context.WithCancel(t.Context())
	stop()
	fresh := newRunProfileState()
	if err := s.runProfileSchedule(ctx2, "run", 2, phase, fresh); !errors.Is(err, context.Canceled) || len(fresh.overrides) != 0 {
		t.Fatalf("canceled schedule survived: %v", err)
	}
	base, _ := model.BuiltInNodeConfig("full")
	request := model.CreateNodeRequest{ID: "next-run", Group: "workers", Profile: "baseline", Type: "full", Role: "worker", Config: base}
	got, release, err := profileCreateRequest(context.WithValue(t.Context(), profileRuntimeKey{}, fresh), request)
	defer release()
	if err != nil || *got.Config.GossipSub.Params.D != 6 || got.ProfileRevision != 0 {
		t.Fatal("previous run modified a new run")
	}
}

func TestProfileScheduleSerializesAdmissionAndHonorsGateCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	state := newRunProfileState()
	phase := controllerProfilePhase(t)
	base, _ := model.BuiltInNodeConfig("full")
	request := model.CreateNodeRequest{ID: "admitting", Group: "workers", Profile: "baseline", Type: "full", Role: "worker", Config: base}
	ctx := context.WithValue(t.Context(), profileRuntimeKey{}, state)
	_, release, err := profileCreateRequest(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.runProfileSchedule(t.Context(), "run", 2, phase, state) }()
	select {
	case err := <-done:
		t.Fatalf("schedule passed an in-flight admission: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, release, err := profileCreateRequest(ctx, request)
	if err != nil || *got.Config.GossipSub.Params.D != 8 {
		t.Fatalf("post-change admission missed updated profile: %v", err)
	}
	release()
	if err := state.gate.Acquire(t.Context(), profileWriteWeight); err != nil {
		t.Fatal(err)
	}
	blocked, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := profileCreateRequest(blocked, request); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled admission waited for a schedule")
	}
	state.gate.Release(profileWriteWeight)
}

func TestProfileScheduleTimingUsesAbsoluteExperimentDeadline(t *testing.T) {
	phase := controllerProfilePhase(t)
	phase.Schedule.Reference = "experiment-start"
	phase.Schedule.Changes[0].After = "10s"
	plan := newTimingPlan(scenario.Scenario{Phases: []scenario.Phase{{Action: "wait", Duration: "4s", Repeat: 1}, phase}})
	if plan.duration != 10 {
		t.Fatalf("experiment deadline accumulated phase time: %v", plan.duration)
	}
}

func TestProfileScheduleStopAllCancelsTimersAndResetsFutureProfiles(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	jobs := newPhaseJobs()
	phase := controllerProfilePhase(t)
	phase.Schedule.Changes = append(phase.Schedule.Changes, model.ProfileChange{After: "1h", Set: phase.Schedule.Changes[0].Set})
	old := jobs.profiles
	if err := jobs.startContext(t.Context(), "conditions", func(ctx context.Context) error { return s.runProfileSchedule(ctx, "run", 2, phase, old) }, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		found := false
		for _, event := range s.state.recentEvents() {
			if event.Type == "schedule_change" && event.Fields["stage"] == "apply" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first change never applied")
		}
		time.Sleep(time.Millisecond)
	}
	if err := s.runPhase(t.Context(), "run", 2, scenario.Phase{Action: "stop-all"}, nil, jobs, time.Second); err != nil {
		t.Fatal(err)
	}
	if jobs.profiles == old || len(jobs.profiles.overrides) != 0 || jobs.profiles.revision != 0 {
		t.Fatal("stop-all retained prior-generation profiles")
	}
	if len(old.overrides) != 1 {
		t.Fatal("future scheduled change escaped cancellation")
	}
}
