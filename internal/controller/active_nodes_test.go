package controller

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestActiveDirectoryFollowsLifecycleAndRetainsInventory(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: now.Add(-time.Hour), Capacity: 10}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	request := model.CreateNodeRequest{ID: "boot", RunID: "run", Group: "boot", Role: "boot"}
	if !s.recordCreatedNode(request, agent.ID, model.Node{ID: request.ID, State: model.NodeStarting}) {
		t.Fatal("creation was not recorded")
	}
	assertVisible := func(want int) {
		t.Helper()
		if got := len(s.state.dashboardSnapshot().Nodes); got != want {
			t.Fatalf("dashboard nodes=%d, want %d", got, want)
		}
		if got := len(s.state.inventory().Nodes); got != 1 {
			t.Fatalf("terminal inventory lost: %d", got)
		}
	}
	assertBoots := func(want int) {
		t.Helper()
		response := httptest.NewRecorder()
		s.handleBootstrap(response, httptest.NewRequest("GET", "/api/v1/bootstrap?runId=run", nil))
		var nodes []map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil || len(nodes) != want {
			t.Fatalf("bootstrap=%s, want %d nodes, error=%v", response.Body.String(), want, err)
		}
	}
	assertVisible(1)
	assertBoots(0)
	node := model.Node{ID: request.ID, RunID: request.RunID, AgentID: agent.ID, Group: "boot", Role: "boot", State: model.NodeReady, PeerID: "peer", Addresses: []string{"/ip4/10.0.0.1/tcp/20000"}}
	agent.LastSeen = now
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	assertBoots(1)
	s.markNodeStoppingAndReleaseCapacity(node.ID)
	assertVisible(0)
	assertBoots(0)
	node.State = model.NodeStopped
	agent.LastSeen = now.Add(time.Second)
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	assertVisible(0)
	if len(s.state.activeNodeIDs) != 0 {
		t.Fatal("confirmed exit retained an active index entry")
	}
	// A delayed response cannot reinsert the terminal peer into the directory.
	s.recordCreatedNode(request, agent.ID, model.Node{ID: request.ID, State: model.NodeReady, LastSeen: now.Add(time.Hour)})
	assertVisible(0)
	assertBoots(0)
}

func TestBootstrapDoesNotComputeExperimentMetrics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.setNodeLocked(model.Node{ID: "boot", RunID: "run", Role: "boot", State: model.NodeReady, PeerID: "peer", Addresses: []string{"address"}})
	s.state.experiments["run"] = model.Experiment{ID: "run", State: "running", StartedAt: time.Now()}
	a := newRunMetricAccumulator()
	s.state.runMetrics["run"] = a
	response := httptest.NewRecorder()
	s.handleBootstrap(response, httptest.NewRequest("GET", "/api/v1/bootstrap?runId=run", nil))
	if response.Code != 200 || !a.cachedAt.IsZero() {
		t.Fatalf("address lookup rebuilt metrics: status=%d cachedAt=%s", response.Code, a.cachedAt)
	}
}

func TestDashboardActiveViewMatchesFullSnapshotFiltering(t *testing.T) {
	s := newState(t.TempDir())
	for _, state := range []string{model.NodeStarting, model.NodeReady, model.NodeStopping, model.NodeStopped, model.NodeFailed} {
		s.setNodeLocked(model.Node{ID: state, State: state})
	}
	full, active := dashboardSnapshot(s.snapshot()), dashboardSnapshot(s.dashboardSnapshot())
	full.GeneratedAt = active.GeneratedAt
	if !reflect.DeepEqual(full, active) {
		t.Fatalf("active view changed dashboard semantics:\nfull=%+v\nactive=%+v", full, active)
	}
}

func TestSettledHistogramsReuseHistoryAndRefreshLateEvidence(t *testing.T) {
	a := newRunMetricAccumulator()
	for _, event := range windowEvents(windowStart("receiver", "session", 0, "topic"), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22), windowDeliveredEvent("receiver", "session", 2, 12)) {
		a.observe(event)
	}
	now := windowTestEpoch.Add(30 * time.Second)
	_, first := a.livePrometheusSummary("run", now)
	key := propagationSeriesKey{"agent-receiver", "topic"}
	if first[key] == nil || first[key].count != 1 {
		t.Fatalf("stable delivery missing: %+v", first)
	}
	_, second := a.livePrometheusSummary("run", now.Add(time.Hour))
	if second[key] != first[key] {
		t.Fatal("settled historical histogram was rebuilt")
	}
	// An earlier valid observation must correct latency and bucket counts even
	// if it arrives immediately after a scrape. The old shared map stays immutable.
	a.observe(windowDeliveredEvent("receiver", "session", 4, 11))
	_, refreshed := a.livePrometheusSummary("run", now.Add(time.Hour+time.Millisecond))
	if refreshed[key] == first[key] || refreshed[key].count != 1 || refreshed[key].sum != 1 || first[key].sum != 2 {
		t.Fatalf("late evidence or immutable snapshot broken: first=%+v refreshed=%+v", first[key], refreshed[key])
	}
}
