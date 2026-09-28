package controller

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/prometheus/client_golang/prometheus"
)

func TestJoinNetworkScheduleCarriesCurrentExecutionStart(t *testing.T) {
	requests := make(chan model.CreateNodeRequest, 3)
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.CreateNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		requests <- request
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer agent.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.agents["agent"] = model.Agent{ID: "agent", URL: agent.URL, State: model.AgentOnline}
	phase := scenario.Phase{Group: "workers", Count: 2, Node: model.NodeConfig{Network: model.NetworkConfig{Schedule: &model.NetworkSchedule{Reference: "experiment-start", Changes: []model.NetworkChange{{After: "10m", Set: model.NetworkConfig{Delay: "100ms"}}}}}}}
	original := time.Now().UTC().Add(-2 * time.Minute)
	s.state.experiments["run"] = model.Experiment{ID: "run", StartedAt: original}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.runJoin(ctx, "run", 1, phase, rand.New(rand.NewSource(42))); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if req := <-requests; !req.ExperimentStartedAt.Equal(original) || req.Config.Network.Schedule.Clock() != "experiment-start" {
			t.Fatalf("join lost execution clock: %+v", req)
		}
	}
	restarted := time.Now().UTC()
	s.state.experiments["run"] = model.Experiment{ID: "run", StartedAt: restarted}
	phase.Count = 1
	if err := s.runJoin(ctx, "run", 2, phase, rand.New(rand.NewSource(43))); err != nil {
		t.Fatal(err)
	}
	if req := <-requests; !req.ExperimentStartedAt.Equal(restarted) {
		t.Fatalf("retry retained old start time: %+v", req)
	}
}

func TestNetworkMetricsWaitForScheduledPeerApplication(t *testing.T) {
	s := newState(t.TempDir())
	node := networkMetricTestNode("peer", model.NodeStarting, `{"delay":"10ms"}`)
	node.Metadata["networkPending"] = "true"
	s.nodes["peer"] = node
	registry := prometheus.NewRegistry()
	registry.MustRegister(newNetworkCollector(s))
	if values := networkMetricFamilies(t, registry); len(values) != 0 {
		t.Fatal("unapplied scheduled config reported as effective")
	}
	node.Metadata["networkPending"] = "false"
	node.Metadata["network"] = `{"delay":"100ms"}`
	s.nodes["peer"] = node
	values := networkMetricFamilies(t, registry)
	labels := map[string]string{"run_id": "run", "agent_id": "agent", "group": "workers", "profile": "wan", "scope": "p2p", "stat": "mean"}
	if got := networkMetricGauge(t, values, "kpl_network_configured_delay_seconds", labels); got != 0.1 {
		t.Fatalf("effective delay: %v", got)
	}
}

func TestNetworkScheduleDashboardKeepsEffectiveStateWithoutRepeatingPlans(t *testing.T) {
	plan := strings.Repeat("large scheduled network plan ", 8192)
	node := model.Node{ID: "peer", State: model.NodeReady, Metadata: map[string]string{
		"networkSchedule": plan, "networkRequested": plan,
		"network": `{"delay":"100ms"}`, "networkRevision": "1", "networkPending": "false",
	}}
	snapshot := model.Snapshot{Nodes: []model.Node{node}}
	frame, err := newDashboardFrame(snapshot, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.data) > 2048 || strings.Contains(string(frame.data), "networkSchedule") || strings.Contains(string(frame.data), "networkRequested") {
		t.Fatal("network plan leaked into dashboard traffic")
	}
	view := dashboardSnapshot(snapshot)
	if view.Nodes[0].Metadata["network"] != node.Metadata["network"] || view.Nodes[0].Metadata["networkRevision"] != "1" || view.Nodes[0].Metadata["networkPending"] != "false" {
		t.Fatal("applied state lost from topology")
	}
	if node.Metadata["networkSchedule"] != plan || node.Metadata["networkRequested"] != plan {
		t.Fatal("dashboard projection mutated full REST metadata")
	}
	node.Metadata["network"] = `{"delay":"200ms"}`
	node.Metadata["networkRevision"] = "2"
	next, err := newDashboardFrame(snapshot, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	delta, err := next.delta(frame)
	if err != nil || len(delta) > 2048 || !strings.Contains(string(delta), "200ms") || strings.Contains(string(delta), "networkSchedule") {
		t.Fatalf("wrong/bloated network delta: %s %v", delta, err)
	}
}
