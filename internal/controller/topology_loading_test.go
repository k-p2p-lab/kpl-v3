package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func denseTopologyState(t testing.TB) *state {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil).state
	now := time.Now().UTC()
	s.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline, LastSeen: now, Capacity: 1000, ActiveNodes: 1000}
	for i := range 1000 {
		neighbors := make([]string, 0, 50)
		for offset := 1; offset <= 25; offset++ {
			neighbors = append(neighbors, fmt.Sprintf("peer-%04d", (i+offset)%1000), fmt.Sprintf("peer-%04d", (i-offset+1000)%1000))
		}
		node := model.Node{ID: fmt.Sprintf("node-%04d", i), PeerID: fmt.Sprintf("peer-%04d", i), RunID: "run", AgentID: "agent", State: model.NodeReady,
			LastSeen: now, ConnectedPeers: neighbors, RoutingPeers: neighbors, MeshPeers: map[string][]string{"topic": neighbors}, OverlayObservedAt: now}
		s.setNodeLocked(node)
		s.nodeReportTimes[node.ID] = now
	}
	return s
}

func BenchmarkTopologyDashboardLoadOn(b *testing.B) {
	benchmarkTopologyDashboardLoad(b, true)
}

func BenchmarkTopologyDashboardLoadOff(b *testing.B) {
	benchmarkTopologyDashboardLoad(b, false)
}

func benchmarkTopologyDashboardLoad(b *testing.B, includeTopology bool) {
	s := denseTopologyState(b)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		frame, err := newDashboardFrame(s.snapshotForTopologyView(true, includeTopology), time.Now())
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(frame.data)), "frame-bytes")
	}
}

func TestDashboardOverviewOffKeepsPopulationAndMetrics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil).state
	s.agents["disabled"] = model.Agent{ID: "disabled", Disabled: true}
	for _, status := range []string{model.NodeReady, model.NodeStarting, "unknown", model.NodeStopping, model.NodeStopped, model.NodeFailed} {
		s.setNodeLocked(model.Node{ID: status, State: status, ConnectedPeers: []string{"peer"}})
	}
	s.setNodeLocked(model.Node{ID: "disabled-peer", State: model.NodeReady, AgentID: "disabled"})
	now := time.Now().UTC()
	s.experiments["run"] = model.Experiment{ID: "run", State: "running", StartedAt: now}
	if err := s.appendRunEvents("run", []model.TraceEvent{{RunID: "run", NodeID: "ready", EventID: "publish", Type: "publish", MessageID: "message", Timestamp: now}}); err != nil {
		t.Fatal(err)
	}
	full, summary := s.dashboardSnapshot(), s.snapshotForTopologyView(true, false)
	expected := &model.TopologySummary{Included: false, Total: 3, Ready: 1, Starting: 1, Other: 1}
	if !reflect.DeepEqual(summary.Topology, expected) || len(summary.Nodes) != 0 || len(summary.Edges) != 0 {
		t.Fatalf("incorrect summary: %+v", summary)
	}
	if len(full.Nodes) != 3 || full.Topology == nil || !full.Topology.Included || len(s.nodes["ready"].ConnectedPeers) != 1 {
		t.Fatal("omitting the graph changed topology source records")
	}
	if summary.Metrics.Published != 1 || full.Metrics.Published != summary.Metrics.Published || summary.Metrics.RunID != "run" || summary.Metrics.DeliveryRatioAvailable {
		t.Fatalf("omitting the graph changed measurement evidence: %+v", summary.Metrics)
	}
	if !reflect.DeepEqual(full.Agents, summary.Agents) || !reflect.DeepEqual(full.Experiments, summary.Experiments) || !reflect.DeepEqual(full.Events, summary.Events) {
		t.Fatal("omitting the graph removed non-topology data")
	}
}

func TestDashboardSummaryDoesNotWaitForTopologyFlight(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	flight := &dashboardSnapshotFlight{done: make(chan struct{})}
	s.dashboardFlight = flight
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	summary, err := s.dashboardStreamSnapshotForTopology(ctx, nil, false)
	if err != nil || summary == nil || s.dashboardFlight != flight {
		t.Fatalf("summary blocked on topology: %v", err)
	}
	if cached, err := s.dashboardStreamSnapshotForTopology(ctx, nil, false); err != nil || cached != summary {
		t.Fatalf("summary cache not shared: %v", err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := s.dashboardStreamSnapshotForTopology(canceled, nil, false); err != context.Canceled {
		t.Fatalf("canceled summary waiter: %v", err)
	}
	close(flight.done)
}

type topologySnapshotWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *topologySnapshotWriter) FlushError() error {
	w.ResponseRecorder.Flush()
	w.cancel()
	return nil
}

func TestDashboardTopologyStreamModes(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state = denseTopologyState(t)
	for _, query := range []string{"&topology=0", "&topology=1", ""} {
		ctx, cancel := context.WithCancel(t.Context())
		response := &topologySnapshotWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
		s.handleStream(response, httptest.NewRequest("GET", "/api/v1/stream?view=dashboard"+query, nil).WithContext(ctx))
		cancel()
		if response.Code != 200 {
			t.Fatalf("stream mode %s: %d", query, response.Code)
		}
		data := strings.SplitN(response.Body.String(), "data: ", 2)
		if len(data) != 2 {
			t.Fatalf("snapshot missing: %s", response.Body)
		}
		var snapshot model.Snapshot
		if err := json.Unmarshal([]byte(strings.TrimSpace(data[1])), &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Topology == nil || snapshot.Topology.Total != 1000 || snapshot.Topology.Ready != 1000 {
			t.Fatal("population omitted")
		}
		if query == "&topology=0" {
			if snapshot.Topology.Included || len(snapshot.Nodes) != 0 || len(snapshot.Edges) != 0 || response.Body.Len() > 10000 {
				t.Fatal("off stream retained graph data")
			}
		} else if !snapshot.Topology.Included || len(snapshot.Nodes) != 1000 || len(snapshot.Edges) != 75000 {
			t.Fatal("on/default stream lost topology")
		}
	}
	for _, query := range []string{"?view=dashboard&topology=invalid", "?topology=0"} {
		response := httptest.NewRecorder()
		s.handleStream(response, httptest.NewRequest("GET", "/api/v1/stream"+query, nil))
		if response.Code != 400 {
			t.Fatalf("invalid stream mode %s accepted: %d", query, response.Code)
		}
	}
	if len(s.state.watchers) != 0 {
		t.Fatal("mode switch retained stream watchers")
	}
}

func TestDashboardSummaryDeltaUpdatesPopulation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil).state
	s.setNodeLocked(model.Node{ID: "peer", State: model.NodeStarting})
	first, err := newDashboardFrame(s.snapshotForTopologyView(true, false), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.setNodeLocked(model.Node{ID: "peer", State: model.NodeReady})
	second, err := newDashboardFrame(s.snapshotForTopologyView(true, false), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	patch, err := second.delta(first)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal(applyDashboardTestPatch(t, first.data, patch), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Topology == nil || snapshot.Topology.Ready != 1 || snapshot.Topology.Starting != 0 || len(snapshot.Nodes) != 0 {
		t.Fatalf("population delta lost: %s", patch)
	}
}
