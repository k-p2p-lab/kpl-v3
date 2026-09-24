package controller

import (
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestNetworkEdgesAreDeduplicated(t *testing.T) {
	nodes := []model.Node{
		{ID: "a", PeerID: "peer-a", State: model.NodeReady, ConnectedPeers: []string{"peer-b"}},
		{ID: "b", PeerID: "peer-b", State: model.NodeReady, ConnectedPeers: []string{"peer-a"}},
	}
	edges := networkEdges(nodes)
	if len(edges) != 1 || edges[0].Source != "a" || edges[0].Target != "b" {
		t.Fatalf("unexpected edges: %+v", edges)
	}
}

func TestCalculateMetrics(t *testing.T) {
	experiments := []model.Experiment{{ID: "run-1", StartedAt: time.Now()}}
	nodes := []model.Node{{ID: "a", RunID: "run-1", State: model.NodeReady}, {ID: "b", RunID: "run-1", State: model.NodeReady}}
	events := []model.TraceEvent{
		{RunID: "run-1", NodeID: "b", Type: "deliver", MessageID: "m1", LatencyMS: 10, Timestamp: time.Unix(2, 0), Fields: map[string]any{"payloadEncoding": "envelope", "latencyAvailable": true}},
		{RunID: "run-1", NodeID: "a", Type: "publish", MessageID: "m1", Timestamp: time.Unix(1, 0), Fields: map[string]any{"targetNodeIds": []string{"b"}}},
		{RunID: "run-1", NodeID: "b", Type: "duplicate", MessageID: "m1", Timestamp: time.Unix(3, 0)},
	}
	metrics := calculateMetrics(nodes, experiments, events)
	if metrics.Published != 1 || metrics.Delivered != 1 || metrics.Duplicates != 1 {
		t.Fatalf("unexpected counters: %+v", metrics)
	}
	if metrics.Reachability != 1 || metrics.P95LatencyMS != 10 {
		t.Fatalf("unexpected propagation metrics: %+v", metrics)
	}
}

func TestRawDeliveryBeforePublisherClockPreservesReachWithoutLatency(t *testing.T) {
	nodes := []model.Node{{ID: "a", RunID: "run"}, {ID: "b", RunID: "run"}}
	events := []model.TraceEvent{
		{RunID: "run", NodeID: "b", Type: "deliver", MessageID: "raw-hash", LatencyMS: -1, Timestamp: time.Unix(1, 0)},
		{RunID: "run", NodeID: "a", Type: "publish", MessageID: "raw-hash", Timestamp: time.Unix(2, 0), Fields: map[string]any{"targetNodeIds": []string{"b"}}},
	}
	metrics := calculateMetrics(nodes, []model.Experiment{{ID: "run"}}, events)
	if metrics.Reachability != 1 || metrics.Delivered != 1 || metrics.AverageLatencyMS != 0 || metrics.P95LatencyMS != 0 {
		t.Fatalf("clock skew lost raw delivery or fabricated latency: %+v", metrics)
	}
}

func TestExperimentOrderIsDeterministicAcrossEqualStartTimes(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 2, 3, 0, time.UTC)
	// This explicit order includes conflicting IDs, duplicate iterations, and
	// a legacy run without batch metadata between two distinct batch keys.
	want := []model.Experiment{
		{ID: "newest", BatchID: "z", Iteration: 10, StartedAt: now.Add(time.Second)},
		{ID: "started-a-2", BatchID: "a", Iteration: 2, StartedAt: now},
		{ID: "started-a-10", BatchID: "a", Iteration: 10, StartedAt: now},
		{ID: "started-b-1", BatchID: "b", Iteration: 1, StartedAt: now.In(time.FixedZone("same instant", 9*60*60))},
		{ID: "older", BatchID: "a", Iteration: 1, StartedAt: now.Add(-time.Second)},
		{ID: "queued-a-2-a", BatchID: "a", Iteration: 2, State: "queued"},
		{ID: "queued-a-2-z", BatchID: "a", Iteration: 2, State: "queued"},
		{ID: "queued-a-10", BatchID: "a", Iteration: 10, State: "queued"},
		{ID: "ab-legacy", State: "queued"},
		{ID: "queued-b-2", BatchID: "b", Iteration: 2, State: "queued"},
		{ID: "queued-b-10", BatchID: "b", Iteration: 10, State: "queued"},
	}
	// Check every ordered pair, including self-comparison, so same-batch
	// ordering cannot form a cycle when another batch or legacy run intervenes.
	for i, a := range want {
		for j, b := range want {
			if got := experimentBefore(a, b); got != (i < j) {
				t.Fatalf("experimentBefore(%s, %s) = %v, want %v", a.ID, b.ID, got, i < j)
			}
		}
	}
	for offset := range want {
		s := newState("")
		// Rotate a reversed insertion order. inventory and snapshot must both
		// produce the same order independently of insertion and map traversal.
		for index := range want {
			experiment := want[(offset+len(want)-1-index)%len(want)]
			s.experiments[experiment.ID] = experiment
		}
		for attempt := 0; attempt < 4; attempt++ {
			for name, snapshot := range map[string]model.Snapshot{"inventory": s.inventory(), "snapshot": s.snapshot()} {
				if len(snapshot.Experiments) != len(want) {
					t.Fatalf("%s lost experiments: %+v", name, snapshot.Experiments)
				}
				for index, experiment := range snapshot.Experiments {
					if experiment.ID != want[index].ID {
						t.Fatalf("%s[%d] = %s, want %s (offset %d, attempt %d)", name, index, experiment.ID, want[index].ID, offset, attempt)
					}
				}
			}
		}
	}
}

func TestExperimentOrderPreservesLatestRunMetrics(t *testing.T) {
	now := time.Date(2026, 9, 14, 1, 2, 3, 0, time.UTC)
	for _, state := range []string{"running", "completed"} {
		t.Run(state, func(t *testing.T) {
			s := newState("")
			for _, experiment := range []model.Experiment{
				{ID: "first", BatchID: "batch", Iteration: 1, State: "completed", StartedAt: now.Add(-time.Minute)},
				{ID: "latest", BatchID: "batch", Iteration: 2, State: state, StartedAt: now},
				{ID: "queued", BatchID: "batch", Iteration: 3, State: "queued"},
				{ID: "other", BatchID: "a", Iteration: 1, State: "completed", StartedAt: now.Add(-time.Second)},
			} {
				s.experiments[experiment.ID] = experiment
			}
			if got := s.snapshot().Metrics.RunID; got != "latest" {
				t.Fatalf("metric scope = %s, want latest %s iteration", got, state)
			}
		})
	}
}
