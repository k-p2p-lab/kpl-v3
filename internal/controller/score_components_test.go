package controller

import (
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func scoreComponentFixture(at time.Time, count int, value float64) *model.PeerScoreSample {
	sample := &model.PeerScoreSample{ObservedAt: at, IntervalSeconds: 1, Components: make(map[string]model.ScoreStatistic)}
	for _, key := range model.ScoreComponentKeys {
		sample.Components[key] = model.ScoreStatistic{Count: count, Mean: value, Min: value, Max: value}
	}
	return sample
}
func TestScoreComponentsPersistAndWeightPairsAcrossObservers(t *testing.T) {
	now := time.Now().UTC()
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, server, "scored", "running", now)
	server.state.experiments[run.ID] = run
	server.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline, LastSeen: now}
	for _, entry := range []struct {
		id, group string
		sample    *model.PeerScoreSample
	}{{"a", "workers", scoreComponentFixture(now, 1, 10)}, {"b", "workers", scoreComponentFixture(now, 3, -2)}, {"stale", "workers", scoreComponentFixture(now.Add(-time.Minute), 10, 100)}} {
		server.state.nodes[entry.id] = model.Node{ID: entry.id, PeerID: entry.id, RunID: run.ID, AgentID: "agent", Group: entry.group, State: model.NodeReady, LastSeen: now, ScoreSample: entry.sample}
		server.state.nodeReportTimes[entry.id] = now
	}
	if err := server.state.recordAnalysisObservation(run.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := server.state.appendRunEvents(run.ID, []model.TraceEvent{{RunID: run.ID, NodeID: "a", Type: "peer_score", Timestamp: now, ScoreSample: scoreComponentFixture(now, 1, 10)}}); err != nil {
		t.Fatal(err)
	}
	if len(server.state.events) != 0 {
		t.Fatal("periodic scores displaced network activity feed")
	}
	restarted := New(server.config, nil)
	analysis := analysisRequest(t, restarted, run.ID)
	if len(analysis.Observations) != 1 || analysis.EventCount != 1 {
		t.Fatal("missing observation")
	}
	for _, group := range analysis.Observations[0].Groups {
		got := group.ScoreComponents["p1"]
		if got.Count != 4 || got.Mean != 1 || got.Min != -2 || got.Max != 10 {
			t.Fatalf("wrong pair weighting or stale inclusion: %+v", got)
		}
	}
	raw := dashboardSnapshot(model.Snapshot{Nodes: []model.Node{server.state.nodes["a"]}})
	if raw.Nodes[0].ScoreSample != nil || server.state.nodes["a"].ScoreSample == nil {
		t.Fatal("detailed score summary leaked into SSE or mutated source")
	}
}
