package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func analysisWriteLines[T any](t *testing.T, server *Server, id, name string, values []T) {
	t.Helper()
	var data bytes.Buffer
	for _, value := range values {
		if err := json.NewEncoder(&data).Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(server.config.DataDir, "runs", id, name), data.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func analysisRequest(t *testing.T, server *Server, id string) resultAnalysis {
	t.Helper()
	response := resultRequest(server, http.MethodGet, "/api/v1/experiments/"+id+"/analysis")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("analysis must not be cached by HTTP clients")
	}
	var result resultAnalysis
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestResultAnalysisFullHistoryAndDeliveryDedupAfterRestart(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run", "completed", time.Unix(1, 0))
	targets := make([]string, 500)
	for i := range targets {
		targets[i] = fmt.Sprintf("receiver-%d", i)
	}
	events := []model.TraceEvent{cohortPublish("message", "topic", targets)}
	for _, node := range targets {
		events = append(events, cohortDelivery("message", "topic", node, 20))
	}
	events = append(events, cohortDuplicate("message", targets[0], "dup"))
	// HTTP retries and another run in an imported log cannot inflate the analysis.
	events = append(events, events[0], events[1], model.TraceEvent{RunID: "another", Type: "publish"})
	analysisWriteLines(t, server, "run", "events.jsonl", events)
	restarted := New(server.config, nil)
	result := analysisRequest(t, restarted, "run")
	if result.EventCount != 502 || result.Metrics.LatencySamples != 500 || result.Metrics.ExpectedDeliveries != 500 || result.Metrics.EligibleDuplicates != 1 {
		t.Fatalf("lost history or counted retries: %+v", result)
	}
	if !reflect.DeepEqual(result.LatencyCDF, []analysisPoint{{20, 1}}) || !reflect.DeepEqual(result.LatencyHistogram, []analysisPoint{{20, 500}}) {
		t.Fatalf("distributions: %v %v", result.LatencyCDF, result.LatencyHistogram)
	}
	if result.BinSeconds != 1 || len(result.Timeline) != 2 || result.Timeline[0].Publish != 1 || result.Timeline[0].Deliver != 500 || result.Timeline[1].Duplicate != 1 {
		t.Fatalf("timeline: %+v", result.Timeline)
	}
	if result.Observations == nil || len(result.Observations) != 0 {
		t.Fatal("old archives need an empty observation array")
	}
}

func TestResultAnalysisUsesSnapshotBoundaryAndSessionWindows(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "window-run", "running", windowTestEpoch)
	events := windowEvents(windowStart("receiver", "session", 0, "topic"), windowDeliveredEvent("receiver", "session", 2, 12), windowEvent("receiver", "session", 3, "measurement_checkpoint", 22))
	analysisWriteLines(t, server, "window-run", "events.jsonl", events)
	snapshot, err := server.captureResultFiles("window-run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	snapshot.exportedAt = windowTestEpoch.Add(15 * time.Second)
	pending, err := analyzeResult(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Metrics.PendingPublications != 1 || len(pending.LatencyCDF) != 0 {
		t.Fatalf("premature sample: %+v", pending)
	}
	snapshot.exportedAt = windowTestEpoch.Add(30 * time.Second)
	mature, err := analyzeResult(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mature.Metrics, windowSummary(events, 30)) || !reflect.DeepEqual(mature.LatencyCDF, []analysisPoint{{2000, 1}}) {
		t.Fatalf("session-window mismatch: %+v", mature)
	}
}

func TestResultAnalysisBoundsChartSizeAndKeepsLatestObservation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run", "completed", time.Unix(1, 0))
	var events []model.TraceEvent
	for i := 0; i < 1000; i++ {
		events = append(events, model.TraceEvent{RunID: "run", Type: "publish", Timestamp: time.Unix(int64(i), 0)})
	}
	analysisWriteLines(t, server, "run", "events.jsonl", events)
	var observations []analysisObservation
	for i := 0; i < 3000; i++ {
		observations = append(observations, analysisObservation{RunID: "run", At: time.Unix(int64(i*5), 0), Groups: []analysisGroup{}})
	}
	analysisWriteLines(t, server, "run", "observations.jsonl", observations)
	result := analysisRequest(t, server, "run")
	total := 0
	for _, bin := range result.Timeline {
		total += bin.Publish
	}
	if len(result.Timeline) > 360 || total != 1000 || result.BinSeconds <= 1 {
		t.Fatalf("binning dropped events: count=%d total=%d width=%d", len(result.Timeline), total, result.BinSeconds)
	}
	if len(result.Observations) > 1440 || result.ObservationCount != 3000 || !result.Observations[0].At.Equal(observations[0].At) || !result.Observations[len(result.Observations)-1].At.Equal(observations[2999].At) {
		t.Fatal("sampling lost endpoints or exceeded limit")
	}
	values := make([]float64, 10000)
	for i := range values {
		values[i] = float64(i)
	}
	cdf, hist := analysisDistribution(values)
	sum := 0.0
	for _, bin := range hist {
		sum += bin.Y
	}
	if len(cdf) > 201 || cdf[len(cdf)-1].Y != 1 || len(hist) > 30 || sum != 10000 {
		t.Fatal("distribution bounds or mass incorrect")
	}
}

func TestResultAnalysisErrorsAndCapturedFileBoundary(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "empty", "completed", time.Unix(1, 0))
	empty := analysisRequest(t, server, "empty")
	if len(empty.LatencyCDF) != 0 || empty.Metrics.DeliveryRatioAvailable || empty.EventCount != 0 {
		t.Fatal("empty result fabricated data")
	}
	for _, check := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/api/v1/experiments/missing/analysis", 404},
		{http.MethodPost, "/api/v1/experiments/empty/analysis", 405},
	} {
		response := resultRequest(server, check.method, check.path)
		if response.Code != check.status {
			t.Fatalf("%s %s: %d", check.method, check.path, response.Code)
		}
	}
	analysisWriteLines(t, server, "empty", "events.jsonl", []model.TraceEvent{{RunID: "empty", Type: "publish"}})
	snapshot, err := server.captureResultFiles("empty", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	file, err := os.OpenFile(filepath.Join(server.config.DataDir, "runs", "empty", "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString("broken json\n"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	captured, err := analyzeResult(context.Background(), snapshot)
	if err != nil || captured.EventCount != 1 || captured.UntimedEvents != 1 {
		t.Fatalf("read past snapshot: %+v %v", captured, err)
	}
	response := resultRequest(server, http.MethodGet, "/api/v1/experiments/empty/analysis")
	if response.Code != 422 || !strings.Contains(response.Body.String(), "events.jsonl line 2") {
		t.Fatalf("corruption hidden: %d %s", response.Code, response.Body)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = analyzeResult(ctx, snapshot); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestAnalysisObservationLayersGroupsAndMissingReports(t *testing.T) {
	now := time.Now().UTC()
	agents := map[string]model.Agent{"agent": {ID: "agent", State: "online", LastSeen: now}}
	nodes := []model.Node{
		{ID: "a", RunID: "run", AgentID: "agent", Group: "worker", PeerID: "pa", State: model.NodeReady, LastSeen: now, OverlayObservedAt: now, ConnectedPeers: []string{"pb", "pc"}, MeshPeers: map[string][]string{"one": {"pb", "pc"}, "two": {"pb"}}, PeerScores: map[string]float64{"pb": -2, "pc": 2, "bad": math.NaN()}},
		{ID: "b", RunID: "run", AgentID: "agent", Group: "worker", PeerID: "pb", State: model.NodeReady, LastSeen: now, OverlayObservedAt: now, ConnectedPeers: []string{"pc"}, MeshPeers: map[string][]string{"one": {"pc"}}},
		{ID: "c", RunID: "run", AgentID: "agent", Group: "boot", PeerID: "pc", State: model.NodeReady, LastSeen: now, OverlayObservedAt: now},
		{ID: "legacy", RunID: "run", AgentID: "agent", PeerID: "pd", State: model.NodeReady, LastSeen: now},
		{ID: "stale", RunID: "run", AgentID: "agent", PeerID: "pe", State: model.NodeReady, LastSeen: now.Add(-time.Hour), PeerScores: map[string]float64{"pa": -100}},
	}
	observation := makeAnalysisObservation("run", now, nodes, agents)
	all := observation.Groups[0]
	if all.Group != "" || all.Ready != 5 || all.Reporting != 4 || all.ScoreCount != 2 || all.ScoreObservers != 1 || *all.ScoreMean != 0 || *all.NegativeScoreRatio != .5 {
		t.Fatalf("counts or scores: %+v", all)
	}
	layers := map[string]analysisLayer{}
	for _, l := range all.Layers {
		layers[l.Protocol] = l
	}
	gossip, transport, kad := layers["gossipsub"], layers["transport"], layers["kademlia"]
	if gossip.Nodes != 3 || *gossip.AverageDegree != 2 || *gossip.Clustering != 1 || !reflect.DeepEqual(gossip.Degrees, []analysisPoint{{2, 3}}) {
		t.Fatalf("topic dedup or triangles: %+v", gossip)
	}
	if transport.Nodes != 4 || *transport.AverageDegree != 1.5 || *transport.Clustering != .75 || kad.Nodes != 3 || *kad.AverageDegree != 0 {
		t.Fatalf("layer distinction: transport=%+v kademlia=%+v", transport, kad)
	}
	for _, g := range observation.Groups {
		if g.Group == "worker" && (g.Layers[2].Nodes != 2 || *g.Layers[2].AverageDegree != 2) {
			t.Fatalf("group excluded cross-group neighbors: %+v", g)
		}
	}
	absent := makeAnalysisObservation("run", now, nil, agents).Groups[0]
	if absent.ScoreMean != nil || absent.Layers[0].AverageDegree != nil || absent.Layers[0].Clustering != nil {
		t.Fatal("missing observations represented as zero")
	}
}

func TestAnalysisObservationsPersistExportAndStopAfterDeletion(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, server, "observed", "running", time.Now().UTC())
	server.state.experiments[run.ID] = run
	if err := server.state.recordAnalysisObservation(run.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	restarted := New(server.config, nil)
	result := analysisRequest(t, restarted, run.ID)
	if result.ObservationCount != 1 || result.Result.State != "interrupted" {
		t.Fatalf("restart lost observations: %+v", result)
	}
	response := resultRequest(server, http.MethodGet, "/api/v1/experiments/observed/download")
	if response.Code != 200 {
		t.Fatalf("download: %d %s", response.Code, response.Body)
	}
	files := decodeResultZIP(t, response.Body.Bytes())
	if len(files["observations.jsonl"]) == 0 {
		t.Fatal("observations missing from ZIP")
	}
	run.State = "completed"
	server.state.experiments[run.ID] = run
	if err := server.state.recordAnalysisObservation(run.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if got := analysisRequest(t, server, run.ID).ObservationCount; got != 1 {
		t.Fatalf("terminal result grew: %d", got)
	}
	deleted := resultRequest(server, http.MethodDelete, "/api/v1/results/observed")
	if deleted.Code != 204 {
		t.Fatalf("delete: %d %s", deleted.Code, deleted.Body)
	}
	if err := server.state.recordAnalysisObservation(run.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", run.ID)); !os.IsNotExist(err) {
		t.Fatalf("recreated deleted result: %v", err)
	}
	if got := resultRequest(server, http.MethodGet, "/api/v1/experiments/observed/analysis").Code; got != 404 {
		t.Fatalf("deleted analysis: %d", got)
	}
}

func TestResultAnalysisKeepsFiniteLatencyMeansAcrossDefinitions(t *testing.T) {
	for _, definition := range []string{"dispatch-cohort-v1", sessionWindowDefinition} {
		t.Run(definition, func(t *testing.T) {
			id := "run"
			events := []model.TraceEvent{cohortPublish("message", "topic", []string{"a", "b"}), cohortDelivery("message", "topic", "a", math.MaxFloat64), cohortDelivery("message", "topic", "b", math.MaxFloat64)}
			if definition == sessionWindowDefinition {
				id = "window-run"
				events = windowEvents()
				for _, receiver := range []string{"a", "b"} {
					delivery := windowDeliveredEvent(receiver, "session-"+receiver, 2, 12)
					delivery.LatencyMS = math.MaxFloat64
					events = append(events, windowStart(receiver, "session-"+receiver, 0, "topic"), delivery, windowEvent(receiver, "session-"+receiver, 3, "measurement_checkpoint", 22))
				}
			}
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			resultFixture(t, server, id, "completed", windowTestEpoch)
			analysisWriteLines(t, server, id, "events.jsonl", events)
			result := analysisRequest(t, server, id)
			if result.Metrics.Definition != definition || result.Metrics.LatencySamples != 2 || result.Metrics.AverageLatencyMS != math.MaxFloat64 || result.Metrics.P95LatencyMS != math.MaxFloat64 {
				t.Fatalf("finite latency samples lost or summary overflowed: %+v", result.Metrics)
			}
			if len(result.LatencyCDF) != 1 || result.LatencyCDF[0].Y != 1 || math.Abs(result.LatencyCDF[0].X/math.MaxFloat64-1) > 1e-15 {
				t.Fatalf("invalid finite distribution: %+v", result.LatencyCDF)
			}
			response := resultRequest(server, http.MethodGet, "/api/v1/experiments/"+id+"/download")
			if response.Code != http.StatusOK {
				t.Fatalf("archive failed: status=%d body=%s", response.Code, response.Body)
			}
			archive := decodeResultZIP(t, response.Body.Bytes())
			var metrics model.Metrics
			if err := json.Unmarshal(archive["metrics.json"], &metrics); err != nil {
				t.Fatal(err)
			}
			if metrics.AverageLatencyMS != result.Metrics.AverageLatencyMS {
				t.Fatal("analysis and archive latency means diverged")
			}
		})
	}
}
