package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func batchFixture(t *testing.T, s *Server, batch, id, state string, iteration, expected, publishes int) {
	t.Helper()
	start := time.Unix(1000+int64(iteration)*100, 0).UTC()
	run := model.Experiment{ID: id, Name: "Repeated scenario", BatchID: batch, Iteration: iteration, Repetitions: expected, State: state, StartedAt: start, FinishedAt: start.Add(time.Minute)}
	if err := s.persistManifest(run, []byte("version: 1\nname: repeated\nphases:\n  - action: stop-all\n")); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	for i := 0; i < publishes; i++ {
		err = json.NewEncoder(file).Encode(model.TraceEvent{RunID: id, EventID: fmt.Sprintf("e%d", i), Type: "publish", NodeID: "publisher", MessageID: fmt.Sprintf("m%d", i), Topic: "topic", Timestamp: start.Add(time.Duration(i) * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if state == "running" || state == "queued" {
		s.state.mu.Lock()
		s.state.experiments[id] = run
		s.state.mu.Unlock()
	}
}
func awaitBatch(t *testing.T, s *Server, id string) batchAnalysisStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := s.batchAnalysisStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.State != "queued" && status.State != "running" {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("batch analysis did not settle")
	return batchAnalysisStatus{}
}
func TestBatchAnalysisPersistsMeansAndKeepsIndividualAnalysis(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "first", "completed", 1, 4, 1)
	batchFixture(t, s, "batch", "second", "completed", 2, 4, 3)
	batchFixture(t, s, "batch", "failed", "failed", 3, 4, 9)
	batchFixture(t, s, "other", "unrelated", "completed", 1, 2, 20)
	_, err := s.startAnalysisJob(context.Background(), "first", false)
	if err != nil {
		t.Fatal(err)
	}
	individual := awaitAnalysisJob(t, s, "first")
	started, err := s.startBatchAnalysis(context.Background(), "batch", false)
	if err != nil {
		t.Fatal(err)
	}
	completed := awaitBatch(t, s, "batch")
	if completed.State != "completed" || completed.CompletedRuns != 2 || completed.TotalRuns != 2 || completed.Progress != 100 {
		t.Fatalf("status %+v", completed)
	}
	response := resultRequest(s, http.MethodGet, completed.ResultURL)
	var result batchAnalysisResult
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("artifact: %d %s", response.Code, response.Body)
	}
	stat := result.Summary["metrics.published"]
	if stat.Average == nil || *stat.Average != 2 || stat.Count != 2 || stat.Deviation == nil || math.Abs(*stat.Deviation-math.Sqrt(2)) > 1e-10 {
		t.Fatalf("mean: %+v", stat)
	}
	if len(result.Runs) != 2 || len(result.Excluded) != 1 || result.Excluded[0].ID != "failed" || result.MissingRuns != 1 || result.ExpectedRuns != 4 {
		t.Fatalf("selection: %+v", result)
	}
	if result.Runs[0].Result.ID != "first" || result.Runs[1].Result.ID != "second" {
		t.Fatal("unstable run order")
	}
	after, err := s.analysisJobStatus("first")
	if err != nil || after.ID != individual.ID {
		t.Fatal("batch replaced individual analysis")
	}
	if result.Runs[0].Research.Overview == nil || len(result.Runs[0].Research.Messages) != 0 {
		t.Fatal("batch did not compact message details")
	}
	restarted := New(s.config, nil)
	reused, err := restarted.startBatchAnalysis(context.Background(), "batch", false)
	if err != nil || reused.ID != started.ID || reused.State != "completed" {
		t.Fatalf("restart reuse: %+v %v", reused, err)
	}
	if got := resultRequest(restarted, http.MethodGet, completed.ResultURL); got.Code != 200 {
		t.Fatalf("restarted artifact: %s", got.Body)
	}
	list := resultRequest(restarted, http.MethodGet, "/api/v1/results")
	var results []savedResult
	if json.Unmarshal(list.Body.Bytes(), &results) != nil {
		t.Fatal(list.Body)
	}
	for _, run := range results {
		if run.BatchID == "batch" && (run.BatchAnalysis == nil || run.BatchAnalysis.ID != started.ID) {
			t.Fatal("batch status missing from Saved results")
		}
	}
	refreshed, err := restarted.startBatchAnalysis(context.Background(), "batch", true)
	if err != nil || refreshed.ID == started.ID {
		t.Fatalf("refresh: %+v %v", refreshed, err)
	}
	if awaitBatch(t, restarted, "batch").State != "completed" {
		t.Fatal("refresh failed")
	}
	if resultRequest(restarted, http.MethodGet, completed.ResultURL).Code != http.StatusConflict {
		t.Fatal("old job ID returned a new artifact")
	}
}
func TestBatchAnalysisRequestCancellationAndDuplicateAdmission(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 2, 1)
	batchFixture(t, s, "batch", "two", "completed", 2, 2, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := s.apiTestHandler(ctx)
	s.analysisSlots <- struct{}{}
	reqCtx, cancelReq := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/batch-analysis-jobs/batch", nil).WithContext(reqCtx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	cancelReq()
	if response.Code != http.StatusAccepted {
		t.Fatalf("admission: %d %s", response.Code, response.Body)
	}
	var first batchAnalysisStatus
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	second, err := s.startBatchAnalysis(ctx, "batch", false)
	if err != nil || first.ID != second.ID {
		t.Fatal("duplicate admission")
	}
	<-s.analysisSlots
	if got := awaitBatch(t, s, "batch"); got.State != "completed" {
		t.Fatalf("request cancellation stopped batch: %+v", got)
	}
}
func TestBatchAnalysisEligibilityAndMembershipChange(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 3, 1)
	batchFixture(t, s, "batch", "two", "completed", 2, 3, 1)
	batchFixture(t, s, "batch", "three", "running", 3, 3, 1)
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); !errors.Is(err, errResultBusy) {
		t.Fatalf("active batch admitted: %v", err)
	}
	s.state.mu.Lock()
	delete(s.state.experiments, "three")
	s.state.mu.Unlock()
	batchFixture(t, s, "batch", "three", "completed", 3, 3, 1)
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	first := awaitBatch(t, s, "batch")
	if err := s.deleteSavedResult("one"); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(s, http.MethodGet, "/api/v1/batch-analysis-jobs/batch")
	var status batchAnalysisStatus
	if json.Unmarshal(response.Body.Bytes(), &status) != nil || status.State != "idle" {
		t.Fatalf("stale result not invalidated: %s", response.Body)
	}
	next, err := s.startBatchAnalysis(context.Background(), "batch", false)
	if err != nil || next.ID == first.ID {
		t.Fatalf("membership refresh: %v", err)
	}
	if got := awaitBatch(t, s, "batch"); got.State != "completed" || got.TotalRuns != 2 {
		t.Fatalf("membership selection: %+v", got)
	}
}
func TestBatchAnalysisHandlesMoreThanIndividualQueueLimit(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for i := 1; i <= 33; i++ {
		batchFixture(t, s, "batch", fmt.Sprintf("run-%02d", i), "completed", i, 33, 0)
	}
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if got := awaitBatch(t, s, "batch"); got.State != "completed" || got.CompletedRuns != 33 {
		t.Fatalf("large batch: %+v", got)
	}
}
func TestBatchAnalysisAuthShutdownAndInterruption(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 2, 0)
	batchFixture(t, s, "batch", "two", "completed", 2, 2, 0)
	if response := resultRequest(s, http.MethodPost, "/api/v1/batch-analysis-jobs/batch"); response.Code != 401 {
		t.Fatalf("unauthorized start: %d", response.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.analysisSlots <- struct{}{}
	if _, err := s.startBatchAnalysis(ctx, "batch", false); err != nil {
		t.Fatal(err)
	}
	cancel()
	s.analysisWorkers.Wait()
	<-s.analysisSlots
	if got := awaitBatch(t, s, "batch"); got.State != "interrupted" {
		t.Fatalf("interruption: %+v", got)
	}
	restarted := New(s.config, nil)
	if _, err := restarted.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if got := awaitBatch(t, restarted, "batch"); got.State != "completed" {
		t.Fatalf("retry: %+v", got)
	}
	restarted.cancelMu.Lock()
	restarted.shuttingDown = true
	restarted.cancelMu.Unlock()
	if _, err := restarted.startBatchAnalysis(context.Background(), "batch", true); err == nil {
		t.Fatal("admitted during shutdown")
	}
}
func TestBatchAnalysisFailureIsExplicitAndRetryable(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 2, 0)
	batchFixture(t, s, "batch", "two", "completed", 2, 2, 0)
	filename := filepath.Join(s.config.DataDir, currentRunsDirectory, "two", "events.jsonl")
	if err := os.WriteFile(filename, []byte("invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if got := awaitBatch(t, s, "batch"); got.State != "failed" || got.Error == "" {
		t.Fatalf("partial batch reported success: %+v", got)
	}
	if err := os.WriteFile(filename, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if awaitBatch(t, s, "batch").State != "completed" {
		t.Fatal("retry failed")
	}
}
func TestBatchSummaryUsesEqualRunWeightAndExcludesUndefinedValues(t *testing.T) {
	a := resultAnalysis{Metrics: model.Metrics{LatencySamples: 1, AverageLatencyMS: 10, DeliveryRatioAvailable: true, Reachability: .2, Bandwidth: &model.BandwidthSummary{Samples: 1, SentBytes: 100}}, Research: &researchAnalysis{Summary: map[string]analysisStatistic{"frt": {Average: numberPointer(1)}}}}
	b := resultAnalysis{Metrics: model.Metrics{LatencySamples: 99, AverageLatencyMS: 30, DeliveryRatioAvailable: true, Reachability: .8}, Research: &researchAnalysis{Summary: map[string]analysisStatistic{"frt": {Average: numberPointer(3)}}}}
	c := resultAnalysis{Metrics: model.Metrics{AverageLatencyMS: 1000, Reachability: 0}}
	summary := batchSummary([]resultAnalysis{a, b, c})
	for key, want := range map[string]float64{"metrics.averageLatencyMs": 20, "metrics.reachability": .5, "research.frt": 2, "bandwidth.sentBytes": 100} {
		if got := summary[key]; got.Average == nil || math.Abs(*got.Average-want) > 1e-9 {
			t.Fatalf("%s: %+v", key, got)
		}
	}
	if summary["metrics.averageLatencyMs"].Count != 2 || summary["bandwidth.sentBytes"].Count != 1 || summary["bandwidth.sentBytes"].Deviation != nil {
		t.Fatal("invalid n or single-run SD")
	}
}

func TestBatchCompactionDistinguishesZeroReceiptsFromMissingTiming(t *testing.T) {
	at := time.Unix(1000, 0)
	zero := resultAnalysis{Result: savedResult{StartedAt: at}, Research: &researchAnalysis{EligiblePopulation: 1, Messages: []researchMessage{{At: at, Metrics: map[string]analysisStatistic{}}}}}
	compactBatchAnalysis(&zero)
	if got := zero.Research.Overview.ReceiversTime; len(got) != 1 || got[0].Y != 0 {
		t.Fatal("zero receipts were excluded from the batch")
	}
	unknown := resultAnalysis{Result: savedResult{StartedAt: at}, Research: &researchAnalysis{EligiblePopulation: 1, Messages: []researchMessage{{At: at, Nodes: []researchNode{{ID: "receiver", Source: "unknown"}}, Metrics: map[string]analysisStatistic{}}}}}
	compactBatchAnalysis(&unknown)
	if len(unknown.Research.Overview.ReceiversTime) != 0 || len(unknown.Research.Overview.ReceiversHop) != 0 {
		t.Fatal("missing time/hop evidence became zero")
	}
}

func TestBatchMembershipIgnoresDisplayCacheAndOrder(t *testing.T) {
	a := savedResult{ID: "a", BatchID: "batch", State: "completed", Iteration: 1, Repetitions: 2}
	b := a
	b.ID = "b"
	b.Iteration = 2
	before := batchMembership([]savedResult{a, b})
	bytes := int64(100)
	a.DownloadBytes = &bytes
	a.Analysis = &analysisJobStatus{State: "completed"}
	a.BatchAnalysis = &batchAnalysisStatus{}
	if batchMembership([]savedResult{b, a}) != before {
		t.Fatal("UI cache invalidated batch membership")
	}
	b.State = "failed"
	if batchMembership([]savedResult{a, b}) == before {
		t.Fatal("changed result state was ignored")
	}
}

func TestBatchRejectsIncompatibleMetricDefinitionsWithoutRejectingMissingEvidence(t *testing.T) {
	a := resultAnalysis{Metrics: model.Metrics{Definition: "dispatch-cohort-v1", LatencySamples: 1}}
	b := resultAnalysis{Metrics: model.Metrics{Definition: sessionWindowDefinition, LatencySamples: 1}}
	if validateBatchDefinitions([]resultAnalysis{a, b}) == nil {
		t.Fatal("averaged incompatible measurement populations")
	}
	b.Metrics.LatencySamples = 0
	if err := validateBatchDefinitions([]resultAnalysis{a, b}); err != nil {
		t.Fatal("missing evidence should be excluded at metric level:", err)
	}
}
