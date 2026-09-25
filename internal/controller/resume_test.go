package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const resumeScenario = "name: resume-test\nseed: 42\njobShutdownTimeout: 1s\nphases:\n  - action: join\n    group: workers\n    count: 1\n"

type resumeFixture struct {
	server      *Server
	agent       *lifecycleTestAgent
	api         *httptest.Server
	fail        atomic.Bool
	createCount atomic.Int32
	failAt      atomic.Int32
	creates     chan string
}

func newResumeFixture(t *testing.T) *resumeFixture {
	t.Helper()
	f := &resumeFixture{agent: &lifecycleTestAgent{firstCreate: make(chan struct{}), nodes: make(map[string]model.Node)}, creates: make(chan string, 100)}
	f.fail.Store(true)
	var fenceMu sync.Mutex
	fences := make(map[string]uint64)
	f.api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes" {
			raw, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var request model.CreateNodeRequest
			_ = json.Unmarshal(raw, &request)
			fenceMu.Lock()
			fenced := request.Generation <= fences[request.RunID]
			fenceMu.Unlock()
			if fenced {
				http.Error(w, "run generation is fenced", http.StatusConflict)
				return
			}
			f.creates <- request.RunID
			attempt := f.createCount.Add(1)
			if f.fail.Load() || attempt == f.failAt.Load() {
				http.Error(w, "injected create failure", http.StatusInternalServerError)
				return
			}
		}
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/") {
			runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/nodes")
			generation, _ := strconv.ParseUint(r.URL.Query().Get("generation"), 10, 64)
			f.agent.mu.Lock()
			succeeds := f.agent.fenceStatus == 0
			f.agent.mu.Unlock()
			if succeeds {
				fenceMu.Lock()
				fences[runID] = max(fences[runID], generation)
				fenceMu.Unlock()
			}
		}
		f.agent.serveHTTP(w, r)
	}))
	t.Cleanup(f.api.Close)
	f.server = newLifecycleTestController(t)
	f.register(t, f.server)
	return f
}

func (f *resumeFixture) register(t *testing.T, server *Server) {
	t.Helper()
	if _, err := server.state.registerAgent(model.Agent{ID: "agent-1", URL: f.api.URL, Capacity: 100}); err != nil {
		t.Fatal(err)
	}
}

func (f *resumeFixture) failedBatch(t *testing.T, count int) []model.Experiment {
	t.Helper()
	if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), count); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	if len(runs) != count || runs[0].State != "failed" {
		t.Fatalf("missing initial failure: %+v", runs)
	}
	for _, run := range runs[1:] {
		if run.State != "canceled" || !run.StartedAt.IsZero() {
			t.Fatalf("unexpected attempted run: %+v", run)
		}
	}
	return runs
}

func persistedExperiment(t *testing.T, server *Server, id string) model.Experiment {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(server.config.DataDir, currentRunsDirectory, id, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run model.Experiment
	if err := json.Unmarshal(raw, &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func TestResumePreservesFailedResultAndRunsOnlyRemainingIterations(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-controller"
		if restart {
			name = "after-restart"
		}
		t.Run(name, func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			failedPath := filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[0].ID, "experiment.json")
			before, _ := os.ReadFile(failedPath)
			server := f.server
			if restart {
				server = New(f.server.config, f.server.logger)
				f.register(t, server)
			}
			f.fail.Store(false)
			first, err := server.ResumeScenarioBatch(context.Background(), runs[0].BatchID)
			if err != nil {
				t.Fatal(err)
			}
			if first.ID != runs[1].ID || first.Iteration != 2 || first.Repetitions != 3 || first.State != "queued" {
				t.Fatalf("wrong continuation: %+v", first)
			}
			waitRepetitions(t, server)
			after, _ := os.ReadFile(failedPath)
			if !bytes.Equal(before, after) {
				t.Fatal("failed result was overwritten")
			}
			for _, original := range runs[1:] {
				run := persistedExperiment(t, server, original.ID)
				if run.State != "completed" || run.Iteration != original.Iteration || run.Repetitions != 3 || run.BatchID != original.BatchID || run.Seed != original.Seed || run.StartedAt.IsZero() || run.Error != "" {
					t.Fatalf("remaining run lost identity or failed: %+v", run)
				}
			}
			for _, run := range runs {
				if got := <-f.creates; got != run.ID {
					t.Fatalf("created for %s, want %s", got, run.ID)
				}
			}
			if len(f.creates) != 0 {
				t.Fatal("an attempted run was repeated")
			}
			server.cancelMu.Lock()
			owned := len(server.repeatBatches) + len(server.cancels)
			server.cancelMu.Unlock()
			if owned != 0 {
				t.Fatal("resume scheduler leaked batch ownership")
			}
		})
	}
}

func TestResumeRetriesCleanupBeforeAnyNewPeerAndPreservesPendingOnFailure(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	<-f.creates
	f.fail.Store(false)
	f.agent.mu.Lock()
	f.agent.fenceStatus = http.StatusInternalServerError
	f.agent.mu.Unlock()
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if len(f.creates) != 0 {
		t.Fatal("created a Peer before previous cleanup succeeded")
	}
	for _, original := range runs[1:] {
		run := persistedExperiment(t, f.server, original.ID)
		if run.State != "canceled" || !run.StartedAt.IsZero() || !strings.Contains(run.Error, "cleanup previous run") {
			t.Fatalf("lost retryable remainder: %+v", run)
		}
	}
	f.agent.mu.Lock()
	f.agent.fenceStatus = 0
	f.agent.mu.Unlock()
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if got := persistedExperiment(t, f.server, runs[2].ID); got.State != "completed" {
		t.Fatalf("cleanup retry did not complete: %+v", got)
	}
}

func TestResumeSkipsEachNewFailureAndKeepsOriginalIterationNumbers(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if got := persistedExperiment(t, f.server, runs[1].ID); got.State != "failed" {
		t.Fatalf("expected second failure: %+v", got)
	}
	f.fail.Store(false)
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if got := persistedExperiment(t, f.server, runs[2].ID); got.State != "completed" || got.Iteration != 3 {
		t.Fatalf("last run did not complete: %+v", got)
	}
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) {
		t.Fatalf("completed batch resumed: %v", err)
	}
	for _, run := range runs {
		if got := <-f.creates; got != run.ID {
			t.Fatalf("unexpected attempt %s, want %s", got, run.ID)
		}
	}
}

func TestResumeAPIRejectsDuplicatesAndCanStopDuringCleanup(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	<-f.creates
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	blocking := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		f.agent.serveHTTP(w, r)
	}))
	defer blocking.Close()
	defer close(release)
	if _, err := f.server.state.registerAgent(model.Agent{ID: "agent-1", URL: blocking.URL, Capacity: 100}); err != nil {
		t.Fatal(err)
	}
	handler := f.server.apiTestHandler(context.Background())
	post := func(ctx context.Context) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/result-batches/"+runs[0].BatchID+"/resume", nil).WithContext(ctx)
		authenticateRequest(t, f.server, request)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	upload, cancel := context.WithCancel(context.Background())
	first := post(upload)
	cancel()
	if first.Code != http.StatusAccepted {
		t.Fatalf("resume: %d %s", first.Code, first.Body.String())
	}
	<-entered
	if duplicate := post(context.Background()); duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate: %d %s", duplicate.Code, duplicate.Body.String())
	}
	if err := f.server.deleteSavedResult(runs[0].ID); !errors.Is(err, errResultBusy) {
		t.Fatalf("cleanup source not protected: %v", err)
	}
	if err := f.server.StopScenario(runs[0].ID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if len(f.creates) != 0 {
		t.Fatal("stop during cleanup still created a Peer")
	}
	for _, original := range runs[1:] {
		if got := persistedExperiment(t, f.server, original.ID); !got.StartedAt.IsZero() || got.State != "canceled" {
			t.Fatalf("canceled resume executed: %+v", got)
		}
	}
}

func TestResumeRejectsInvalidRemainderWithoutChangingEarlierMembers(t *testing.T) {
	for _, file := range []string{"events.jsonl", "observations.jsonl", "scenario.yaml"} {
		t.Run(file, func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			target := filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[2].ID, file)
			if err := os.WriteFile(target, []byte("unexpected prior data"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) {
				t.Fatalf("invalid remainder accepted: %v", err)
			}
			for _, run := range runs[1:] {
				if got := persistedExperiment(t, f.server, run.ID); got.State != "canceled" {
					t.Fatalf("partial resume admission: %+v", got)
				}
			}
		})
	}
}

func TestResumePreservesSuccessfulRunsBeforeTheFailure(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	f.failAt.Store(2)
	if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), 3); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	if runs[0].State != "completed" || runs[1].State != "failed" || runs[2].State != "canceled" {
		t.Fatalf("unexpected initial batch: %+v", runs)
	}
	before := make(map[string][]byte)
	for _, run := range runs[:2] {
		raw, err := os.ReadFile(filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
		if err != nil {
			t.Fatal(err)
		}
		before[run.ID] = raw
	}
	resumed, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != runs[2].ID || resumed.Iteration != 3 {
		t.Fatalf("wrong remainder: %+v", resumed)
	}
	waitRepetitions(t, f.server)
	for id, original := range before {
		raw, err := os.ReadFile(filepath.Join(f.server.config.DataDir, currentRunsDirectory, id, "experiment.json"))
		if err != nil || !bytes.Equal(raw, original) {
			t.Fatalf("attempted result changed: %s %v", id, err)
		}
	}
	if got := persistedExperiment(t, f.server, runs[2].ID); got.State != "completed" {
		t.Fatalf("last run failed: %+v", got)
	}
	if got := f.createCount.Load(); got != 3 {
		t.Fatalf("created %d times for a three-run batch", got)
	}
}

func TestResumeRejectsBusyResultsWithoutRequeueing(t *testing.T) {
	for _, busy := range []string{"download", "run-analysis", "batch-analysis"} {
		t.Run(busy, func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			switch busy {
			case "download":
				f.server.resultDownloads = map[string]int{runs[0].ID: 1}
			case "run-analysis":
				f.server.analysisJobs[runs[1].ID] = &analysisJob{status: analysisJobStatus{State: "running"}}
			case "batch-analysis":
				f.server.batchAnalysisJobs = map[string]*batchAnalysisJob{runs[0].BatchID: {status: batchAnalysisStatus{analysisJobStatus: analysisJobStatus{State: "queued"}}}}
			}
			if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); !errors.Is(err, errResultBusy) {
				t.Fatalf("busy batch admitted: %v", err)
			}
			if got := persistedExperiment(t, f.server, runs[1].ID); got.State != "canceled" {
				t.Fatalf("busy request changed state: %+v", got)
			}
		})
	}
}

func TestResumeRequiresFailureAndRejectsShutdown(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	f.server.shuttingDown = true
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); err == nil {
		t.Fatal("admitted during shutdown")
	}
	f.server.shuttingDown = false
	f.server.updateExperiment(runs[0].ID, func(run *model.Experiment) { run.State = "canceled"; run.Error = "" })
	if _, err := f.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) {
		t.Fatalf("resumed deliberate stop without failure: %v", err)
	}
	if _, err := f.server.ResumeScenarioBatch(context.Background(), "../runs"); !errors.Is(err, errResultNotFound) {
		t.Fatalf("accepted path as batch ID: %v", err)
	}
}
