package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func retryMembers(t *testing.T, server *Server, batchID string) []savedResult {
	t.Helper()
	waitRepetitions(t, server)
	members, err := server.batchMembers(context.Background(), batchID)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
	return members
}

func TestRetryRestartsFailedIterationAndPreservesCompletedResults(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-controller", true: "after-restart"}[restart], func(t *testing.T) {
			f := newResumeFixture(t)
			f.fail.Store(false)
			f.failAt.Store(2)
			if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), 3); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, f.server)
			if runs[0].State != "completed" || runs[1].State != "failed" {
				t.Fatalf("initial batch: %+v", runs)
			}
			before := map[string][]byte{}
			for _, run := range runs {
				p := filepath.Join(f.server.config.DataDir, "runs", run.ID, "experiment.json")
				before[p], _ = os.ReadFile(p)
			}
			// A failed attempt's partial log must never enter the new attempt
			// or prevent analyzing the completed retry.
			oldLog := filepath.Join(f.server.config.DataDir, "runs", runs[1].ID, "events.jsonl")
			if err := os.WriteFile(oldLog, []byte("partial old failure log\n"), 0600); err != nil {
				t.Fatal(err)
			}
			server := f.server
			if restart {
				server = New(server.config, server.logger)
				f.register(t, server)
			}
			first, err := server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
			if err != nil {
				t.Fatal(err)
			}
			if first.ID == runs[1].ID || first.Iteration != 2 || first.BatchID != runs[0].BatchID || first.Repetitions != 3 || first.Seed != runs[1].Seed || !reflect.DeepEqual(first.PreviousRunIDs, []string{runs[1].ID}) {
				t.Fatalf("incorrect retry identity: %+v", first)
			}
			current := retryMembers(t, server, first.BatchID)
			if len(current) != 3 || current[0].ID != runs[0].ID {
				t.Fatalf("lost completed iteration: %+v", current)
			}
			for i, member := range current {
				if member.State != "completed" || member.Iteration != i+1 {
					t.Fatalf("retry incomplete: %+v", member)
				}
				if i > 0 && member.ID == runs[i].ID {
					t.Fatal("reused a fenced run ID")
				}
				if got := persistedExperiment(t, server, member.ID); got.Seed != runs[i].Seed {
					t.Fatalf("changed seed: %+v", got)
				}
			}
			for p, original := range before {
				got, err := os.ReadFile(p)
				if err != nil || !bytes.Equal(original, got) {
					t.Fatalf("changed prior result %s: %v", p, err)
				}
			}
			if got := f.createCount.Load(); got != 4 {
				t.Fatalf("created %d times; want two initial attempts plus two retries", got)
			}
			all, err := server.allBatchMembers(context.Background(), first.BatchID)
			if err != nil || len(all) != 5 {
				t.Fatalf("missing attempt history: %+v %v", all, err)
			}
			status, err := server.startBatchAnalysis(context.Background(), first.BatchID, false)
			if err != nil {
				t.Fatal(err)
			}
			server.analysisWorkers.Wait()
			if status.ExpectedRuns != 3 || status.TotalRuns != 3 {
				t.Fatalf("history inflated batch mean: %+v", status)
			}
			status, err = server.batchAnalysisStatus(first.BatchID)
			if err != nil || status.State != "completed" {
				t.Fatalf("retry batch mean: %+v %v", status, err)
			}
			deleted, err := server.deleteSavedBatch(context.Background(), first.BatchID)
			if err != nil || len(deleted) != 5 {
				t.Fatalf("group deletion omitted attempt history: %v %v", deleted, err)
			}
		})
	}
}

func TestRetryAcceptsInterruptedAndCanceledBatches(t *testing.T) {
	for _, stored := range []string{"running", "queued", "canceled"} {
		t.Run(stored, func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			f.server.updateExperiment(runs[0].ID, func(run *model.Experiment) {
				run.State = stored
				if stored == "queued" {
					run.StartedAt = time.Time{}
				}
			})
			server := f.server
			if stored != "canceled" {
				server = New(server.config, server.logger)
				f.register(t, server)
			}
			f.fail.Store(false)
			first, err := server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
			if err != nil {
				t.Fatal(err)
			}
			if first.Iteration != 1 {
				t.Fatalf("skipped interrupted/canceled attempt: %+v", first)
			}
			for _, member := range retryMembers(t, server, first.BatchID) {
				if member.State != "completed" {
					t.Fatalf("incomplete retry: %+v", member)
				}
			}
		})
	}
}

func TestRetryIncludesSingleRunAndLastFailedIteration(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(map[int]string{1: "single", 3: "last-iteration"}[count], func(t *testing.T) {
			f := newResumeFixture(t)
			f.fail.Store(false)
			f.failAt.Store(int32(count))
			if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), count); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, f.server)
			first, err := f.server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
			if err != nil {
				t.Fatal(err)
			}
			if first.Iteration != count {
				t.Fatalf("wrong retry boundary: %+v", first)
			}
			current := retryMembers(t, f.server, first.BatchID)
			if len(current) != count || current[count-1].State != "completed" {
				t.Fatalf("retry failed: %+v", current)
			}
			if f.createCount.Load() != int32(count+1) {
				t.Fatal("completed iterations repeated")
			}
			if _, err := f.server.RetryScenarioBatch(context.Background(), first.BatchID); !errors.Is(err, errBatchNotResumable) {
				t.Fatalf("retried completed batch: %v", err)
			}
		})
	}
}

func TestRetryCleansPreviousPeersBeforeCreatingAndRecoversAfterCleanupFailure(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 2)
	<-f.creates
	f.fail.Store(false)
	f.agent.mu.Lock()
	f.agent.fenceStatus = http.StatusInternalServerError
	f.agent.nodes["leftover"] = model.Node{ID: "leftover", RunID: runs[0].ID, Generation: 17, State: model.NodeReady}
	f.agent.mu.Unlock()
	first, err := f.server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	current := retryMembers(t, f.server, first.BatchID)
	if len(f.creates) != 0 || f.agent.nodeCount() != 1 {
		t.Fatal("created before old Peer cleanup succeeded")
	}
	for _, member := range current {
		run := persistedExperiment(t, f.server, member.ID)
		if run.State != "canceled" || !run.StartedAt.IsZero() || !strings.Contains(run.Error, "cleanup previous run") {
			t.Fatalf("lost retryable cleanup failure: %+v", run)
		}
	}
	// Deleting a previous attempt's saved result must not lose its cleanup ID.
	if err := f.server.deleteSavedResult(runs[0].ID); err != nil {
		t.Fatal(err)
	}
	f.agent.mu.Lock()
	f.agent.fenceStatus = 0
	f.agent.mu.Unlock()
	server := New(f.server.config, f.server.logger)
	f.register(t, server)
	second, err := server.RetryScenarioBatch(context.Background(), first.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	current = retryMembers(t, server, second.BatchID)
	if len(current) != 2 || current[0].State != "completed" || current[1].State != "completed" || f.agent.nodeCount() != 0 {
		t.Fatalf("cleanup/retry failed: %+v", current)
	}
	if !reflect.DeepEqual(second.PreviousRunIDs, []string{runs[0].ID, first.ID}) {
		t.Fatalf("lost cleanup ancestry: %+v", second)
	}
}

func TestRetryAPIDeduplicatesAndAllowsStopDuringCleanup(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 2)
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
	post := func(ctx context.Context, authenticated bool) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/result-batches/"+runs[0].BatchID+"/retry", nil).WithContext(ctx)
		if authenticated {
			authenticateRequest(t, f.server, request)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := post(context.Background(), false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated retry: %d", response.Code)
	}
	upload, cancel := context.WithCancel(context.Background())
	response := post(upload, true)
	cancel()
	if response.Code != http.StatusAccepted {
		t.Fatalf("retry: %d %s", response.Code, response.Body)
	}
	var first model.Experiment
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup never started")
	}
	if response := post(context.Background(), true); response.Code != http.StatusConflict {
		t.Fatalf("duplicate accepted: %d %s", response.Code, response.Body)
	}
	if err := f.server.deleteSavedResult(runs[0].ID); !errors.Is(err, errResultBusy) {
		t.Fatalf("cleanup source was not protected: %v", err)
	}
	if err := f.server.StopScenario(first.ID); err != nil {
		t.Fatal(err)
	}
	for _, member := range retryMembers(t, f.server, first.BatchID) {
		if member.State != "canceled" || !member.StartedAt.IsZero() {
			t.Fatalf("stop started a run: %+v", member)
		}
	}
	if len(f.creates) != 0 {
		t.Fatal("stop during cleanup still created Peers")
	}
}

func TestRetryRejectsBusyOrInvalidBatchBeforeReservation(t *testing.T) {
	for _, fault := range []string{"download", "run-analysis", "batch-analysis", "shutdown", "bad-scenario", "missing-scenario", "duplicate-iteration"} {
		t.Run(fault, func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			switch fault {
			case "download":
				f.server.resultDownloads = map[string]int{runs[0].ID: 1}
			case "run-analysis":
				f.server.analysisJobs[runs[1].ID] = &analysisJob{status: analysisJobStatus{State: "running"}}
			case "batch-analysis":
				f.server.batchAnalysisJobs = map[string]*batchAnalysisJob{runs[0].BatchID: {status: batchAnalysisStatus{analysisJobStatus: analysisJobStatus{State: "queued"}}}}
			case "shutdown":
				f.server.shuttingDown = true
			case "bad-scenario":
				if err := os.WriteFile(filepath.Join(f.server.config.DataDir, "runs", runs[2].ID, "scenario.yaml"), []byte("invalid scenario"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-scenario":
				if err := os.Remove(filepath.Join(f.server.config.DataDir, "runs", runs[2].ID, "scenario.yaml")); err != nil {
					t.Fatal(err)
				}
			case "duplicate-iteration":
				f.server.updateExperiment(runs[2].ID, func(run *model.Experiment) { run.Iteration = 2 })
			}
			if _, err := f.server.RetryScenarioBatch(context.Background(), runs[0].BatchID); err == nil {
				t.Fatal("invalid retry accepted")
			}
			entries, err := os.ReadDir(filepath.Join(f.server.config.DataDir, "runs"))
			if err != nil || len(entries) != 3 {
				t.Fatalf("partial retry reservation: %v %v", entries, err)
			}
			if len(f.creates) != 1 {
				t.Fatal("invalid request created another Peer")
			}
			f.server.shuttingDown = false
		})
	}
}

func TestRetryCanFailAgainThenContinueBySkippingThatAttempt(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	first, err := f.server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	current := retryMembers(t, f.server, first.BatchID)
	if current[0].State != "failed" || current[1].State != "canceled" {
		t.Fatalf("expected repeated failure: %+v", current)
	}
	f.fail.Store(false)
	resumed, err := f.server.ResumeScenarioBatch(context.Background(), first.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != current[1].ID || resumed.Iteration != 2 {
		t.Fatalf("did not reuse the unstarted retry: %+v", resumed)
	}
	current = retryMembers(t, f.server, first.BatchID)
	if len(current) != 3 || current[0].ID != first.ID || current[0].State != "failed" || current[1].State != "completed" || current[2].State != "completed" {
		t.Fatalf("continuation after retry failed: %+v", current)
	}
}
