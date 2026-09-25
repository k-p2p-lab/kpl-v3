package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func lifecycleSaved(t *testing.T, s *Server, id, state string, iteration, total int, previous []string) {
	t.Helper()
	raw := []byte("name: lifecycle\nphases:\n - action: wait\n   duration: 1h\n")
	run := model.Experiment{ID: id, BatchID: "group", Iteration: iteration, Repetitions: total, ExecutionID: "old-execution", Name: "lifecycle", State: state, StartedAt: time.Now().UTC().Add(-time.Hour), FinishedAt: time.Now().UTC(), PreviousRunIDs: previous}
	if err := s.persistManifest(run, raw); err != nil {
		t.Fatal(err)
	}
}
func TestExecutionFenceRejectsLateStopsDuringRetryAndAppend(t *testing.T) {
	for _, action := range []string{"retry", "append"} {
		t.Run(action, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			state := "canceled"
			if action == "append" {
				state = "completed"
			}
			lifecycleSaved(t, s, "old", state, 1, 1, nil)
			var first model.Experiment
			var err error
			if action == "append" {
				first, err = s.AppendScenarioBatch(context.Background(), "group", 1, 1)
			} else {
				first, err = s.RetryScenarioBatch(context.Background(), "group")
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.StopScenario(first.ID); s.runs.Wait() }()
			if err = s.StopScenario("old"); err == nil {
				t.Fatal("old member canceled new execution")
			}
			for _, token := range []string{"", "old-execution"} {
				r := httptest.NewRequest("POST", "/api/v1/experiments/"+first.ID+"/stop", nil)
				r.Header.Set("X-KPL-Execution", token)
				w := httptest.NewRecorder()
				s.apiTestHandler(context.Background()).ServeHTTP(w, r)
				if w.Code != 409 {
					t.Fatalf("stale token accepted: %d %s", w.Code, w.Body)
				}
			}
			if err = s.stopScenarioRequest(first.ID, first.ExecutionID); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, s)
			if len(runs) != 1 || runs[0].State != "canceled" {
				t.Fatalf("new stop: %+v", runs)
			}
		})
	}
}
func TestExecutionFenceProtectsReusedContinuationID(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	lifecycleSaved(t, s, "failed", "failed", 1, 2, nil)
	pending := model.Experiment{ID: "pending", BatchID: "group", Iteration: 2, Repetitions: 2, State: "canceled", ExecutionID: "old-execution"}
	if err := s.persistManifest(pending, []byte("name: lifecycle\nphases:\n - action: wait\n   duration: 1h\n")); err != nil {
		t.Fatal(err)
	}
	first, err := s.ResumeScenarioBatch(context.Background(), "group")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.StopScenario(first.ID); s.runs.Wait() }()
	if first.ID != "pending" || first.ExecutionID == "old-execution" {
		t.Fatal("continuation identity not renewed")
	}
	if err = s.stopScenarioRequest(first.ID, "old-execution"); !errors.Is(err, errExecutionChanged) {
		t.Fatal(err)
	}
}
func TestDeletedLatestAttemptDoesNotRevivePriorFailure(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	lifecycleSaved(t, s, "old", "failed", 1, 2, nil)
	lifecycleSaved(t, s, "new", "completed", 1, 2, []string{"old"})
	lifecycleSaved(t, s, "second", "completed", 2, 2, nil)
	s.state.experiments["old"] = model.Experiment{ID: "old", BatchID: "group", State: "failed", Iteration: 1, Repetitions: 2}
	if err := s.deleteSavedResult("new"); err != nil {
		t.Fatal(err)
	}
	if !s.state.experiments["old"].Superseded {
		t.Fatal("live dashboard revived retired attempt")
	}
	restarted := New(s.config, nil)
	members, err := restarted.batchMembers(context.Background(), "group")
	if err != nil || len(members) != 1 || members[0].ID != "second" {
		t.Fatalf("old attempt revived: %+v %v", members, err)
	}
	record, err := restarted.readBatchExtension("group")
	if err != nil || !record.Retired["old"] || record.Current[1] != "new" || record.Deleted["new"].State != "completed" {
		t.Fatalf("history lost: %+v %v", record, err)
	}
}
func TestLateLogsInvalidateBatchMeanWithoutExplicitRefresh(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	runs := completedAppendBatch(t, s, 2)
	id := runs[0].BatchID
	if _, err := s.startBatchAnalysis(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	old := awaitBatch(t, s, id)
	appendPublished(t, s, runs[0].ID, 4)
	first, err := s.startBatchAnalysis(context.Background(), id, false)
	if err != nil || first.ID == old.ID {
		t.Fatalf("stale mean reused: %+v %v", first, err)
	}
	latest := awaitBatch(t, s, id)
	var result batchAnalysisResult
	r := resultRequest(s, "GET", latest.ResultURL)
	if json.Unmarshal(r.Body.Bytes(), &result) != nil || result.Summary["metrics.published"].Average == nil || *result.Summary["metrics.published"].Average != 2 {
		t.Fatal(r.Body.String())
	}
}
func TestSubmissionReceiptIsExclusiveDurableAndPayloadBound(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	var group sync.WaitGroup
	ids := make(chan string, 5)
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			run, err := s.submitScenario(context.Background(), []byte(appendScenario), 2, "browser-request")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- run.BatchID
		}()
	}
	group.Wait()
	close(ids)
	var id string
	for value := range ids {
		if id != "" && id != value {
			t.Fatal("duplicate batches")
		}
		id = value
	}
	waitRepetitions(t, s)
	restarted := New(s.config, nil)
	run, err := restarted.submitScenario(context.Background(), []byte(appendScenario), 2, "browser-request")
	if err != nil || run.BatchID != id {
		t.Fatalf("restart duplicate: %+v %v", run, err)
	}
	if _, err = restarted.submitScenario(context.Background(), []byte(appendScenario), 3, "browser-request"); !errors.Is(err, errSubmissionConflict) {
		t.Fatal(err)
	}
	if err = restarted.deleteSavedResult(id); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.submitScenario(context.Background(), []byte(appendScenario), 2, "browser-request"); !errors.Is(err, errSubmissionDeleted) {
		t.Fatal(err)
	}
}
func TestPersistedParticipantsFenceAgentsBeforeTheyReregister(t *testing.T) {
	var deletes atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
		}
		w.WriteHeader(204)
	}))
	defer api.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "owned", State: "running"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	if err := s.rememberRunAgent(run.ID, model.Agent{ID: "agent", URL: api.URL, RunDrain: true}); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	if err := restarted.stopRunGeneration(context.Background(), run.ID, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if deletes.Load() != 1 {
		t.Fatal("unregistered owner escaped cleanup")
	}
}
func TestRecordingFailureCancelsRunAndSurvivesMetadataRepair(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, err := s.StartScenario(context.Background(), []byte("name: write-failure\nphases:\n - action: wait\n   duration: 1h\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.StopScenario(run.ID); s.runs.Wait() }()
	s.state.recordRunWriteError(run.ID, errors.New("disk full"))
	runs := waitRepetitions(t, s)
	if runs[0].State != "failed" || runs[0].DataState != "incomplete" {
		t.Fatalf("recording failure hidden: %+v", runs[0])
	}
	s.retryFailedRunWrites()
	saved := persistedExperiment(t, s, run.ID)
	if saved.IntegrityError == "" || saved.State != "failed" {
		t.Fatalf("repair lost failure: %+v", saved)
	}
}
func TestInitialAdmissionCrashRollsBackAndCorruptGroupDoesNotBlockStartup(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	pending := model.Experiment{ID: "initial", BatchID: "initial", Iteration: 1, Repetitions: 2, State: "queued"}
	if err := s.persistManifest(pending, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	root, err := s.resultGroupDirectory("initial", true)
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, batchExtensionFile, batchExtension{Version: 1, BatchID: "initial", Repetitions: 2, PendingTotal: 2, Pending: []string{"initial", "second"}})
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	bad, err := s.resultGroupDirectory("broken", true)
	if err != nil {
		t.Fatal(err)
	}
	err = bad.WriteFile(batchExtensionFile, []byte("broken"), 0600)
	bad.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.recoverBatchExtensions(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "initial")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial initial admission survived")
	}
}

func TestCleanupAndTelemetryFailuresRemainDistinctAndRetriesAreCounted(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var agent model.Agent
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			writeJSON(w, 200, model.AgentHeartbeat{Agent: agent})
			return
		}
		if r.Method == http.MethodPost && fail.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer api.Close()
	agent = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline, RunDrain: true, Capacity: 10, LastSeen: time.Now()}
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	first, err := s.StartScenarioRepeated(context.Background(), []byte(appendScenario), 2)
	if err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, s)
	if runs[0].State != "failed" || runs[0].CleanupState != "complete" || runs[0].DataState != "incomplete" || runs[1].State != "canceled" {
		t.Fatalf("drain failure hidden: %+v", runs)
	}
	fail.Store(false)
	if _, err = s.RetryScenarioBatch(context.Background(), first.BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, s)
	if err = s.deleteSavedResult(first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.startBatchAnalysis(context.Background(), first.BatchID, false); err != nil {
		t.Fatal(err)
	}
	job := awaitBatch(t, s, first.BatchID)
	if job.State != "completed" {
		t.Fatalf("analysis: %+v", job)
	}
	var result batchAnalysisResult
	response := resultRequest(s, "GET", job.ResultURL)
	if json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatal(response.Body.String())
	}
	if result.Reliability.Attempts != 3 || result.Reliability.Failed != 1 || result.Reliability.Retries != 2 || len(result.Runs) != 2 {
		t.Fatalf("attempt history lost: %+v", result.Reliability)
	}
}

func TestActiveRunStorageGuardSignalsCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunMinFreeBytes: ^uint64(0)}, nil)
	s.state.experiments["run"] = model.Experiment{ID: "run", State: "running"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go s.watchRunStorage(ctx, done, "run", cancel)
	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("active storage guard did not cancel")
	}
	s.state.mu.RLock()
	run := s.state.experiments["run"]
	s.state.mu.RUnlock()
	if run.DataState != "incomplete" || run.IntegrityError == "" {
		t.Fatalf("storage failure not recorded: %+v", run)
	}
}

func TestContinuationAdmissionCrashRestoresReusedRunIDs(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	before := []model.Experiment{
		{ID: "pending-one", BatchID: "resume", Iteration: 2, Repetitions: 3, State: "canceled", ExecutionID: "old", Error: "previous cancellation", Seed: 42},
		{ID: "pending-two", BatchID: "resume", Iteration: 3, Repetitions: 3, State: "canceled", ExecutionID: "old", Error: "previous cancellation", Seed: 43},
	}
	for _, run := range before {
		if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := s.resultGroupDirectory("resume", true)
	if err != nil {
		t.Fatal(err)
	}
	err = writeBatchRecord(root, batchExtension{Version: 1, BatchID: "resume", Repetitions: 3, PendingTotal: 3, Pending: []string{"pending-one", "pending-two"}, Before: before})
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Crash after only the first reused manifest has been rewritten.
	rewritten := before[0]
	rewritten.State, rewritten.ExecutionID, rewritten.Error = "queued", "new", ""
	if err := s.persistExperiment(rewritten); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	if err := restarted.recoverBatchExtensions(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, old := range before {
		got := persistedExperiment(t, restarted, old.ID)
		if got.State != old.State || got.ExecutionID != old.ExecutionID || got.Error != old.Error || got.Seed != old.Seed || !got.StartedAt.IsZero() {
			t.Fatalf("rollback lost original: %+v", got)
		}
	}
	record, err := restarted.readBatchExtension("resume")
	if err != nil || len(record.Pending) != 0 || len(record.Before) != 0 {
		t.Fatalf("rollback remained pending: %+v %v", record, err)
	}
}

func TestSingleRunStopAllSeparatesClosedFromRetainedPeers(t *testing.T) {
	s, _ := newLifecycleTestControllerWithAgent(t)
	run, err := s.StartScenario(context.Background(), []byte("name: closed-single\nphases:\n - action: join\n   group: workers\n   count: 1\n - action: stop-all\n"))
	if err != nil {
		t.Fatal(err)
	}
	finished := waitForLifecycleExperiment(t, s, run.ID)
	if finished.State != "completed" || finished.CleanupState != "complete" || finished.DataState != "unverified" {
		t.Fatalf("closed single run: %+v", finished)
	}
}
