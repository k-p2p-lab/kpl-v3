package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const appendScenario = "version: 1\nname: extended-group\nseed: 42\nphases:\n - action: wait\n   duration: 1ms\n"

func completedAppendBatch(t *testing.T, s *Server, count int) []model.Experiment {
	t.Helper()
	if _, err := s.StartScenarioRepeated(context.Background(), []byte(appendScenario), count); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, s)
	if len(runs) != count {
		t.Fatalf("expected %d runs, got %d", count, len(runs))
	}
	for _, run := range runs {
		if run.State != "completed" {
			t.Fatalf("initial run: %+v", run)
		}
	}
	return runs
}
func appendPublished(t *testing.T, s *Server, id string, count int) {
	t.Helper()
	events := []model.TraceEvent{}
	for i := 0; i < count; i++ {
		events = append(events, model.TraceEvent{RunID: id, EventID: fmt.Sprintf("publication-%d", i), NodeID: "publisher", MessageID: fmt.Sprintf("message-%d", i), Type: "publish", Topic: "topic", Timestamp: time.Now().UTC()})
	}
	if err := s.state.persistEvents(id, events); err != nil {
		t.Fatal(err)
	}
}
func appendRequest(s *Server, ctx context.Context, id, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	s.apiTestHandler(ctx).ServeHTTP(r, httptest.NewRequest("POST", "/api/v1/result-batches/"+id+"/append", strings.NewReader(body)))
	return r
}

func TestAppendTenCompletedRunsIncludesAllTwentyInStatistics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	original := completedAppendBatch(t, s, 10)
	id := original[0].BatchID
	before := map[string][]byte{}
	for _, run := range original {
		appendPublished(t, s, run.ID, 1)
		data, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
		if err != nil {
			t.Fatal(err)
		}
		before[run.ID] = data
	}
	putGroupNote(t, s, id, "Whole group observation", "0")
	putNote(t, s, original[0].ID, "Original run observation", "0")
	if _, err := s.startBatchAnalysis(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	old := awaitBatch(t, s, id)
	if old.State != "completed" || old.TotalRuns != 10 {
		t.Fatalf("old statistics: %+v", old)
	}
	for _, run := range original {
		archiveTestRun(t, s, run.ID)
	}
	restarted := New(s.config, nil)
	restarted.archiveIOCheck = func() { t.Fatal("append waited on NAS") }
	first, err := restarted.AppendScenarioBatch(context.Background(), id, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if first.Iteration != 11 || first.Repetitions != 20 || first.BatchID != id || first.Seed != 42 {
		t.Fatalf("extension: %+v", first)
	}
	added := waitRepetitions(t, restarted)
	if len(added) != 10 {
		t.Fatalf("reran old members: %d", len(added))
	}
	for i, run := range added {
		if run.State != "completed" || run.Iteration != i+11 || run.Repetitions != 20 {
			t.Fatalf("appended run: %+v", run)
		}
		appendPublished(t, restarted, run.ID, 3)
	}
	if _, err := restarted.AppendScenarioBatch(context.Background(), id, 10, 10); !errors.Is(err, errBatchNotAppendable) {
		t.Fatalf("duplicate admission: %v", err)
	}
	response := resultRequest(restarted, "GET", "/api/v1/results")
	var listed []savedResult
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &listed) != nil || len(listed) != 20 {
		t.Fatalf("list: %d %s", response.Code, response.Body)
	}
	for _, run := range listed {
		if run.Repetitions != 20 || run.BatchID != id || run.BatchAnalysis != nil && run.BatchAnalysis.State == "completed" {
			t.Fatalf("old group size or cached mean reused: %+v", run)
		}
	}
	for _, run := range original {
		data, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
		if err != nil || !bytes.Equal(data, before[run.ID]) {
			t.Fatal("existing experiment data was rewritten")
		}
	}
	if note := groupNoteRequest(restarted, "GET", id, ""); !strings.Contains(note.Body.String(), "Whole group observation") {
		t.Fatal("group note lost")
	}
	if note := noteRequest(restarted, "GET", original[0].ID, ""); !strings.Contains(note.Body.String(), "Original run observation") {
		t.Fatal("original note lost")
	}
	restarted.archiveIOCheck = nil
	job, err := restarted.startBatchAnalysis(context.Background(), id, false)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID == old.ID {
		t.Fatal("reused pre-extension analysis")
	}
	job = awaitBatch(t, restarted, id)
	if job.State != "completed" || job.TotalRuns != 20 || job.ExpectedRuns != 20 {
		t.Fatalf("new statistics: %+v", job)
	}
	var result batchAnalysisResult
	response = resultRequest(restarted, "GET", job.ResultURL)
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("artifact: %d %s", response.Code, response.Body)
	}
	stat := result.Summary["metrics.published"]
	if len(result.Runs) != 20 || result.ExpectedRuns != 20 || result.MissingRuns != 0 || stat.Count != 20 || stat.Average == nil || *stat.Average != 2 || stat.Deviation == nil || math.Abs(*stat.Deviation-math.Sqrt(20.0/19)) > 1e-9 {
		t.Fatalf("wrong combined mean: runs=%d expected=%d statistic=%+v", len(result.Runs), result.ExpectedRuns, stat)
	}
	for _, run := range result.Runs {
		if run.Result.Repetitions != 20 {
			t.Fatal("analysis retained the old group total")
		}
	}
	again := New(s.config, nil)
	last, err := again.AppendScenarioBatch(context.Background(), id, 1, 20)
	if err != nil {
		t.Fatal(err)
	}
	if last.Iteration != 21 || last.Repetitions != 21 {
		t.Fatalf("second extension: %+v", last)
	}
	waitRepetitions(t, again)
}

func TestAppendAdmissionIsExclusiveAndKeepsRunsAfterRequestCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	runs := completedAppendBatch(t, s, 2)
	id := runs[0].BatchID
	var wg sync.WaitGroup
	responses := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- appendRequest(s, context.Background(), id, `{"additionalRuns":2,"expectedRepetitions":2}`).Code
		}()
	}
	wg.Wait()
	close(responses)
	codes := map[int]int{}
	for code := range responses {
		codes[code]++
	}
	if codes[202] != 1 || codes[409] != 1 {
		t.Fatalf("duplicate concurrent admission: %v", codes)
	}
	waitRepetitions(t, s)
	requestCtx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest("POST", "/api/v1/result-batches/"+id+"/append", strings.NewReader(`{"additionalRuns":2,"expectedRepetitions":4}`)).WithContext(requestCtx)
	response := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(response, request)
	cancel()
	if response.Code != 202 {
		t.Fatalf("append: %d %s", response.Code, response.Body)
	}
	all := waitRepetitions(t, s)
	if len(all) != 6 {
		t.Fatalf("request cancellation lost additions: %d", len(all))
	}
	for _, run := range all {
		if run.State != "completed" || run.Repetitions != 6 {
			t.Fatalf("stale live total: %+v", run)
		}
	}
}

func TestAppendCleanupFailureCanRetryOnlyNewIterations(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), 2); err != nil {
		t.Fatal(err)
	}
	original := waitRepetitions(t, f.server)
	if len(original) != 2 || original[1].State != "completed" {
		t.Fatalf("initial batch: %+v", original)
	}
	f.agent.mu.Lock()
	f.agent.fenceStatus = 500
	f.agent.mu.Unlock()
	first, err := f.server.AppendScenarioBatch(context.Background(), original[0].BatchID, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	if got := persistedExperiment(t, f.server, first.ID); got.State != "canceled" || !got.StartedAt.IsZero() {
		t.Fatalf("cleanup failure started a new run: %+v", got)
	}
	if f.createCount.Load() != 2 {
		t.Fatal("created peers before cleanup completed")
	}
	f.agent.mu.Lock()
	f.agent.fenceStatus = 0
	f.agent.mu.Unlock()
	if _, err := f.server.RetryScenarioBatch(context.Background(), original[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	members, err := f.server.batchMembers(context.Background(), original[0].BatchID)
	if err != nil || len(members) != 4 {
		t.Fatalf("membership: %d %v", len(members), err)
	}
	for _, run := range members {
		if run.State != "completed" || run.Repetitions != 4 {
			t.Fatalf("retry: %+v", run)
		}
	}
	if f.createCount.Load() != 4 {
		t.Fatal("retry repeated previously completed iterations")
	}
}

func TestAppendRecoversUncommittedReservationsAndPreservesCommittedRuns(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint("committed-", committed), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			original := completedAppendBatch(t, s, 2)
			id := original[0].BatchID
			putGroupNote(t, s, id, "Keep this note", "0")
			pending := model.Experiment{ID: "pending-third", BatchID: id, Iteration: 3, Repetitions: 3, Name: "extended-group", State: "queued", Seed: 42, TotalPhases: 1}
			if err := s.persistManifest(pending, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			record := batchExtension{Version: 1, BatchID: id, Repetitions: 2, Pending: []string{pending.ID}}
			if committed {
				record.Repetitions = 3
				record.Pending = nil
			}
			root, err := s.resultGroupDirectory(id, true)
			if err != nil {
				t.Fatal(err)
			}
			err = writeAnalysisJSON(root, batchExtensionFile, record)
			root.Close()
			if err != nil {
				t.Fatal(err)
			}
			restarted := New(s.config, nil)
			before := resultRequest(restarted, "GET", "/api/v1/results")
			if !committed && strings.Contains(before.Body.String(), pending.ID) {
				t.Fatal("uncommitted reservation was visible")
			}
			if err := restarted.recoverBatchExtensions(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, pending.ID))
			if committed && err != nil || !committed && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("wrong recovery boundary: %v", err)
			}
			if note := groupNoteRequest(restarted, "GET", id, ""); !strings.Contains(note.Body.String(), "Keep this note") {
				t.Fatal("recovery removed group note")
			}
			if committed {
				if _, err := restarted.RetryScenarioBatch(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := restarted.AppendScenarioBatch(context.Background(), id, 1, 2); err != nil {
					t.Fatal(err)
				}
			}
			waitRepetitions(t, restarted)
			members, err := restarted.batchMembers(context.Background(), id)
			if err != nil || len(members) != 3 {
				t.Fatalf("recovered group: %d %v", len(members), err)
			}
			for _, run := range members {
				if run.State != "completed" || run.Repetitions != 3 {
					t.Fatalf("recovery result: %+v", run)
				}
			}
		})
	}
}

func TestAppendValidationAndAdmissionConflicts(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	runs := completedAppendBatch(t, s, 2)
	id := runs[0].BatchID
	for _, body := range []string{`{}`, `{"additionalRuns":0,"expectedRepetitions":2}`, `{"additionalRuns":99,"expectedRepetitions":2}`, `{"additionalRuns":1.5,"expectedRepetitions":2}`, `{"additionalRuns":1,"expectedRepetitions":2,"extra":1}`} {
		if r := appendRequest(s, context.Background(), id, body); r.Code != 400 {
			t.Fatalf("invalid append accepted: %s: %d", body, r.Code)
		}
	}
	if r := appendRequest(s, context.Background(), "missing", `{"additionalRuns":1,"expectedRepetitions":2}`); r.Code != 404 {
		t.Fatalf("unknown group: %d", r.Code)
	}
	if r := appendRequest(s, context.Background(), id, `{"additionalRuns":1,"expectedRepetitions":1}`); r.Code != 409 {
		t.Fatalf("stale size accepted: %d", r.Code)
	}
	s.config.RunMinFreeBytes = math.MaxUint64
	if r := appendRequest(s, context.Background(), id, `{"additionalRuns":1,"expectedRepetitions":2}`); r.Code != 507 {
		t.Fatalf("full storage accepted: %d %s", r.Code, r.Body)
	}
	s.config.RunMinFreeBytes = 0
	snapshot, err := s.captureResult(runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if r := appendRequest(s, context.Background(), id, `{"additionalRuns":1,"expectedRepetitions":2}`); r.Code != 409 {
		t.Fatalf("download conflict: %d", r.Code)
	}
	snapshot.close()
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, runs[1].ID, "scenario.yaml"), []byte(appendScenario+"# different\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if r := appendRequest(s, context.Background(), id, `{"additionalRuns":1,"expectedRepetitions":2}`); r.Code != 409 {
		t.Fatalf("mixed scenarios accepted: %d", r.Code)
	}
}

func TestAppendRequiresBrowserSessionAndCSRF(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	runs := completedAppendBatch(t, s, 2)
	id := runs[0].BatchID
	body := `{"additionalRuns":1,"expectedRepetitions":2}`
	if r := appendRequest(s, context.Background(), id, body); r.Code != 401 {
		t.Fatalf("unauthenticated append: %d", r.Code)
	}
	cookie := loginCookie(t, s)
	for _, header := range []bool{false, true} {
		request := httptest.NewRequest("POST", "/api/v1/result-batches/"+id+"/append", strings.NewReader(body))
		request.AddCookie(cookie)
		if header {
			request.Header.Set("X-KPL-Request", "dashboard")
		}
		response := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(response, request)
		want := 403
		if header {
			want = 202
		}
		if response.Code != want {
			t.Fatalf("CSRF=%v: %d %s", header, response.Code, response.Body)
		}
	}
	waitRepetitions(t, s)
}
