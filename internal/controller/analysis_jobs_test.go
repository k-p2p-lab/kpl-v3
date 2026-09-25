package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func jobRequest(t *testing.T, handler http.Handler, method, path string) (*httptest.ResponseRecorder, analysisJobStatus) {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
	var job analysisJobStatus
	if response.Code == 200 || response.Code == 202 {
		if err := json.Unmarshal(response.Body.Bytes(), &job); err != nil {
			t.Fatal(err)
		}
	}
	return response, job
}
func awaitAnalysisJob(t *testing.T, s *Server, id string) analysisJobStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := s.analysisJobStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if status.State != "queued" && status.State != "running" {
			return status
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("analysis job did not complete")
	return analysisJobStatus{}
}

func TestAnalysisJobSurvivesRequestCancellationAndTwoMinuteWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		resultFixture(t, s, "run", "completed", time.Unix(1, 0))
		serverCtx, cancelServer := context.WithCancel(context.Background())
		defer cancelServer()
		handler := s.apiTestHandler(serverCtx)
		s.analysisSlots <- struct{}{}
		requestCtx, cancelRequest := context.WithCancel(context.Background())
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/analysis-jobs/run", nil).WithContext(requestCtx))
		cancelRequest()
		if response.Code != 202 {
			t.Fatalf("start: %d %s", response.Code, response.Body)
		}
		var first analysisJobStatus
		if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
			t.Fatal(err)
		}
		_, second := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run")
		if first.ID == "" || first.ID != second.ID {
			t.Fatal("duplicate POST created another job")
		}
		synctest.Wait()
		time.Sleep(3 * time.Minute)
		_, pending := jobRequest(t, handler, http.MethodGet, "/api/v1/analysis-jobs/run")
		if pending.State != "queued" {
			t.Fatalf("job inherited request timeout: %+v", pending)
		}
		<-s.analysisSlots
		s.analysisWorkers.Wait()
		_, complete := jobRequest(t, handler, http.MethodGet, "/api/v1/analysis-jobs/run")
		if complete.State != "completed" || complete.Progress != 100 || complete.ResultURL == "" || complete.SnapshotAt.IsZero() {
			t.Fatalf("completion: %+v", complete)
		}
	})
}

func TestAnalysisJobArtifactPersistsAndIsReusedUntilExplicitRefresh(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Unix(1, 0))
	handler := s.apiTestHandler(context.Background())
	response, first := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run")
	if response.Code != 202 {
		t.Fatalf("start: %s", response.Body)
	}
	complete := awaitAnalysisJob(t, s, "run")
	if complete.State != "completed" {
		t.Fatalf("analysis: %+v", complete)
	}
	artifact := httptest.NewRecorder()
	handler.ServeHTTP(artifact, httptest.NewRequest(http.MethodGet, complete.ResultURL, nil))
	if artifact.Code != 200 || !strings.Contains(artifact.Header().Get("Content-Disposition"), "run-analysis.json") {
		t.Fatalf("download: %d %s", artifact.Code, artifact.Body)
	}
	var analysis resultAnalysis
	if err := json.Unmarshal(artifact.Body.Bytes(), &analysis); err != nil {
		t.Fatal(err)
	}
	if analysis.AnalysisID != first.ID || analysis.Result.ID != "run" {
		t.Fatal("artifact is from another snapshot")
	}
	s.analysisWorkers.Wait()
	// The saved artifact remains usable even if the source is no longer analyzable.
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "run", "events.jsonl"), []byte("invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	handler = restarted.apiTestHandler(context.Background())
	_, loaded := jobRequest(t, handler, http.MethodGet, "/api/v1/analysis-jobs/run")
	_, reused := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run")
	if loaded.State != "completed" || loaded.ID != first.ID || reused.ID != first.ID {
		t.Fatal("restart or reopen discarded cached analysis")
	}
	downloaded := httptest.NewRecorder()
	handler.ServeHTTP(downloaded, httptest.NewRequest(http.MethodGet, complete.ResultURL, nil))
	if downloaded.Code != 200 || downloaded.Body.String() != artifact.Body.String() {
		t.Fatal("persisted download changed")
	}
	_, refreshed := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run?refresh=1")
	if refreshed.ID == first.ID {
		t.Fatal("explicit refresh reused the old snapshot")
	}
	failed := awaitAnalysisJob(t, restarted, "run")
	if failed.State != "failed" || !strings.Contains(failed.Error, "events.jsonl line 1") {
		t.Fatalf("failed analysis lost error: %+v", failed)
	}
	restarted.analysisWorkers.Wait()
	pendingArtifact := httptest.NewRecorder()
	handler.ServeHTTP(pendingArtifact, httptest.NewRequest(http.MethodGet, complete.ResultURL, nil))
	if pendingArtifact.Code != 409 {
		t.Fatal("download returned an obsolete artifact for a new attempt")
	}
}

func TestAnalysisJobsReportProgressAndDoNotResurrectDeletedResults(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Unix(1, 0))
	line := `{"runId":"run","type":"test","timestamp":"2026-09-09T00:00:00Z"}` + "\n"
	content := strings.Repeat(line, 3000)
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "run", "events.jsonl"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFiles("run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	var read int64
	phases := map[string]bool{}
	ctx := context.WithValue(context.Background(), analysisProgressKey{}, analysisProgress(func(phase string, n int64) { read += n; phases[phase] = true }))
	if _, err := analyzeResult(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if read != int64(len(content)) || !phases["events.jsonl"] || !phases["aggregating"] {
		t.Fatalf("progress bytes=%d phases=%v", read, phases)
	}
	s.analysisSlots <- struct{}{}
	handler := s.apiTestHandler(context.Background())
	response, _ := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run")
	if response.Code != 202 {
		t.Fatalf("start: %s", response.Body)
	}
	listing := httptest.NewRecorder()
	handler.ServeHTTP(listing, httptest.NewRequest(http.MethodGet, "/api/v1/results", nil))
	if !strings.Contains(listing.Body.String(), `"phase":"queued"`) {
		t.Fatal("saved result list omits job progress")
	}
	if err := s.deleteSavedResult("run"); err != nil {
		t.Fatal(err)
	}
	<-s.analysisSlots
	s.analysisWorkers.Wait()
	response, _ = jobRequest(t, handler, http.MethodGet, "/api/v1/analysis-jobs/run")
	if response.Code != 404 {
		t.Fatalf("deleted job still readable: %d", response.Code)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "run")); !os.IsNotExist(err) {
		t.Fatal("worker recreated deleted result")
	}
}

func TestAnalysisJobShutdownAndRestartExposeRetryableInterruption(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Unix(1, 0))
	s.analysisSlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	first, err := s.startAnalysisJob(ctx, "run", false)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	s.analysisWorkers.Wait()
	<-s.analysisSlots
	status, err := s.analysisJobStatus("run")
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "interrupted" {
		t.Fatalf("shutdown: %+v", status)
	}
	// Simulate a hard restart before the queued state could be updated on disk.
	first.State = "running"
	if err := s.persistAnalysisJob(first); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	status, err = restarted.analysisJobStatus("run")
	if err != nil {
		t.Fatal(err)
	}
	if status.State != "interrupted" || !strings.Contains(status.Error, "restarted") {
		t.Fatalf("orphaned job: %+v", status)
	}
	if _, err := restarted.startAnalysisJob(context.Background(), "run", false); err != nil {
		t.Fatal(err)
	}
	if got := awaitAnalysisJob(t, restarted, "run"); got.State != "completed" {
		t.Fatalf("retry: %+v", got)
	}
	restarted.analysisWorkers.Wait()
}

func TestAnalysisJobRequiresLoginForReadsAndMutations(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "job-password"}, nil)
	resultFixture(t, s, "run", "completed", time.Unix(1, 0))
	handler := s.apiTestHandler(context.Background())
	response, _ := jobRequest(t, handler, http.MethodPost, "/api/v1/analysis-jobs/run")
	if response.Code != 401 {
		t.Fatal("analysis start bypassed session authentication")
	}
	response, _ = jobRequest(t, handler, http.MethodGet, "/api/v1/analysis-jobs/run")
	if response.Code != http.StatusUnauthorized {
		t.Fatal("analysis status bypassed session authentication")
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/analysis-jobs/run", nil)
	authenticateRequest(t, s, request)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("authenticated polling: %d", response.Code)
	}
}

func TestAnalysisJobQueueIsBoundedAndShutdownClosesAdmission(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.analysisSlots <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); s.analysisWorkers.Wait(); <-s.analysisSlots }()
	for i := 0; i <= analysisJobLimit; i++ {
		id := fmt.Sprintf("queued-%d", i)
		resultFixture(t, s, id, "completed", time.Unix(1, 0))
		_, err := s.startAnalysisJob(ctx, id, false)
		if i < analysisJobLimit && err != nil {
			t.Fatal(err)
		}
		if i == analysisJobLimit && !errors.Is(err, errAnalysisQueueFull) {
			t.Fatalf("queue admitted excess work: %v", err)
		}
	}
	cancel()
	if _, err := s.startAnalysisJob(ctx, "queued-0", false); err == nil {
		t.Fatal("shutdown admitted new work")
	}
}

func TestAnalysisJobSummaryPreservesModelsWithoutMessagePayloads(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "compact", "completed", time.Unix(1, 0))
	events := []model.TraceEvent{cohortPublish("m", "topic", []string{"a"}), cohortDelivery("m", "topic", "a", 20)}
	for i := range events {
		events[i].RunID = "compact"
	}
	analysisWriteLines(t, server, "compact", "events.jsonl", events)
	_, err := server.startAnalysisJob(context.Background(), "compact", false)
	if err != nil {
		t.Fatal(err)
	}
	job := awaitAnalysisJob(t, server, "compact")
	if job.AnalysisVersion != currentAnalysisVersion {
		t.Fatal("unversioned new artifact")
	}
	for _, kind := range []string{"result", "summary"} {
		response := resultRequest(server, http.MethodGet, "/api/v1/analysis-jobs/compact/"+kind+"?jobId="+job.ID)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", kind, response.Code, response.Body)
		}
		var data resultAnalysis
		if err := json.Unmarshal(response.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
		if data.Research == nil || data.Research.MessageCount != 1 {
			t.Fatal("lost message count")
		}
		want := 1
		if kind == "summary" {
			want = 0
		}
		if len(data.Research.Messages) != want {
			t.Fatalf("%s message rows=%d", kind, len(data.Research.Messages))
		}
		researchClose(t, data.Research.Summary["frt"].Average, .02)
	}
}
