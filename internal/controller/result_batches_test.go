package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestResultBatchDeletionRemovesOnlyItsRunsAndMean(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	for i, state := range []string{"completed", "completed", "failed", "canceled"} {
		batchFixture(t, s, "batch", "run-"+string(rune('a'+i)), state, i+1, 4, 1)
	}
	batchFixture(t, s, "other", "unrelated", "completed", 1, 2, 1)
	if _, err := s.startAnalysisJob(context.Background(), "run-a", false); err != nil {
		t.Fatal(err)
	}
	awaitAnalysisJob(t, s, "run-a")
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if job := awaitBatch(t, s, "batch"); job.State != "completed" {
		t.Fatalf("batch setup: %+v", job)
	}
	s.state.experiments["run-a"] = model.Experiment{ID: "run-a", BatchID: "batch", State: "completed"}
	s.state.runMetrics["run-a"] = newRunMetricAccumulator()
	s.state.events = []model.TraceEvent{{RunID: "run-a"}, {RunID: "unrelated"}}
	if response := resultRequest(s, http.MethodDelete, "/api/v1/result-batches/batch"); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated deletion: %d", response.Code)
	}
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/result-batches/batch", nil)
	authenticateRequest(t, s, request)
	response := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(response, request)
	var body struct {
		DeletedIDs []string `json:"deletedIds"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatalf("delete: %d %s", response.Code, response.Body)
	}
	if !reflect.DeepEqual(body.DeletedIDs, []string{"run-a", "run-b", "run-c", "run-d"}) {
		t.Fatalf("wrong deletion scope: %+v", body)
	}
	for _, id := range body.DeletedIDs {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("run %s remains: %v", id, err)
		}
		if _, err := os.Stat(filepath.Join(s.config.DataDir, ".deleted-results", id)); err != nil {
			t.Fatalf("missing late-event fence for %s: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "batch-analyses", "batch")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("batch mean remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "unrelated", "experiment.json")); err != nil {
		t.Fatalf("another group was changed: %v", err)
	}
	if len(s.state.experiments) != 0 || len(s.state.runMetrics) != 0 || len(s.state.events) != 1 || s.state.events[0].RunID != "unrelated" {
		t.Fatal("deleted runs remain in live state")
	}
	if s.analysisJobs["run-a"] != nil || s.batchAnalysisJobs["batch"] != nil {
		t.Fatal("deleted analysis jobs remain cached")
	}
	if _, err := s.deleteSavedBatch(context.Background(), "batch"); !errors.Is(err, errResultNotFound) {
		t.Fatalf("retry after success: %v", err)
	}
}

func TestResultBatchDeletionPreflightsEveryMember(t *testing.T) {
	for _, reason := range []string{"running", "queued", "finalizing", "batch-member", "download", "unreadable-active"} {
		t.Run(reason, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			batchFixture(t, s, "batch", "first", "completed", 1, 2, 1)
			batchFixture(t, s, "batch", "last", "completed", 2, 2, 1)
			if err := s.persistBatchAnalysis(batchAnalysisStatus{BatchID: "batch"}); err != nil {
				t.Fatal(err)
			}
			switch reason {
			case "running", "queued", "unreadable-active":
				s.state.experiments["last"] = model.Experiment{ID: "last", BatchID: "batch", State: "running"}
				if reason == "queued" {
					run := s.state.experiments["last"]
					run.State = "queued"
					s.state.experiments["last"] = run
				}
				if reason == "unreadable-active" {
					if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "last", "experiment.json"), []byte("invalid"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "finalizing":
				s.cancels["last"] = func() {}
			case "batch-member":
				s.repeatBatches = map[string]*repeatBatch{"last": {}}
			case "download":
				snapshot, err := s.captureResult("last")
				if err != nil {
					t.Fatal(err)
				}
				defer snapshot.close()
			}
			response := resultRequest(s, http.MethodDelete, "/api/v1/result-batches/batch")
			if response.Code != http.StatusConflict {
				t.Fatalf("busy deletion: %d %s", response.Code, response.Body)
			}
			for _, relative := range []string{"current-run/first/experiment.json", "current-run/last/experiment.json", "batch-analyses/batch/job.json"} {
				if _, err := os.Stat(filepath.Join(s.config.DataDir, relative)); err != nil {
					t.Fatalf("preflight altered %s: %v", relative, err)
				}
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, ".deleted-results")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("preflight created tombstones: %v", err)
			}
		})
	}
}

func TestResultBatchDeletionCancelsAnalysesWithoutResurrection(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 2, 1)
	batchFixture(t, s, "batch", "two", "completed", 2, 2, 1)
	for range cap(s.analysisSlots) {
		s.analysisSlots <- struct{}{}
	}
	if _, err := s.startAnalysisJob(context.Background(), "one", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startBatchAnalysis(context.Background(), "batch", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deleteSavedBatch(context.Background(), "batch"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { s.analysisWorkers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deleted analyses did not stop")
	}
	for _, relative := range []string{"current-run/one", "current-run/two", "batch-analyses/batch"} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, relative)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("analysis recreated %s: %v", relative, err)
		}
	}
}

func TestResultBatchDeletionHandlesOrphanMeanAndUnsafePaths(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "only", "failed", 2, 3, 0)
	if _, err := s.deleteSavedBatch(context.Background(), "batch"); err != nil {
		t.Fatal(err)
	}
	if err := s.persistBatchAnalysis(batchAnalysisStatus{BatchID: "orphan"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deleteSavedBatch(context.Background(), "orphan"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "..", "../batch", "batch/file", "missing"} {
		if _, err := s.deleteSavedBatch(context.Background(), id); !errors.Is(err, errResultNotFound) {
			t.Fatalf("unsafe/missing %q: %v", id, err)
		}
	}
	if response := resultRequest(s, http.MethodGet, "/api/v1/result-batches/batch"); response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected method: %d", response.Code)
	}
	batchFixture(t, s, "linked", "keep", "completed", 1, 2, 0)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(s.config.DataDir, "batch-analyses", "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deleteSavedBatch(context.Background(), "linked"); err == nil {
		t.Fatal("accepted symlinked mean directory")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "keep", "experiment.json")); err != nil {
		t.Fatalf("unsafe mean changed the run: %v", err)
	}
}
