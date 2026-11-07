package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func analysisGuardFixture(t *testing.T, batch bool, corrupt bool) (*Server, string, func() analysisJobStatus) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	t.Cleanup(s.analysisWorkers.Wait)
	id, path := "source", "/api/v1/analysis-jobs/source"
	status := func() analysisJobStatus {
		value, err := s.analysisJobStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	if batch {
		batchFixture(t, s, "batch", "source", "completed", 1, 2, 1)
		batchFixture(t, s, "batch", "second", "completed", 2, 2, 1)
		path = "/api/v1/batch-analysis-jobs/batch"
		status = func() analysisJobStatus {
			value, err := s.batchAnalysisStatus("batch")
			if err != nil {
				t.Fatal(err)
			}
			return value.analysisJobStatus
		}
	} else {
		resultFixture(t, s, id, "completed", time.Now())
	}
	if corrupt {
		if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl"), []byte("invalid event json\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return s, path, status
}

func analysisGuardPOST(t *testing.T, s *Server, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	authenticateRequest(t, s, r)
	w := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(w, r)
	return w
}

func analysisGuardBody(id, state string) []byte {
	data, _ := json.Marshal(map[string]any{"expectedAnalysis": map[string]string{"id": id, "state": state}})
	return data
}

func TestAnalysisGuardPreventsReplayAfterFastFailure(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "batch"}[batch], func(t *testing.T) {
			s, path, status := analysisGuardFixture(t, batch, true)
			body := analysisGuardBody("", "idle")
			if w := analysisGuardPOST(t, s, path, body); w.Code != http.StatusAccepted {
				t.Fatalf("first start: %d %s", w.Code, w.Body)
			}
			s.analysisWorkers.Wait()
			first := status()
			if first.State != "failed" || first.ID == "" {
				t.Fatalf("source did not fail quickly: %+v", first)
			}
			for replay := 0; replay < 3; replay++ {
				w := analysisGuardPOST(t, s, path, body)
				if w.Code != http.StatusConflict || !bytes.Contains(w.Body.Bytes(), []byte(`"code":"analysis-attempt-changed"`)) {
					t.Fatalf("replay %d admitted another job: %d %s", replay, w.Code, w.Body)
				}
				if got := status(); got.ID != first.ID {
					t.Fatal("replay changed the current analysis attempt")
				}
			}
			if w := analysisGuardPOST(t, s, path, analysisGuardBody(first.ID, first.State)); w.Code != http.StatusAccepted {
				t.Fatalf("intentional retry refused: %d %s", w.Code, w.Body)
			}
			s.analysisWorkers.Wait()
			if got := status(); got.ID == first.ID || got.State != "failed" {
				t.Fatalf("fresh expected attempt did not permit one retry: %+v", got)
			}
		})
	}
}

func TestAnalysisGuardChecksObservedStateAfterAnActiveJobFails(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "batch"}[batch], func(t *testing.T) {
			s, path, _ := analysisGuardFixture(t, batch, true)
			observed := analysisJobStatus{Version: 1, ID: "observed-analysis", State: "running", RunID: "source"}
			if batch {
				s.batchAnalysisJobs["batch"] = &batchAnalysisJob{status: batchAnalysisStatus{analysisJobStatus: observed, BatchID: "batch"}}
			} else {
				s.analysisJobs["source"] = &analysisJob{status: observed}
			}
			body := analysisGuardBody(observed.ID, observed.State)
			if w := analysisGuardPOST(t, s, path, body); w.Code != http.StatusAccepted {
				t.Fatalf("active job was not reused: %d %s", w.Code, w.Body)
			}
			s.analysisJobMu.Lock()
			if batch {
				s.batchAnalysisJobs["batch"].status.State = "failed"
			} else {
				s.analysisJobs["source"].status.State = "failed"
			}
			s.analysisJobMu.Unlock()
			if w := analysisGuardPOST(t, s, path, body); w.Code != http.StatusConflict {
				t.Fatalf("same-ID state change admitted a new job: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestAnalysisGuardKeepsValidCompletedCacheAndArtifacts(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "batch"}[batch], func(t *testing.T) {
			s, path, status := analysisGuardFixture(t, batch, false)
			if w := analysisGuardPOST(t, s, path, analysisGuardBody("", "idle")); w.Code != http.StatusAccepted {
				t.Fatalf("first cache creation: %d %s", w.Code, w.Body)
			}
			s.analysisWorkers.Wait()
			completed := status()
			if completed.State != "completed" {
				t.Fatalf("fixture did not complete: %+v", completed)
			}
			before := resultRequest(s, http.MethodGet, completed.ResultURL)
			if before.Code != http.StatusOK {
				t.Fatalf("completed artifact unavailable: %d", before.Code)
			}
			if w := analysisGuardPOST(t, s, path, analysisGuardBody("stale-analysis", "failed")); w.Code != http.StatusOK {
				t.Fatalf("valid cache was not reused: %d %s", w.Code, w.Body)
			}
			after := resultRequest(s, http.MethodGet, completed.ResultURL)
			if after.Code != http.StatusOK || !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) || status().ID != completed.ID {
				t.Fatal("guard changed a completed artifact or analysis identity")
			}
			if w := analysisGuardPOST(t, s, path+"?refresh=1", analysisGuardBody("stale-analysis", "failed")); w.Code != http.StatusConflict {
				t.Fatalf("stale forced refresh bypassed guard: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestAnalysisGuardRejectsMalformedRequestsBeforeAdmission(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "batch"}[batch], func(t *testing.T) {
			s, path, _ := analysisGuardFixture(t, batch, false)
			for _, body := range []string{
				`null`, `[]`, `{"expectedAnalysis":null}`, `{"expectedAnalysis":{"state":"idle"}}`,
				`{"expectedAnalysis":{"id":"","state":"failed"}}`, `{"expectedAnalysis":{"id":"invalid/id","state":"idle"}}`,
				`{"expectedAnalysis":{"id":"valid","state":"unknown"}}`, `{"expectedAnalysis":{"id":"","state":"idle","extra":1}}`,
				`{"extra":1}`, `{} {}`, strings.Repeat(" ", 17<<10),
			} {
				if w := analysisGuardPOST(t, s, path, []byte(body)); w.Code != http.StatusBadRequest {
					t.Fatalf("invalid guard request admitted: %d %s", w.Code, w.Body)
				}
			}
			if len(s.analysisJobs) != 0 || len(s.batchAnalysisJobs) != 0 || len(s.state.snapshot().WorkQueue) != 0 {
				t.Fatal("invalid request created analysis work")
			}
		})
	}
}

func TestBatchAnalysisGuardAcceptsPublicStaleProjectionAndBlocksItsReplay(t *testing.T) {
	s, path, status := analysisGuardFixture(t, true, false)
	if w := analysisGuardPOST(t, s, path, analysisGuardBody("", "idle")); w.Code != http.StatusAccepted {
		t.Fatalf("initial analysis: %d %s", w.Code, w.Body)
	}
	s.analysisWorkers.Wait()
	completed := status()
	if completed.State != "completed" {
		t.Fatalf("initial status: %+v", completed)
	}
	before := resultRequest(s, http.MethodGet, completed.ResultURL)
	if before.Code != http.StatusOK {
		t.Fatal("completed batch artifact is unavailable")
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "source", "events.jsonl"), []byte("corrupt changed input\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, path, nil)
	authenticateRequest(t, s, r)
	w := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(w, r)
	var observed batchAnalysisStatus
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &observed) != nil || observed.State != "idle" || !observed.Stale || observed.ID != completed.ID {
		t.Fatalf("public stale projection: %d %s", w.Code, w.Body)
	}
	body := analysisGuardBody(observed.ID, observed.State)
	if w := analysisGuardPOST(t, s, path, body); w.Code != http.StatusAccepted {
		t.Fatalf("public projection was not admitted: %d %s", w.Code, w.Body)
	}
	s.analysisWorkers.Wait()
	if failed := status(); failed.State != "failed" || failed.ID == completed.ID {
		t.Fatalf("replacement did not fail as expected: %+v", failed)
	}
	if w := analysisGuardPOST(t, s, path, body); w.Code != http.StatusConflict {
		t.Fatalf("stale projection replay admitted another job: %d %s", w.Code, w.Body)
	}
	after := resultRequest(s, http.MethodGet, completed.ResultURL+"&saved=1")
	if after.Code != http.StatusOK || !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) {
		t.Fatal("failed guarded replacement lost the previous batch artifact")
	}
}

func TestRunAnalysisGuardDoesNotAcceptBatchOnlyProjection(t *testing.T) {
	s, path, status := analysisGuardFixture(t, false, false)
	if w := analysisGuardPOST(t, s, path, analysisGuardBody("", "idle")); w.Code != http.StatusAccepted {
		t.Fatalf("initial analysis: %d %s", w.Code, w.Body)
	}
	s.analysisWorkers.Wait()
	completed := status()
	if completed.State != "completed" {
		t.Fatalf("initial status: %+v", completed)
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "source", "events.jsonl"), []byte("changed corrupt source\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if w := analysisGuardPOST(t, s, path, analysisGuardBody(completed.ID, "idle")); w.Code != http.StatusConflict {
		t.Fatalf("Run analysis incorrectly accepted a batch-only projection: %d %s", w.Code, w.Body)
	}
	if status().ID != completed.ID {
		t.Fatal("invalid Run projection created a new attempt")
	}
}

func TestAnalysisGuardKeepsEmptyAndObjectLegacyRequests(t *testing.T) {
	for _, body := range []string{"", " \n\t ", "{}"} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/analysis-jobs/source", strings.NewReader(body))
		if expected, err := decodeAnalysisExpectation(httptest.NewRecorder(), request); err != nil || expected != nil {
			t.Fatalf("legacy body %q changed contract: expected=%+v err=%v", body, expected, err)
		}
	}
}
