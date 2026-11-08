package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestFailedResultDeleteRejectsInvalidStateQueries(t *testing.T) {
	for _, query := range []string{
		"expectedState=", "expectedState", "expectedState=completed", "expectedState=FAILED",
		"expectedState=failed&expectedState=failed", "expectedState=failed&expectedState=completed",
		"expectedState=%ZZ", "expectedState=failed;ignored=value",
	} {
		t.Run(query, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			run, _ := resultFixture(t, s, "keep-invalid", "failed", time.Now())
			response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?"+query)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid state query status=%d: %s", response.Code, response.Body)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")); err != nil {
				t.Fatalf("invalid query deleted the result: %v", err)
			}
		})
	}
}

func TestFailedResultDeletePreservesOtherStatesAndGenericDelete(t *testing.T) {
	for _, state := range []string{"failed", "completed", "canceled", "interrupted", "queued", "running"} {
		t.Run(state, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			run, _ := resultFixture(t, s, "guard-state", state, time.Now())
			path := "/api/v1/results/" + run.ID
			response := resultRequest(s, http.MethodDelete, path+"?expectedState=failed")
			if state == "failed" {
				if response.Code != http.StatusNoContent {
					t.Fatalf("failed delete status=%d: %s", response.Code, response.Body)
				}
				if again := resultRequest(s, http.MethodDelete, path+"?expectedState=failed"); again.Code != http.StatusNotFound {
					t.Fatalf("repeated delete status=%d", again.Code)
				}
				return
			}
			var body struct{ Code string }
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if response.Code != http.StatusConflict || body.Code != "result-state-changed" {
				t.Fatalf("state mismatch status=%d: %s", response.Code, response.Body)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")); err != nil {
				t.Fatalf("state mismatch changed the result: %v", err)
			}
			// Unloaded running/queued metadata is historical interrupted work.
			// The ordinary delete contract still permits all terminal records.
			if generic := resultRequest(s, http.MethodDelete, path+"?unrelated=value"); generic.Code != http.StatusNoContent {
				t.Fatalf("ordinary delete changed behavior: %d %s", generic.Code, generic.Body)
			}
		})
	}
}

func TestFailedResultDeleteUsesCurrentLiveStateInsteadOfStaleManifest(t *testing.T) {
	for _, liveState := range []string{"failed", "completed", "running", "queued"} {
		t.Run(liveState, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			storedState := "failed"
			if liveState == "failed" {
				storedState = "completed"
			}
			run, _ := resultFixture(t, s, "live-authority", storedState, time.Now())
			run.State = liveState
			s.state.experiments[run.ID] = run
			response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?expectedState=failed")
			if liveState == "failed" {
				if response.Code != http.StatusNoContent {
					t.Fatalf("live failure not deleted: %d %s", response.Code, response.Body)
				}
				return
			}
			if response.Code != http.StatusConflict {
				t.Fatalf("live %s was deleted: %d %s", liveState, response.Code, response.Body)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")); err != nil {
				t.Fatalf("live state mismatch deleted the result: %v", err)
			}
		})
	}
}

func TestFailedResultDeleteRechecksStateAfterAcquiringDeletionLocks(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, s, "became-completed", "failed", time.Now())
	s.state.experiments[run.ID] = run
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	response := make(chan int, 1)
	go func() {
		response <- resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?expectedState=failed").Code
	}()
	deadline := time.Now().Add(time.Second)
	for s.cancelMu.TryLock() {
		s.cancelMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("deletion did not reach its serialization locks")
		}
		time.Sleep(time.Millisecond)
	}
	// A completion committed while the request waited must win over the UI's
	// previously observed failure. The guard must run inside persistMu as well.
	s.state.mu.Lock()
	run.State = "completed"
	s.state.experiments[run.ID] = run
	s.state.mu.Unlock()
	s.state.persistMu.Unlock()
	locked = false
	select {
	case status := <-response:
		if status != http.StatusConflict {
			t.Fatalf("changed state was deleted after lock wait: %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("conditional deletion did not finish")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")); err != nil {
		t.Fatalf("completed result was removed: %v", err)
	}
}

func TestFailedResultDeleteKeepsExistingBusyProtections(t *testing.T) {
	for _, busy := range []string{"finalizing", "repeat", "download", "read-pin"} {
		t.Run(busy, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			run, _ := resultFixture(t, s, "busy-failure", "failed", time.Now())
			switch busy {
			case "finalizing":
				s.cancels[run.ID] = func() {}
			case "repeat":
				s.repeatBatches = map[string]*repeatBatch{run.ID: {}}
			case "download":
				s.resultDownloads = map[string]int{run.ID: 1}
			case "read-pin":
				s.resultReadPins[run.ID] = 1
			}
			if response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?expectedState=failed"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "still active") {
				t.Fatalf("busy deletion lost its guard: %d %s", response.Code, response.Body)
			}
		})
	}
}

func TestFailedResultDeleteRetainsBatchProvenanceAndCleanupMarker(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "mixed-batch", "failed-attempt", "failed", 1, 2, 1)
	batchFixture(t, s, "mixed-batch", "successful-attempt", "completed", 2, 2, 1)
	run := model.Experiment{ID: "failed-attempt", State: "failed", StartedAt: time.Now(), CleanupState: "failed", BatchID: "mixed-batch", Iteration: 1, Repetitions: 2}
	s.state.experiments[run.ID] = run
	if err := s.rememberRunAgent(run.ID, model.Agent{ID: "old-owner", URL: "http://127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?expectedState=failed")
	if response.Code != http.StatusNoContent {
		t.Fatalf("failed member delete: %d %s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "successful-attempt", "experiment.json")); err != nil {
		t.Fatalf("cleanup deleted the successful member: %v", err)
	}
	record, err := s.readBatchExtension("mixed-batch")
	if err != nil || record == nil || record.Deleted[run.ID].State != "failed" || record.Current[1] != run.ID {
		t.Fatalf("cleanup lost failed current-attempt provenance: %+v %v", record, err)
	}
	for _, path := range []string{filepath.Join(pendingWorkCleanupDirectory, run.ID), filepath.Join(ownershipDirectory, run.ID+".json")} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, path)); err != nil {
			t.Fatalf("cleanup lost %s: %v", path, err)
		}
	}
	ids, err := New(s.config, nil).pendingWorkCleanupIDs()
	if err != nil || !slices.Contains(ids, run.ID) {
		t.Fatalf("cleanup marker did not survive Controller restart: %v %v", ids, err)
	}
}

func TestFailedResultDeleteCompletesAlreadyFencedPartialDeletion(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "partial-batch", "partial-failed", "failed", 1, 2, 1)
	batchFixture(t, s, "partial-batch", "keep-success", "completed", 2, 2, 1)
	interruptedResultDeletion(t, s, "partial-failed")
	s = New(s.config, nil)
	if response := resultRequest(s, http.MethodDelete, "/api/v1/results/partial-failed?expectedState=failed"); response.Code != http.StatusNoContent {
		t.Fatalf("fenced partial deletion could not finish: %d %s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "partial-failed")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial deletion left source directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "keep-success", "experiment.json")); err != nil {
		t.Fatalf("partial deletion changed successful member: %v", err)
	}
}

func TestFailedResultDeleteFailsClosedForUnreadableState(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, s, "unreadable-state", "failed", time.Now())
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")
	if err := os.WriteFile(path, []byte("{invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID+"?expectedState=failed"); response.Code != http.StatusInternalServerError {
		t.Fatalf("unreadable state status=%d %s", response.Code, response.Body)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unreadable result was removed: %v", err)
	}
	if response := resultRequest(s, http.MethodDelete, "/api/v1/results/"+run.ID); response.Code != http.StatusInternalServerError {
		// Generic deletion also needs batch provenance before its first marker.
		t.Fatalf("ordinary invalid-metadata delete unexpectedly changed: %d %s", response.Code, response.Body)
	}
}
