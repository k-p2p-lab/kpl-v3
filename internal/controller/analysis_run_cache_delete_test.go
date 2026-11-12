package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func writeCacheDeletionGroup(t *testing.T, s *Server, record batchExtension) {
	t.Helper()
	root, err := s.resultGroupDirectory(record.BatchID, true)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := writeBatchRecord(root, record); err != nil {
		t.Fatal(err)
	}
}

func TestBatchRunCacheGroupDeletionCleansMissingHistory(t *testing.T) {
	for _, membership := range []string{"current", "current-only", "deleted", "retired", "previous"} {
		t.Run(membership, func(t *testing.T) {
			s, analysis := batchRunCacheFixture(t)
			if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
				t.Fatal(err)
			}
			s.archiveIOCheck = func() { t.Fatal("group cache cleanup read archived source data") }
			record := batchExtension{Version: 1, BatchID: analysis.Result.BatchID, Repetitions: 2}
			if membership != "current-only" {
				batchFixture(t, s, record.BatchID, "remaining", "completed", 2, 2, 1)
			}
			switch membership {
			case "current", "current-only":
				record.Current = map[int]string{1: analysis.Result.ID}
			case "deleted":
				record.Deleted = map[string]attemptRecord{analysis.Result.ID: attemptOf(analysis.Result)}
			case "retired":
				record.Retired = map[string]bool{analysis.Result.ID: true}
			case "previous":
				contents, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "remaining", "experiment.json"))
				if err != nil {
					t.Fatal(err)
				}
				var run model.Experiment
				if err := json.Unmarshal(contents, &run); err != nil {
					t.Fatal(err)
				}
				run.PreviousRunIDs = []string{analysis.Result.ID}
				if err := s.persistExperiment(run); err != nil {
					t.Fatal(err)
				}
			}
			writeCacheDeletionGroup(t, s, record)
			// Simulate an external source-directory loss after the derived cache
			// committed. The durable group retains the only cleanup identity.
			if err := os.RemoveAll(filepath.Join(s.config.DataDir, currentRunsDirectory, analysis.Result.ID)); err != nil {
				t.Fatal(err)
			}
			unrelated := filepath.Join(s.config.DataDir, batchRunCacheDirectory, "unrelated", "keep")
			if err := os.MkdirAll(filepath.Dir(unrelated), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(unrelated, []byte("keep"), 0600); err != nil {
				t.Fatal(err)
			}
			deleted, err := s.deleteSavedBatch(t.Context(), record.BatchID)
			expected := 1
			if membership == "current-only" {
				expected = 0
			}
			if err != nil || len(deleted) != expected {
				t.Fatalf("missing source blocked group cleanup: deleted=%v error=%v", deleted, err)
			}
			for _, relative := range []string{filepath.Join(batchRunCacheDirectory, analysis.Result.ID), filepath.Join(currentRunsDirectory, analysis.Result.ID), filepath.Join(currentRunsDirectory, "remaining"), filepath.Join(resultGroupsDirectory, record.BatchID)} {
				if _, err := os.Stat(filepath.Join(s.config.DataDir, relative)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("group cleanup retained or recreated %s: %v", relative, err)
				}
			}
			if _, err := os.Stat(unrelated); err != nil {
				t.Fatalf("group cleanup removed another Run cache: %v", err)
			}
		})
	}
}

func TestBatchRunCacheMissingPinnedSourcePreflightsGroupDeletion(t *testing.T) {
	s, analysis := batchRunCacheFixture(t)
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	batchFixture(t, s, analysis.Result.BatchID, "remaining", "completed", 2, 2, 1)
	writeCacheDeletionGroup(t, s, batchExtension{Version: 1, BatchID: analysis.Result.BatchID, Repetitions: 2, Current: map[int]string{1: analysis.Result.ID, 2: "remaining"}})
	if err := s.persistBatchAnalysis(batchAnalysisStatus{BatchID: analysis.Result.BatchID}); err != nil {
		t.Fatal(err)
	}
	snapshot, hit, err := s.captureBatchRunAnalysisCache(t.Context(), analysis.Result)
	if err != nil || !hit || snapshot == nil {
		t.Fatalf("cache fixture did not acquire a Run pin: hit=%t error=%v", hit, err)
	}
	defer snapshot.close()
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, currentRunsDirectory, analysis.Result.ID)); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(s, http.MethodDelete, "/api/v1/result-batches/"+analysis.Result.BatchID)
	if response.Code != http.StatusConflict {
		t.Fatalf("missing cached reader did not block deletion: %d %s", response.Code, response.Body)
	}
	for _, relative := range []string{filepath.Join(currentRunsDirectory, "remaining", "experiment.json"), filepath.Join(batchRunCacheDirectory, analysis.Result.ID, batchRunCacheIndexFile), filepath.Join(resultGroupsDirectory, analysis.Result.BatchID, batchExtensionFile), filepath.Join("batch-analyses", analysis.Result.BatchID, batchJobFile)} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, relative)); err != nil {
			t.Fatalf("busy preflight changed %s: %v", relative, err)
		}
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, ".deleted-results")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("busy preflight committed a deletion fence: %v", err)
	}
	snapshot.close()
	response = resultRequest(s, http.MethodDelete, "/api/v1/result-batches/"+analysis.Result.BatchID)
	if response.Code != http.StatusOK {
		t.Fatalf("released cache pin still blocked deletion: %d %s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, batchRunCacheDirectory, analysis.Result.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful group deletion retained a missing Run cache: %v", err)
	}
}
