package controller

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCanceledArchiveResolutionDoesNotReadNAS(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "archived", "completed", time.Now().UTC())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "event")}}); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, "archived")
	reads := 0
	s.archiveIOCheck = func() { reads++ }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 64 {
		snapshot, err := s.captureResultFilesContext(ctx, "archived", false)
		if snapshot != nil {
			snapshot.close()
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled capture: %v", err)
		}
	}
	if reads != 0 {
		t.Fatalf("already-canceled readers opened the NAS %d times", reads)
	}
	if len(s.archiveReadSlots) != 0 || len(s.resultReadPins) != 0 {
		t.Fatalf("canceled readers retained slots or pins: %d, %v", len(s.archiveReadSlots), s.resultReadPins)
	}
}

func TestArchivedAnalysisRecoversAfterStorageReconnects(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "archived", "completed", 1, 2, 2)
	if _, err := s.startAnalysisJob(context.Background(), "archived", false); err != nil {
		t.Fatal(err)
	}
	first := awaitAnalysisJob(t, s, "archived")
	s.analysisWorkers.Wait()
	if first.State != "completed" {
		t.Fatalf("initial analysis: %+v", first)
	}
	original := resultRequest(s, http.MethodGet, first.ResultURL)
	if original.Code != http.StatusOK {
		t.Fatalf("initial artifact: %d %s", original.Code, original.Body)
	}
	archiveTestRun(t, s, "archived")
	archive := filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived")
	offline := archive + "-offline"
	if err := os.Rename(archive, offline); err != nil {
		t.Fatal(err)
	}
	// Restart while offline: the local metadata must retain the completed job,
	// while its archived artifact truthfully reports storage unavailability.
	s = New(s.config, nil)
	cached, err := s.startAnalysisJob(context.Background(), "archived", false)
	if err != nil || cached.State != "completed" || cached.ID != first.ID {
		t.Fatalf("offline cache identity: %+v, %v", cached, err)
	}
	if unavailable := resultRequest(s, http.MethodGet, first.ResultURL); unavailable.Code != http.StatusServiceUnavailable {
		t.Fatalf("offline artifact: %d %s", unavailable.Code, unavailable.Body)
	}
	if _, err := s.startAnalysisJob(context.Background(), "archived", true); err != nil {
		t.Fatal(err)
	}
	failed := awaitAnalysisJob(t, s, "archived")
	s.analysisWorkers.Wait()
	if failed.State != "failed" || failed.SavedAnalysisID != first.ID {
		t.Fatalf("offline refresh discarded the saved analysis: %+v", failed)
	}
	if err := os.Rename(offline, archive); err != nil {
		t.Fatal(err)
	}
	savedURL := first.ResultURL + "&saved=1"
	for _, server := range []*Server{s, New(s.config, nil)} {
		restored := resultRequest(server, http.MethodGet, savedURL)
		if restored.Code != http.StatusOK || restored.Body.String() != original.Body.String() {
			t.Fatalf("reconnected saved artifact: %d %s", restored.Code, restored.Body)
		}
	}
	if _, err := s.startAnalysisJob(context.Background(), "archived", true); err != nil {
		t.Fatal(err)
	}
	replacement := awaitAnalysisJob(t, s, "archived")
	s.analysisWorkers.Wait()
	if replacement.State != "completed" || replacement.ID == first.ID || replacement.SourceHash != first.SourceHash || replacement.SavedAnalysisID != "" {
		t.Fatalf("reconnected refresh: %+v", replacement)
	}
	if current := resultRequest(s, http.MethodGet, replacement.ResultURL); current.Code != http.StatusOK {
		t.Fatalf("replacement artifact: %d %s", current.Code, current.Body)
	}
	if len(s.archiveReadSlots) != 0 || len(s.resultReadPins) != 0 {
		t.Fatalf("reconnected readers retained slots or pins: %d, %v", len(s.archiveReadSlots), s.resultReadPins)
	}
}
