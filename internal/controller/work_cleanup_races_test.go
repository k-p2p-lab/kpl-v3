package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCleanupRecoveryDoesNotRestoreDeletedSnapshot(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "deleted-during-recovery", State: "completed", CleanupState: "retained", DataState: "collecting", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now()}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	// The recovery scan has read this retained manifest without control locks.
	restored := map[string]model.Experiment{run.ID: run}
	candidates := map[string]bool{run.ID: true}
	if err := s.deleteSavedResult(run.ID); err != nil {
		t.Fatal(err)
	}
	// Commit the now stale snapshot after deletion published its durable fence.
	if err := s.mergeRecoveredWorkCleanup(context.Background(), candidates, restored, nil); err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	_, exists := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if exists {
		t.Fatal("cleanup recovery resurrected deleted historical metadata")
	}
	if !slices.Contains(s.queueCleanup, run.ID) {
		t.Fatal("deleted history lost its independent Peer cleanup obligation")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup recovery recreated a deleted result directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); err != nil {
		t.Fatalf("cleanup recovery lost the deletion-safe pending marker: %v", err)
	}
}

func TestConfirmedCleanupAndResultDeletionRetireMarkerAtomically(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "delete-after-cleanup", State: "completed", CleanupState: "complete", DataState: "complete", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now()}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(run.ID)
	if err := s.writePendingWorkCleanup(run.ID); err != nil {
		s.cancelMu.Unlock()
		t.Fatal(err)
	}
	unlock := sync.OnceFunc(s.cancelMu.Unlock)
	done := make(chan struct{})
	var cleanupErr error
	go func() {
		cleanupErr = s.completeQueueCleanup([]string{run.ID})
		close(done)
	}()
	t.Cleanup(func() {
		unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	// Keep deletion's admission lock held while completed cleanup tries to
	// retire its barrier. It must not clear only the disk half of the barrier.
	select {
	case <-done:
		t.Fatalf("cleanup bypassed deletion serialization: %v", cleanupErr)
	case <-time.After(50 * time.Millisecond):
	}
	marker := filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cleanup cleared its marker before acquiring deletion's lock: %v", err)
	}
	s.analysisJobMu.Lock()
	s.state.persistMu.Lock()
	err := s.deleteSavedResultLocked(run.ID)
	s.state.persistMu.Unlock()
	s.analysisJobMu.Unlock()
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if cleanupErr != nil {
			t.Fatal(cleanupErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("confirmed cleanup did not finish after deletion released its lock")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result deletion recreated an already confirmed cleanup marker: %v", err)
	}
	if slices.Contains(s.queueCleanup, run.ID) {
		t.Fatal("confirmed cleanup retained its in-memory barrier")
	}
}
