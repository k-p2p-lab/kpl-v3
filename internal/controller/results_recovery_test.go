package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Simulate a crash after durable history/fence publication and after unlinking
// experiment.json, but before the rest of the local directory was removed.
func interruptedResultDeletion(t *testing.T, s *Server, id string) {
	t.Helper()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	runs, err := s.openResultRuns()
	if err != nil {
		t.Fatal(err)
	}
	defer runs.Close()
	if err := s.retainBatchCurrentLocked(runs, id); err != nil {
		t.Fatal(err)
	}
	if err := s.markResultDeletedLocked(id); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json")); err != nil {
		t.Fatal(err)
	}
}

func TestResultDeletionRetryFinishesAfterMetadataWasRemoved(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-controller", true: "restarted"}[restart], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			batchFixture(t, s, "batch", "partial", "completed", 1, 2, 1)
			batchFixture(t, s, "batch", "keep", "completed", 2, 2, 1)
			interruptedResultDeletion(t, s, "partial")
			if restart {
				s = New(s.config, nil)
			}
			if err := s.deleteSavedResult("partial"); err != nil {
				t.Fatalf("partially deleted result cannot be retried: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "partial")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retry retained deleted source files: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "keep", "experiment.json")); err != nil {
				t.Fatalf("retry changed another result: %v", err)
			}
			record, err := s.readBatchExtension("batch")
			if err != nil || record == nil || record.Deleted["partial"].State != "completed" {
				t.Fatalf("retry lost already-persisted attempt history: %+v %v", record, err)
			}
		})
	}
}

func TestBatchDeletionRetryFinishesFencedMembersMissingMetadata(t *testing.T) {
	for _, allPartial := range []bool{false, true} {
		t.Run(map[bool]string{false: "some-members-hidden", true: "all-members-hidden"}[allPartial], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			batchFixture(t, s, "batch", "partial", "completed", 1, 2, 1)
			batchFixture(t, s, "batch", "keep", "completed", 2, 2, 1)
			interruptedResultDeletion(t, s, "partial")
			if allPartial {
				interruptedResultDeletion(t, s, "keep")
			}
			s = New(s.config, nil)
			deleted, err := s.deleteSavedBatch(context.Background(), "batch")
			if err != nil || len(deleted) != 2 {
				t.Fatalf("batch cleanup omitted fenced members: %v %v", deleted, err)
			}
			for _, id := range []string{"partial", "keep"} {
				if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("batch retry skipped fenced member %s: %v", id, err)
				}
			}
		})
	}
}

func TestResultDeletionDuringArchiveCannotResurrectSources(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "deleted-during-archive"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "before-delete")}}); err != nil {
		t.Fatal(err)
	}
	archive, err := s.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	archive.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	s.archiveIOCheck = func() { enterOnce.Do(func() { close(entered); <-release }) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.archiveRun(ctx, id, 0) }()
	t.Cleanup(func() { cancel(); releaseOnce.Do(func() { close(release) }) })
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("archive did not reach NAS resolution")
	}
	if err := s.deleteSavedResult(id); err != nil {
		t.Fatalf("NAS transfer held local deletion: %v", err)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "late-after-delete")}}); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, errResultNotFound) {
			t.Fatalf("archive committed a deleted local result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("archive did not finish after release")
	}
	if err := s.cleanDeletedArchives(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	if err := restarted.archiveMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{currentRunsDirectory, archivedRunsDirectory} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, directory, id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("deleted evidence reappeared in %s: %v", directory, err)
		}
	}
}
