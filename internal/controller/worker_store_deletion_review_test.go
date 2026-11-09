package controller

import (
	"os"
	"path/filepath"
	"testing"
)

func TestColdSummaryDoesNotResurrectCommittedDeletionWithLocalFilesRemaining(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run, err := f.server.StartScenario(t.Context(), []byte("version: 4\nname: cold-deletion\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	if len(runMetricValues(t, f.server, run.ID)) == 0 {
		t.Fatal("fixture did not expose historical metrics")
	}
	// Model a committed tombstone followed by failed local directory removal.
	// The canonical source and summary deliberately remain on disk.
	f.server.state.persistMu.Lock()
	err = f.server.markResultDeletedLocked(run.ID)
	f.server.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	f.server.state.metrics.deleteRun(run.ID)
	f.server.state.mu.Lock()
	delete(f.server.state.experiments, run.ID)
	delete(f.server.state.workerSummaries, run.ID)
	f.server.state.mu.Unlock()
	if _, err := os.Stat(filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID, workerSummaryFile)); err != nil {
		t.Fatalf("fixture removed local evidence instead of modeling failed cleanup: %v", err)
	}
	if values := runMetricValues(t, f.server, run.ID); len(values) != 0 {
		t.Fatalf("cold historical projection resurrected a deleted Run: %v", values)
	}
	if _, known := f.server.state.workerMetricSummary(run.ID); known {
		t.Fatal("direct summary lookup bypassed the durable deletion fence")
	}
	closed, err := f.server.isRuntimeRetired(run.ID)
	if err != nil || !closed {
		t.Fatalf("deletion lost old lifecycle fencing: %v", err)
	}
}

func TestCommittedDeletionReleasesRuntimeProjectionBeforeLocalRemovalError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can remove files despite the fixture directory permissions")
	}
	f := newControllerWorkerFixture(t)
	run, err := f.server.StartScenario(t.Context(), []byte("version: 4\nname: partial-local-deletion\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	directory := filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID)
	if err := os.Chmod(directory, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(directory, 0700)
	if err := f.server.deleteSavedResult(run.ID); err == nil {
		t.Fatal("fixture did not reject physical directory cleanup")
	}
	if deleted, err := f.server.state.resultDeletedLocked(run.ID); err != nil || !deleted {
		t.Fatalf("logical deletion was not committed: %v", err)
	}
	if values := runMetricValues(t, f.server, run.ID); len(values) != 0 {
		t.Fatalf("failed physical cleanup retained runtime projections: %v", values)
	}
	f.server.state.mu.RLock()
	_, retained := f.server.state.experiments[run.ID]
	f.server.state.mu.RUnlock()
	if retained {
		t.Fatal("committed deletion retained its in-memory experiment")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.server.deleteSavedResult(run.ID); err != nil {
		t.Fatalf("physical cleanup could not be retried: %v", err)
	}
}
