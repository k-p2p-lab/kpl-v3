package controller

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func listedStorageState(t *testing.T, s *Server, id string) string {
	t.Helper()
	response := resultRequest(s, "GET", "/api/v1/results")
	var results []savedResult
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &results) != nil {
		t.Fatalf("list failed: %d %s", response.Code, response.Body)
	}
	for _, run := range results {
		if run.ID == id && run.Storage != nil {
			return run.Storage.State
		}
	}
	t.Fatalf("missing result/storage: %s", response.Body)
	return ""
}

func TestArchiveSameContentRewriteRefreshesManifestAndClearsPending(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "same-content"
	resultFixture(t, s, id, "completed", time.Now())
	putNote(t, s, id, "Preserve this note", "0")
	archiveTestRun(t, s, id)
	if got := listedStorageState(t, s, id); got != "archived" {
		t.Fatal(got)
	}
	names := []string{"experiment.json", "scenario.yaml", resultNoteFile}
	remoteTimes := make(map[string]time.Time)
	for _, name := range names {
		remote, err := os.Stat(filepath.Join(s.config.DataDir, archivedRunsDirectory, id, name))
		if err != nil {
			t.Fatal(err)
		}
		remoteTimes[name] = remote.ModTime()
		path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		// Same bytes, new timestamp: the old worker skipped the copy and never
		// updated the hint used by resultStorageStatus, leaving pending forever.
		if err := os.Chtimes(path, info.ModTime(), info.ModTime().Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	s.state.markRunArchiveDirty(id)
	if got := listedStorageState(t, s, id); got != "pending" {
		t.Fatalf("before reconciliation: %s", got)
	}
	if err := s.archiveMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := listedStorageState(t, s, id); got != "archived" {
		t.Fatalf("after reconciliation: %s", got)
	}
	if s.state.archivePending[id] {
		t.Fatal("reconciled result remained queued")
	}
	for _, name := range names {
		remote, err := os.Stat(filepath.Join(s.config.DataDir, archivedRunsDirectory, id, name))
		if err != nil || !remote.ModTime().Equal(remoteTimes[name]) {
			t.Fatalf("identical %s was uploaded again: %v", name, err)
		}
	}
}

func TestStorageRevisionTracksArchivingAndLateLocalWritesWithoutNASProbes(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "storage-revision"
	s.archiveIOCheck = func() { t.Fatal("storage overview performed a NAS operation") }
	initial := archiveOverviewForTest(t, s).ResultsRevision
	resultFixture(t, s, id, "completed", time.Now())
	s.state.markRunArchiveDirty(id)
	dirty := archiveOverviewForTest(t, s).ResultsRevision
	if initial == "" || dirty == initial {
		t.Fatal("new local data did not change storage revision")
	}
	for range 100 {
		s.state.markRunArchiveDirty(id)
	}
	if archiveOverviewForTest(t, s).ResultsRevision != dirty {
		t.Fatal("each telemetry batch triggers a full result-list refresh")
	}
	s.archiveIOCheck = nil
	archiveTestRun(t, s, id)
	s.archiveIOCheck = func() { t.Fatal("storage overview performed a NAS operation") }
	archived := archiveOverviewForTest(t, s).ResultsRevision
	if archived == dirty || listedStorageState(t, s, id) != "archived" {
		t.Fatal("archive completion was not published")
	}
	// A late write must notify even before the background queue has removed
	// the version whose archive was just published.
	putNote(t, s, id, "Late note", "0")
	if latest := archiveOverviewForTest(t, s).ResultsRevision; latest == archived || listedStorageState(t, s, id) != "pending" {
		t.Fatal("late local writes after archiving were not reported")
	}
	restarted := New(s.config, nil)
	if archiveOverviewForTest(t, restarted).ResultsRevision == archived {
		t.Fatal("Controller restart reused an old storage revision")
	}
}

func TestDeletionReleasesArchiveQueueAndNotifiesOtherBrowsers(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.archiveIOCheck = func() { t.Fatal("deletion performed a NAS operation") }
	const removed, retained = "delete-pending", "keep-pending"
	for _, id := range []string{removed, retained} {
		resultFixture(t, s, id, "completed", time.Now())
		s.state.markRunArchiveDirty(id)
	}
	before := archiveOverviewForTest(t, s).ResultsRevision
	if err := s.deleteSavedResult(removed); err != nil {
		t.Fatal(err)
	}
	if after := archiveOverviewForTest(t, s).ResultsRevision; after == before {
		t.Error("deletion did not notify browsers polling storage revisions")
	}
	s.state.archiveQueueMu.Lock()
	defer s.state.archiveQueueMu.Unlock()
	if _, exists := s.state.archiveVersions[removed]; exists {
		t.Error("deleted run retained its archive version")
	}
	if _, exists := s.state.archivePending[removed]; exists {
		t.Error("deleted run retained its pending archive work")
	}
	if _, exists := s.state.archiveDirtyNotified[removed]; exists {
		t.Error("deleted run retained its archive notification state")
	}
	if s.state.archiveVersions[retained] == 0 || !s.state.archivePending[retained] || !s.state.archiveDirtyNotified[retained] {
		t.Error("deletion discarded another run's pending archive work")
	}
}

func TestArchiveStartupDoesNotQueueTombstonedLocalRecords(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "partial-deletion"
	resultFixture(t, s, id, "completed", time.Now())
	// A crash can leave the local directory after the deletion fence is saved.
	if err := s.markResultDeletedLocked(id); err != nil {
		t.Fatal(err)
	}
	s = New(s.config, nil)
	if err := s.archiveMaintenance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, exists := s.state.archiveVersions[id]; exists || s.state.archivePending[id] || s.state.archiveDirtyNotified[id] {
		t.Fatal("archive startup recreated bookkeeping for a deleted result")
	}
}
