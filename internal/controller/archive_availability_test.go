package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestArchiveMaintenanceContinuesAfterDeletionFailure(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "upload", "completed", time.Now())
	archive, err := s.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	archive.Close()
	// A stale or damaged individual archive cannot be safely removed. Keep its
	// tombstone, but do not prevent unrelated results from reaching this store.
	bad := filepath.Join(s.config.DataDir, archivedRunsDirectory, "bad-deletion")
	if err := os.WriteFile(bad, []byte("preserve damaged entry"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.markResultDeletedLocked("bad-deletion"); err != nil {
		t.Fatal(err)
	}
	goodDeletion := filepath.Join(s.config.DataDir, archivedRunsDirectory, "z-good-deletion")
	if err := os.Mkdir(goodDeletion, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.markResultDeletedLocked("z-good-deletion"); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveMaintenance(t.Context()); err == nil {
		t.Fatal("unsafe deletion was not reported")
	} else if archiveFailureKind(err) != "maintenance" {
		t.Fatalf("individual failure reported archive unavailable: %v", err)
	}
	if _, err := os.Stat(goodDeletion); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("one failed deletion blocked another: %v", err)
	}
	if got := listedStorageState(t, s, "upload"); got != "archived" {
		t.Fatalf("healthy destination did not receive unrelated result: %s", got)
	}
	if s.state.archivePending["upload"] {
		t.Fatal("completed upload remained queued")
	}
	if data, err := os.ReadFile(bad); err != nil || string(data) != "preserve damaged entry" {
		t.Fatalf("failed deletion discarded evidence: %q %v", data, err)
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveMaintenance(t.Context()); err != nil {
		t.Fatalf("repaired deletion did not recover: %v", err)
	}
}

func TestArchiveMaintenanceContinuesAfterImportFailure(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	archive, err := s.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	archive.Close()
	for _, id := range []string{"a-blocked", "z-imported"} {
		resultFixture(t, s, id, "completed", time.Now())
		if err := os.Rename(filepath.Join(s.config.DataDir, currentRunsDirectory, id), filepath.Join(s.config.DataDir, archivedRunsDirectory, id)); err != nil {
			t.Fatal(err)
		}
	}
	blocked := filepath.Join(s.config.DataDir, currentRunsDirectory, "a-blocked")
	if err := os.WriteFile(blocked, []byte("preserve local conflict"), 0600); err != nil {
		t.Fatal(err)
	}
	resultFixture(t, s, "upload", "completed", time.Now())
	if err := s.archiveMaintenance(t.Context()); err == nil {
		t.Fatal("conflicting local index was not reported")
	} else if archiveFailureKind(err) != "maintenance" {
		t.Fatalf("individual failure reported archive unavailable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "z-imported", "experiment.json")); err != nil {
		t.Fatalf("one failed import prevented another: %v", err)
	}
	if got := listedStorageState(t, s, "upload"); got != "archived" {
		t.Fatalf("one failed import prevented upload: %s", got)
	}
	if data, err := os.ReadFile(blocked); err != nil || string(data) != "preserve local conflict" {
		t.Fatalf("import overwrote conflicting local evidence: %q %v", data, err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	// The failed result is retried on the next scheduled archive scan.
	s.archiveLastScan = time.Time{}
	if err := s.archiveMaintenance(t.Context()); err != nil {
		t.Fatalf("repaired import did not recover: %v", err)
	}
}

func TestArchiveIdentityFailuresAreSpecificAndDoNotRebind(t *testing.T) {
	for _, fault := range []string{"missing", "changed", "unreadable"} {
		t.Run(fault, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			root, err := s.openArchiveStore()
			if err != nil {
				t.Fatal(err)
			}
			root.Close()
			expectedPath := filepath.Join(s.config.DataDir, ".archive-store-id")
			expected, err := os.ReadFile(expectedPath)
			if err != nil {
				t.Fatal(err)
			}
			identityPath := filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")
			if err := os.Remove(identityPath); err != nil {
				t.Fatal(err)
			}
			want := "identity is missing"
			switch fault {
			case "changed":
				if err := os.WriteFile(identityPath, []byte("different-store"), 0600); err != nil {
					t.Fatal(err)
				}
				want = "identity changed"
			case "unreadable":
				if err := os.Mkdir(identityPath, 0700); err != nil {
					t.Fatal(err)
				}
				want = "read archive storage identity"
			}
			if opened, err := s.openArchiveStore(); err == nil {
				opened.Close()
				t.Fatal("identity failure was ignored")
			} else if archiveFailureKind(err) != "unavailable" || !strings.Contains(err.Error(), want) {
				t.Fatalf("wrong identity diagnostic: kind=%s error=%v", archiveFailureKind(err), err)
			}
			if got, err := os.ReadFile(expectedPath); err != nil || !bytes.Equal(got, expected) {
				t.Fatalf("ordinary store access rebound identity: %q %v", got, err)
			}
		})
	}
}

func TestArchiveIdentityCreationDoesNotOverwriteExistingStore(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	existing := []byte("existing-store-id")
	if err := root.WriteFile(".kpl-store-id", existing, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := createArchiveIdentity(root)
	if err != nil || !bytes.Equal(got, existing) {
		t.Fatalf("identity initializer replaced an existing store: %q %v", got, err)
	}
	actual, err := root.ReadFile(".kpl-store-id")
	if err != nil || !bytes.Equal(actual, existing) {
		t.Fatalf("identity file changed: %q %v", actual, err)
	}
}

func TestKnownArchiveStoreDoesNotWaitForInitializationMutex(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	root, err := s.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	root.Close()
	s.archiveIdentityInitMu.Lock()
	defer s.archiveIdentityInitMu.Unlock()
	done := make(chan error, 1)
	go func() {
		opened, err := s.openArchiveStore()
		if err == nil {
			err = opened.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("established archive reads waited for the initialization mutex")
	}
}

func TestArchiveOverviewDistinguishesStoreLocalAndMaintenanceFailures(t *testing.T) {
	for _, kind := range []string{"unavailable", "local", "maintenance"} {
		t.Run(kind, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			root, err := s.openArchiveStore()
			if err != nil {
				t.Fatal(err)
			}
			root.Close()
			switch kind {
			case "unavailable":
				err = os.Remove(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id"))
			case "local":
				err = os.Remove(filepath.Join(s.config.DataDir, ".archive-store-id"))
				if err == nil {
					err = os.Mkdir(filepath.Join(s.config.DataDir, ".archive-store-id"), 0700)
				}
			case "maintenance":
				err = os.WriteFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, "bad-deletion"), []byte("conflict"), 0600)
				if err == nil {
					err = s.markResultDeletedLocked("bad-deletion")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.runArchiveLoop(ctx)
			}()
			defer func() { cancel(); <-done }()
			deadline := time.Now().Add(2 * time.Second)
			for {
				status := archiveOverviewForTest(t, s)
				if !status.LastCheckedAt.IsZero() && !status.Checking {
					if status.Error == "" || status.ErrorKind != kind || !status.LocalSpaceKnown {
						t.Fatalf("wrong archive failure classification: %+v", status)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("archive worker did not complete its first check")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestArchiveOverviewMarksFailedSpaceInspectionUnknown(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	blocked := filepath.Join(s.config.DataDir, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	s.config.DataDir = blocked
	s.archiveIOCheck = func() { t.Fatal("storage status probed NAS") }
	status := archiveOverviewForTest(t, s)
	if status.LocalSpaceKnown || status.ErrorKind != "local" || status.Error == "" {
		t.Fatalf("failed local space inspection was treated as a measurement: %+v", status)
	}
}

func TestConcurrentArchiveStoreInitializationKeepsOneIdentity(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const readers = 32
	start := make(chan struct{})
	errorsFound := make(chan error, readers)
	var tasks sync.WaitGroup
	for range readers {
		tasks.Go(func() {
			<-start
			root, err := s.openArchiveStore()
			if err == nil {
				err = root.Close()
			}
			errorsFound <- err
		})
	}
	close(start)
	tasks.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Errorf("concurrent archive initialization: %v", err)
		}
	}
	expected, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id"))
	if err != nil || len(actual) == 0 || !bytes.Equal(actual, expected) {
		t.Fatalf("archive identity was raced: expected=%q actual=%q err=%v", expected, actual, err)
	}
	// A working but different store must still be refused; this protection is
	// independent of errors in an individual result or background operation.
	if err := os.WriteFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id"), []byte("another-store"), 0600); err != nil {
		t.Fatal(err)
	}
	if root, err := s.openArchiveStore(); err == nil {
		root.Close()
		t.Fatal("a different archive identity was adopted")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, ".archive-store-id")); errors.Is(err, os.ErrNotExist) {
		t.Fatal("archive mismatch cleared the local identity")
	}
}
