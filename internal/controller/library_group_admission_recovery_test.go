package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeCommittedLibraryBatch(t *testing.T, s *Server, id string) {
	t.Helper()
	root, err := s.resultGroupDirectory(id, true)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := writeBatchRecord(root, batchExtension{Version: 1, BatchID: id, Repetitions: 1, Current: map[int]string{1: id}}); err != nil {
		t.Fatal(err)
	}
}

func TestLibraryGroupAdmissionRecoveryAcrossCrashBoundaries(t *testing.T) {
	for _, boundary := range []string{"intent-only", "assigned", "committed", "moved", "ungrouped", "group-deleted", "restore-previous", "previous-deleted"} {
		t.Run(boundary, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("recovery"))
			group := groups.Groups[0].ID
			groups = newTestLibraryGroup(t, s, "Other group", groups.Revision)
			other := groups.Groups[1].ID
			const batchID = "interrupted-admission"
			const key = "batch:" + batchID
			intent := libraryGroupAdmission{Version: 1, BatchID: batchID, GroupID: group}
			want := ""
			if boundary != "intent-only" {
				groups.Memberships[key] = group
			}
			switch boundary {
			case "committed", "moved", "ungrouped", "group-deleted":
				writeCommittedLibraryBatch(t, s, batchID)
				want = group
				if boundary == "moved" {
					groups.Memberships[key], want = other, other
				} else if boundary == "ungrouped" || boundary == "group-deleted" {
					delete(groups.Memberships, key)
					want = ""
				}
				if boundary == "group-deleted" {
					groups.Groups = groups.Groups[1:]
					delete(groups.Memberships, "scenario:"+source.ID)
				}
			case "restore-previous":
				intent.PreviousGroup, want = other, other
			case "previous-deleted":
				intent.PreviousGroup = other
				groups.Groups = groups.Groups[:1]
			}
			if err := s.writeLibraryGroups(groups); err != nil {
				t.Fatal(err)
			}
			if err := s.writeLibraryGroupAdmission(intent); err != nil {
				t.Fatal(err)
			}
			restarted := New(s.config, nil)
			if err := restarted.recoverLibraryGroupAdmissions(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := restarted.readLibraryGroups()
			if err != nil || got.Memberships[key] != want || len(got.Groups) != len(groups.Groups) {
				t.Fatalf("recovery changed the wrong group: got=%+v want=%q error=%v", got, want, err)
			}
			if boundary != "group-deleted" && got.Memberships["scenario:"+source.ID] != group {
				t.Fatal("recovery changed the source scenario group")
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, libraryGroupAdmissionFile)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovered intent was not removed: %v", err)
			}
			if err := restarted.recoverLibraryGroupAdmissions(context.Background()); err != nil {
				t.Fatal(err)
			}
			again, err := restarted.readLibraryGroups()
			if err != nil || again.Revision != got.Revision {
				t.Fatalf("repeat recovery changed the groups: %+v %v", again, err)
			}
		})
	}
}

func TestLibraryGroupAdmissionRecoversPreviousIntentBeforeNewSubmission(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("next"))
	group := groups.Groups[0].ID
	groups.Memberships["batch:orphaned"] = group
	if err := s.writeLibraryGroups(groups); err != nil {
		t.Fatal(err)
	}
	if err := s.writeLibraryGroupAdmission(libraryGroupAdmission{Version: 1, BatchID: "orphaned", GroupID: group}); err != nil {
		t.Fatal(err)
	}
	first := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, s, source.ID, source.YAML, 1, ""))
	waitRepetitions(t, s)
	got, err := s.readLibraryGroups()
	if err != nil || got.Memberships["batch:orphaned"] != "" || got.Memberships["batch:"+first.BatchID] != group {
		t.Fatalf("new admission failed to recover previous intent: %+v %v", got, err)
	}
}

func TestLibraryGroupAdmissionRejectsUnsafePendingJournal(t *testing.T) {
	for _, kind := range []string{"invalid-json", "oversize", "bad-id", "unknown-field", "trailing", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			source, before := groupedScenarioFixture(t, s, validSavedScenarioYAML("blocked"))
			path := filepath.Join(s.config.DataDir, libraryGroupAdmissionFile)
			if kind == "symlink" {
				outside := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(outside, []byte("keep this file"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			} else {
				body := "invalid JSON"
				switch kind {
				case "oversize":
					body = strings.Repeat(" ", libraryGroupAdmissionLimit+1)
				case "bad-id":
					body = `{"version":1,"batchId":"../outside","groupId":"` + before.Groups[0].ID + `"}`
				case "unknown-field":
					body = `{"version":1,"batchId":"batch","groupId":"` + before.Groups[0].ID + `","unknown":true}`
				case "trailing":
					body = `{"version":1,"batchId":"batch","groupId":"` + before.Groups[0].ID + `"} {}`
				}
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.recoverLibraryGroupAdmissions(context.Background()); err == nil {
				t.Fatal("unsafe pending admission was accepted")
			}
			if _, err := s.submitScenarioFromLibrary(context.Background(), []byte(source.YAML), 1, "", source.ID); err == nil {
				t.Fatal("new admission overwrote an invalid pending journal")
			}
			got, err := s.readLibraryGroups()
			if err != nil || got.Revision != before.Revision || len(s.state.snapshot().Experiments) != 0 {
				t.Fatalf("invalid intent changed groups or started an experiment: %+v %v", got, err)
			}
			if _, err := os.Lstat(path); err != nil {
				t.Fatalf("invalid intent was removed: %v", err)
			}
		})
	}
}

func TestLibraryGroupAdmissionCleanupFailureDoesNotRejectCommittedAdmission(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("committed"))
	s.cancelMu.Lock()
	s.state.persistMu.Lock()
	err := s.admitScenarioResultGroup(context.Background(), source.ID, "committed-batch", func() error {
		writeCommittedLibraryBatch(t, s, "committed-batch")
		// Make only journal cleanup fail after the batch has been committed.
		path := filepath.Join(s.config.DataDir, libraryGroupAdmissionFile)
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := os.Mkdir(path, 0700); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(path, "occupied"), []byte("occupied"), 0600)
	})
	s.state.persistMu.Unlock()
	s.cancelMu.Unlock()
	if err != nil {
		t.Fatalf("committed admission was reported as failed: %v", err)
	}
	got, err := s.readLibraryGroups()
	if err != nil || got.Memberships["batch:committed-batch"] != groups.Groups[0].ID {
		t.Fatalf("committed membership was lost: %+v %v", got, err)
	}
}

func TestLibraryGroupAdmissionFailedRecoveryReportsGroupStorageError(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	source, _ := groupedScenarioFixture(t, s, validSavedScenarioYAML("failed-recovery"))
	failure := errors.New("batch admission failed")
	s.cancelMu.Lock()
	s.state.persistMu.Lock()
	err := s.admitScenarioResultGroup(context.Background(), source.ID, "failed-batch", func() error {
		if err := os.WriteFile(filepath.Join(s.config.DataDir, libraryGroupAdmissionFile), []byte("unreadable journal"), 0600); err != nil {
			return err
		}
		return failure
	})
	s.state.persistMu.Unlock()
	s.cancelMu.Unlock()
	if !errors.Is(err, failure) || !errors.Is(err, errScenarioResultGroup) {
		t.Fatalf("recovery storage error must retain the admission cause and HTTP 500 classification: %v", err)
	}
}
