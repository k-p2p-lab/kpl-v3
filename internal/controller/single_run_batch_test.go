package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func legacySingleBatchFixture(t *testing.T, s *Server, state string) model.Experiment {
	t.Helper()
	run := completedAppendBatch(t, s, 1)[0]
	batchID := run.BatchID
	run.BatchID, run.Iteration, run.Repetitions = "", 0, 0
	run.State = state
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, resultGroupsDirectory, batchID)); err != nil {
		t.Fatal(err)
	}
	s.state.mu.Lock()
	s.state.experiments[run.ID] = run
	s.state.mu.Unlock()
	return run
}

func TestSingleRunGroupAppendPreservesOriginalAcrossRestartAndArchive(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, archived := range []bool{false, true} {
			name := map[bool]string{false: "current", true: "legacy"}[legacy] + map[bool]string{false: "/local", true: "/archived"}[archived]
			t.Run(name, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir()}, nil)
				var original model.Experiment
				if legacy {
					original = legacySingleBatchFixture(t, s, "completed")
				} else {
					original = completedAppendBatch(t, s, 1)[0]
					if original.BatchID != original.ID || original.Iteration != 1 || original.Repetitions != 1 {
						t.Fatalf("single admission did not store a one-Run group: %+v", original)
					}
				}
				appendPublished(t, s, original.ID, 1)
				metadataPath := filepath.Join(s.config.DataDir, currentRunsDirectory, original.ID, "experiment.json")
				metadata, err := os.ReadFile(metadataPath)
				if err != nil {
					t.Fatal(err)
				}
				if archived {
					archiveTestRun(t, s, original.ID)
				}
				revision, err := s.runSourceRevision(original.ID)
				if err != nil {
					t.Fatal(err)
				}
				restarted := New(s.config, nil)
				restarted.archiveIOCheck = func() { t.Fatal("group promotion or append waited on NAS") }
				members, err := restarted.batchMembers(t.Context(), original.ID)
				if err != nil || len(members) != 1 || members[0].BatchID != original.ID || members[0].Iteration != 1 || members[0].Repetitions != 1 {
					t.Fatalf("single result did not project as a one-Run group: %+v %v", members, err)
				}
				first, err := restarted.AppendScenarioBatch(t.Context(), original.ID, 2, 1)
				if err != nil {
					t.Fatal(err)
				}
				if first.ID == original.ID || first.BatchID != original.ID || first.Iteration != 2 || first.Repetitions != 3 || first.Seed != original.Seed {
					t.Fatalf("single extension did not retain its original group/seed: %+v", first)
				}
				added := waitRepetitions(t, restarted)
				if len(added) != 2 {
					t.Fatalf("append reran the old completed result: %+v", added)
				}
				for _, run := range added {
					if run.State != "completed" || run.ID == original.ID || run.Iteration < 2 || run.Iteration > 3 || run.Repetitions != 3 {
						t.Fatalf("unexpected appended member: %+v", run)
					}
				}
				if _, err := restarted.AppendScenarioBatch(t.Context(), original.ID, 2, 1); !errors.Is(err, errBatchNotAppendable) {
					t.Fatalf("stale admission was accepted twice: %v", err)
				}
				members, err = restarted.batchMembers(t.Context(), original.ID)
				if err != nil || len(members) != 3 {
					t.Fatalf("extended membership: %+v %v", members, err)
				}
				for _, member := range members {
					if member.BatchID != original.ID || member.Repetitions != 3 {
						t.Fatalf("old member lost the committed group size: %+v", member)
					}
				}
				if after, err := os.ReadFile(metadataPath); err != nil || !bytes.Equal(metadata, after) {
					t.Fatalf("promotion rewrote the original manifest: %v", err)
				}
				if after, err := restarted.runSourceRevision(original.ID); err != nil || after != revision {
					t.Fatalf("group extension changed the original source identity: before=%s after=%s error=%v", revision, after, err)
				}
				again := New(s.config, nil)
				again.archiveIOCheck = func() { t.Fatal("repeated append waited on NAS") }
				last, err := again.AppendScenarioBatch(t.Context(), original.ID, 1, 3)
				if err != nil || last.Iteration != 4 || last.Repetitions != 4 || last.Seed != original.Seed {
					t.Fatalf("restart lost the upgraded group: %+v %v", last, err)
				}
				waitRepetitions(t, again)
			})
		}
	}
}

func TestLegacySingleGroupRetryAndDeletionRetainReplayIdentity(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	original := legacySingleBatchFixture(t, s, "failed")
	restarted := New(s.config, nil)
	first, err := restarted.RetryScenarioBatch(context.Background(), original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == original.ID || first.BatchID != original.ID || first.Iteration != 1 || first.Repetitions != 1 || first.Seed != original.Seed {
		t.Fatalf("legacy retry did not keep its logical iteration: %+v", first)
	}
	waitRepetitions(t, restarted)
	if err := restarted.deleteSavedResult(original.ID); err != nil {
		t.Fatal(err)
	}
	members, err := restarted.batchMembers(t.Context(), original.ID)
	if err != nil || len(members) != 1 || members[0].ID != first.ID {
		t.Fatalf("deleting an old standalone attempt lost its successor: %+v %v", members, err)
	}
	replay, err := restarted.readBatchReplay(original.ID)
	if err != nil || replay == nil || replay.Seeds[1] != original.Seed || replay.ScenarioYAML != appendScenario {
		t.Fatalf("legacy replay was not retained independently: %+v %v", replay, err)
	}
}

func TestSingleRunProjectionDoesNotInventMissingMultiRunMetadata(t *testing.T) {
	for _, value := range []struct {
		batch                  string
		iteration, repetitions int
	}{{"known-batch", 1, 1}, {"", 2, 2}, {"", 0, 3}, {"", -1, 0}} {
		batch, iteration, repetitions := normalizeLegacySingleRunBatch("run", value.batch, value.iteration, value.repetitions)
		if batch != value.batch || iteration != value.iteration || repetitions != value.repetitions {
			t.Fatalf("ambiguous metadata was silently repaired: %+v", value)
		}
	}
}

func TestLegacySingleLibraryAliasSurvivesExtensionAndFirstRunDeletion(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	original := legacySingleBatchFixture(t, s, "completed")
	snapshot := newTestLibraryGroup(t, s, "Kept classification", "0")
	group := snapshot.Groups[0].ID
	snapshot = moveTestLibraryItems(t, s, group, []string{"run:" + original.ID}, snapshot.Revision)
	restarted := New(s.config, nil)
	if _, err := restarted.AppendScenarioBatch(t.Context(), original.ID, 1, 1); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, restarted)
	if err := restarted.deleteSavedResult(original.ID); err != nil {
		t.Fatal(err)
	}
	snapshot = librarySnapshot(t, libraryRequest(t, restarted, http.MethodGet, "", nil), http.StatusOK)
	if snapshot.Memberships["run:"+original.ID] != group {
		t.Fatal("deleting the original standalone Run lost its surviving group's classification")
	}
	before := snapshot.Revision
	snapshot = moveTestLibraryItems(t, restarted, "", []string{"batch:" + original.ID}, snapshot.Revision)
	if snapshot.Revision == before || len(snapshot.Memberships) != 0 {
		t.Fatal("canonical ungroup failed to remove the historical membership alias")
	}
	snapshot = moveTestLibraryItems(t, restarted, group, []string{"batch:" + original.ID}, snapshot.Revision)
	if snapshot.Memberships["batch:"+original.ID] != group || snapshot.Memberships["run:"+original.ID] != "" {
		t.Fatal("canonical move recreated a stale standalone alias")
	}
	response := libraryRequest(t, restarted, http.MethodPut, "/members", map[string]any{"groupId": "", "keys": []string{"batch:" + original.ID}, "revision": before})
	if response.Code != http.StatusConflict {
		t.Fatalf("alias migration bypassed revision checks: %d %s", response.Code, response.Body)
	}
	if _, err := restarted.deleteSavedBatch(t.Context(), original.ID); err != nil {
		t.Fatal(err)
	}
	snapshot = librarySnapshot(t, libraryRequest(t, restarted, http.MethodGet, "", nil), http.StatusOK)
	if len(snapshot.Memberships) != 0 || len(snapshot.Groups) != 1 {
		t.Fatal("last member deletion retained classification or deleted its library group")
	}
}

func TestLegacySingleCanonicalMoveRemovesAliasEvenWhenAlreadyAssigned(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	original := legacySingleBatchFixture(t, s, "completed")
	snapshot := newTestLibraryGroup(t, s, "Classification", "0")
	group := snapshot.Groups[0].ID
	snapshot = moveTestLibraryItems(t, s, group, []string{"run:" + original.ID}, snapshot.Revision)
	snapshot = moveTestLibraryItems(t, s, group, []string{"batch:" + original.ID}, snapshot.Revision)
	if snapshot.Memberships["batch:"+original.ID] != group || snapshot.Memberships["run:"+original.ID] != "" || len(snapshot.Memberships) != 1 {
		t.Fatal("canonical grouping did not replace the standalone alias")
	}
}

func TestSingleRunDeletionRetainsOnlyCommittedGroupRecovery(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "committed-group", true: "legacy-standalone"}[legacy], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			var original model.Experiment
			if legacy {
				original = legacySingleBatchFixture(t, s, "completed")
			} else {
				original = completedAppendBatch(t, s, 1)[0]
			}
			if err := s.deleteSavedResult(original.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, original.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted result directory survived: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, ".deleted-results", original.ID)); err != nil {
				t.Fatalf("deleted source lost its durable late-event fence: %v", err)
			}
			restarted := New(s.config, nil)
			response := resultRequest(restarted, http.MethodGet, "/api/v1/results")
			var listed []savedResult
			if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &listed) != nil {
				t.Fatalf("result listing: %d %s", response.Code, response.Body)
			}
			record, err := restarted.readBatchExtension(original.ID)
			if err != nil {
				t.Fatal(err)
			}
			if legacy {
				if len(listed) != 0 || record != nil {
					t.Fatalf("deleting an unpromoted standalone result invented retained history: %+v %+v", listed, record)
				}
				return
			}
			if len(listed) != 1 || !listed[0].Missing || listed[0].ID != original.ID || record == nil || record.Current[1] != original.ID {
				t.Fatalf("committed single-group recovery was lost: %+v %+v", listed, record)
			}
			first, err := restarted.RetryScenarioBatch(t.Context(), original.ID)
			if err != nil || first.ID == original.ID || first.Iteration != 1 || first.Repetitions != 1 || first.Seed != original.Seed {
				t.Fatalf("missing single-group iteration did not use a fresh attempt: %+v %v", first, err)
			}
			waitRepetitions(t, restarted)
		})
	}
}
