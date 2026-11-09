package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func listedMissingResults(t *testing.T, s *Server) []savedResult {
	t.Helper()
	w := httptest.NewRecorder()
	s.handleResults(w, httptest.NewRequest("GET", "/api/v1/results", nil))
	if w.Code != 200 {
		t.Fatalf("list status %d: %s", w.Code, w.Body.String())
	}
	var results []savedResult
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	return results
}

func removeMissingResult(t *testing.T, s *Server, id string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); err != nil {
		t.Fatal(err)
	}
}

func TestRetryMissingResultsRetainsResolvedSeedsAndCompletedFiles(t *testing.T) {
	for _, allMissing := range []bool{false, true} {
		t.Run(map[bool]string{false: "one-missing", true: "all-missing"}[allMissing], func(t *testing.T) {
			f := newResumeFixture(t)
			f.fail.Store(false)
			raw := []byte(strings.Replace(resumeScenario, "seed: 42", "seed: 0", 1))
			if _, err := f.server.StartScenarioRepeated(t.Context(), raw, 3); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, f.server)
			keptPath := filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[0].ID, "experiment.json")
			before, _ := os.ReadFile(keptPath)
			for i, run := range runs {
				if allMissing || i == 1 {
					removeMissingResult(t, f.server, run.ID)
				}
			}
			s := New(f.server.config, f.server.logger)
			f.register(t, s)
			listed := listedMissingResults(t, s)
			var expected []string
			for _, run := range listed {
				if run.Missing {
					if run.RetryUnavailable != "" {
						t.Fatal(run.RetryUnavailable)
					}
					expected = append(expected, run.ID)
				}
			}
			want := 1
			if allMissing {
				want = 3
			}
			if len(expected) != want {
				t.Fatalf("missing rows: %+v", listed)
			}
			first, err := s.resumeScenarioBatchExpected(t.Context(), runs[0].BatchID, true, expected)
			if err != nil {
				t.Fatal(err)
			}
			members := retryMembers(t, s, first.BatchID)
			if len(members) != 3 {
				t.Fatalf("lost slots: %+v", members)
			}
			for i, member := range members {
				if member.State != "completed" {
					t.Fatalf("missing retry failed: %+v", member)
				}
				actual := persistedExperiment(t, s, member.ID)
				if actual.Seed != runs[i].Seed {
					t.Fatalf("changed resolved seed: %d != %d", actual.Seed, runs[i].Seed)
				}
				if allMissing || i == 1 {
					if member.ID == runs[i].ID || len(member.PreviousRunIDs) != 1 || member.PreviousRunIDs[0] != runs[i].ID {
						t.Fatalf("wrong ancestry: %+v", member)
					}
					if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, runs[i].ID)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("recreated missing source: %v", err)
					}
				}
			}
			if !allMissing {
				after, _ := os.ReadFile(keptPath)
				if !bytes.Equal(before, after) {
					t.Fatal("modified available completed result")
				}
			}
			if _, err := s.resumeScenarioBatchExpected(t.Context(), first.BatchID, true, expected); !errors.Is(err, errBatchAttemptsChanged) {
				t.Fatalf("stale missing request admitted twice: %v", err)
			}
		})
	}
}

func TestRetryMissingLegacyRecoversSavedScenarioWithoutInventingRunIdentity(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	if _, err := f.server.StartScenarioRepeated(t.Context(), []byte(resumeScenario), 3); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	// Emulate a group predating both the replay snapshot and Current mapping.
	if err := os.RemoveAll(filepath.Join(f.server.config.DataDir, resultGroupsDirectory, runs[0].BatchID)); err != nil {
		t.Fatal(err)
	}
	removeMissingResult(t, f.server, runs[1].ID)
	s := New(f.server.config, f.server.logger)
	f.register(t, s)
	listed := listedMissingResults(t, s)
	var missing savedResult
	for _, run := range listed {
		if run.Missing {
			missing = run
		}
	}
	if missing.ID != missingIterationID(runs[0].BatchID, 2) || missing.RetryUnavailable != "" {
		t.Fatalf("legacy missing: %+v", missing)
	}
	first, err := s.resumeScenarioBatchExpected(t.Context(), runs[0].BatchID, true, []string{missing.ID})
	if err != nil {
		t.Fatal(err)
	}
	current := retryMembers(t, s, first.BatchID)
	if current[1].ID == missing.ID || current[1].State != "completed" || len(current[1].PreviousRunIDs) != 0 {
		t.Fatalf("invented attempt ancestry: %+v", current)
	}
	if seed := persistedExperiment(t, s, current[1].ID).Seed; seed != 42 {
		t.Fatalf("changed saved scenario seed: %d", seed)
	}
	f.agent.mu.Lock()
	defer f.agent.mu.Unlock()
	for _, node := range f.agent.nodes {
		if node.RunID == missing.ID {
			t.Fatal("synthetic identity reached Agent")
		}
	}
}

func TestRetryMissingLegacyShowsUnavailableInputs(t *testing.T) {
	for _, noScenario := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown-generated-seed", true: "no-saved-scenario"}[noScenario], func(t *testing.T) {
			f := newResumeFixture(t)
			f.fail.Store(false)
			raw := []byte(strings.Replace(resumeScenario, "seed: 42", "seed: 0", 1))
			if _, err := f.server.StartScenarioRepeated(t.Context(), raw, 2); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, f.server)
			if err := os.Remove(filepath.Join(f.server.config.DataDir, resultGroupsDirectory, runs[0].BatchID, batchReplayFile)); err != nil {
				t.Fatal(err)
			}
			removeMissingResult(t, f.server, runs[1].ID)
			if noScenario {
				removeMissingResult(t, f.server, runs[0].ID)
			}
			s := New(f.server.config, f.server.logger)
			f.register(t, s)
			listed := listedMissingResults(t, s)
			found := false
			for _, run := range listed {
				if run.Missing {
					found = true
					if !strings.Contains(run.RetryUnavailable, map[bool]string{false: "generated seed", true: "no saved scenario"}[noScenario]) {
						t.Fatalf("no useful reason: %+v", run)
					}
				}
			}
			if !found {
				t.Fatal("missing group disappeared instead of showing reason")
			}
			if _, err := s.RetryScenarioBatch(t.Context(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) {
				t.Fatalf("invented replay input: %v", err)
			}
		})
	}
}

func TestRetryMissingRejectsPartiallyRetainedGeneratedSeeds(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	raw := []byte(strings.Replace(resumeScenario, "seed: 42", "seed: 0", 1))
	if _, err := f.server.StartScenarioRepeated(t.Context(), raw, 3); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	replay, err := f.server.readBatchReplay(runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	delete(replay.Seeds, 3)
	root, err := f.server.resultGroupDirectory(runs[0].BatchID, false)
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, batchReplayFile, replay)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	removeMissingResult(t, f.server, runs[1].ID)
	removeMissingResult(t, f.server, runs[2].ID)
	if _, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) {
		t.Fatalf("invented a later missing seed: %v", err)
	}
	record, err := f.server.readBatchExtension(runs[0].BatchID)
	if err != nil || record.Current[2] != runs[1].ID || record.Current[3] != runs[2].ID || len(record.Pending) != 0 {
		t.Fatalf("partial retry changed admission: %+v %v", record, err)
	}
}

func TestRetryMissingFenceFailureDoesNotCommitNewAttempts(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	if _, err := f.server.StartScenarioRepeated(t.Context(), []byte(resumeScenario), 2); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	removeMissingResult(t, f.server, runs[1].ID)
	if err := os.WriteFile(filepath.Join(f.server.config.DataDir, lostResultWritesDirectory), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID); err == nil {
		t.Fatal("admitted without a durable old-ID fence")
	}
	record, err := f.server.readBatchExtension(runs[0].BatchID)
	if err != nil || record.Current[2] != runs[1].ID || len(record.Pending) != 0 {
		t.Fatalf("fence failure committed an orphan: %+v %v", record, err)
	}
}

func TestRetryMissingBlocksLiveAndUnreadableMembers(t *testing.T) {
	for _, mode := range []string{"running", "queued", "finalizing", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			f := newResumeFixture(t)
			f.fail.Store(false)
			if _, err := f.server.StartScenarioRepeated(t.Context(), []byte(resumeScenario), 2); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, f.server)
			if mode == "corrupt" {
				if err := os.WriteFile(filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[1].ID, "experiment.json"), []byte("{broken"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				removeMissingResult(t, f.server, runs[1].ID)
				f.server.state.mu.Lock()
				live := f.server.state.experiments[runs[1].ID]
				if mode != "finalizing" {
					live.State = mode
				}
				f.server.state.experiments[live.ID] = live
				f.server.state.mu.Unlock()
				if mode == "finalizing" {
					f.server.cancels[live.ID] = func() {}
					defer delete(f.server.cancels, live.ID)
				}
			}
			_, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID)
			want := errResultBusy
			if mode == "corrupt" {
				want = errBatchNotResumable
			}
			if !errors.Is(err, want) {
				t.Fatalf("unsafe retry admitted: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.server.config.DataDir, lostResultWritesDirectory, runs[1].ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed admission fenced source: %v", err)
			}
			record, err := f.server.readBatchExtension(runs[0].BatchID)
			if err != nil || record.Current[2] != runs[1].ID || len(record.Pending) != 0 {
				t.Fatalf("failed admission changed Current: %+v %v", record, err)
			}
		})
	}
}

func TestRetryMissingKeepsExistingBatchArtifactDownloadable(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	runs := completedAppendBatch(t, s, 2)
	if _, err := s.startBatchAnalysis(t.Context(), runs[0].BatchID, false); err != nil {
		t.Fatal(err)
	}
	job := awaitBatch(t, s, runs[0].BatchID)
	if job.State != "completed" {
		t.Fatalf("analysis failed: %+v", job)
	}
	artifact := "/api/v1/batch-analysis-jobs/" + runs[0].BatchID + "/result?jobId=" + job.ID + "&saved=1"
	before := resultRequest(s, "GET", artifact)
	for _, run := range runs {
		removeMissingResult(t, s, run.ID)
	}
	status := resultRequest(s, "GET", "/api/v1/batch-analysis-jobs/"+runs[0].BatchID)
	var current batchAnalysisStatus
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &current) != nil || current.ID != job.ID || !current.Stale {
		t.Fatalf("lost analysis status: %d %s", status.Code, status.Body)
	}
	after := resultRequest(s, "GET", artifact)
	if before.Code != 200 || after.Code != 200 || !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) {
		t.Fatalf("lost saved artifact: %d %d", before.Code, after.Code)
	}
}

func TestRetryMissingWriteFenceAcknowledgesLateIngressAndAllowsRestoredReads(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	if _, err := f.server.StartScenarioRepeated(t.Context(), []byte(resumeScenario), 2); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	id := runs[1].ID
	metadata, err := os.ReadFile(filepath.Join(f.server.config.DataDir, currentRunsDirectory, id, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	removeMissingResult(t, f.server, id)
	first, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	retryMembers(t, f.server, first.BatchID)
	s := New(f.server.config, f.server.logger)
	f.register(t, s)
	events := model.EventBatch{Events: []model.TraceEvent{{RunID: id, EventID: "late-after-loss", Type: "publish", MessageID: "late", Topic: "topic"}}}
	if err := s.state.appendEvents(events); err != nil {
		t.Fatalf("late ingress must be acknowledged: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late ingress recreated lost sources: %v", err)
	}
	// Restoration is an explicit filesystem operation, not a replay side effect.
	dir := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "experiment.json"), metadata, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "scenario.yaml"), []byte(resumeScenario), 0600); err != nil {
		t.Fatal(err)
	}
	listed := listedMissingResults(t, s)
	found := false
	for _, run := range listed {
		if run.ID == id {
			found = true
			if !run.Superseded || run.Missing {
				t.Fatalf("restored attempt not readable as history: %+v", run)
			}
		}
	}
	if !found {
		t.Fatal("write-only fence hid the restored attempt")
	}
	response := httptest.NewRecorder()
	s.handleResultDownload(response, httptest.NewRequest("GET", "/", nil), id)
	if response.Code != 200 {
		t.Fatalf("restored result cannot be downloaded: %d %s", response.Code, response.Body)
	}
	if err := s.state.appendEvents(events); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late ingress mutated restored result: %v", err)
	}
	if _, err := s.startAnalysisJob(t.Context(), id, false); err != nil {
		t.Fatalf("write-only fence blocked restored analysis: %v", err)
	}
	if job := awaitAnalysisJob(t, s, id); job.State != "completed" {
		t.Fatalf("restored analysis failed: %+v", job)
	}
}

func TestRetryMissingReplaySnapshotUsesScenarioLimit(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	base := "name: large-replay\nseed: 42\nphases: [{action: wait, duration: 1ms}]\n"
	raw := []byte(base + strings.Repeat("\n", min(resultMetadataLimit, scenarioYAMLLimit-len(base))))
	first, err := s.StartScenarioRepeated(t.Context(), raw, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, s)
	info, err := os.Stat(filepath.Join(s.config.DataDir, resultGroupsDirectory, first.BatchID, batchReplayFile))
	if err != nil || info.Size() <= resultMetadataLimit {
		t.Fatalf("large replay snapshot: %v %v", info, err)
	}
	removeMissingResult(t, s, first.ID)
	retry, err := s.RetryScenarioBatch(t.Context(), first.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range retryMembers(t, s, retry.BatchID) {
		if run.State != "completed" {
			t.Fatalf("large replay failed: %+v", run)
		}
	}
	saved, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, retry.ID, "scenario.yaml"))
	if err != nil || !bytes.Equal(saved, raw) {
		t.Fatalf("replay changed large source: %v", err)
	}
}

func TestRetryMissingDoesNotPreventDeletingResultWithMissingScenario(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained-snapshot", true: "legacy-no-snapshot"}[legacy], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			runs := completedAppendBatch(t, s, 2)
			if legacy {
				if err := os.Remove(filepath.Join(s.config.DataDir, resultGroupsDirectory, runs[0].BatchID, batchReplayFile)); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Remove(filepath.Join(s.config.DataDir, currentRunsDirectory, runs[0].ID, "scenario.yaml")); err != nil {
				t.Fatal(err)
			}
			if err := s.deleteSavedResult(runs[0].ID); err != nil {
				t.Fatalf("damaged result cannot be explicitly deleted: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, runs[0].ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("result was not deleted: %v", err)
			}
		})
	}
}

func TestRetryMissingRestoredWriteFenceRequiresFreshRetry(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 3)
	// Simulate a fence synced immediately before an interrupted admission.
	f.server.state.persistMu.Lock()
	err := f.server.markResultWriteFencedLocked(runs[1].ID)
	f.server.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.server.ResumeScenarioBatch(t.Context(), runs[0].BatchID); !errors.Is(err, errBatchNotResumable) || !strings.Contains(err.Error(), "fresh Run ID") {
		t.Fatalf("continued fenced identity: %v", err)
	}
	f.fail.Store(false)
	first, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range retryMembers(t, f.server, first.BatchID) {
		if run.State != "completed" || run.ID == runs[1].ID {
			t.Fatalf("fresh retry failed: %+v", run)
		}
	}
}

func TestRetryDeletedResultAndLegacyAppendBackfillSeeds(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	raw := []byte(strings.Replace(resumeScenario, "seed: 42", "seed: 0", 1))
	if _, err := f.server.StartScenarioRepeated(t.Context(), raw, 2); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	if err := os.Remove(filepath.Join(f.server.config.DataDir, resultGroupsDirectory, runs[0].BatchID, batchReplayFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.server.AppendScenarioBatch(t.Context(), runs[0].BatchID, 1, 2); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	replay, err := f.server.readBatchReplay(runs[0].BatchID)
	if err != nil || replay.Seeds[1] != runs[0].Seed || replay.Seeds[2] != runs[1].Seed || len(replay.Seeds) != 3 {
		t.Fatalf("append did not backfill seeds: %+v %v", replay, err)
	}
	if err := f.server.deleteSavedResult(runs[0].ID); err != nil {
		t.Fatal(err)
	}
	first, err := f.server.RetryScenarioBatch(t.Context(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Iteration != 1 || first.Seed != runs[0].Seed || first.Repetitions != 3 {
		t.Fatalf("deleted retry inputs changed: %+v", first)
	}
	for _, run := range retryMembers(t, f.server, first.BatchID) {
		if run.State != "completed" {
			t.Fatalf("deleted retry failed: %+v", run)
		}
	}
}

func TestRetryMissingRunsUsesOSWorkerCleanupWithoutRestoringLostSummary(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: missing-worker\nseed: 0\njobShutdownTimeout: 5s\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n")
	first, err := f.server.StartScenarioRepeated(t.Context(), raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	f.server.state.mu.RLock()
	var runs []model.Experiment
	for _, run := range f.server.state.experiments {
		if run.BatchID == first.BatchID {
			runs = append(runs, run)
		}
	}
	f.server.state.mu.RUnlock()
	sort.Slice(runs, func(i, j int) bool { return runs[i].Iteration < runs[j].Iteration })
	for _, run := range runs {
		if run.State != "completed" {
			t.Fatalf("initial run failed: %+v", run)
		}
	}
	removeMissingResult(t, f.server, runs[0].ID)
	// Leave a cached, stale summary too: cleanup must not try writing it into
	// the missing source directory or turn the lost attempt into a sample.
	f.server.state.mu.Lock()
	f.server.state.workerSummaries[runs[0].ID] = runWorkerSummary{RunID: runs[0].ID}
	f.server.state.mu.Unlock()
	retry, err := f.server.resumeScenarioBatchExpected(t.Context(), first.BatchID, true, []string{runs[0].ID})
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	result := persistedExperiment(t, f.server, retry.ID)
	if result.State != "completed" || result.CleanupState != "complete" || result.Seed != runs[0].Seed {
		t.Fatalf("worker retry failed: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[0].ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lost source/summary was recreated: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	fenced := false
	for _, id := range f.stops {
		if id == runs[0].ID {
			fenced = true
		}
	}
	if !fenced || !f.drained[runs[0].ID] || len(f.nodes) != 0 {
		t.Fatalf("missing run cleanup was skipped: stops=%v drained=%v", f.stops, f.drained)
	}
	for _, cmd := range f.commands {
		if cmd.ProcessState == nil {
			t.Fatal("OS worker not reaped")
		}
	}
}
