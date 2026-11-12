package controller

import (
	"encoding/json"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type incrementalBatchFixture struct {
	server   *Server
	original []model.Experiment
	status   batchAnalysisStatus
	result   batchAnalysisResult
}

func incrementalBatchAnalyze(t *testing.T, s *Server, id string, force bool) (batchAnalysisStatus, batchAnalysisResult) {
	t.Helper()
	if _, err := s.startBatchAnalysis(t.Context(), id, force); err != nil {
		t.Fatal(err)
	}
	status := awaitBatch(t, s, id)
	s.analysisWorkers.Wait()
	s.workWorkers.Wait()
	if status.State != "completed" {
		t.Fatalf("batch did not complete: %+v", status)
	}
	return status, readBatchArtifact(t, s, status.ResultURL)
}

func newIncrementalBatchFixture(t *testing.T, worker bool) incrementalBatchFixture {
	t.Helper()
	var s *Server
	if worker {
		s, _ = isolatedAnalysisServer(t)
	} else {
		s = New(ServerConfig{DataDir: t.TempDir()}, nil)
	}
	original := completedAppendBatch(t, s, 2)
	appendPublished(t, s, original[0].ID, 1)
	appendPublished(t, s, original[1].ID, 3)
	status, result := incrementalBatchAnalyze(t, s, original[0].BatchID, false)
	if status.AnalyzedRuns != 2 || status.ReusedRuns != 0 {
		t.Fatalf("first batch did not calculate both new Runs: %+v", status)
	}
	for _, run := range result.Runs {
		if run.Research == nil || run.Research.Overview == nil || run.Research.MessageCount == 0 {
			t.Fatalf("fixture does not exercise compact research data: %+v", run.Research)
		}
	}
	return incrementalBatchFixture{server: s, original: original, status: status, result: result}
}

func incrementalBatchRestart(t *testing.T, f incrementalBatchFixture) *Server {
	t.Helper()
	s := New(f.server.config, nil)
	s.workerCommand = f.server.workerCommand
	return s
}

func incrementalBatchAppendOne(t *testing.T, s *Server, id string) model.Experiment {
	t.Helper()
	// Exercise actual analysis workers independently from experiment-runtime
	// recovery: the old Runs have deliberately injected raw events and archives.
	workers := s.config.RunWorkers
	s.config.RunWorkers = false
	defer func() { s.config.RunWorkers = workers }()
	first, err := s.AppendScenarioBatch(t.Context(), id, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, s)
	appendPublished(t, s, first.ID, 5)
	return first
}

func incrementalBatchRuns(result batchAnalysisResult) map[string]resultAnalysis {
	runs := make(map[string]resultAnalysis, len(result.Runs))
	for _, run := range result.Runs {
		runs[run.Result.ID] = run
	}
	return runs
}

func assertIncrementalBatchMatchesFresh(t *testing.T, got, fresh batchAnalysisResult) {
	t.Helper()
	if !reflect.DeepEqual(got.Summary, fresh.Summary) || !reflect.DeepEqual(got.Reliability, fresh.Reliability) ||
		got.ExpectedRuns != fresh.ExpectedRuns || got.MissingRuns != fresh.MissingRuns || !reflect.DeepEqual(got.Excluded, fresh.Excluded) {
		t.Fatalf("incremental aggregation differs from a full calculation: got=%+v fresh=%+v", got.Summary, fresh.Summary)
	}
	current := incrementalBatchRuns(got)
	for _, run := range fresh.Runs {
		cached, ok := current[run.Result.ID]
		if !ok || !reflect.DeepEqual(cached.Metrics, run.Metrics) || !reflect.DeepEqual(cached.Research, run.Research) {
			t.Fatalf("incremental scientific output differs for Run %s", run.Result.ID)
		}
	}
}

func TestBatchIncrementalAppendReadsOnlyNewRunAndMatchesFullAnalysis(t *testing.T) {
	for _, worker := range []bool{false, true} {
		for _, seedOnly := range []bool{false, true} {
			name := map[bool]string{false: "inline", true: "worker"}[worker] + map[bool]string{false: "/saved-run-cache", true: "/legacy-batch-artifact"}[seedOnly]
			t.Run(name, func(t *testing.T) {
				f := newIncrementalBatchFixture(t, worker)
				for _, run := range f.original {
					archiveTestRun(t, f.server, run.ID)
				}
				if seedOnly {
					if err := os.RemoveAll(filepath.Join(f.server.config.DataDir, batchRunCacheDirectory)); err != nil {
						t.Fatal(err)
					}
				}
				s := incrementalBatchRestart(t, f)
				incrementalBatchAppendOne(t, s, f.status.BatchID)
				var archiveReads atomic.Int32
				s.archiveIOCheck = func() { archiveReads.Add(1) }
				status, got := incrementalBatchAnalyze(t, s, f.status.BatchID, false)
				if status.ReusedRuns != 2 || status.AnalyzedRuns != 1 || archiveReads.Load() != 0 {
					t.Fatalf("extension reread existing archived Runs: reused=%d analyzed=%d archive=%d", status.ReusedRuns, status.AnalyzedRuns, archiveReads.Load())
				}
				if len(got.Runs) != 3 || got.ExpectedRuns != 3 || got.Summary["metrics.published"].Average == nil || *got.Summary["metrics.published"].Average != 3 {
					t.Fatalf("extended batch did not aggregate all three Runs: %+v", got.Summary["metrics.published"])
				}
				current := incrementalBatchRuns(got)
				for _, old := range f.result.Runs {
					run := current[old.Result.ID]
					if run.Result.Repetitions != 3 || !run.AsOf.Equal(old.AsOf) || !reflect.DeepEqual(run.Research, old.Research) {
						t.Fatalf("reusing %s lost old overview or current group metadata", old.Result.ID)
					}
				}
				forced, fresh := incrementalBatchAnalyze(t, s, f.status.BatchID, true)
				if forced.ReusedRuns != 0 || forced.AnalyzedRuns != 3 || archiveReads.Load() == 0 {
					t.Fatalf("explicit reanalysis failed to read every contributor: %+v archive=%d", forced, archiveReads.Load())
				}
				assertIncrementalBatchMatchesFresh(t, got, fresh)
				for _, run := range fresh.Runs {
					if !run.AsOf.After(current[run.Result.ID].AsOf) {
						t.Fatalf("forced reanalysis retained a cached boundary for %s", run.Result.ID)
					}
				}
			})
		}
	}
}

func TestBatchIncrementalLateEventRecalculatesOnlyAffectedRun(t *testing.T) {
	f := newIncrementalBatchFixture(t, false)
	s := incrementalBatchRestart(t, f)
	incrementalBatchAppendOne(t, s, f.status.BatchID)
	_, before := incrementalBatchAnalyze(t, s, f.status.BatchID, false)
	changed := f.original[0].ID
	if err := s.state.persistEvents(changed, []model.TraceEvent{{RunID: changed, EventID: "late-new-event", NodeID: "publisher", MessageID: "late-message", Type: "publish", Topic: "topic", Timestamp: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	status, got := incrementalBatchAnalyze(t, s, f.status.BatchID, false)
	if status.ReusedRuns != 2 || status.AnalyzedRuns != 1 {
		t.Fatalf("late evidence invalidated unrelated Run calculations: %+v", status)
	}
	previous := incrementalBatchRuns(before)
	for _, run := range got.Runs {
		if run.Result.ID == changed {
			if run.Metrics.Published != 2 || !run.AsOf.After(previous[changed].AsOf) {
				t.Fatalf("late evidence was omitted from the changed Run: %+v", run.Metrics)
			}
		} else if !run.AsOf.Equal(previous[run.Result.ID].AsOf) {
			t.Fatalf("unchanged Run %s was recalculated", run.Result.ID)
		}
	}
	if mean := got.Summary["metrics.published"].Average; mean == nil || math.Abs(*mean-10.0/3) > 1e-9 {
		t.Fatalf("updated aggregate lost late publication: %v", mean)
	}
}

func TestBatchIncrementalDamagedCacheFallsBackOnlyForThatRun(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			f := newIncrementalBatchFixture(t, false)
			for _, run := range f.original {
				archiveTestRun(t, f.server, run.ID)
			}
			s := incrementalBatchRestart(t, f)
			incrementalBatchAppendOne(t, s, f.status.BatchID)
			root := filepath.Join(s.config.DataDir, batchRunCacheDirectory, f.original[0].ID)
			if damage == "missing" {
				if err := os.RemoveAll(root); err != nil {
					t.Fatal(err)
				}
			} else {
				data, err := os.ReadFile(filepath.Join(root, batchRunCacheIndexFile))
				if err != nil {
					t.Fatal(err)
				}
				var index struct{ ArtifactSlot int }
				if err := json.Unmarshal(data, &index); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, analysisArtifactFile(batchResultFile, index.ArtifactSlot)), []byte("broken cache\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// With no old aggregate available to seed this entry, its own raw
			// source must be used while the healthy sibling remains reusable.
			if err := os.Remove(filepath.Join(s.config.DataDir, "batch-analyses", f.status.BatchID, analysisArtifactFile(batchResultFile, f.status.ArtifactSlot))); err != nil {
				t.Fatal(err)
			}
			status, got := incrementalBatchAnalyze(t, s, f.status.BatchID, false)
			if status.ReusedRuns != 1 || status.AnalyzedRuns != 2 {
				t.Fatalf("damaged cache either aborted analysis or invalidated healthy siblings: %+v", status)
			}
			previous, current := incrementalBatchRuns(f.result), incrementalBatchRuns(got)
			if id := f.original[1].ID; !current[id].AsOf.Equal(previous[id].AsOf) {
				t.Fatal("healthy sibling was recalculated")
			}
		})
	}
}

func TestBatchIncrementalFailurePreservesSavedAggregateAndCompletedRunCaches(t *testing.T) {
	f := newIncrementalBatchFixture(t, false)
	s := incrementalBatchRestart(t, f)
	added := incrementalBatchAppendOne(t, s, f.status.BatchID)
	original := resultRequest(s, http.MethodGet, f.status.ResultURL)
	if original.Code != http.StatusOK {
		t.Fatal(original.Body)
	}
	eventPath := filepath.Join(s.config.DataDir, currentRunsDirectory, added.ID, "events.jsonl")
	if err := os.WriteFile(eventPath, []byte("invalid event\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startBatchAnalysis(t.Context(), f.status.BatchID, false); err != nil {
		t.Fatal(err)
	}
	failed := awaitBatch(t, s, f.status.BatchID)
	s.analysisWorkers.Wait()
	s.workWorkers.Wait()
	if failed.State != "failed" {
		t.Fatalf("invalid added input unexpectedly completed: %+v", failed)
	}
	saved := resultRequest(s, http.MethodGet, f.status.ResultURL+"&saved=1")
	if saved.Code != http.StatusOK || saved.Body.String() != original.Body.String() {
		t.Fatalf("failed extension analysis replaced the old published aggregate: %d", saved.Code)
	}
	if err := os.WriteFile(eventPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	appendPublished(t, s, added.ID, 5)
	status, _ := incrementalBatchAnalyze(t, s, f.status.BatchID, false)
	if status.ReusedRuns != 2 || status.AnalyzedRuns != 1 {
		t.Fatalf("retry lost previously completed Run caches: %+v", status)
	}
}

func TestRemoteIncrementalBatchStreamsCachedAnalysesInsteadOfOldRawResults(t *testing.T) {
	f := newIncrementalBatchFixture(t, true)
	for _, run := range f.original {
		archiveTestRun(t, f.server, run.ID)
	}
	s := incrementalBatchRestart(t, f)
	incrementalBatchAppendOne(t, s, f.status.BatchID)
	var archiveReads atomic.Int32
	s.archiveIOCheck = func() { archiveReads.Add(1) }
	entered, release, reaped := gatedAnalysisWorker(t, s)
	local := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, local.entered, "local analysis slot")
	if _, err := s.startBatchAnalysis(t.Context(), f.status.BatchID, false); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, entered, "remote incremental analysis")
	release()
	status := awaitBatch(t, s, f.status.BatchID)
	s.analysisWorkers.Wait()
	if status.State != "completed" || status.ReusedRuns != 2 || status.AnalyzedRuns != 1 || archiveReads.Load() != 0 || !reaped() {
		t.Fatalf("remote cache reuse failed or read old NAS sources: %+v reads=%d", status, archiveReads.Load())
	}
	local.once.Do(func() { close(local.release) })
	queueTestAwait(t, local.finished, "local analysis release")
	s.workWorkers.Wait()
	got := readBatchArtifact(t, s, status.ResultURL)
	if got.Summary["metrics.published"].Average == nil || *got.Summary["metrics.published"].Average != 3 {
		t.Fatal("remote cache reuse omitted a contributing Run")
	}
}
