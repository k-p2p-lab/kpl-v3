package controller

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func summaryFreshnessFixture(t *testing.T, deadline time.Time) (*Server, string, runWorkerSummary) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "run-summary-freshness"
	run, _ := resultFixture(t, s, id, "completed", time.Now().UTC())
	s.state.experiments[id] = run
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "original-event")}}); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	current, err := sourceRevisionAt(root, id)
	if err != nil {
		t.Fatal(err)
	}
	events, cursor, err := eventInputAt(root, id)
	if err != nil {
		t.Fatal(err)
	}
	summary := runWorkerSummary{RunID: id, Metrics: model.Metrics{RunID: id, Published: 7}, At: time.Now().UTC(), SourceRevision: current, EventRevision: events, Cursor: cursor, Retired: true, NextRefreshAt: deadline, Counters: []workerCounterSeries{{Family: "events", Labels: []string{id, "agent", "publish", "topic"}, Value: 7}}, CountersComplete: true, EventsComplete: true}
	if err := writeAnalysisJSON(root, workerSummaryFile, summary); err != nil {
		t.Fatal(err)
	}
	return s, id, summary
}

func requireSummaryProjectionUnchanged(t *testing.T, before, after runWorkerSummary) {
	t.Helper()
	after.SourceRevision = before.SourceRevision
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("metadata rebase changed the scientific projection: before=%+v after=%+v", before, after)
	}
}

func TestRetiredSummaryMetadataRebasePreservesProjectionAndDeadline(t *testing.T) {
	for _, future := range []bool{false, true} {
		name := "no-deadline"
		var deadline time.Time
		if future {
			name, deadline = "future-deadline", time.Now().Add(time.Hour).UTC()
		}
		t.Run(name, func(t *testing.T) {
			s, id, before := summaryFreshnessFixture(t, deadline)
			s.updateExperiment(id, func(run *model.Experiment) { run.Name = "metadata correction" })
			after, ok := s.state.workerMetricSummary(id)
			current, err := s.runSourceRevision(id)
			if err != nil || !ok || after.SourceRevision != current || after.SourceRevision == before.SourceRevision || s.state.summaryDirty[id] || !s.state.summaryDue[id].Equal(deadline) {
				t.Fatalf("metadata-only correction remained stale or lost its deadline: summary=%+v dirty=%v due=%v err=%v", after, s.state.summaryDirty[id], s.state.summaryDue[id], err)
			}
			requireSummaryProjectionUnchanged(t, before, after)
			if len(s.state.workerSummaries) != 0 {
				t.Fatal("rebase retained a historical summary in the parent heap")
			}
		})
	}
}

func TestRetiredSummaryRebaseRequiresUnchangedEventsAndUnexpiredWindow(t *testing.T) {
	for _, mode := range []string{"append", "same-size-replacement", "same-inode-rewrite", "legacy", "incomplete-binding", "wrong-cursor", "expired-window"} {
		t.Run(mode, func(t *testing.T) {
			var deadline time.Time
			if mode == "expired-window" {
				deadline = time.Now().Add(-time.Second).UTC()
			}
			s, id, before := summaryFreshnessFixture(t, deadline)
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			path := filepath.Join(directory, "events.jsonl")
			switch mode {
			case "append":
				if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "late-event")}}); err != nil {
					t.Fatal(err)
				}
			case "same-size-replacement", "same-inode-rewrite":
				original, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				changed := bytes.ReplaceAll(original, []byte("original-event"), []byte("replaced-event"))
				if len(changed) != len(original) || bytes.Equal(original, changed) {
					t.Fatal("same-size event mutation fixture failed")
				}
				if mode == "same-size-replacement" {
					err = writeFileAtomic(path, changed, 0644)
				} else {
					err = os.WriteFile(path, changed, 0644)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "legacy", "incomplete-binding", "wrong-cursor":
				if mode == "legacy" {
					before.EventRevision = ""
				} else if mode == "incomplete-binding" {
					before.EventsComplete = false
				} else {
					before.Cursor--
				}
				root, err := os.OpenRoot(directory)
				if err != nil {
					t.Fatal(err)
				}
				err = writeAnalysisJSON(root, workerSummaryFile, before)
				root.Close()
				if err != nil {
					t.Fatal(err)
				}
			}
			s.updateExperiment(id, func(run *model.Experiment) { run.Name = "metadata after pending correction" })
			after, ok := s.state.workerMetricSummary(id)
			if !ok || !s.state.summaryDirty[id] || !reflect.DeepEqual(before, after) {
				t.Fatalf("pending event/window correction was hidden or previous summary replaced: mode=%s before=%+v after=%+v dirty=%v", mode, before, after, s.state.summaryDirty[id])
			}
		})
	}
}

func TestRetiredSummaryMetadataRebaseSurvivesRestart(t *testing.T) {
	deadline := time.Now().Add(time.Hour).UTC()
	s, id, before := summaryFreshnessFixture(t, deadline)
	run := s.state.experiments[id]
	run.Name = "persisted just before Controller restart"
	s.state.persistMu.Lock()
	err := s.persistExperiment(run)
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(ServerConfig{DataDir: s.config.DataDir, RunWorkers: true}, nil)
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	after, ok := restarted.state.workerMetricSummary(id)
	current, err := restarted.runSourceRevision(id)
	if err != nil || !ok || after.SourceRevision != current || restarted.state.summaryDirty[id] || !restarted.state.summaryDue[id].Equal(deadline) {
		t.Fatalf("restart did not rebase metadata-only drift: summary=%+v dirty=%v err=%v", after, restarted.state.summaryDirty[id], err)
	}
	requireSummaryProjectionUnchanged(t, before, after)
}

func TestRetiredSummaryRestartDetectsStaleEventRevisionEvenWithCurrentSource(t *testing.T) {
	s, id, before := summaryFreshnessFixture(t, time.Time{})
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "late-event")}}); err != nil {
		t.Fatal(err)
	}
	// A partial live summary can carry the current metadata revision while
	// still explicitly describing an earlier committed event boundary.
	before.SourceRevision = ""   // A live worker does not claim a full-file snapshot.
	before.EventsComplete = true // A child cannot self-certify this stale claim.
	if err := s.publishWorkerSummary(before); err != nil {
		t.Fatal(err)
	}
	stored, ok := s.state.workerMetricSummary(id)
	if !ok || stored.EventsComplete || !s.state.summaryDirty[id] {
		t.Fatal("parent accepted a child-supplied complete flag for stale events")
	}
	restarted := New(ServerConfig{DataDir: s.config.DataDir, RunWorkers: true}, nil)
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	after, ok := restarted.state.workerMetricSummary(id)
	if !ok || !restarted.state.summaryDirty[id] || !reflect.DeepEqual(stored, after) {
		t.Fatal("restart treated a matching broad source revision as proof of complete events")
	}
}

func TestRetiredSummaryFailedRebasePreservesPreviousArtifact(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("filesystem write permissions cannot fail under root")
	}
	s, id, before := summaryFreshnessFixture(t, time.Time{})
	run := s.state.experiments[id]
	run.Name = "metadata before unavailable summary storage"
	s.state.persistMu.Lock()
	err := s.persistExperiment(run)
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	path := filepath.Join(directory, workerSummaryFile)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	s.state.summaryDirty[id] = true
	if err := os.Chmod(directory, 0500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(directory, 0700)
	s.state.persistMu.Lock()
	rebased, rebaseErr := s.state.rebaseRetiredWorkerSummaryLocked(id, time.Now().UTC())
	s.state.persistMu.Unlock()
	retained, err := os.ReadFile(path)
	after, ok := s.state.workerMetricSummary(id)
	if rebased || rebaseErr == nil || err != nil || !bytes.Equal(original, retained) || !ok || !reflect.DeepEqual(before, after) || !s.state.summaryDirty[id] {
		t.Fatalf("failed rebase destroyed a completed projection or hid work: rebased=%v error=%v dirty=%v read=%v", rebased, rebaseErr, s.state.summaryDirty[id], err)
	}
}

func TestFinalRunMetadataKeepsInlineSummaryFreshWithoutRecalculation(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run, err := f.server.StartScenario(t.Context(), []byte("version: 4\nname: inline-summary\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	summary, ok := f.server.state.workerMetricSummary(run.ID)
	current, err := f.server.runSourceRevision(run.ID)
	if err != nil || !ok || !summary.Retired || !summary.EventsComplete || !strings.HasPrefix(summary.EventRevision, "events-v1:") || summary.SourceRevision != current || f.server.state.summaryDirty[run.ID] {
		t.Fatalf("final metadata invalidated its inline summary: summary=%+v dirty=%v err=%v", summary, f.server.state.summaryDirty[run.ID], err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.starts) != 1 || f.starts[0] != "controller-run" {
		t.Fatalf("metadata finalization reconstructed the journal in an extra worker: %v", f.starts)
	}
}

func TestRecomputedSummaryRetainsParentEventIdentityAcrossWorkerIPC(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run, err := f.server.StartScenario(t.Context(), []byte("version: 4\nname: corrected-summary\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	if err := f.server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(run.ID, "late-for-reconstruction")}}); err != nil {
		t.Fatal(err)
	}
	if err := f.server.computeWorkerSummary(t.Context(), run.ID); err != nil {
		t.Fatal(err)
	}
	summary, ok := f.server.state.workerMetricSummary(run.ID)
	root, err := os.OpenRoot(filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID))
	if err != nil {
		t.Fatal(err)
	}
	currentEvents, eventErr := eventRevisionAt(root, run.ID)
	root.Close()
	if eventErr != nil || !ok || summary.EventRevision != currentEvents || f.server.state.summaryDirty[run.ID] {
		t.Fatalf("summary worker lost the parent file-identity evidence: eventRevision=%s expected=%s dirty=%v err=%v", summary.EventRevision, currentEvents, f.server.state.summaryDirty[run.ID], eventErr)
	}
	f.server.updateExperiment(run.ID, func(run *model.Experiment) { run.Name = "metadata after event correction" })
	after, ok := f.server.state.workerMetricSummary(run.ID)
	if !ok || f.server.state.summaryDirty[run.ID] {
		t.Fatal("reconstructed summary cannot rebase a later metadata-only update")
	}
	requireSummaryProjectionUnchanged(t, summary, after)
}

func TestArchivedCompleteSummaryRetagsWithoutNASReadOrExtraWorkerAfterRestart(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run, err := f.server.StartScenario(t.Context(), []byte("version: 4\nname: archived-event-binding\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	before, ok := f.server.state.workerMetricSummary(run.ID)
	if !ok || !before.EventsComplete {
		t.Fatal("fixture did not seal a complete event binding")
	}
	archiveTestRun(t, f.server, run.ID)
	archive := filepath.Join(f.server.config.DataDir, archivedRunsDirectory)
	if err := os.Rename(archive, archive+"-offline"); err != nil {
		t.Fatal(err)
	}
	restarted := New(ServerConfig{DataDir: f.server.config.DataDir, RunWorkers: true}, nil)
	restarted.workerCommand = f.server.workerCommand
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	after, ok := restarted.state.workerMetricSummary(run.ID)
	if !ok || after.EventRevision == before.EventRevision || after.SourceRevision != before.SourceRevision || !after.EventsComplete || len(restarted.state.workerSummaryDue(time.Now().UTC())) != 0 {
		t.Fatalf("archive move made an unchanged complete source require NAS reconstruction: summary=%+v dirty=%v", after, restarted.state.summaryDirty)
	}
	before.EventRevision = after.EventRevision
	requireSummaryProjectionUnchanged(t, before, after)
	// Even an explicit idle maintenance pass must use the local complete
	// binding rather than launch a summary process or open the offline NAS.
	if !restarted.tryRefreshIdleRunMetrics(t.Context(), run.ID) {
		t.Fatal("fixture could not enter idle maintenance")
	}
	f.mu.Lock()
	starts := append([]string(nil), f.starts...)
	f.mu.Unlock()
	if len(starts) != 1 || starts[0] != "controller-run" || len(restarted.state.workerSummaryDue(time.Now().UTC())) != 0 {
		t.Fatalf("unchanged archived source launched extra computation: starts=%v dirty=%v", starts, restarted.state.summaryDirty)
	}
	// The new physical marker also supports later metadata-only updates.
	restarted.state.experiments[run.ID] = f.server.state.experiments[run.ID]
	restarted.updateExperiment(run.ID, func(run *model.Experiment) { run.Name = "metadata after archive retag" })
	metadata, ok := restarted.state.workerMetricSummary(run.ID)
	if !ok || restarted.state.summaryDirty[run.ID] {
		t.Fatal("metadata-only update after archive retag required unavailable NAS data")
	}
	requireSummaryProjectionUnchanged(t, after, metadata)
}
