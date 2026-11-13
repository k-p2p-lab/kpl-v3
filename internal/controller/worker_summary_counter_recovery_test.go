package controller

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func counterRecoveryRawSources(t *testing.T, directory string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	for _, name := range analysisSourceNames {
		data, err := os.ReadFile(filepath.Join(directory, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		files[name] = data
	}
	return files
}

func TestRetiredSummaryCounterLossReplaysOriginalEventsAfterRestart(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "legacy-counter-binding"
		if complete {
			name = "complete-counter-binding"
		}
		t.Run(name, func(t *testing.T) {
			f := newControllerWorkerFixture(t)
			run := summaryBoundaryCompleteRun(t, f)
			before, err := f.server.state.readWorkerSummary(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if before.Metrics.Published != 1 || len(before.Counters) == 0 || !before.Retired || !before.EventsComplete {
				t.Fatalf("fixture lacks a completed publication and sealed counters: %+v", before)
			}
			directory := filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID)
			raw := counterRecoveryRawSources(t, directory)
			// Simulate the previous cold-retirement write, which left the
			// scientific summary intact but discarded its durable counters.
			damaged := before
			damaged.Counters = nil
			damaged.CountersComplete = complete
			root, err := os.OpenRoot(directory)
			if err != nil {
				t.Fatal(err)
			}
			err = writeAnalysisJSON(root, workerSummaryFile, damaged)
			root.Close()
			if err != nil {
				t.Fatal(err)
			}

			restarted := New(f.server.config, f.server.logger)
			restarted.workerCommand = f.server.workerCommand
			if err := restarted.loadWorkerSummaries(); err != nil {
				t.Fatal(err)
			}
			if !restarted.state.summaryDirty[run.ID] || len(restarted.state.workerSummaries) != 0 {
				t.Fatal("restart reused missing counters or retained a historical summary in memory")
			}
			if !restarted.tryRefreshIdleRunMetrics(t.Context(), run.ID) {
				t.Fatal("counter recovery did not claim idle maintenance")
			}
			summaryBoundaryAssertReleased(t, restarted)
			after, err := restarted.state.readWorkerSummary(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !after.CountersComplete || !after.EventsComplete || !after.Retired || len(after.Counters) == 0 {
				t.Fatalf("counter recovery did not publish a complete durable projection: %+v", after)
			}
			if !reflect.DeepEqual(before.Metrics, after.Metrics) || !reflect.DeepEqual(before.Nodes, after.Nodes) {
				t.Fatal("counter replay changed the saved scientific metrics or terminal Node evidence")
			}
			if !reflect.DeepEqual(raw, counterRecoveryRawSources(t, directory)) {
				t.Fatal("counter recovery rewrote original source files")
			}
			if restarted.state.summaryDirty[run.ID] || len(restarted.state.workerSummaries) != 0 || len(restarted.state.metrics.sealed) != 0 || len(restarted.state.runMetrics) != 0 {
				t.Fatal("counter recovery retained correction work or historical runtime projections")
			}
			// Verify the value through the monitoring consumer, including a
			// subsequent restart whose memory cannot mask a missing artifact.
			for _, view := range []*Server{restarted, New(f.server.config, f.server.logger)} {
				metrics := gatherTestMetrics(t, view.state)
				requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": run.ID, "agent_id": "agent", "event_type": "publish", "topic": "topic"}, 1)
				if response := resultRequest(view, http.MethodGet, "/api/v1/prometheus/agent-targets"); response.Code != http.StatusOK {
					t.Fatalf("shared-state API unavailable after counter recovery: %d", response.Code)
				}
			}
			// A repaired binding must return to the normal cheap reuse path.
			if !restarted.tryRefreshIdleRunMetrics(t.Context(), run.ID) {
				t.Fatal("repaired Run could not enter an idle verification pass")
			}
			summaryBoundaryAssertReleased(t, restarted)
			f.mu.Lock()
			starts := append([]string(nil), f.starts...)
			f.mu.Unlock()
			if !reflect.DeepEqual(starts, []string{"controller-run", "controller-summary"}) {
				t.Fatalf("counter recovery did not use exactly one isolated calculation: %v", starts)
			}
		})
	}
}

func TestRetiredSummaryCounterLossKeepsPreviousArtifactWhenSourceUnavailable(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run := summaryBoundaryCompleteRun(t, f)
	archiveTestRun(t, f.server, run.ID)
	archive := filepath.Join(f.server.config.DataDir, archivedRunsDirectory)
	if err := os.Rename(archive, archive+"-offline"); err != nil {
		t.Fatal(err)
	}
	summary, err := f.server.state.readWorkerSummary(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	summary.Counters = nil
	directory := filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID)
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, workerSummaryFile, summary)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, workerSummaryFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(f.server.config, f.server.logger)
	restarted.workerCommand = f.server.workerCommand
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	if !restarted.state.summaryDirty[run.ID] {
		t.Fatal("unavailable original source hid the missing-counter correction")
	}
	if !restarted.tryRefreshIdleRunMetrics(t.Context(), run.ID) {
		t.Fatal("unavailable-source correction did not acquire idle maintenance")
	}
	summaryBoundaryAssertReleased(t, restarted)
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || !restarted.state.summaryDirty[run.ID] {
		t.Fatalf("failed correction changed the previous artifact or discarded retry work: %v", err)
	}
	if response := resultRequest(restarted, http.MethodGet, "/api/v1/prometheus/agent-targets"); response.Code != http.StatusOK {
		t.Fatalf("shared-state API unavailable after source failure: %d", response.Code)
	}
}

func TestRetiredSummaryWithoutPublicationsCanReuseEmptyCounters(t *testing.T) {
	s, id, summary := summaryFreshnessFixture(t, time.Time{})
	summary.Metrics.Published = 0
	summary.Counters = nil
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, workerSummaryFile, summary)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, s.logger)
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	if restarted.state.summaryDirty[id] {
		t.Fatal("an empty publication population forced repeated counter reconstruction")
	}
}
