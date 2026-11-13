package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// A panic under the inventory/persistence locks used to be hidden by idle
// maintenance's deferred lock acquisition. Recover only in this isolated,
// single-threaded fixture so the old failure reports promptly instead of
// leaving the complete package test run deadlocked during cleanup.
func retireColdRunForTest(t *testing.T, s *Server, id string) (err error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			stateHeld := !s.state.mu.TryLock()
			s.state.mu.Unlock()
			persistenceHeld := !s.state.persistMu.TryLock()
			s.state.persistMu.Unlock()
			t.Fatalf("cold Run retirement panicked: %v (state lock held=%t, persistence lock held=%t)", recovered, stateHeld, persistenceHeld)
		}
	}()
	return s.retireRunRuntime(t.Context(), id)
}

func TestColdRestartRetirementPreservesDurableSummaryAndAPI(t *testing.T) {
	for _, mode := range []struct {
		name              string
		retired, complete bool
	}{
		{"final-summary-before-retirement", false, true},
		{"legacy-saved-counters", false, false},
		{"already-retired-summary", true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			original, id, before := summaryFreshnessFixture(t, time.Time{})
			before.Retired = mode.retired
			before.CountersComplete = mode.complete
			before.Counters = []workerCounterSeries{{Family: "events", Labels: []string{id, "agent", "published", "topic"}, Value: 7}}
			if mode.retired {
				before.Nodes = []workerNodeCount{{AgentID: "agent", Group: "workers", Role: "full", Type: "full", State: string(model.NodeStopped), Count: 3}}
			}
			root, err := os.OpenRoot(filepath.Join(original.config.DataDir, currentRunsDirectory, id))
			if err != nil {
				t.Fatal(err)
			}
			err = writeAnalysisJSON(root, workerSummaryFile, before)
			root.Close()
			if err != nil {
				t.Fatal(err)
			}

			// Production startup loads refresh obligations, while historical
			// summary payloads and counters remain only in local durable files.
			restarted := New(original.config, original.logger)
			restarted.state.experiments[id] = original.state.experiments[id]
			if err := restarted.loadWorkerSummaries(); err != nil {
				t.Fatal(err)
			}
			if len(restarted.state.workerSummaries) != 0 || len(restarted.state.metrics.sealed) != 0 {
				t.Fatal("cold startup unexpectedly retained historical projections in memory")
			}
			if err := retireColdRunForTest(t, restarted, id); err != nil {
				t.Fatal(err)
			}
			after, err := restarted.state.readWorkerSummary(id)
			if err != nil {
				t.Fatal(err)
			}
			if !after.Retired || !reflect.DeepEqual(before.Metrics, after.Metrics) || !before.At.Equal(after.At) {
				t.Fatal("cold retirement changed durable metrics or observation time")
			}
			if !reflect.DeepEqual(before.Counters, after.Counters) {
				t.Fatalf("cold retirement changed durable counters: before=%+v after=%+v", before.Counters, after.Counters)
			}
			if !reflect.DeepEqual(before.Nodes, after.Nodes) {
				t.Fatalf("cold retirement changed durable node counts: before=%+v after=%+v", before.Nodes, after.Nodes)
			}
			if len(restarted.state.workerSummaries) != 0 || len(restarted.state.metrics.sealed) != 0 {
				t.Fatal("cold retirement retained historical projections in the parent heap")
			}
			if done, err := restarted.isRuntimeRetired(id); err != nil || !done {
				t.Fatalf("cold retirement did not preserve a durable fence: done=%t err=%v", done, err)
			}
			response := httptest.NewRecorder()
			restarted.handlePrometheusAgentTargets(response, httptest.NewRequest(http.MethodGet, "/api/v1/prometheus/agent-targets", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("shared-state API unavailable after recovery: status=%d body=%s", response.Code, response.Body)
			}
		})
	}
}

// Exercise the real maintenance defers in an isolated OS process. Before the
// fix a nil-map panic retained state.mu, then endRunMetricMaintenance blocked
// while unwinding that panic. The timeout kills only this test helper.
func TestColdRestartIdleCleanupKeepsSharedStateResponsive(t *testing.T) {
	const helper = "cold-restart-idle-cleanup"
	if os.Args[len(os.Args)-1] == helper {
		original, id, summary := summaryFreshnessFixture(t, time.Time{})
		summary.Retired = false
		root, err := os.OpenRoot(filepath.Join(original.config.DataDir, currentRunsDirectory, id))
		if err != nil {
			t.Fatal(err)
		}
		err = writeAnalysisJSON(root, workerSummaryFile, summary)
		root.Close()
		if err != nil {
			t.Fatal(err)
		}
		original.updateExperiment(id, func(run *model.Experiment) { run.CleanupState = "pending" })
		restarted := New(original.config, original.logger)
		if err := restarted.loadWorkerSummaries(); err != nil {
			t.Fatal(err)
		}
		if !restarted.tryRefreshIdleRunMetrics(t.Context(), id) {
			t.Fatal("idle maintenance did not acquire the recovered Run")
		}
		summaryBoundaryAssertReleased(t, restarted)
		response := httptest.NewRecorder()
		restarted.handlePrometheusAgentTargets(response, httptest.NewRequest(http.MethodGet, "/api/v1/prometheus/agent-targets", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("shared-state API failed after idle cleanup: %d", response.Code)
		}
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestColdRestartIdleCleanupKeepsSharedStateResponsive$", "-test.timeout=8s", "--", helper)
	command.Env = append(os.Environ(), "TMPDIR="+t.TempDir())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("cold restart idle cleanup failed or deadlocked: %v\n%s", err, output)
	}
}
