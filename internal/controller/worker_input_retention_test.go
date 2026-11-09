package controller

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestWorkerTerminalHistoryReleasesLargeInputAndRecoversItForAppendAndRetry(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: retained-input\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n# " + strings.Repeat("x", 512<<10) + "\n")
	first, err := f.server.StartScenarioRepeated(t.Context(), raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	verify := func(want int) {
		t.Helper()
		f.server.state.mu.RLock()
		runs := make([]model.Experiment, 0, len(f.server.state.experiments))
		for _, run := range f.server.state.experiments {
			runs = append(runs, run)
		}
		f.server.state.mu.RUnlock()
		if len(runs) != want {
			t.Fatalf("expected %d attempts, got %d", want, len(runs))
		}
		for _, run := range runs {
			if run.State == "running" || run.State == "queued" || run.ScenarioYAML != "" {
				t.Fatalf("terminal runtime retained its launch input: %s state=%s retained=%d", run.ID, run.State, len(run.ScenarioYAML))
			}
			source, err := os.ReadFile(filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID, "scenario.yaml"))
			if err != nil || !bytes.Equal(source, raw) {
				t.Fatalf("input release changed durable source for %s: %v", run.ID, err)
			}
		}
	}
	verify(2)
	if _, err := f.server.AppendScenarioBatch(t.Context(), first.BatchID, 1, 2); err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	verify(3)
	// Retry admission must use the preserved input file even though the parent
	// no longer retains any full YAML for this terminal attempt.
	f.server.updateExperiment(first.ID, func(run *model.Experiment) {
		run.State, run.Error, run.DataState = "failed", "fixture terminal failure", "incomplete"
	})
	if _, err := f.server.RetryScenarioBatch(t.Context(), first.BatchID); err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	verify(4)
}
