package controller

import (
	"context"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTimingRunMetricMaintenanceSuppressesAndRestoresFIFOForecast(t *testing.T) {
	s := newState(t.TempDir())
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	short := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
	long := parsedTimingPlan(t, "phases: [{action: wait, duration: 30s}]")
	runs := []model.Experiment{
		{ID: "next-batch-2", BatchID: "next", ExecutionID: "next-execution", State: "queued", Iteration: 2, Repetitions: 2},
		{ID: "later-submission", BatchID: "later", ExecutionID: "later-execution", State: "queued", QueuePosition: 1},
		{ID: "next-batch-1", BatchID: "next", ExecutionID: "next-execution", State: "queued", Iteration: 1, Repetitions: 2},
	}
	for _, run := range runs {
		plan := short
		if run.ID == "later-submission" {
			plan = long
		}
		s.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
	}
	want := []time.Duration{20 * time.Second, 50 * time.Second, 10 * time.Second}
	checkForecast := func(at time.Time) {
		t.Helper()
		s.estimateRunFinishesLocked(runs, at)
		for i, run := range runs {
			if run.Timing == nil || !run.Timing.EstimatedFinishAt.Equal(at.Add(want[i])) || run.Timing.RemainingSeconds != want[i].Seconds() {
				t.Fatalf("maintenance-free FIFO estimate for %s = %+v; want +%s", run.ID, run.Timing, want[i])
			}
			if !run.StartedAt.IsZero() {
				t.Fatalf("forecast started an unadmitted Run: %+v", run)
			}
		}
		for _, index := range []int{0, 2} {
			if finish := runs[index].Timing.BatchEstimatedFinishAt; finish == nil || !finish.Equal(at.Add(20*time.Second)) {
				t.Fatalf("batch forecast was not restored: %+v", runs[index])
			}
		}
	}
	checkForecast(now)
	s.runMetricMaintenance = true
	s.estimateRunFinishesLocked(runs, now.Add(time.Minute))
	for _, run := range runs {
		if run.Timing != nil || !run.StartedAt.IsZero() {
			t.Fatalf("unbounded prior-Run maintenance leaked a start or forecast: %+v", run)
		}
	}
	s.runMetricMaintenance = false
	checkForecast(now.Add(2 * time.Minute))
}

func TestTimingAdmissionDuringIdleMetricMaintenanceHasNoStartOrForecast(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	if !s.beginRunMetricMaintenance(nil) {
		t.Fatal("idle maintenance did not acquire admission")
	}
	ctx, cancel := context.WithCancel(t.Context())
	maintenanceEnded := false
	t.Cleanup(func() {
		cancel()
		s.runs.Wait()
		if !maintenanceEnded {
			s.endRunMetricMaintenance(nil)
		}
		s.workWorkers.Wait()
	})
	s.state.mu.RLock()
	active := s.state.runMetricMaintenance
	s.state.mu.RUnlock()
	if !active {
		t.Fatal("admission exclusion was not published to the timing projection")
	}
	first, err := s.StartScenarioRepeated(ctx, []byte("name: pending-maintenance\nphases: [{action: wait, duration: 10s}]\n"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.State != "queued" || !first.StartedAt.IsZero() || first.Timing != nil {
		t.Fatalf("admission promised a start or ETA before maintenance finished: %+v", first)
	}
	runs := s.state.experimentInventory()
	if len(runs) != 2 {
		t.Fatalf("maintenance changed accepted batch membership: %d", len(runs))
	}
	for _, run := range runs {
		if run.State != "queued" || !run.StartedAt.IsZero() || run.Timing != nil {
			t.Fatalf("snapshot promised a start or ETA during maintenance: %+v", run)
		}
	}
	// Cancel while admission is still held; this test never starts an OS Run
	// worker, and cancellation cannot release the maintenance-owned barrier.
	cancel()
	s.runs.Wait()
	s.state.mu.RLock()
	active = s.state.runMetricMaintenance
	s.state.mu.RUnlock()
	if !active {
		t.Fatal("canceling a queued batch released another Run's maintenance")
	}
	s.endRunMetricMaintenance(nil)
	maintenanceEnded = true
	s.workWorkers.Wait()
	s.state.mu.RLock()
	active = s.state.runMetricMaintenance
	s.state.mu.RUnlock()
	if active {
		t.Fatal("finished maintenance left queued ETA projection blocked")
	}
}
