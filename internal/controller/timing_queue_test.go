package controller

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTimingAccumulatesDifferentFIFOSequences(t *testing.T) {
	s := newState(t.TempDir())
	start := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	short := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
	long := parsedTimingPlan(t, "phases: [{action: wait, duration: 30s}]")
	runs := []model.Experiment{
		{ID: "last", BatchID: "C", ExecutionID: "c", Iteration: 1, State: "queued", QueuePosition: 2},
		{ID: "middle", BatchID: "B", ExecutionID: "b", Iteration: 1, State: "queued", QueuePosition: 1},
		{ID: "second", BatchID: "A", ExecutionID: "a", Iteration: 2, Repetitions: 2, State: "queued"},
		{ID: "first", BatchID: "A", ExecutionID: "a", Iteration: 1, Repetitions: 2, State: "running", StartedAt: start},
	}
	for _, run := range runs {
		plan := short
		if run.ID == "middle" {
			plan = long
		}
		s.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
	}
	s.estimateRunFinishesLocked(runs, start.Add(4*time.Second))
	for i, want := range []time.Duration{60 * time.Second, 50 * time.Second, 20 * time.Second, 10 * time.Second} {
		if got := runs[i].Timing; got == nil || !got.EstimatedFinishAt.Equal(start.Add(want)) || got.RemainingSeconds != want.Seconds()-4 {
			t.Fatalf("FIFO estimate for %s = %+v; want finish +%s", runs[i].ID, got, want)
		}
	}
	// An overdue predecessor still occupies its slot, so later submissions
	// move forward from now instead of appearing to finish in the past.
	s.estimateRunFinishesLocked(runs, start.Add(15*time.Second))
	if !runs[3].Timing.Overdue || !runs[1].Timing.EstimatedFinishAt.Equal(start.Add(55*time.Second)) {
		t.Fatalf("overdue predecessor was not accumulated: %+v", runs)
	}
}

func TestTimingRecoveryIgnoresEarlierExecutionFailures(t *testing.T) {
	for _, oldState := range []string{"failed", "canceled", "interrupted"} {
		t.Run(oldState, func(t *testing.T) {
			s := newState(t.TempDir())
			plan := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
			start := time.Now().UTC()
			runs := []model.Experiment{
				{ID: "old", BatchID: "batch", ExecutionID: "old-execution", State: oldState, Iteration: 1, Repetitions: 3},
				{ID: "retry", BatchID: "batch", ExecutionID: "retry-execution", State: "running", Iteration: 2, Repetitions: 3, StartedAt: start},
				{ID: "remaining", BatchID: "batch", ExecutionID: "retry-execution", State: "queued", Iteration: 3, Repetitions: 3},
			}
			for _, run := range runs[1:] {
				s.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
			}
			s.estimateRunFinishesLocked(runs, start)
			if runs[0].Timing != nil || runs[1].Timing == nil || runs[2].Timing == nil || runs[2].Timing.RemainingSeconds != 20 {
				t.Fatalf("old %s attempt suppressed current execution ETA: %+v", oldState, runs)
			}
			if runs[2].Timing.ObservedRuns != 0 || runs[2].Timing.Basis != "scenario" {
				t.Fatal("failed history became a successful observation")
			}
		})
	}
}

func TestTimingBatchFinishCoversInterleavedExecutions(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		s := newState(t.TempDir())
		start := time.Now().UTC()
		now := start.Add(15 * time.Second)
		short := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
		middle := parsedTimingPlan(t, "phases: [{action: wait, duration: 30s}]")
		if unknown {
			middle = parsedTimingPlan(t, "phases: [{action: stop-all}]")
		}
		runs := []model.Experiment{
			{ID: "first", BatchID: "A", ExecutionID: "original", State: "running", StartedAt: start, Iteration: 1, Repetitions: 3},
			{ID: "second", BatchID: "A", ExecutionID: "original", State: "queued", Iteration: 2, Repetitions: 3},
			{ID: "middle", BatchID: "B", ExecutionID: "other", State: "queued", QueuePosition: 1},
			{ID: "added", BatchID: "A", ExecutionID: "append", State: "queued", QueuePosition: 2, Iteration: 3, Repetitions: 3},
		}
		for _, run := range runs {
			plan := short
			if run.ID == "middle" {
				plan = middle
			}
			s.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
		}
		s.estimateRunFinishesLocked(runs, now)
		for _, i := range []int{0, 1, 3} {
			got := runs[i].Timing
			if unknown {
				if i < 2 && (got == nil || got.BatchEstimatedFinishAt != nil) || i == 3 && got != nil {
					t.Fatalf("unknown intervening work did not suppress only affected forecasts: %+v", runs[i])
				}
			} else if got == nil || got.BatchEstimatedFinishAt == nil || !got.BatchEstimatedFinishAt.Equal(start.Add(65*time.Second)) || got.BatchRemainingSeconds != 50 || !got.BatchOverdue {
				t.Fatalf("batch forecast omitted later execution: %+v", runs[i])
			}
		}
	}
}

func TestTimingUnknownOrBlockedPredecessorsKeepLaterFinishUnknown(t *testing.T) {
	for _, blocked := range []string{"unknown", "cleanup", "stop"} {
		t.Run(blocked, func(t *testing.T) {
			s := newState(t.TempDir())
			known := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
			unknown := parsedTimingPlan(t, "phases: [{action: stop-all}]")
			now := time.Now().UTC()
			runs := []model.Experiment{
				{ID: "active", State: "running", StartedAt: now},
				{ID: "blocked", State: "queued", QueuePosition: 1},
				{ID: "later", State: "queued", QueuePosition: 2},
			}
			for _, run := range runs {
				s.runTimings[run.ID] = &runTiming{plan: known, phases: make([]phaseTiming, 1)}
			}
			switch blocked {
			case "unknown":
				s.runTimings["blocked"].plan = unknown
			case "cleanup":
				runs[1].PhaseName = "Waiting for previous Peer cleanup"
			case "stop":
				runs[1].StopRequested = true
			}
			s.estimateRunFinishesLocked(runs, now)
			if runs[0].Timing == nil || runs[1].Timing != nil || runs[2].Timing != nil {
				t.Fatalf("unresolved predecessor leaked a forecast: %+v", runs)
			}
		})
	}
}

func TestTimingTerminalSubmissionStillOwningQueueSlotBlocksLaterEstimates(t *testing.T) {
	for _, position := range []int{0, 1} {
		s := newState(t.TempDir())
		plan := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
		now := time.Now().UTC()
		runs := []model.Experiment{
			{ID: "retiring", State: "failed", QueuePosition: position},
			{ID: "later", State: "queued", QueuePosition: position + 1},
		}
		if position == 1 {
			runs = append(runs, model.Experiment{ID: "active", State: "running", StartedAt: now})
			s.runTimings["active"] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
		}
		s.runTimings["later"] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
		s.estimateRunFinishesLocked(runs, now)
		if runs[1].Timing != nil {
			t.Fatalf("retiring slot %d omitted from queue forecast: %+v", position, runs[1])
		}
	}
}

func TestTimingQueuedAdmissionIncludesTheExistingQueue(t *testing.T) {
	s := newLifecycleTestController(t)
	first := queueRun(t, s, queueLongScenario, 1)
	second := queueRun(t, s, queueLongScenario, 2)
	if second.QueuePosition != 1 || second.Timing == nil || second.Timing.RemainingSeconds < 7100 || second.Timing.BatchRemainingSeconds < 10700 {
		t.Fatalf("queued admission omitted prior work: %+v", second)
	}
	if err := s.StopScenario(second.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.StopScenario(first.ID); err != nil {
		t.Fatal(err)
	}
	s.runs.Wait()
}

func TestTimingRetryAndContinuationAfterFailure(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			t.Run(map[bool]string{false: "continue", true: "retry"}[retry]+map[bool]string{false: "-live", true: "-restored"}[restart], func(t *testing.T) {
				f := newResumeFixture(t)
				raw := resumeScenario + "  - action: wait\n    duration: 1h\n"
				first := queueRun(t, f.server, raw, 3)
				waitRepetitions(t, f.server)
				f.fail.Store(false)
				s := f.server
				if restart {
					s = New(s.config, s.logger)
					f.register(t, s)
				}
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(func() { cancel(); s.runs.Wait() })
				run, err := s.resumeScenarioBatch(ctx, first.BatchID, retry)
				if err != nil {
					t.Fatal(err)
				}
				if run.Timing == nil || run.Timing.RemainingSeconds < 3500 {
					t.Fatalf("recovery admission omitted its forecast: %+v", run)
				}
				queueAwait(t, s, run.ID, func(e model.Experiment) bool { return e.State == "running" && e.Phase == 2 })
				for _, live := range s.state.experimentInventory() {
					if live.ExecutionID == run.ExecutionID && (live.State == "running" || live.State == "queued") {
						if live.Timing == nil || live.Timing.RemainingSeconds < 3500 {
							t.Fatalf("recovery has no forecast: %+v", live)
						}
					}
				}
			})
		}
	}
}

func TestTimingAppendAdmissionIncludesPrecedingSubmission(t *testing.T) {
	s := newLifecycleTestController(t)
	completed := completedAppendBatch(t, s, 2)
	active := queueRun(t, s, queueLongScenario, 1)
	appended, err := s.AppendScenarioBatch(t.Context(), completed[0].BatchID, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if appended.QueuePosition != 1 || appended.Timing == nil || appended.Timing.RemainingSeconds < 3500 || appended.Timing.BatchEstimatedFinishAt == nil {
		t.Fatalf("appended runs did not include the prior submission: %+v", appended)
	}
	if err := s.StopScenario(appended.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.StopScenario(active.ID); err != nil {
		t.Fatal(err)
	}
	s.runs.Wait()
}

func TestTimingFIFOProjectionKeepsSaturatedForecastSerializable(t *testing.T) {
	s := newState(t.TempDir())
	plan := parsedTimingPlan(t, "phases: [{action: wait, duration: 2000000h, repeat: 1000000}]")
	now := time.Now().UTC()
	var runs []model.Experiment
	for i := range 40 {
		id := "queued-" + strconv.Itoa(i)
		runs = append(runs, model.Experiment{ID: id, State: "queued", QueuePosition: i})
		s.runTimings[id] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
	}
	s.estimateRunFinishesLocked(runs, now)
	for _, run := range runs {
		if run.Timing == nil || run.Timing.EstimatedFinishAt.Before(now) {
			t.Fatalf("unrepresentable queued estimate: %+v", run)
		}
	}
	if _, err := json.Marshal(runs); err != nil {
		t.Fatalf("queue projection broke the snapshot API: %v", err)
	}
}
