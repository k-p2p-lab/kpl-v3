package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func parsedTimingPlan(t *testing.T, yaml string) *timingPlan {
	t.Helper()
	spec, err := scenario.Parse([]byte("version: 2\nname: timing\n" + yaml))
	if err != nil {
		t.Fatal(err)
	}
	return newTimingPlan(spec)
}

func TestTimingPlanExecutionSemantics(t *testing.T) {
	tests := []struct {
		name, yaml string
		seconds    float64
	}{
		{"repeated wait", "phases: [{action: wait, duration: 10s, repeat: 3}]", 30},
		{"serial publish gaps", "phases: [{action: publish, group: p, count: 4, repeat: 2, interval: {model: fixed, value: 2s}}]", 12},
		{"parallel publish delay", "phases: [{action: publish, group: p, count: 4, parallel: true, parallelism: 1, interval: {model: fixed, value: 2s}}]", 2},
		{"parallel publish default", "phases: [{action: publish, group: p, count: 4, parallel: true}]", 1},
		{"parallel join ignores gaps", "phases: [{action: join, group: p, count: 4, parallel: true, parallelism: 1, interval: {model: fixed, value: 2s}}, {action: wait, duration: 3s}]", 3},
		{"parallel leave ignores gaps", "phases: [{action: leave, group: p, count: 4, parallel: true, interval: {model: fixed, value: 2s}}, {action: wait, duration: 3s}]", 3},
		{"readiness allowance", "phases: [{action: wait-ready, timeout: 40s}]", 40},
		{"overlapping drain", "onExit: drain\nphases: [{action: publish, job: p, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: wait, duration: 4s}]", 10},
		{"background cancellation", "onExit: cancel\nphases: [{action: publish, job: p, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: wait, duration: 4s}]", 4},
		{"barrier does not add full timeout", "phases: [{action: publish, job: p, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: wait, duration: 4s}, {action: wait-jobs, jobs: [p], timeout: 2m}, {action: wait, duration: 2s}]", 12},
		{"barrier timeout", "phases: [{action: publish, job: p, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: wait-jobs, timeout: 2s}]", 2},
		{"selective dependencies", "onExit: cancel\nphases: [{action: publish, job: a, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: publish, job: b, await: false, group: p, count: 4, interval: {model: fixed, value: 1s}}, {action: wait-jobs, jobs: [b]}]", 3},
		{"stop-all clears reused jobs", "onExit: drain\nphases: [{action: publish, job: p, await: false, group: p, count: 11, interval: {model: fixed, value: 1s}}, {action: wait, duration: 2s}, {action: stop-all}, {action: publish, job: p, await: false, group: p, count: 3, interval: {model: fixed, value: 1s}}]", 4},
		{"delivery window is not a sleep", "phases: [{action: publish, group: p, count: 1, deliveryWindow: 1h}, {action: wait, duration: 3s}]", 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parsedTimingPlan(t, tc.yaml).duration; math.Abs(got-tc.seconds) > 1e-8 {
				t.Fatalf("duration=%v, want %v", got, tc.seconds)
			}
		})
	}
}

func TestTimingDistributionSamplingIsBoundedAndRepeatable(t *testing.T) {
	for _, d := range []scenario.Distribution{
		{Model: "normal", Mean: "-5s", Sigma: "1s", Min: "2s", Max: "2s"},
		{Model: "pareto", XM: "1s", Alpha: .5, Min: "2s", Max: "2s"},
		{Model: "gamma", Alpha: 2, Scale: "1s", Min: "2s", Max: "2s"},
		{Model: "lognormal", Mu: 0, LogSigma: 1, Min: "2s", Max: "2s"},
	} {
		if got := estimatedInterval(d, 3, false); got != 6 {
			t.Fatalf("%s: %v", d.Model, got)
		}
		if got := estimatedInterval(d, 3, true); math.Abs(got-2) > 1e-9 {
			t.Fatalf("%s parallel: %v", d.Model, got)
		}
	}
	d := scenario.Distribution{Model: "exponential", Mean: "2s"}
	if a, b := estimatedInterval(d, 100, false), estimatedInterval(d, 100, false); a != b || a <= 0 {
		t.Fatalf("unstable estimates: %v %v", a, b)
	}
	plan := parsedTimingPlan(t, "phases: [{action: wait, duration: 2000000h, repeat: 1000000}]")
	if math.IsInf(plan.duration, 0) || estimateDuration(plan.duration) <= 0 {
		t.Fatalf("duration overflow: %v", plan.duration)
	}
}

func TestTimingProjectionReplacesPredictionsWithActualBoundaries(t *testing.T) {
	plan := parsedTimingPlan(t, "phases: [{action: wait-ready, timeout: 1m}, {action: wait, duration: 30s}]")
	start := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	observed := []phaseTiming{{started: start, finished: start.Add(5 * time.Second)}, {started: start.Add(6 * time.Second)}}
	if got := projectRunDuration(plan, observed, start, nil); got != 36 {
		t.Fatalf("actual ready/start boundaries ignored: %v", got)
	}
	observed[1].finished = start.Add(37 * time.Second)
	if got := projectRunDuration(plan, observed, start, nil); got != 37 {
		t.Fatalf("actual end ignored: %v", got)
	}
}

func TestTimingGroupUsesSuccessfulRunsAndQueueOrder(t *testing.T) {
	plan := parsedTimingPlan(t, "phases: [{action: wait, duration: 10s}]")
	s := newState(t.TempDir())
	start := time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC)
	// Deliberately shuffled, including another independently running group.
	runs := []model.Experiment{
		{ID: "third", BatchID: "batch", Iteration: 3, Repetitions: 3, State: "queued"},
		{ID: "first", BatchID: "batch", Iteration: 1, Repetitions: 3, State: "completed", StartedAt: start, FinishedAt: start.Add(14 * time.Second)},
		{ID: "second", BatchID: "batch", Iteration: 2, Repetitions: 3, State: "running", StartedAt: start.Add(14 * time.Second)},
		{ID: "other", State: "running", StartedAt: start.Add(20 * time.Second)},
	}
	for _, run := range runs {
		s.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, 1)}
	}
	s.runTimings["first"].phases[0] = phaseTiming{started: start, finished: start.Add(12 * time.Second)}
	s.runTimings["second"].phases[0] = phaseTiming{started: start.Add(14 * time.Second)}
	s.estimateRunFinishesLocked(runs, start.Add(20*time.Second))
	if runs[1].Timing != nil {
		t.Fatal("completed run has ETA")
	}
	for index, offset := range map[int]time.Duration{0: 42 * time.Second, 2: 28 * time.Second, 3: 30 * time.Second} {
		if timing := runs[index].Timing; timing == nil || !timing.EstimatedFinishAt.Equal(start.Add(offset)) {
			t.Fatalf("run %s: %+v", runs[index].ID, timing)
		}
	}
	if got := runs[0].Timing; got.Basis != "observed-runs" || got.ObservedRuns != 1 || got.RemainingSeconds != 22 || got.BatchEstimatedFinishAt == nil || !got.BatchEstimatedFinishAt.Equal(start.Add(42*time.Second)) {
		t.Fatalf("batch estimate: %+v", got)
	}
	if runs[3].Timing.Basis != "scenario" || runs[3].Timing.BatchEstimatedFinishAt != nil {
		t.Fatal("independent run inherited another group's history")
	}

	// Overrun must not make queued runs appear to have finished in the past.
	s.estimateRunFinishesLocked(runs, start.Add(50*time.Second))
	if !runs[2].Timing.Overdue || runs[2].Timing.RemainingSeconds != 0 || !runs[0].Timing.EstimatedFinishAt.Equal(start.Add(64*time.Second)) || !runs[0].Timing.BatchOverdue {
		t.Fatalf("late run not propagated: %+v %+v", runs[2].Timing, runs[0].Timing)
	}
	// Failed/canceled history never produces a successful continuation forecast.
	runs[1].State = "failed"
	s.estimateRunFinishesLocked(runs, start.Add(50*time.Second))
	if runs[0].Timing != nil || runs[2].Timing != nil {
		t.Fatal("failed batch still promises a completion")
	}
}

func TestTimingLifecyclePublishesAndRetiresLiveEstimates(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, err := s.StartScenarioRepeated(context.Background(), []byte("name: eta\nphases: [{action: wait, duration: 1h}]\n"), 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.StopScenario(run.ID); s.runs.Wait() })
	if run.Timing == nil || run.Timing.BatchEstimatedFinishAt == nil {
		t.Fatal("submission has no timing")
	}
	snapshot := s.state.snapshot()
	sort.Slice(snapshot.Experiments, func(i, j int) bool { return snapshot.Experiments[i].Iteration < snapshot.Experiments[j].Iteration })
	for i, exp := range snapshot.Experiments {
		if exp.Timing == nil || exp.Timing.RemainingSeconds < 3500*float64(i+1) {
			t.Fatalf("missing queued ETA: %+v", exp)
		}
		stored, err := os.ReadFile(filepath.Join(s.config.DataDir, "runs", exp.ID, "experiment.json"))
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(stored, &fields); err != nil {
			t.Fatal(err)
		}
		if _, exists := fields["timing"]; exists {
			t.Fatal("live forecast was persisted")
		}
	}
	if err := s.StopScenario(run.ID); err != nil {
		t.Fatal(err)
	}
	for _, exp := range waitRepetitions(t, s) {
		if exp.Timing != nil {
			t.Fatalf("stopped run retained ETA: %+v", exp)
		}
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	if len(s.state.runTimings) != 0 {
		t.Fatal("finished batch leaked timing observations")
	}
}

func TestTimingUnknownWorkDoesNotInventFinish(t *testing.T) {
	plan := parsedTimingPlan(t, "phases: [{action: stop-all}]")
	s := newState(t.TempDir())
	runs := []model.Experiment{{ID: "unknown", State: "running", StartedAt: time.Now()}}
	s.runTimings["unknown"] = &runTiming{plan: plan}
	s.estimateRunFinishesLocked(runs, time.Now())
	if runs[0].Timing != nil {
		t.Fatal("zero timing budget was presented as a known finish")
	}
	data, err := json.Marshal(runs)
	if err != nil || strings.Contains(string(data), "estimatedFinishAt") {
		t.Fatalf("unknown ETA serialization: %s %v", data, err)
	}
}

func TestTimingStreamRetainsNotificationsUntilCacheReflectsThem(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server := httptest.NewServer(s.apiTestHandler(context.Background()))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/stream", nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	readSnapshot := func() model.Snapshot {
		t.Helper()
		for scanner.Scan() {
			if data, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
				var snapshot model.Snapshot
				if err := json.Unmarshal([]byte(data), &snapshot); err != nil {
					t.Fatal(err)
				}
				return snapshot
			}
		}
		t.Fatalf("stream ended before an updated snapshot: %v", scanner.Err())
		return model.Snapshot{}
	}
	if len(readSnapshot().Experiments) != 0 {
		t.Fatal("unexpected initial run")
	}
	// Another client refreshes the shared empty cache between this stream's
	// ticks; the first tick after admission will therefore reuse that cache.
	time.Sleep(snapshotInterval / 2)
	s.snapshotMu.Lock()
	s.snapshotData = nil
	s.snapshotMu.Unlock()
	if _, err := s.streamSnapshot(); err != nil {
		t.Fatal(err)
	}
	run, err := s.StartScenario(context.Background(), []byte("name: streamed eta\nphases: [{action: wait, duration: 1h}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.StopScenario(run.ID); s.runs.Wait() }()
	for {
		snapshot := readSnapshot()
		if len(snapshot.Experiments) == 0 {
			continue
		}
		if snapshot.Experiments[0].Timing == nil {
			t.Fatal("new snapshot omitted ETA")
		}
		break
	}
}
