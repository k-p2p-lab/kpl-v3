package controller

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// Plans are shared by a submission. Observations live only while its scheduler runs;
// snapshots never parse YAML or sample distributions under the state lock.
type timingPlan struct {
	phases   []scenario.Phase
	seconds  []float64
	drain    bool
	duration float64
}
type phaseTiming struct{ started, finished time.Time }
type runTiming struct {
	plan   *timingPlan
	phases []phaseTiming
}
type timingHistory struct {
	count    int
	duration float64
	tail     float64
	phases   []float64
	samples  []int
}

const maxEstimateSeconds = float64(math.MaxInt64) / float64(time.Second)

func boundedEstimate(seconds float64) float64 {
	if math.IsNaN(seconds) || seconds <= 0 {
		return 0
	}
	return min(seconds, maxEstimateSeconds)
}
func estimateDuration(seconds float64) time.Duration {
	if seconds >= maxEstimateSeconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(boundedEstimate(seconds) * float64(time.Second))
}

func newTimingPlan(spec scenario.Scenario) *timingPlan {
	p := &timingPlan{phases: spec.Phases, seconds: make([]float64, len(spec.Phases)), drain: spec.OnExit == "drain"}
	for i, phase := range spec.Phases {
		var seconds float64
		switch phase.Action {
		case "schedule":
			if phase.Schedule != nil && len(phase.Schedule.Changes) > 0 {
				duration, _ := time.ParseDuration(phase.Schedule.Changes[len(phase.Schedule.Changes)-1].After)
				seconds = duration.Seconds()
			}
		case "wait":
			duration, _ := time.ParseDuration(phase.Duration)
			seconds = duration.Seconds()
		case "wait-ready", "mesh-freeze", "topology":
			// Use the phase's configured allowance until actual execution
			// provides an observed duration for readiness or freeze commands.
			duration, _ := time.ParseDuration(phase.Timeout)
			seconds = duration.Seconds()
		case "join", "publish", "leave":
			if phase.Parallel {
				// Parallel join/leave ignore interval; parallel publish delays
				// each request independently (default: one second).
				if phase.Action == "publish" {
					if phase.Interval.Model == "" {
						seconds = 1
					} else {
						seconds = estimatedInterval(phase.Interval, max(1, phase.Count), true)
					}
				}
			} else {
				seconds = estimatedInterval(phase.Interval, max(0, phase.Count-1), false)
			}
		}
		p.seconds[i] = boundedEstimate(seconds * float64(max(1, phase.Repeat)))
	}
	p.duration = projectRunDuration(p, nil, time.Time{}, nil)
	return p
}

// A fixed, bounded sample uses the runtime's own clamping/distribution semantics
// without consuming the experiment's RNG. The order-statistic weights estimate
// the longest independent delay for a parallel publication batch.
func estimatedInterval(d scenario.Distribution, count int, parallel bool) float64 {
	if count <= 0 || d.Model == "" {
		return 0
	}
	if d.Model == "fixed" {
		seconds := d.Sample(nil).Seconds()
		if !parallel {
			seconds *= float64(count)
		}
		return boundedEstimate(seconds)
	}
	const samples = 256
	values := make([]float64, samples)
	rng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = d.Sample(rng).Seconds()
	}
	var total float64
	if parallel {
		sort.Float64s(values)
		for i, value := range values {
			weight := math.Pow(float64(i+1)/samples, float64(count)) - math.Pow(float64(i)/samples, float64(count))
			total += value * weight
		}
	} else {
		for _, value := range values {
			total += value / samples
		}
		total *= float64(count)
	}
	return boundedEstimate(total)
}

// Replay the phase dependency graph, replacing predicted phase boundaries with
// actual ones as they arrive. Background jobs overlap; barriers wait only for
// their dependencies; stop-all resets job names and onExit decides whether the
// remaining background work contributes to the finish.
func projectRunDuration(plan *timingPlan, observed []phaseTiming, start time.Time, history *timingHistory) float64 {
	cursor := 0.0
	jobs := make(map[string]float64)
	for i, phase := range plan.phases {
		begin := cursor
		var observation phaseTiming
		if i < len(observed) {
			observation = observed[i]
		}
		if !observation.started.IsZero() {
			begin = max(0, observation.started.Sub(start).Seconds())
		}
		seconds := plan.seconds[i]
		learned := history != nil && i < len(history.samples) && history.samples[i] > 0
		if learned {
			seconds = history.phases[i] / float64(history.samples[i])
		}
		end := boundedEstimate(begin + seconds)
		if !learned && phase.Action == "schedule" && phase.Schedule != nil && phase.Schedule.Clock() == "experiment-start" {
			end = max(begin, seconds)
		}
		if !learned && (phase.Action == "wait-jobs" || phase.Action == "wait-ready" && len(phase.Jobs) > 0) {
			dependency := begin
			if len(phase.Jobs) == 0 {
				for _, finish := range jobs {
					dependency = max(dependency, finish)
				}
			} else {
				for _, id := range phase.Jobs {
					dependency = max(dependency, jobs[id])
				}
			}
			end = boundedEstimate(dependency + seconds)
			// The timeout covers both dependencies and the readiness barrier.
			timeout, _ := time.ParseDuration(phase.Timeout)
			if timeout > 0 {
				end = min(end, boundedEstimate(begin+timeout.Seconds()*float64(max(1, phase.Repeat))))
			}
		}
		if !observation.finished.IsZero() {
			end = max(begin, observation.finished.Sub(start).Seconds())
		}
		if phase.ShouldAwait() {
			cursor = end
		} else {
			jobs[phase.Job] = end
		}
		if phase.Action == "stop-all" {
			clear(jobs)
		}
	}
	if plan.drain {
		for _, end := range jobs {
			cursor = max(cursor, end)
		}
	}
	return boundedEstimate(cursor)
}

func (s *state) recordPhaseTiming(id string, phase int, finished bool, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if timing := s.runTimings[id]; timing != nil && phase >= 0 && phase < len(timing.phases) {
		if finished {
			timing.phases[phase].finished = at
		} else {
			timing.phases[phase].started = at
		}
	}
}

func (s *state) timingHistoryLocked(runs []model.Experiment, indices []int, plan *timingPlan) timingHistory {
	h := timingHistory{phases: make([]float64, len(plan.phases)), samples: make([]int, len(plan.phases))}
	for _, index := range indices {
		run := runs[index]
		if run.State != "completed" || run.Error != "" || run.StartedAt.IsZero() || !run.FinishedAt.After(run.StartedAt) {
			continue
		}
		timing := s.runTimings[run.ID]
		if timing == nil || timing.plan != plan {
			continue
		}
		h.count++
		duration := run.FinishedAt.Sub(run.StartedAt).Seconds()
		h.duration += duration
		h.tail += max(0, duration-projectRunDuration(plan, timing.phases, run.StartedAt, nil))
		for i, phase := range timing.phases {
			if !phase.started.IsZero() && !phase.finished.Before(phase.started) {
				h.phases[i] += phase.finished.Sub(phase.started).Seconds()
				h.samples[i]++
			}
		}
	}
	if h.count > 0 {
		h.duration /= float64(h.count)
		h.tail /= float64(h.count)
	}
	return h
}

// Caller holds state.mu. Only copies in the response are annotated; live
// predictions are never written to experiment.json. ExecutionID distinguishes
// a retry/continuation from failed history retained in the same result batch.
func (s *state) estimateRunFinishesLocked(runs []model.Experiment, now time.Time) {
	type groupKey struct{ batch, execution string }
	type timingGroup struct {
		key      groupKey
		indices  []int
		position int
		active   bool
	}
	byKey := make(map[groupKey]*timingGroup)
	for i := range runs {
		runs[i].Timing = nil
		if runs[i].Superseded {
			continue
		}
		batch := runs[i].BatchID
		if batch == "" {
			batch = runs[i].ID
		}
		key := groupKey{batch, runs[i].ExecutionID}
		group := byKey[key]
		if group == nil {
			group = &timingGroup{key: key}
			byKey[key] = group
		}
		group.indices = append(group.indices, i)
		if runs[i].State == "running" || runs[i].State == "queued" {
			group.active = true
			group.position = max(group.position, runs[i].QueuePosition)
		}
	}
	var groups []*timingGroup
	for _, group := range byKey {
		if group.active {
			groups = append(groups, group)
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
		if a.position != b.position {
			return a.position < b.position
		}
		if a.key.batch != b.key.batch {
			return a.key.batch < b.key.batch
		}
		return a.key.execution < b.key.execution
	})
	cursor, known, overdue := now, true, false
	nextPosition := 0
	for first := 0; first < len(groups); {
		// A terminal submission can still own its FIFO slot while finishing
		// cleanup. Do not omit that unresolved predecessor from the forecast.
		if groups[first].position != nextPosition {
			known = false
		}
		end, positionKnown, positionOverdue := cursor, known, overdue
		last := first
		// Equal positions can occur in legacy/in-memory views with independent
		// running groups. Project those together, then advance the FIFO cursor.
		for last < len(groups) && groups[last].position == groups[first].position {
			if known {
				finish, groupKnown, groupOverdue := s.estimateTimingGroupLocked(runs, groups[last].indices, now, cursor, overdue)
				if finish.After(end) {
					end = finish
				}
				positionKnown = positionKnown && groupKnown
				positionOverdue = positionOverdue || groupOverdue
			}
			last++
		}
		cursor, known, overdue = end, positionKnown, positionOverdue
		nextPosition = groups[first].position + 1
		first = last
	}
	setBatchFinishEstimates(runs, now)
}

// Several submissions can belong to one result batch. Its finish must cover
// all pending members, including later additions separated by other submissions.
func setBatchFinishEstimates(runs []model.Experiment, now time.Time) {
	type summary struct {
		finish  time.Time
		known   bool
		overdue bool
	}
	batches := make(map[string]*summary)
	for _, run := range runs {
		if run.Superseded || run.State != "running" && run.State != "queued" {
			continue
		}
		key := run.BatchID
		if key == "" {
			key = run.ID
		}
		batch := batches[key]
		if batch == nil {
			batch = &summary{known: true}
			batches[key] = batch
		}
		if run.Timing == nil {
			batch.known = false
			continue
		}
		if run.Timing.EstimatedFinishAt.After(batch.finish) {
			batch.finish = run.Timing.EstimatedFinishAt
		}
		batch.overdue = batch.overdue || run.Timing.Overdue || run.Timing.BatchOverdue
	}
	for i := range runs {
		run := &runs[i]
		if run.Timing == nil || run.Repetitions <= 1 {
			continue
		}
		key := run.BatchID
		if key == "" {
			key = run.ID
		}
		run.Timing.BatchEstimatedFinishAt = nil
		run.Timing.BatchRemainingSeconds, run.Timing.BatchOverdue = 0, false
		if batch := batches[key]; batch != nil && batch.known {
			run.Timing.BatchEstimatedFinishAt = &batch.finish
			run.Timing.BatchRemainingSeconds = max(0, batch.finish.Sub(now).Seconds())
			run.Timing.BatchOverdue = batch.overdue
		}
	}
}

func (s *state) estimateTimingGroupLocked(runs []model.Experiment, indices []int, now, cursor time.Time, predecessorOverdue bool) (time.Time, bool, bool) {
	var plan *timingPlan
	for _, i := range indices {
		run := runs[i]
		if run.State == "failed" || run.State == "canceled" || run.State == "interrupted" || run.StopRequested || run.State == "queued" && run.PhaseName != "" {
			return cursor, false, predecessorOverdue
		}
		if run.State == "running" || run.State == "queued" {
			if timing := s.runTimings[run.ID]; timing != nil {
				plan = timing.plan
			}
		}
	}
	if plan == nil {
		return cursor, false, predecessorOverdue
	}
	sort.Slice(indices, func(i, j int) bool {
		a, b := runs[indices[i]], runs[indices[j]]
		if a.Iteration != b.Iteration {
			return a.Iteration < b.Iteration
		}
		return a.ID < b.ID
	})
	history := s.timingHistoryLocked(runs, indices, plan)
	duration, basis := plan.duration, "scenario"
	if history.count > 0 {
		duration, basis = history.duration, "observed-runs"
	}
	var pending []int
	var batchFinish time.Time
	overdue := predecessorOverdue
	complete := true
	for _, i := range indices {
		run := &runs[i]
		if run.State != "running" && run.State != "queued" {
			continue
		}
		seconds, start := duration, cursor
		if run.State == "running" && !run.StartedAt.IsZero() {
			start = run.StartedAt
			if timing := s.runTimings[run.ID]; timing != nil {
				seconds = boundedEstimate(projectRunDuration(plan, timing.phases, start, &history) + history.tail)
			}
		}
		if seconds <= 0 {
			complete = false
			continue
		}
		if !complete && run.State == "queued" {
			continue
		}
		finish := start.Add(estimateDuration(seconds))
		// Bound the whole queue horizon, not just individual runs. Repeated
		// saturated durations must not overflow JSON's supported year range.
		if horizon := now.Add(time.Duration(math.MaxInt64)); finish.After(horizon) {
			finish = horizon
		}
		run.Timing = &model.ExperimentTiming{
			EstimatedFinishAt: finish, RemainingSeconds: max(0, finish.Sub(now).Seconds()),
			Basis: basis, ObservedRuns: history.count, Overdue: finish.Before(now),
		}
		overdue = overdue || run.Timing.Overdue
		batchFinish = finish
		pending = append(pending, i)
		cursor = finish
		if cursor.Before(now) {
			cursor = now
		}
	}
	if !complete {
		return cursor, false, overdue
	}
	for _, i := range pending {
		if runs[i].Repetitions > 1 {
			runs[i].Timing.BatchEstimatedFinishAt = &batchFinish
			runs[i].Timing.BatchRemainingSeconds = max(0, batchFinish.Sub(now).Seconds())
			runs[i].Timing.BatchOverdue = overdue
		}
	}
	return cursor, true, overdue
}

// Admission and idempotent replay return the same complete-queue forecast as
// snapshots. Calling the estimator on only the submitted runs omits their
// predecessors and cannot produce a correct absolute finish time.
func (s *state) experimentWithTiming(id string) (model.Experiment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.experiments[id]; !ok {
		return model.Experiment{}, false
	}
	runs := make([]model.Experiment, 0, len(s.experiments))
	for _, run := range s.experiments {
		runs = append(runs, run)
	}
	s.estimateRunFinishesLocked(runs, time.Now().UTC())
	for _, run := range runs {
		if run.ID == id {
			return run, true
		}
	}
	return model.Experiment{}, false
}
