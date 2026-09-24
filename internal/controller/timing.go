package controller

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// Plans are shared by a batch. Observations live only while its scheduler runs;
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
		case "wait":
			duration, _ := time.ParseDuration(phase.Duration)
			seconds = duration.Seconds()
		case "wait-ready":
			// Readiness is not predictable before observing a run. Use its
			// configured allowance, then replace it with observed phase times.
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
// predictions are never written to experiment.json.
func (s *state) estimateRunFinishesLocked(runs []model.Experiment, now time.Time) {
	groups := make(map[string][]int)
	for i := range runs {
		runs[i].Timing = nil
		key := runs[i].BatchID
		if key == "" {
			key = runs[i].ID
		}
		groups[key] = append(groups[key], i)
	}
	for _, indices := range groups {
		var plan *timingPlan
		blocked := false
		for _, i := range indices {
			run := runs[i]
			if run.State == "failed" || run.State == "canceled" || run.State == "interrupted" {
				blocked = true
			}
			if run.State == "running" || run.State == "queued" {
				if timing := s.runTimings[run.ID]; timing != nil {
					plan = timing.plan
				}
			}
		}
		if plan == nil || blocked {
			continue
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
		cursor := now
		var pending []int
		var batchFinish time.Time
		overdue, incomplete := false, false
		for _, i := range indices {
			run := &runs[i]
			if run.State != "running" && run.State != "queued" {
				continue
			}
			seconds := duration
			start := cursor
			if run.State == "running" && !run.StartedAt.IsZero() {
				start = run.StartedAt
				if timing := s.runTimings[run.ID]; timing != nil {
					seconds = boundedEstimate(projectRunDuration(plan, timing.phases, start, &history) + history.tail)
				}
			}
			if seconds <= 0 {
				incomplete = true
				continue
			}
			finish := start.Add(estimateDuration(seconds))
			run.Timing = &model.ExperimentTiming{
				EstimatedFinishAt: finish, RemainingSeconds: max(0, finish.Sub(now).Seconds()),
				Basis: basis, ObservedRuns: history.count, Overdue: finish.Before(now),
			}
			overdue = overdue || run.Timing.Overdue
			batchFinish = finish
			pending = append(pending, i)
			// A late active run still occupies the queue. Do not predict that
			// subsequent runs already finished while it is actually running.
			cursor = finish
			if cursor.Before(now) {
				cursor = now
			}
		}
		if incomplete {
			// Unknown predecessors make a queued run's absolute finish unknown.
			for _, i := range pending {
				if runs[i].State == "queued" {
					runs[i].Timing = nil
				}
			}
			continue
		}
		for _, i := range pending {
			if runs[i].Repetitions > 1 {
				runs[i].Timing.BatchEstimatedFinishAt = &batchFinish
				runs[i].Timing.BatchRemainingSeconds = max(0, batchFinish.Sub(now).Seconds())
				runs[i].Timing.BatchOverdue = overdue
			}
		}
	}
}
