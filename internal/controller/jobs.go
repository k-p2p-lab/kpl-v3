package controller

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type phaseJob struct {
	done   chan struct{}
	err    error
	cancel context.CancelFunc
}

type phaseJobs struct {
	profiles *runProfileState
	mu       sync.RWMutex
	jobs     map[string]*phaseJob
	changed  chan struct{}
}

func newPhaseJobs() *phaseJobs {
	return &phaseJobs{jobs: make(map[string]*phaseJob), profiles: newRunProfileState(), changed: make(chan struct{})}
}

func (j *phaseJobs) start(id string, run func() error, completed func(error)) error {
	return j.startJob(id, nil, run, completed)
}

func (j *phaseJobs) startContext(parent context.Context, id string, run func(context.Context) error, completed func(error)) error {
	ctx, cancel := context.WithCancel(parent)
	err := j.startJob(id, cancel, func() error {
		defer cancel()
		return run(ctx)
	}, completed)
	if err != nil {
		cancel()
	}
	return err
}

func (j *phaseJobs) startJob(id string, cancel context.CancelFunc, run func() error, completed func(error)) error {
	j.mu.Lock()
	if _, exists := j.jobs[id]; exists {
		j.mu.Unlock()
		return fmt.Errorf("job %q already exists", id)
	}
	job := &phaseJob{done: make(chan struct{}), cancel: cancel}
	j.jobs[id] = job
	j.mu.Unlock()
	go func() {
		err := run()
		if completed != nil {
			completed(err)
		}
		j.mu.Lock()
		job.err = err
		close(job.done)
		close(j.changed)
		j.changed = make(chan struct{})
		j.mu.Unlock()
	}()
	return nil
}

func (j *phaseJobs) cancelAll() {
	j.mu.RLock()
	cancels := make([]context.CancelFunc, 0, len(j.jobs))
	for _, job := range j.jobs {
		if job.cancel != nil {
			cancels = append(cancels, job.cancel)
		}
	}
	j.mu.RUnlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// reset forgets a drained generation of jobs so later phases can reuse job
// identifiers and wait-all only observes jobs started after the reset. The
// operation is all-or-nothing: no job is removed while any registered job is
// still running.
func (j *phaseJobs) reset() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for id, job := range j.jobs {
		select {
		case <-job.done:
		default:
			return fmt.Errorf("cannot reset jobs while job %q is still running", id)
		}
	}
	clear(j.jobs)
	return nil
}

func (j *phaseJobs) wait(ctx context.Context, ids []string, timeout time.Duration) error {
	return j.waitSelected(ctx, ids, timeout, true)
}

// waitDone waits until every selected job has stopped, regardless of each
// job's result. Cleanup must drain all selected workers before removing peers.
func (j *phaseJobs) waitDone(ctx context.Context, ids []string, timeout time.Duration) error {
	return j.waitSelected(ctx, ids, timeout, false)
}

func (j *phaseJobs) waitSelected(ctx context.Context, ids []string, timeout time.Duration, failFast bool) error {
	selectedIDs, selectedJobs, err := j.selectJobs(ids)
	if err != nil || len(selectedJobs) == 0 {
		return err
	}
	waitCtx, cancel := jobWaitContext(ctx, timeout)
	defer cancel()
	label := "wait for jobs"
	if !failFast {
		label = "wait for jobs to stop"
	}
	pending := make([]int, len(selectedJobs))
	for i := range pending {
		pending[i] = i
	}
	for {
		if err := waitCtx.Err(); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		j.mu.RLock()
		// Capture the broadcast before inspecting completion. A job finishing
		// after this scan closes this exact channel, so no wakeup can be lost.
		changed := j.changed
		remaining := pending[:0]
		for _, index := range pending {
			job := selectedJobs[index]
			select {
			case <-job.done:
				if failFast && job.err != nil {
					j.mu.RUnlock()
					return fmt.Errorf("job %q: %w", selectedIDs[index], job.err)
				}
			default:
				remaining = append(remaining, index)
			}
		}
		j.mu.RUnlock()
		pending = remaining
		if len(pending) == 0 {
			return nil
		}
		// A broadcast avoids reflect.Select's 65,536-case limit and retires
		// every completed job in one scan, including already-drained batches.
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%s: %w", label, waitCtx.Err())
		case <-changed:
		}
	}
}

func (j *phaseJobs) selectJobs(ids []string) ([]string, []*phaseJob, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if len(ids) == 0 {
		ids = make([]string, 0, len(j.jobs))
		for id := range j.jobs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}
	selectedIDs := make([]string, 0, len(ids))
	selectedJobs := make([]*phaseJob, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		job, exists := j.jobs[id]
		if !exists {
			return nil, nil, fmt.Errorf("job %q not found", id)
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		selectedIDs = append(selectedIDs, id)
		selectedJobs = append(selectedJobs, job)
	}
	return selectedIDs, selectedJobs, nil
}

func jobWaitContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return ctx, func() {}
}
