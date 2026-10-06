package controller

import (
	"context"
	"fmt"
	"reflect"
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
}

func newPhaseJobs() *phaseJobs {
	return &phaseJobs{jobs: make(map[string]*phaseJob), profiles: newRunProfileState()}
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
	selectedIDs, selectedJobs, err := j.selectJobs(ids)
	if err != nil {
		return err
	}
	if len(selectedJobs) == 0 {
		return nil
	}

	waitCtx, cancel := jobWaitContext(ctx, timeout)
	defer cancel()

	// Observe every selected job at once. Waiting on jobs one-by-one can hide a
	// fast failure behind an unrelated slow job, and map iteration made that
	// behavior non-deterministic when waiting for all jobs.
	cases := jobSelectCases(waitCtx, selectedJobs)
	remaining := len(selectedJobs)
	for remaining > 0 {
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			return fmt.Errorf("wait for jobs: %w", waitCtx.Err())
		}
		jobIndex := chosen - 1
		job := selectedJobs[jobIndex]
		j.mu.RLock()
		err := job.err
		j.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("job %q: %w", selectedIDs[jobIndex], err)
		}
		cases[chosen].Chan = reflect.Value{}
		remaining--
	}
	return nil
}

// waitDone waits until every selected job has stopped, regardless of each
// job's result. It is intended for cancellation and cleanup paths where all
// workers must be drained before resources are removed. Only lookup errors and
// cancellation/deadline errors from the wait itself are returned.
func (j *phaseJobs) waitDone(ctx context.Context, ids []string, timeout time.Duration) error {
	_, selectedJobs, err := j.selectJobs(ids)
	if err != nil {
		return err
	}
	if len(selectedJobs) == 0 {
		return nil
	}

	waitCtx, cancel := jobWaitContext(ctx, timeout)
	defer cancel()
	cases := jobSelectCases(waitCtx, selectedJobs)
	remaining := len(selectedJobs)
	for remaining > 0 {
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			return fmt.Errorf("wait for jobs to stop: %w", waitCtx.Err())
		}
		cases[chosen].Chan = reflect.Value{}
		remaining--
	}
	return nil
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

func jobSelectCases(ctx context.Context, jobs []*phaseJob) []reflect.SelectCase {
	cases := make([]reflect.SelectCase, len(jobs)+1)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	for i, job := range jobs {
		cases[i+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(job.done)}
	}
	return cases
}
