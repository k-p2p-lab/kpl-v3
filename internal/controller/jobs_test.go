package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitJobsEmptyCohortHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := newPhaseJobs().wait(ctx, nil, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("empty wait-jobs returned %v", err)
	}
}

func TestPhaseJobsWaitsForNamedBackgroundJob(t *testing.T) {
	jobs := newPhaseJobs()
	finished := make(chan struct{})
	if err := jobs.start("churn", func() error {
		<-finished
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	go func() { close(finished) }()
	if err := jobs.wait(context.Background(), []string{"churn"}, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseJobsPropagatesBackgroundError(t *testing.T) {
	jobs := newPhaseJobs()
	want := errors.New("boom")
	if err := jobs.start("bad", func() error { return want }, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.wait(context.Background(), nil, time.Second); !errors.Is(err, want) {
		t.Fatalf("got %v, want wrapped %v", err, want)
	}
}

func TestPhaseJobsSignalsDoneAfterCompletedCallback(t *testing.T) {
	jobs := newPhaseJobs()
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	if err := jobs.start("callback", func() error { return nil }, func(error) {
		close(callbackStarted)
		<-releaseCallback
	}); err != nil {
		t.Fatal(err)
	}
	<-callbackStarted

	waited := make(chan error, 1)
	go func() {
		waited <- jobs.wait(context.Background(), []string{"callback"}, time.Second)
	}()
	select {
	case err := <-waited:
		t.Fatalf("wait returned before completed callback finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseCallback)
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
}

func TestPhaseJobsWaitAllFailsFast(t *testing.T) {
	jobs := newPhaseJobs()
	releaseSlow := make(chan struct{})
	defer close(releaseSlow)
	if err := jobs.start("slow", func() error {
		<-releaseSlow
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	want := errors.New("boom")
	if err := jobs.start("bad", func() error { return want }, nil); err != nil {
		t.Fatal(err)
	}

	waited := make(chan error, 1)
	go func() {
		waited <- jobs.wait(context.Background(), nil, time.Second)
	}()
	select {
	case err := <-waited:
		if !errors.Is(err, want) {
			t.Fatalf("got %v, want wrapped %v", err, want)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("wait-all hid a completed failure behind a slow job")
	}
}

func TestPhaseJobsWaitPreservesContextCancellation(t *testing.T) {
	jobs := newPhaseJobs()
	release := make(chan struct{})
	defer close(release)
	if err := jobs.start("blocked", func() error {
		<-release
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := jobs.wait(ctx, []string{"blocked"}, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context cancellation", err)
	}
}

func TestPhaseJobsWaitTimeout(t *testing.T) {
	jobs := newPhaseJobs()
	release := make(chan struct{})
	defer close(release)
	if err := jobs.start("blocked", func() error {
		<-release
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.wait(context.Background(), []string{"blocked"}, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
}

func TestPhaseJobsWaitRejectsUnknownJob(t *testing.T) {
	jobs := newPhaseJobs()
	err := jobs.wait(context.Background(), []string{"missing"}, time.Second)
	if err == nil || !strings.Contains(err.Error(), `job "missing" not found`) {
		t.Fatalf("got %v, want unknown-job error", err)
	}
}

func TestPhaseJobsCancelAll(t *testing.T) {
	jobs := newPhaseJobs()
	for _, id := range []string{"first", "second"} {
		id := id
		if err := jobs.startContext(context.Background(), id, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}, nil); err != nil {
			t.Fatal(err)
		}
	}

	jobs.cancelAll()
	for _, id := range []string{"first", "second"} {
		if err := jobs.wait(context.Background(), []string{id}, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("job %q returned %v, want context cancellation", id, err)
		}
	}
}

func TestPhaseJobsResetClearsOnlyAfterEveryJobStops(t *testing.T) {
	jobs := newPhaseJobs()
	release := make(chan struct{})
	if err := jobs.start("finished", func() error { return nil }, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.start("running", func() error {
		<-release
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.waitDone(context.Background(), []string{"finished"}, time.Second); err != nil {
		t.Fatal(err)
	}

	if err := jobs.reset(); err == nil || !strings.Contains(err.Error(), `job "running" is still running`) {
		t.Fatalf("reset with an active job returned %v", err)
	}
	if err := jobs.wait(context.Background(), []string{"finished"}, time.Second); err != nil {
		t.Fatalf("failed reset partially removed a completed job: %v", err)
	}

	close(release)
	if err := jobs.waitDone(context.Background(), nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := jobs.reset(); err != nil {
		t.Fatal(err)
	}
	if err := jobs.wait(context.Background(), []string{"finished"}, time.Second); err == nil || !strings.Contains(err.Error(), `job "finished" not found`) {
		t.Fatalf("cleared job remained visible: %v", err)
	}
	if err := jobs.start("finished", func() error { return nil }, nil); err != nil {
		t.Fatalf("reset did not allow a job identifier to be reused: %v", err)
	}
	if err := jobs.waitDone(context.Background(), nil, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseJobsResetClearsCanceledJobs(t *testing.T) {
	jobs := newPhaseJobs()
	if err := jobs.startContext(context.Background(), "churn", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, nil); err != nil {
		t.Fatal(err)
	}
	jobs.cancelAll()
	if err := jobs.waitDone(context.Background(), nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := jobs.reset(); err != nil {
		t.Fatal(err)
	}
	if err := jobs.start("churn", func() error { return nil }, nil); err != nil {
		t.Fatalf("canceled job identifier was not reusable after reset: %v", err)
	}
	if err := jobs.waitDone(context.Background(), nil, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseJobsWaitDoneDrainsAllAndIgnoresJobErrors(t *testing.T) {
	jobs := newPhaseJobs()
	releaseSlow := make(chan struct{})
	if err := jobs.start("slow", func() error {
		<-releaseSlow
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.start("bad", func() error { return errors.New("boom") }, nil); err != nil {
		t.Fatal(err)
	}

	waited := make(chan error, 1)
	go func() {
		waited <- jobs.waitDone(context.Background(), nil, time.Second)
	}()
	select {
	case err := <-waited:
		t.Fatalf("waitDone returned before the slow job stopped: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseSlow)
	if err := <-waited; err != nil {
		t.Fatalf("waitDone returned a job error: %v", err)
	}
}

func TestPhaseJobsWaitDoneHonorsSelectionAndTimeout(t *testing.T) {
	jobs := newPhaseJobs()
	releaseBlocked := make(chan struct{})
	defer close(releaseBlocked)
	if err := jobs.start("done", func() error { return errors.New("ignored") }, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.start("blocked", func() error {
		<-releaseBlocked
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.waitDone(context.Background(), []string{"done"}, time.Second); err != nil {
		t.Fatalf("selected completed job: %v", err)
	}
	if err := jobs.waitDone(context.Background(), nil, 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded while draining all jobs", err)
	}
}

func TestRunOperationsHonorsParallelism(t *testing.T) {
	var active atomic.Int32
	var peak atomic.Int32
	var completed atomic.Int32
	err := runOperations(context.Background(), 12, true, 3, make([]time.Duration, 12), false, func(context.Context, int) error {
		current := active.Add(1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		completed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Load() != 12 || peak.Load() > 3 || peak.Load() < 2 {
		t.Fatalf("completed=%d peak=%d", completed.Load(), peak.Load())
	}
}
