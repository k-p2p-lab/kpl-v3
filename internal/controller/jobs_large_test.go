package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

type phaseWaitObservationContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *phaseWaitObservationContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestPhaseJobsAcceptLargeAliasScenario(t *testing.T) {
	// This input fits the public YAML limit but expands beyond reflect.Select's
	// case limit. Parsing or execution must not require an arbitrary job cap.
	const count = 65536
	raw := "version: 3\nname: many jobs\nonExit: drain\nphases:\n- &p {action: publish, group: g, count: 1, await: false}\n" + strings.Repeat("- *p\n", count-1)
	if len(raw) > scenarioYAMLLimit {
		t.Fatal("fixture exceeds the public scenario body limit")
	}
	spec, err := scenario.Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	jobs := newPhaseJobs()
	done := make(chan struct{})
	close(done)
	for _, phase := range spec.Phases {
		jobs.jobs[phase.Job] = &phaseJob{done: done}
	}
	if len(jobs.jobs) != count {
		t.Fatalf("fixture registered %d distinct jobs", len(jobs.jobs))
	}
	if err := jobs.wait(t.Context(), nil, 0); err != nil {
		t.Fatal(err)
	}
	if err := jobs.waitDone(t.Context(), nil, 0); err != nil {
		t.Fatal(err)
	}
}

func TestPhaseJobsLargeWaitersPreserveFailureDrainAndCancellation(t *testing.T) {
	jobs := newPhaseJobs()
	done := make(chan struct{})
	close(done)
	for i := 0; i < 65534; i++ {
		jobs.jobs[fmt.Sprintf("done-%05d", i)] = &phaseJob{done: done}
	}
	releaseSlow, releaseFailure := make(chan struct{}), make(chan struct{})
	finishSlow := sync.OnceFunc(func() { close(releaseSlow) })
	finishFailure := sync.OnceFunc(func() { close(releaseFailure) })
	defer finishSlow()
	defer finishFailure()
	want := errors.New("background failure")
	if err := jobs.start("slow", func() error { <-releaseSlow; return nil }, nil); err != nil {
		t.Fatal(err)
	}
	if err := jobs.start("bad", func() error { <-releaseFailure; return want }, nil); err != nil {
		t.Fatal(err)
	}
	waited, drained := make(chan error, 1), make(chan error, 1)
	waitCtx, stopWaits := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopWaits()
	failureContext := &phaseWaitObservationContext{Context: waitCtx, waiting: make(chan struct{})}
	drainContext := &phaseWaitObservationContext{Context: waitCtx, waiting: make(chan struct{})}
	go func() { waited <- jobs.wait(failureContext, nil, 0) }()
	go func() { drained <- jobs.waitDone(drainContext, nil, 0) }()
	// Done is requested by the wait select after the first completion scan.
	// Both waiters must observe the same later broadcast, not just an already
	// completed failure at entry.
	for _, waiting := range []<-chan struct{}{failureContext.waiting, drainContext.waiting} {
		select {
		case <-waiting:
		case <-waitCtx.Done():
			t.Fatal("large waiters did not reach their pending-job wait")
		}
	}
	finishFailure()
	if err := <-waited; !errors.Is(err, want) {
		t.Fatalf("wait did not report failure independently of the slow job: %v", err)
	}
	select {
	case err := <-drained:
		t.Fatalf("waitDone returned before the slow job drained: %v", err)
	default:
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := jobs.waitDone(ctx, nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("large wait ignored context cancellation: %v", err)
	}
	finishSlow()
	if err := <-drained; err != nil {
		t.Fatalf("waitDone did not ignore completed job failure: %v", err)
	}
}
