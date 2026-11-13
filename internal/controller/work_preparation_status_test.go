package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func preparationTestExperiment(s *Server, ctx context.Context, cancel context.CancelFunc) *workSlot {
	const id = "preparation-run"
	s.state.experiments[id] = model.Experiment{ID: id, State: "queued", BatchID: id, Repetitions: 1, Iteration: 1}
	s.cancelMu.Lock()
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", RunID: id, BatchID: id}, cancel)
	slot.batch = &repeatBatch{queuedIDs: []string{id}}
	s.cancelMu.Unlock()
	return slot
}

func TestQueuedExperimentExplainsAnalysisWorkerCleanupWait(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisAgentToken: remoteTestToken}, nil)
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, nil)
	worker.slot <- struct{}{}
	defer func() { <-worker.slot }()
	probed := make(chan struct{}, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		worker.ServeHTTP(w, r)
		select {
		case probed <- struct{}{}:
		default:
		}
	}))
	defer endpoint.Close()
	agent := analysisPoolAgent("preparation-agent", "preparation-host", endpoint.URL)
	registerAnalysisPoolAgent(t, s, agent)
	if err := s.trackAnalysisWorker(t.Context(), endpoint.URL, agent); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	slot := preparationTestExperiment(s, ctx, cancel)
	defer s.finishWork(slot)
	done := make(chan error, 1)
	go func() { done <- s.waitWork(ctx, slot) }()
	select {
	case <-probed:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup verification did not start")
	}
	s.state.mu.RLock()
	run := s.state.experiments[slot.item.RunID]
	queue := cloneWorkQueue(s.state.workQueue)
	s.state.mu.RUnlock()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup wait ignored cancellation: %v", err)
	}
	if run.PhaseName != "Waiting for analysis worker cleanup" || len(queue) != 1 || queue[0].Phase != run.PhaseName {
		t.Fatalf("queued experiment hid the real cleanup wait: run=%+v queue=%+v", run, queue)
	}
	if run.State != "queued" || !run.StartedAt.IsZero() {
		t.Fatal("preparation started the experimental clock")
	}
}

func TestQueuedExperimentExplainsRecoveryAndCannotStartAfterCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	slot := preparationTestExperiment(s, ctx, cancel)
	defer s.finishWork(slot)
	updates, unsubscribe := s.state.subscribe()
	defer unsubscribe()
	// Hold the existing commit boundary while recovery performs its local
	// metadata scan. Preparation status must not need that persistence lock.
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	done := make(chan error, 1)
	go func() { done <- s.waitWork(ctx, slot) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		s.state.mu.RLock()
		run := s.state.experiments[slot.item.RunID]
		s.state.mu.RUnlock()
		if run.PhaseName == "Recovering previous Run cleanup" {
			if run.State != "queued" || !run.StartedAt.IsZero() {
				t.Fatal("recovery advanced the experimental clock")
			}
			break
		}
		select {
		case <-updates:
		case <-deadline.C:
			t.Fatal("recovery remained an unexplained Waiting to start")
		}
	}
	cancel()
	s.state.persistMu.Unlock()
	locked = false
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery reported that the experiment could start: %v", err)
	}
}

func TestPreparationStatusPreservesActiveRunAndOtherWork(t *testing.T) {
	for _, kind := range []string{"experiment", "analysis"} {
		for _, state := range []string{"queued", "running", "completed"} {
			t.Run(kind+"/"+state, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir()}, nil)
				const id = "existing-run"
				s.state.experiments[id] = model.Experiment{ID: id, State: state, PhaseName: "Existing scenario phase"}
				s.state.workQueue = []model.WorkQueueItem{{ID: "work", Kind: kind, RunID: id}}
				s.updateWorkStatus("work", "Recovering previous Run cleanup", 0)
				want := "Existing scenario phase"
				if kind == "experiment" && state == "queued" {
					want = "Recovering previous Run cleanup"
				}
				if got := s.state.experiments[id].PhaseName; got != want {
					t.Fatalf("preparation replaced the wrong experiment phase: got=%q want=%q", got, want)
				}
			})
		}
	}
}
