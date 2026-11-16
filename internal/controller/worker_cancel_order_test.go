package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestWorkerCancellationPersistsAcrossExecuteAdmission(t *testing.T) {
	f := newControllerWorkerFixture(t)
	for _, mode := range []string{"before-execute", "duplicate-before", "after-execute", "duplicate-after", "normal-fresh-worker"} {
		t.Run(mode, func(t *testing.T) {
			s := f.server
			raw := []byte(queueLongScenario)
			if mode == "normal-fresh-worker" {
				raw = []byte(queueShortScenario)
			}
			started := time.Now().UTC()
			run := model.Experiment{ID: "cancel-order-" + mode, ExecutionID: "cancel-order-epoch-" + mode, Name: "cancel-order", State: "running", Seed: 42, ScenarioYAML: string(raw), StartedAt: started}
			s.state.mu.Lock()
			s.state.experiments[run.ID] = run
			s.state.mu.Unlock()
			if err := s.persistManifest(run, raw); err != nil {
				t.Fatal(err)
			}
			broker := &runWorkerBroker{server: s, runID: run.ID, executionID: run.ExecutionID, generation: 1, finalizing: make(chan struct{}, 1)}
			p, err := workerproc.Start(t.Context(), "controller-run", workerRunStart{RunID: run.ID, Experiment: run, Scenario: raw, DataDir: s.config.DataDir}, broker.handle, workerproc.Options{RunID: run.ID, Command: s.workerCommand, GracePeriod: 50 * time.Millisecond, KillWait: 100 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			broker.process = p
			t.Cleanup(func() {
				broker.revoke()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := p.CloseContext(ctx); err != nil {
					t.Errorf("worker reap: %v", err)
				}
				if err := broker.closeObservationStage(); err != nil {
					t.Errorf("close observation stage: %v", err)
				}
			})
			requestCancel := func() {
				t.Helper()
				if err := p.Call(t.Context(), "cancel", nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			if strings.Contains(mode, "before") {
				requestCancel()
				if strings.HasPrefix(mode, "duplicate") {
					requestCancel()
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			executed := make(chan error, 1)
			go func() { executed <- p.Call(ctx, "execute", nil, nil) }()
			if strings.Contains(mode, "after") {
				queueAwait(t, s, run.ID, func(run model.Experiment) bool { return run.Phase == 1 })
				requestCancel()
				if strings.HasPrefix(mode, "duplicate") {
					requestCancel()
				}
			}
			err = <-executed
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("acknowledged cancellation was forgotten; worker entered the one-hour wait")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Call(t.Context(), "execute", nil, nil); err == nil || !strings.Contains(err.Error(), "already executed") {
				t.Fatalf("second execute was not rejected: %v", err)
			}
			broker.mu.Lock()
			final := broker.final
			sealed := broker.fenced && broker.drained
			broker.mu.Unlock()
			expected := "canceled"
			if mode == "normal-fresh-worker" {
				expected = "completed"
			}
			if final == nil || final.State != expected || final.CleanupState != "complete" || !final.StartedAt.Equal(started) || final.FinishedAt.Before(started) || !sealed {
				t.Fatalf("execution/cancellation changed the terminal contract: mode=%s final=%+v sealed=%t", mode, final, sealed)
			}
			closeCtx, stop := context.WithTimeout(t.Context(), 2*time.Second)
			err = p.CloseContext(closeCtx)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-p.Done():
			default:
				t.Fatal("worker Close returned before actual OS/private RPC reclamation")
			}
			if err := s.retireRunRuntime(t.Context(), run.ID, final); err != nil {
				t.Fatal(err)
			}
			s.state.mu.RLock()
			saved := s.state.experiments[run.ID]
			s.state.mu.RUnlock()
			if saved.State != expected || saved.CleanupState != "complete" {
				t.Fatalf("parent retirement lost final state: %+v", saved)
			}
		})
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) != 5 {
		t.Fatalf("distinct executions shared an OS worker: %d", len(f.commands))
	}
	for _, command := range f.commands {
		if command.ProcessState == nil {
			t.Fatal("OS child was not reaped")
		}
	}
}
