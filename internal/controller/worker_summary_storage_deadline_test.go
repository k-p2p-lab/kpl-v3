package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestSummaryStorageWaitHonorsCancellation(t *testing.T) {
	for _, operation := range []string{"refresh", "capture", "revision", "publish"} {
		for _, mode := range []string{"cancel", "deadline"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
				const id = "summary-storage-deadline"
				run := model.Experiment{ID: id, State: "completed", CleanupState: "complete", DataState: "complete", StartedAt: time.Now().Add(-time.Minute)}
				s.state.experiments[id] = run
				if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, workerSummaryFile)
				if err := s.publishWorkerSummary(runWorkerSummary{RunID: id, Metrics: model.Metrics{RunID: id}, At: time.Now().UTC()}); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				s.state.mu.Lock()
				s.state.summaryDirty[id] = true
				s.state.mu.Unlock()
				ctx, cancel := context.WithCancel(t.Context())
				want := context.Canceled
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				s.state.persistMu.Lock()
				locked := true
				defer func() {
					if locked {
						s.state.persistMu.Unlock()
					}
				}()
				done := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "refresh":
						err = s.refreshRunMetricSummary(ctx, id)
					case "capture":
						var snapshot *resultSnapshot
						snapshot, err = s.captureResultFilesContext(ctx, id, false)
						if snapshot != nil {
							snapshot.close()
						}
					case "revision":
						_, err = s.runSourceRevisionContext(ctx, id)
					case "publish":
						err = s.publishWorkerSummaryContext(ctx, runWorkerSummary{RunID: id, Metrics: model.Metrics{RunID: id}, At: time.Now().UTC(), Cursor: 23})
					}
					done <- err
				}()
				select {
				case err := <-done:
					t.Fatalf("summary operation bypassed persistence mutex: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				if mode == "cancel" {
					cancel()
				}
				<-ctx.Done()
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("summary operation lost cancellation: %v", err)
					}
				case <-time.After(300 * time.Millisecond):
					s.state.persistMu.Unlock()
					locked = false
					late := <-done
					t.Fatalf("summary operation ignored cancellation while waiting for persistence: %v", late)
				}
				s.state.persistMu.Unlock()
				locked = false
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("canceled summary operation replaced durable summary: %v", err)
				}
				s.state.mu.RLock()
				dirty := s.state.summaryDirty[id]
				s.state.mu.RUnlock()
				if !dirty {
					t.Fatal("canceled summary operation discarded deferred metric correction")
				}
			})
		}
	}
}

func TestIdleRecoveryStorageWaitAllowsNewWorkCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "idle-recovery-storage-cancel"
	s.state.experiments[id] = model.Experiment{ID: id, State: "completed", CleanupState: "complete", DataState: "complete"}
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	idleDone := make(chan bool, 1)
	go func() { idleDone <- s.tryRefreshIdleRunMetrics(t.Context(), id) }()
	retirementReviewWait(t, "idle maintenance did not reach cleanup recovery", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("(*Server).mergeRecoveredWorkCleanup"))
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	admitted := make(chan *workSlot, 1)
	blocked := false
	go func() {
		s.cancelMu.Lock()
		slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", Name: "next experiment"}, cancel)
		s.cancelMu.Unlock()
		admitted <- slot
	}()
	var slot *workSlot
	select {
	case slot = <-admitted:
	case <-time.After(300 * time.Millisecond):
		blocked = true
		s.state.persistMu.Unlock()
		locked = false
		slot = <-admitted
	}
	defer s.finishWork(slot)
	select {
	case claimed := <-idleDone:
		if !claimed {
			t.Fatal("idle maintenance did not claim its slot")
		}
	case <-time.After(3 * time.Second):
		if locked {
			s.state.persistMu.Unlock()
			locked = false
		}
		<-idleDone
		t.Fatal("idle maintenance did not yield after actual queue admission canceled it")
	}
	if blocked {
		t.Fatal("recovery held the queue mutex while waiting for persistence, preventing new work from canceling idle maintenance")
	}
	select {
	case <-slot.ready:
	default:
		t.Fatal("idle cancellation did not release the next FIFO slot")
	}
	s.cancelMu.Lock()
	active, idleCancel := s.summaryRefreshActive, s.summaryRefreshIdleCancel != nil
	s.cancelMu.Unlock()
	if active || idleCancel {
		t.Fatalf("canceled idle recovery retained maintenance: active=%t cancel=%t", active, idleCancel)
	}
}
