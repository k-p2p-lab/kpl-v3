package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

// An actual child acknowledges cancellation but deliberately keeps its shutdown
// path alive until the test releases it. Admission must wait for the OS process
// to finish, rather than merely observing context cancellation.
func TestIdlePrioritySummaryWorkerHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "idle-priority-summary" {
		return
	}
	directory := os.Args[len(os.Args)-1]
	err := workerproc.Serve(context.Background(), "controller-summary", func(_ context.Context, _ string, _ json.RawMessage, _ *workerproc.Peer) (workerproc.Handler, error) {
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			if method != "execute" {
				return nil, errors.New("unexpected summary operation")
			}
			if err := os.WriteFile(filepath.Join(directory, "started"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
				return nil, err
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil
	})
	if err := os.WriteFile(filepath.Join(directory, "canceled"), nil, 0600); err != nil {
		os.Exit(3)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "release")); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			os.Exit(4)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func idlePriorityWaitMarker(t *testing.T, directory, name string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(directory, name)); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("idle summary child did not report " + name)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestQueuedWorkPreemptsIdleSummaryButWaitsForActualChildReaping(t *testing.T) {
	for _, kind := range []string{"experiment", "analysis"} {
		t.Run(kind, func(t *testing.T) {
			f := newControllerWorkerFixture(t)
			s := f.server
			old := summaryBoundaryCompleteRun(t, f)
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, old.ID, workerSummaryFile)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(old.ID)}}); err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			release := func() { _ = os.WriteFile(filepath.Join(directory, "release"), nil, 0600) }
			t.Cleanup(release)
			original := s.workerCommand
			var summaryCommand *exec.Cmd
			s.workerCommand = func(workerKind string) *exec.Cmd {
				if workerKind != "controller-summary" {
					return original(workerKind)
				}
				summaryCommand = exec.Command(os.Args[0], "-test.run=^TestIdlePrioritySummaryWorkerHelper$", "--", "idle-priority-summary", directory)
				summaryCommand.Stdout, summaryCommand.Stderr = io.Discard, io.Discard
				return summaryCommand
			}
			idleDone := make(chan bool, 1)
			go func() { idleDone <- s.tryRefreshIdleRunMetrics(t.Context(), old.ID) }()
			idlePriorityWaitMarker(t, directory, "started")
			head := queueTestEnqueue(t, s, kind)
			t.Cleanup(release)
			idlePriorityWaitMarker(t, directory, "canceled")
			s.cancelMu.Lock()
			active, opened := s.summaryRefreshActive, head.slot.opened
			s.cancelMu.Unlock()
			if !active || opened {
				t.Fatalf("canceling idle work released an active child: maintenance=%t opened=%t", active, opened)
			}
			select {
			case <-head.entered:
				t.Fatal("queued work entered before the canceled child finished cleanup")
			default:
			}
			release()
			select {
			case claimed := <-idleDone:
				if !claimed {
					t.Fatal("idle correction never owned maintenance")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("idle correction failed to yield to queued work")
			}
			queueTestAwait(t, head.entered, "queued work after canceled idle child cleanup")
			if summaryCommand.ProcessState == nil || !summaryCommand.ProcessState.Exited() {
				t.Fatal("queued work entered before the actual summary process was reaped")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("interrupted maintenance replaced the completed summary: %v", err)
			}
			s.state.mu.RLock()
			dirty := s.state.summaryDirty[old.ID]
			s.state.mu.RUnlock()
			if !dirty {
				t.Fatal("preemption discarded the historical metric correction")
			}
			head.once.Do(func() { close(head.release) })
			queueTestAwait(t, head.finished, "queued work completion")
			summaryBoundaryAssertReleased(t, s)
		})
	}
}

func TestFreshExperimentDoesNotReclaimUnrelatedHistoricalMetricWork(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	old := summaryBoundaryCompleteRun(t, f)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(old.ID)}}); err != nil {
		t.Fatal(err)
	}
	var historicalCalculations atomic.Int32
	original := s.workerCommand
	s.workerCommand = func(kind string) *exec.Cmd {
		if kind == "controller-summary" {
			historicalCalculations.Add(1)
		}
		return original(kind)
	}
	run, err := s.StartScenario(t.Context(), []byte(summaryBoundaryScenario))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, s)
	s.state.mu.RLock()
	finished, dirty := s.state.experiments[run.ID], s.state.summaryDirty[old.ID]
	s.state.mu.RUnlock()
	if finished.State != "completed" || !dirty || historicalCalculations.Load() != 0 {
		t.Fatalf("fresh experiment consumed unrelated repair work: state=%s oldDirty=%t calculations=%d", finished.State, dirty, historicalCalculations.Load())
	}
	if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 0 {
		t.Fatalf("historical correction ran inside the new experiment: count=%g err=%v", count, err)
	}
	summaryBoundaryAssertReleased(t, s)
}
