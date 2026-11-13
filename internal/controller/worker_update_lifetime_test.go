package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestRunUpdateLifetimeWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "run-update-lifetime-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "controller-run", func(_ context.Context, _ string, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var run model.Experiment
		if err := json.Unmarshal(bootstrap, &run); err != nil {
			return nil, err
		}
		client := &runWorkerClient{peer: peer, runID: run.ID}
		return func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			run.PhaseName = "Admitted worker update"
			return nil, client.call(ctx, "update", run, nil)
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// Detect the real callback at its commit mutex without adding a production
// test hook or relying on the scheduler to win a short sleep-based race.
func waitRunUpdateCommitBoundary(t *testing.T) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	stacks := make([]byte, 1<<20)
	for {
		stack := string(stacks[:runtime.Stack(stacks, true)])
		for _, goroutine := range strings.Split(stack, "\n\n") {
			if strings.Contains(goroutine, "controller.(*runWorkerBroker).dispatch(") &&
				strings.Contains(goroutine, "controller.(*Server).updateExperiment(") &&
				strings.Contains(goroutine, "Mutex).") {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatal("admitted update never reached the persistence commit boundary")
		case <-ticker.C:
		}
	}
}

func TestRunWorkerFailureSerializesWithAdmittedUpdate(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	run := model.Experiment{ID: "blocked-run-update", ExecutionID: "execution-one", State: "running", StartedAt: time.Now().UTC()}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte("name: update-lifetime\nphases: [{action: wait, duration: 1ms}]\n")); err != nil {
		t.Fatal(err)
	}
	broker := &runWorkerBroker{server: s, runID: run.ID, executionID: run.ExecutionID, generation: 1}
	process, err := workerproc.Start(t.Context(), "controller-run", run, broker.handle, workerproc.Options{
		RunID: run.ID,
		Command: func(string) *exec.Cmd {
			command := exec.Command(os.Args[0], "-test.run=^TestRunUpdateLifetimeWorkerHelper$", "--", "run-update-lifetime-helper")
			command.Stdout, command.Stderr = io.Discard, io.Discard
			return command
		},
		GracePeriod: 50 * time.Millisecond,
		KillWait:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	broker.process = process
	t.Cleanup(func() {
		_ = broker.close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := process.CloseContext(ctx); err != nil {
			t.Errorf("reap update worker: %v", err)
		}
	})
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	callDone := make(chan error, 1)
	go func() { callDone <- process.Call(t.Context(), "execute", nil, nil) }()
	waitRunUpdateCommitBoundary(t)
	if err := broker.close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err = process.CloseContext(ctx)
	cancel()
	if !errors.Is(err, workerproc.ErrNotQuiescent) {
		t.Fatalf("OS reaping did not retain the admitted callback boundary: %v", err)
	}
	select {
	case <-process.Done():
		t.Fatal("worker Done ignored the blocked parent update")
	default:
	}
	// Use the actual failure path, rather than directly replacing state while
	// holding this fixture lock: failure and the admitted update must serialize
	// on the same persistence mutex before any later Run can reuse admission.
	failureDone := make(chan struct{})
	go func() {
		defer close(failureDone)
		s.failRunWorker(run.ID, errors.New("worker callback did not quiesce"))
	}()
	s.state.persistMu.Unlock()
	locked = false
	select {
	case <-failureDone:
	case <-time.After(5 * time.Second):
		t.Fatal("worker failure could not finish after the update commit was released")
	}
	select {
	case <-process.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("worker callback did not release the process admission barrier")
	}
	<-callDone
	s.state.mu.RLock()
	final := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if final.State != "failed" || final.FinishedAt.IsZero() || final.CleanupState != "failed" || !strings.Contains(final.Error, "worker callback did not quiesce") {
		t.Fatalf("admitted callback replaced the authoritative failure: %+v", final)
	}
	encoded, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var stored model.Experiment
	if err := json.Unmarshal(encoded, &stored); err != nil || stored.State != "failed" || stored.Error != final.Error {
		t.Fatalf("durable worker failure differs from the final state: state=%q err=%v", stored.State, err)
	}
}
