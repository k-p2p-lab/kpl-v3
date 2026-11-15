package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestControllerCommandDeadlineWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "command-deadline-helper" {
		return
	}
	kind := "controller-command-deadline"
	if len(os.Args) > 2 && os.Args[len(os.Args)-2] == "controller-run-stall" {
		kind = "controller-run"
		// Only this isolated helper ignores TERM, requiring the supervisor's
		// asynchronous KILL/Wait path rather than test teardown to reclaim it.
		signal.Ignore(syscall.SIGTERM)
	}
	err := workerproc.Serve(context.Background(), kind, func(factoryCtx context.Context, _ string, _ json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		server := New(ServerConfig{}, nil)
		client := &runWorkerClient{peer: peer, runID: peer.RunID(), ctx: factoryCtx, server: server}
		return func(callCtx context.Context, method string, _ json.RawMessage) (any, error) {
			if kind == "controller-run" {
				if method == "cancel" {
					return nil, nil
				}
				ctx, cancel := context.WithTimeout(callCtx, 100*time.Millisecond)
				defer cancel()
				if err := client.beginFinalization(ctx); err != nil {
					return nil, err
				}
				for {
					time.Sleep(time.Hour)
				}
			}
			if method == "command-errors" {
				err := client.command(callCtx, "refresh", nil)
				proof := workerCommandErrorProof{WorkerPending: hasPendingRunWorker(err)}
				if err != nil {
					proof.Error = err.Error()
				}
				return proof, nil
			}
			if method == "contended-execute" {
				go func() { _ = client.syncInventory(factoryCtx) }()
				if err := client.call(callCtx, "wait-inventory", nil, nil); err != nil {
					return nil, err
				}
			}
			if method == "stall-after-finalization" {
				ctx, cancel := context.WithTimeout(callCtx, 100*time.Millisecond)
				defer cancel()
				if err := client.beginFinalization(ctx); err != nil {
					return nil, err
				}
				<-factoryCtx.Done()
				return nil, nil
			}
			ctx, cancel := context.WithTimeout(callCtx, 100*time.Millisecond)
			defer cancel()
			err := client.command(ctx, "refresh", nil)
			return errors.Is(err, context.DeadlineExceeded), nil
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	if kind == "controller-run" {
		for {
			time.Sleep(time.Hour)
		}
	}
	os.Exit(0)
}

func TestExpiredFinalizationReapsWorkerBeforeNextRun(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux process-group signal semantics")
	}
	server := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	var starts atomic.Int32
	var oldCommand *exec.Cmd
	nextStarted := make(chan struct{})
	server.workerCommand = func(kind string) *exec.Cmd {
		if kind == "controller-run" {
			switch starts.Add(1) {
			case 1:
				command := exec.Command(os.Args[0], "-test.run=^TestControllerCommandDeadlineWorkerHelper$", "--", "controller-run-stall", "command-deadline-helper")
				command.Stdout, command.Stderr = io.Discard, io.Discard
				oldCommand = command
				return command
			case 2:
				close(nextStarted)
			}
		}
		command := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}
	raw := []byte("version: 4\nname: asynchronous-worker-recovery\njobShutdownTimeout: 2s\nphases: [{action: wait, duration: 1ms}]\n")
	first, err := server.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	var unreaped []*workerproc.Process
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for len(unreaped) == 0 {
		server.workerMu.Lock()
		unreaped = append([]*workerproc.Process(nil), server.unreapedWorkers[first.ID]...)
		server.workerMu.Unlock()
		if len(unreaped) != 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("expired finalization did not retain its live worker")
		}
	}
	if len(unreaped) != 1 {
		t.Fatalf("unexpected unreaped worker count: %d", len(unreaped))
	}
	old := unreaped[0]
	t.Cleanup(func() { _ = old.Close() })
	second, err := server.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-nextStarted:
		select {
		case <-old.Done():
		default:
			t.Fatal("next Run started before the previous OS process and RPCs were reaped")
		}
	case <-old.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("asynchronous KILL/Wait did not finish without test teardown")
	}
	if oldCommand.ProcessState == nil {
		t.Fatal("worker Done did not include OS Wait")
	}
	status, ok := oldCommand.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("isolated TERM-resistant worker was not reclaimed by KILL: %v", oldCommand.ProcessState)
	}
	waitWorkerRuns(t, server)
	server.state.mu.RLock()
	failed := server.state.experiments[first.ID]
	completed := server.state.experiments[second.ID]
	server.state.mu.RUnlock()
	if failed.State != "failed" || failed.DataState != "incomplete" || !strings.Contains(failed.Error, "finalization") || completed.State != "completed" || completed.DataState != "complete" {
		t.Fatalf("recovery changed the failed scientific result or blocked the next Run: failed=%+v next=%+v", failed, completed)
	}
	server.workerMu.Lock()
	retained := len(server.unreapedWorkers[first.ID])
	server.workerMu.Unlock()
	if retained != 0 {
		t.Fatal("reaped worker retained a stale admission barrier")
	}
}

type workerCommandErrorProof struct {
	Error         string
	WorkerPending bool
}

func TestWorkerCommandPreservesOperationAndInventoryErrors(t *testing.T) {
	for _, originalPending := range []bool{false, true} {
		t.Run(map[bool]string{false: "inventory-only", true: "operation-and-inventory"}[originalPending], func(t *testing.T) {
			process, err := workerproc.Start(t.Context(), "controller-command-deadline", struct{}{}, func(_ context.Context, method string, _ json.RawMessage) (any, error) {
				var err error
				if method == "refresh" && originalPending {
					err = &agentRunWorkerPendingError{message: "original pending worker proof"}
				} else if method == "inventory" {
					err = errors.New("inventory snapshot unavailable")
				}
				return workerReply{Error: encodeWorkerError(err)}, nil
			}, workerproc.Options{RunID: "command-error-evidence", GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
				command := exec.Command(os.Args[0], "-test.run=^TestControllerCommandDeadlineWorkerHelper$", "--", "command-deadline-helper")
				command.Stdout, command.Stderr = io.Discard, io.Discard
				return command
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer process.Close()
			var proof workerCommandErrorProof
			if err := process.Call(t.Context(), "command-errors", nil, &proof); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(proof.Error, "inventory snapshot unavailable") || proof.WorkerPending != originalPending || originalPending && !strings.Contains(proof.Error, "original pending worker proof") {
				t.Fatalf("command error evidence was discarded: %+v", proof)
			}
		})
	}
}

func TestFailedRunWorkerDeliveryHasIndependentCleanupBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		raw := []byte("name: failed-worker-delivery\nphases: [{action: wait, duration: 1ms}]\n")
		run := model.Experiment{ID: "failed-worker-delivery", State: "running", StartedAt: time.Now().UTC()}
		server.state.experiments[run.ID] = run
		if err := server.persistManifest(run, raw); err != nil {
			t.Fatal(err)
		}
		agent := model.Agent{ID: "agent", URL: "http://agent", State: model.AgentOnline, LastSeen: time.Now().UTC(), RunDrain: true, Capacity: 1}
		server.state.agents[agent.ID] = agent
		var drained bool
		server.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
			w := httptest.NewRecorder()
			switch r.Method {
			case http.MethodDelete:
				if err := sleepContext(r.Context(), 2*time.Minute); err != nil {
					return nil, err
				}
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
				return w.Result(), nil
			case http.MethodPost:
				if err := sleepContext(r.Context(), 2*time.Minute); err != nil {
					return nil, err
				}
				drained = true
			}
			w.WriteHeader(http.StatusNoContent)
			return w.Result(), nil
		})
		server.failRunWorker(run.ID, errors.New("original worker failure"))
		if !drained {
			t.Fatal("Peer removal consumed the failed worker's final delivery budget")
		}
		result := server.state.experiments[run.ID]
		if result.State != "failed" || result.DataState != "incomplete" || !strings.Contains(result.Error, "original worker failure") || result.TelemetryPending {
			t.Fatalf("successful cleanup discarded the scientific failure or invented pending telemetry: %+v", result)
		}
	})
}

// The command's parent callback actually expires through private IPC. Its
// optional local inventory refresh must not turn that bounded failure into an
// unbounded request with the deadline stripped off.
func TestWorkerCommandDeadlineDoesNotWaitForDetachedInventory(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	process, err := workerproc.Start(t.Context(), "controller-command-deadline", struct{}{}, func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
		switch method {
		case "refresh":
			<-ctx.Done()
			return workerReply{Error: encodeWorkerError(ctx.Err())}, nil
		case "inventory":
			select {
			case <-release:
			case <-ctx.Done():
			}
			return workerReply{Error: encodeWorkerError(errors.New("inventory intentionally unavailable"))}, nil
		default:
			return nil, errors.New("unexpected callback")
		}
	}, workerproc.Options{RunID: "command-deadline", GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestControllerCommandDeadlineWorkerHelper$", "--", "command-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	done := make(chan error, 1)
	var deadlinePreserved bool
	go func() { done <- process.Call(t.Context(), "execute", nil, &deadlinePreserved) }()
	select {
	case err := <-done:
		if err != nil || !deadlinePreserved {
			t.Fatalf("command lost its deadline failure: preserved=%v err=%v", deadlinePreserved, err)
		}
	case <-time.After(600 * time.Millisecond):
		unblock()
		<-done
		t.Fatal("expired command kept waiting for a detached inventory request")
	}
	unblock()
}

// Exercise WorkerFactory itself, including its real runScenario finalization.
// Only the final parent inventory callback is stalled, after the terminal Run
// update has already been received. No Docker or running service is involved.
func TestWorkerFinalSnapshotHonorsScenarioFinalizationBudget(t *testing.T) {
	testWorkerFinalSnapshotBudget(t, 100*time.Millisecond, 0, 600*time.Millisecond)
}

func TestWorkerFinalSnapshotUsesRemainingCollectionBudget(t *testing.T) {
	testWorkerFinalSnapshotBudget(t, 2*time.Second, 1200*time.Millisecond, 1300*time.Millisecond)
}

func testWorkerFinalSnapshotBudget(t *testing.T, budget, drainDelay, guard time.Duration) {
	t.Helper()
	server := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	raw := []byte(fmt.Sprintf("version: 4\nname: final-snapshot-deadline\njobShutdownTimeout: %s\nphases: [{action: wait, duration: 1ms}]\n", budget))
	run := model.Experiment{ID: "final-snapshot-deadline", ExecutionID: "execution", State: "running", StartedAt: time.Now().UTC(), ScenarioYAML: string(raw)}
	server.state.experiments[run.ID] = run
	if err := server.persistManifest(run, raw); err != nil {
		t.Fatal(err)
	}
	broker := &runWorkerBroker{server: server, runID: run.ID, executionID: run.ExecutionID, generation: 1}
	defer broker.close()
	finalInventory := make(chan struct{})
	release := make(chan struct{})
	var terminal, entered bool
	var mu sync.Mutex
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	handler := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "drain" && drainDelay != 0 {
			if err := sleepContext(ctx, drainDelay); err != nil {
				return workerReply{Error: encodeWorkerError(err)}, nil
			}
		}
		mu.Lock()
		if method == "update" {
			var incoming model.Experiment
			if err := json.Unmarshal(raw, &incoming); err != nil {
				mu.Unlock()
				return nil, err
			}
			if incoming.State == "completed" || incoming.State == "failed" || incoming.State == "canceled" {
				terminal = true
			}
		}
		block := method == "inventory" && terminal
		if block && !entered {
			entered = true
			close(finalInventory)
		}
		mu.Unlock()
		if block {
			select {
			case <-release:
			case <-ctx.Done():
				return workerReply{Error: encodeWorkerError(ctx.Err())}, nil
			}
		}
		return broker.handle(ctx, method, raw)
	}
	process, err := workerproc.Start(t.Context(), "controller-run", workerRunStart{RunID: run.ID, Experiment: run, Scenario: raw, DataDir: server.config.DataDir}, handler, workerproc.Options{RunID: run.ID, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", "controller-run")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	broker.process = process
	done := make(chan error, 1)
	go func() { done <- process.Call(t.Context(), "execute", nil, nil) }()
	select {
	case <-finalInventory:
	case err := <-done:
		t.Fatalf("Run never reached final inventory: %v", err)
	case <-time.After(3 * time.Second):
		unblock()
		<-done
		t.Fatal("Run never reached final inventory")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked final inventory lost its deadline: %v", err)
		}
	case <-time.After(guard):
		unblock()
		<-done
		t.Fatal("final snapshot exceeded the scenario finalization budget")
	}
	unblock()
}

func TestWorkerCommandInventoryLockHonorsDeadline(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	process, err := workerproc.Start(t.Context(), "controller-command-deadline", struct{}{}, func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
		switch method {
		case "refresh":
			return workerReply{}, nil
		case "wait-inventory":
			select {
			case <-entered:
				return workerReply{}, nil
			case <-ctx.Done():
				return workerReply{Error: encodeWorkerError(ctx.Err())}, nil
			}
		case "inventory":
			enterOnce.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
			}
			return workerReply{Error: encodeWorkerError(errors.New("observer inventory intentionally unavailable"))}, nil
		default:
			return nil, errors.New("unexpected callback")
		}
	}, workerproc.Options{RunID: "inventory-lock-deadline", GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestControllerCommandDeadlineWorkerHelper$", "--", "command-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	done := make(chan error, 1)
	var deadlinePreserved bool
	go func() { done <- process.Call(t.Context(), "contended-execute", nil, &deadlinePreserved) }()
	select {
	case err := <-done:
		if err != nil || !deadlinePreserved {
			t.Fatalf("inventory lock deadline was lost: preserved=%v err=%v", deadlinePreserved, err)
		}
	case <-time.After(600 * time.Millisecond):
		unblock()
		<-done
		t.Fatal("cleanup command waited for the observer inventory lock after its deadline")
	}
	unblock()
}

func TestParentWatchesFinalizationWithoutExternalCancellation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	broker := &runWorkerBroker{server: server, runID: "parent-finalization-deadline", finalizing: make(chan struct{}, 1)}
	defer broker.close()
	process, err := workerproc.Start(t.Context(), "controller-command-deadline", struct{}{}, broker.handle, workerproc.Options{RunID: broker.runID, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestControllerCommandDeadlineWorkerHelper$", "--", "command-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer process.Close()
	finished := make(chan error, 1)
	go func() { finished <- process.Call(t.Context(), "stall-after-finalization", nil, nil) }()
	done := make(chan error, 1)
	go func() {
		done <- waitRunWorkerExecution(context.Background(), t.Context(), process, finished, broker, scenario.Scenario{})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("local worker stall did not preserve its finalization deadline: %v", err)
		}
	case <-time.After(600 * time.Millisecond):
		_ = process.Close()
		<-done
		t.Fatal("parent required external cancellation to stop finalization")
	}
	select {
	case <-process.Done():
		t.Fatal("timeout alone unexpectedly claimed process reclamation")
	default:
	}
}

func TestWorkerFinalizationNotificationLossRemainsBounded(t *testing.T) {
	for _, delivered := range []bool{false, true} {
		t.Run(map[bool]string{false: "request-not-applied", true: "acknowledgment-lost"}[delivered], func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			raw := []byte("version: 4\nname: lost-finalization-notice\njobShutdownTimeout: 100ms\nphases: [{action: wait, duration: 1ms}]\n")
			run := model.Experiment{ID: "lost-finalization-notice", ExecutionID: "execution", State: "running", StartedAt: time.Now().UTC(), ScenarioYAML: string(raw)}
			server.state.experiments[run.ID] = run
			if err := server.persistManifest(run, raw); err != nil {
				t.Fatal(err)
			}
			broker := &runWorkerBroker{server: server, runID: run.ID, executionID: run.ExecutionID, generation: 1, finalizing: make(chan struct{}, 1)}
			defer broker.close()
			handler := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
				if method == "begin-finalization" {
					if delivered {
						if _, err := broker.handle(ctx, method, raw); err != nil {
							return nil, err
						}
					}
					<-ctx.Done()
					return workerReply{Error: encodeWorkerError(ctx.Err())}, nil
				}
				return broker.handle(ctx, method, raw)
			}
			process, err := workerproc.Start(t.Context(), "controller-run", workerRunStart{RunID: run.ID, Experiment: run, Scenario: raw, DataDir: server.config.DataDir}, handler, workerproc.Options{RunID: run.ID, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
				command := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", "controller-run")
				command.Stdout, command.Stderr = io.Discard, io.Discard
				return command
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer process.Close()
			done := make(chan error, 1)
			go func() { done <- process.Call(t.Context(), "execute", nil, nil) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost notice did not preserve its deadline: %v", err)
				}
			case <-time.After(600 * time.Millisecond):
				_ = process.Close()
				<-done
				t.Fatal("worker required a parent finalization timer after notification loss")
			}
			broker.mu.Lock()
			recorded := !broker.finalizationEnd.IsZero()
			broker.mu.Unlock()
			if recorded != delivered {
				t.Fatalf("fixture did not preserve notification-loss boundary: recorded=%v delivered=%v", recorded, delivered)
			}
		})
	}
}
