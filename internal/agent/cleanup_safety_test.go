package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type cleanupWaitContext struct {
	context.Context
	waiting chan struct{}
}

func (c cleanupWaitContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func TestWaitRunContainersFollowsReplacementCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	waiting := make(chan struct{}, 2)
	first, retry := make(chan struct{}), make(chan struct{})
	proc := &process{node: model.Node{ID: "peer", RunID: "run", Generation: 1, State: model.NodeStopping}, done: first}
	server := &Server{processes: map[string]*process{"peer": proc}}
	done := make(chan error, 1)
	go func() { done <- server.waitRunContainers(cleanupWaitContext{ctx, waiting}, "run", 1) }()
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("cleanup wait did not start")
	}
	server.mu.Lock()
	proc.cleanupErr = errors.New("previous removal failed")
	proc.done = retry
	close(first)
	server.mu.Unlock()
	select {
	case err := <-done:
		t.Fatalf("returned before replacement cleanup completed: %v", err)
	case <-waiting:
	case <-ctx.Done():
		t.Fatal("cleanup wait did not observe replacement")
	}
	server.mu.Lock()
	proc.cleanupErr = nil
	proc.exited = true
	proc.node.State = model.NodeStopped
	close(retry)
	server.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWaitRunContainersRejectsMissingCompletionSignal(t *testing.T) {
	server := &Server{processes: map[string]*process{"peer": {
		node: model.Node{ID: "peer", RunID: "run", State: model.NodeStopping},
	}}}
	if err := server.waitRunContainers(t.Context(), "run", 1); err == nil {
		t.Fatal("active process without a completion signal was reported cleaned up")
	}
}

func TestStopCancelsTerminalStatusUntilContainerActuallyExits(t *testing.T) {
	for _, state := range []string{model.NodeFailed, model.NodeStopped, model.NodeStopping} {
		for _, stop := range []string{"node", "run", "all"} {
			t.Run(state+"/"+stop, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				proc := &process{node: model.Node{ID: "peer", RunID: "run", Generation: 1, State: model.NodeStarting}, cancel: cancel}
				server := &Server{config: Config{Capacity: 1}, processes: map[string]*process{"peer": proc}}
				if err := server.updateNode(model.Node{ID: "peer", State: state}); err != nil {
					t.Fatal(err)
				}
				switch stop {
				case "node":
					if err := server.stopNode("peer"); err != nil {
						t.Fatal(err)
					}
				case "run":
					server.stopRunGeneration("run", 1)
				case "all":
					server.stopAll()
				}
				if ctx.Err() == nil {
					t.Error("terminal status prevented cancellation of a live container")
				}
				if proc.node.State != model.NodeStopping {
					t.Errorf("state = %s, want stopping", proc.node.State)
				}
				if server.snapshotAgent().ActiveNodes != 1 {
					t.Error("capacity released before container cleanup")
				}
			})
		}
	}
}

func TestTelemetryContentionDoesNotBlockAgentState(t *testing.T) {
	for _, action := range []string{"finish", "admission"} {
		t.Run(action, func(t *testing.T) {
			proc := &process{node: model.Node{ID: "peer", RunID: "run", State: model.NodeStopping}, done: make(chan struct{})}
			server := &Server{config: Config{Capacity: 2}, processes: map[string]*process{"peer": proc}}
			server.eventsMu.Lock()
			finished := make(chan struct{})
			started := make(chan struct{})
			go func() {
				defer close(finished)
				close(started)
				if action == "finish" {
					server.finishProcess("peer", proc, nil, nil)
				} else {
					if err := server.lockAdmission(t.Context(), model.CreateNodeRequest{ID: "new", RunID: "run", Group: "workers"}); err != nil {
						t.Error(err)
					} else {
						server.mu.Unlock()
					}
				}
			}()
			defer func() {
				server.eventsMu.Unlock()
				<-finished
				if action == "finish" {
					if !proc.exited || !processCleanupComplete(proc) {
						t.Error("finished cleanup was not published")
					}
					if event, ok := server.terminations["peer"]; !ok || event.Type != "measurement_terminated" {
						t.Error("completion was published without termination evidence")
					}
				}
			}()
			<-started
			// Leave the operation blocked behind an ongoing telemetry disk write.
			select {
			case <-finished:
				t.Fatal("operation bypassed the held telemetry lock")
			case <-time.After(25 * time.Millisecond):
			}
			snapshot := make(chan model.Agent, 1)
			go func() { snapshot <- server.snapshotAgent() }()
			select {
			case agent := <-snapshot:
				if agent.ActiveNodes != 1 {
					t.Fatal("capacity released before termination evidence was retained")
				}
				select {
				case <-proc.done:
					t.Error("cleanup completion preceded termination evidence")
				default:
				}
			case <-time.After(time.Second):
				t.Error("telemetry contention blocked Agent heartbeat/state access")
			}
		})
	}
}

func TestCanceledAdmissionDoesNotReserveAfterTelemetryWait(t *testing.T) {
	server := &Server{config: Config{Capacity: 1}, processes: make(map[string]*process)}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	server.eventsMu.Lock()
	done := make(chan error, 1)
	go func() {
		err := server.lockAdmission(ctx, model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"})
		if err == nil {
			server.mu.Unlock()
		}
		done <- err
	}()
	select {
	case err := <-done:
		server.eventsMu.Unlock()
		t.Fatalf("admission bypassed telemetry wait: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	server.eventsMu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission succeeded: %v", err)
	}
}

func TestWaitRunContainersPreservesCleanupFailure(t *testing.T) {
	failure := errors.New("daemon did not confirm removal")
	server := &Server{processes: map[string]*process{"peer": {
		node: model.Node{ID: "peer", RunID: "run", State: model.NodeFailed}, exited: true, cleanupErr: failure,
	}}}
	if err := server.waitRunContainers(t.Context(), "run", 1); !errors.Is(err, failure) || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("cleanup failure was lost: %v", err)
	}
}
