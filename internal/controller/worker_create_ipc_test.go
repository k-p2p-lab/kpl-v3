package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type workerCreateIPCTestInput struct {
	Call    workerAgentCall
	Timeout time.Duration
}
type workerCreateIPCTestProof struct {
	Node      model.Node
	Failure   *workerDomainError
	Transport bool
}

func TestControllerCreateIPCWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "create-ipc-helper" {
		return
	}
	factory := func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		client := &runWorkerClient{peer: peer, runID: peer.RunID(), ctx: ctx}
		return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
			var request workerCreateIPCTestInput
			if err := json.Unmarshal(input, &request); err != nil {
				return nil, err
			}
			if request.Timeout > 0 {
				var cancel context.CancelFunc
				callCtx, cancel = context.WithTimeout(callCtx, request.Timeout)
				defer cancel()
			}
			var node model.Node
			err := client.callAgent(callCtx, request.Call.BaseURL, request.Call.Method, request.Call.Path, request.Call.Input, &node)
			var transport *workerCallTransportError
			return workerCreateIPCTestProof{Node: node, Failure: encodeWorkerError(err), Transport: errors.As(err, &transport)}, nil
		}, nil
	}
	if err := workerproc.Serve(context.Background(), "controller-create-test", factory); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestWorkerCreateIPCDeadlinePreservesSingleAdmissionAndReplay(t *testing.T) {
	for _, mode := range []string{"recover-ack", "stop-race"} {
		t.Run(mode, func(t *testing.T) {
			accepted := make(chan struct{})
			httpReply := make(chan struct{})
			brokerReply := make(chan struct{})
			var releaseHTTP, releaseBroker sync.Once
			release := func() { releaseHTTP.Do(func() { close(httpReply) }); releaseBroker.Do(func() { close(brokerReply) }) }
			var admitted atomic.Int32
			f := newBrokerCreateFixture(t, func(f *brokerCreateFixture, w http.ResponseWriter, r *http.Request, request model.CreateNodeRequest) {
				if f.calls.Load() == 1 {
					admitted.Add(1)
					f.publishCreatedHeartbeat(t, request)
					close(accepted)
					<-httpReply
				}
				writeJSON(w, 201, model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, AgentID: f.owner.ID, State: model.NodeReady})
			})
			first := atomic.Bool{}
			first.Store(true)
			parent := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
				value, err := f.b.handle(ctx, method, raw)
				if method == "agent" && first.Swap(false) {
					<-brokerReply
				}
				return value, err
			}
			process, err := workerproc.Start(t.Context(), "controller-create-test", map[string]string{"runId": f.b.runID}, parent, workerproc.Options{Command: func(string) *exec.Cmd {
				return exec.Command(os.Args[0], "-test.run=^TestControllerCreateIPCWorkerHelper$", "--", "create-ipc-helper")
			}, Stdout: io.Discard, Stderr: io.Discard})
			if err != nil {
				release()
				t.Fatal(err)
			}
			t.Cleanup(func() { release(); _ = process.Close() })
			var proof workerCreateIPCTestProof
			done := make(chan error, 1)
			go func() {
				done <- process.Call(t.Context(), "create", workerCreateIPCTestInput{Call: f.call, Timeout: 500 * time.Millisecond}, &proof)
			}()
			select {
			case <-accepted:
			case <-time.After(3 * time.Second):
				t.Fatal("initial HTTP create was not admitted")
			}
			select {
			case err = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("private create RPC did not honor its deadline")
			}
			if err != nil || proof.Failure == nil || proof.Failure.Kind != "uncertain" || !proof.Failure.Deadline || !proof.Transport {
				t.Fatalf("IPC deadline lost possible HTTP admission: err=%v proof=%+v", err, proof)
			}
			f.b.server.state.mu.RLock()
			reserved := f.b.server.state.reservations[f.request.ID]
			f.b.server.state.mu.RUnlock()
			if reserved != "" {
				t.Fatal("accepted heartbeat did not consume the original reservation")
			}
			if mode == "stop-race" {
				f.b.server.state.mu.Lock()
				run := f.b.server.state.experiments[f.b.runID]
				run.StopRequested = true
				f.b.server.state.experiments[run.ID] = run
				f.b.server.state.mu.Unlock()
			}
			release()
			proof = workerCreateIPCTestProof{}
			if err := process.Call(t.Context(), "create", workerCreateIPCTestInput{Call: f.call, Timeout: 2 * time.Second}, &proof); err != nil {
				t.Fatal(err)
			}
			if mode == "recover-ack" {
				if proof.Failure != nil || proof.Node.ID != f.request.ID || admitted.Load() != 1 || f.calls.Load() != 2 {
					t.Fatalf("IPC timeout replay duplicated or lost creation: %+v admitted=%d calls=%d", proof, admitted.Load(), f.calls.Load())
				}
				created, _ := json.Marshal(workerCreated{Request: f.request, AgentID: f.owner.ID, Node: proof.Node, Expected: []time.Time{f.owner.StartedAt}})
				if _, err := f.b.dispatch(t.Context(), "created", created); err != nil {
					t.Fatal(err)
				}
			} else {
				if proof.Failure == nil || !proof.Failure.Canceled || proof.Failure.Kind != "uncertain" || f.calls.Load() != 1 {
					t.Fatalf("Stop lost cancellation or redispatched the create: %+v calls=%d", proof, f.calls.Load())
				}
				fence, _ := json.Marshal(workerGeneration{RunID: f.b.runID, Generation: ^uint64(0)})
				if _, err := f.b.dispatch(t.Context(), "stop-generation", fence); err != nil {
					t.Fatal(err)
				}
			}
			f.b.mu.Lock()
			leases := len(f.b.createLeases)
			f.b.mu.Unlock()
			if leases != 0 {
				t.Fatal("acknowledgement or terminal fence retained create evidence")
			}
		})
	}
}

func TestWorkerCreateTransportOnlyExplicitRemoteBusyDefers(t *testing.T) {
	for _, test := range []struct {
		name     string
		failure  error
		deferred bool
	}{
		{"remote-busy", &workerproc.RemoteError{Code: "busy", Message: "request not admitted"}, true},
		{"connection-busy", workerproc.ErrBusy, false},
		{"connection-EOF", io.EOF, false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := classifyWorkerCreateTransport(&workerCallTransportError{test.failure})
			var deferred *agentAdmissionDeferredError
			var uncertain *uncertainNodeCreateError
			if errors.As(err, &deferred) != test.deferred || errors.As(err, &uncertain) == test.deferred {
				t.Fatalf("IPC refusal classification is unsafe: %T %v", err, err)
			}
			if !test.deferred && !errors.Is(err, test.failure) {
				t.Fatal("transport or cancellation cause was discarded")
			}
		})
	}
	parentDenial := &agentAdmissionDeferredError{after: time.Second, reason: "agent-offline"}
	if classifyWorkerCreateTransport(parentDenial) != parentDenial {
		t.Fatal("parent application errors acquired transport uncertainty")
	}
}
