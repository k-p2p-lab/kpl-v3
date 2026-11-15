package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type resetRetryBody struct {
	reader  io.Reader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type resetIdleObserver struct {
	http.RoundTripper
	closed func()
}

func (t resetIdleObserver) CloseIdleConnections() {
	t.closed()
	if transport, ok := t.RoundTripper.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
}

func (r *resetRetryBody) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.reader.Read(p)
}

func TestRunProcessResetRetryAfterReapedWorkerPreservesSuccessfulExit(t *testing.T) {
	testRunProcessResetRetryAfterReaping(t, agentRunProcessFactory(), "")
}

func TestAgentRunFinalizationFailureHelper(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "agent-run-finalization-failure-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "agent-run", func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		handler, err := NewRunWorkerHandler(ctx, kind, raw, peer)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
			if method == "run.finish" {
				return nil, errors.New("fixture Run finalization was rejected")
			}
			return handler(ctx, method, raw)
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAgentRunBlockedFinalizationHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "agent-run-blocked-finalization-helper" {
		return
	}
	endpoint := os.Args[len(os.Args)-2]
	err := workerproc.Serve(context.Background(), "agent-run", func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		handler, err := NewRunWorkerHandler(ctx, kind, raw, peer)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
			if method == "run.finish" {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				if err != nil {
					return nil, err
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					return nil, err
				}
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if err != nil {
					return nil, err
				}
			}
			return handler(ctx, method, raw)
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRunProcessConcurrentResetHonorsWaitingRequestCancellation(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(gate.Close)
	s.config.RunWorkerFactory = func(ctx context.Context, config RunWorkerConfig, broker workerproc.Handler) (*workerproc.Process, error) {
		return workerproc.Start(ctx, "agent-run", config, broker, workerproc.Options{
			Command: func(string) *exec.Cmd {
				return exec.Command(os.Args[0], "-test.run=^TestAgentRunBlockedFinalizationHelper$", "--", gate.URL, "agent-run-blocked-finalization-helper")
			}, Stdout: io.Discard, Stderr: io.Discard, GracePeriod: 100 * time.Millisecond, KillWait: time.Second,
		})
	}
	workerControllerFixture(t, s)
	var idleCloses atomic.Int32
	secondPrepared := make(chan struct{})
	transport := s.docker.waitClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	s.docker.waitClient.Transport = resetIdleObserver{RoundTripper: transport, closed: func() {
		if idleCloses.Add(1) == 2 {
			close(secondPrepared)
		}
	}}
	const runID = "concurrent-finalization"
	worker, err := s.ensureRunWorker(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, firstCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer firstCancel()
	first := make(chan error, 1)
	go func() { first <- s.finishRunWorker(firstCtx, runID) }()
	select {
	case <-entered:
	case <-firstCtx.Done():
		t.Fatal("the first real worker did not enter finalization")
	}
	waitCtx, waitCancel := context.WithCancel(t.Context())
	defer waitCancel()
	second := make(chan error, 1)
	go func() { second <- s.finishRunWorker(waitCtx, runID) }()
	select {
	case <-secondPrepared:
	case <-firstCtx.Done():
		t.Fatal("the second reset did not complete its pre-finalization resource proof")
	}
	// Preparation is complete and only the per-worker finalization is occupied.
	// Allow that waiter to park before canceling, rather than canceling its
	// final heartbeat and accidentally testing a different blocking boundary.
	select {
	case err := <-second:
		t.Fatalf("second reset bypassed the occupied finalization before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	waitCancel()
	select {
	case err := <-second:
		var pending *runWorkerPendingError
		if !errors.Is(err, context.Canceled) || !errors.As(err, &pending) {
			t.Fatalf("canceled reset waiter did not retain its pending barrier: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("canceled reset request remained blocked behind another finalization")
	}
	select {
	case <-worker.process.Done():
		t.Fatal("canceling a second reset terminated the first worker's finalization")
	default:
	}
	if err := s.runWorkerAdmissionReady("next-run"); !errors.Is(err, errRunResetPending) {
		t.Fatalf("canceled reset waiter cleared the shared worker barrier: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("the original finalization lost its result: %v", err)
		}
	case <-firstCtx.Done():
		t.Fatal("released original finalization did not finish")
	}
}

func TestRunProcessResetRetryPreservesFailureBeforeLateIngress(t *testing.T) {
	factory := func(ctx context.Context, config RunWorkerConfig, broker workerproc.Handler) (*workerproc.Process, error) {
		return workerproc.Start(ctx, "agent-run", config, broker, workerproc.Options{
			Command: func(string) *exec.Cmd {
				return exec.Command(os.Args[0], "-test.run=^TestAgentRunFinalizationFailureHelper$", "--", "agent-run-finalization-failure-helper")
			},
			Stdout: io.Discard, Stderr: io.Discard, GracePeriod: 100 * time.Millisecond, KillWait: time.Second,
		})
	}
	testRunProcessResetRetryAfterReaping(t, factory, "fixture Run finalization was rejected")
}

func testRunProcessResetRetryAfterReaping(t *testing.T, factory RunWorkerFactory, expectedFailure string) {
	t.Helper()
	s, _ := newContainerTestServer(t, nil)
	s.config.RunWorkerFactory = factory
	var delivered atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/events/batch" {
			var batch model.EventBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
			}
			for _, event := range batch.Events {
				if event.EventID == "after-worker-wait" {
					delivered.Add(1)
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(controller.Close)
	s.config.ControllerURL = controller.URL
	const runID = "late-ingress-after-worker-wait"
	worker, err := s.ensureRunWorker(t.Context(), runID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(model.EventBatch{Events: []model.TraceEvent{{EventID: "after-worker-wait", RunID: runID, NodeID: "removed-peer", Type: "join", Timestamp: time.Now().UTC()}}})
	if err != nil {
		t.Fatal(err)
	}
	body := &resetRetryBody{reader: strings.NewReader(string(data)), entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(body.release) }) })
	response := httptest.NewRecorder()
	ingressDone := make(chan struct{})
	originalCancel := worker.cancel
	var injectOnce sync.Once
	worker.cancel = func() {
		originalCancel()
		injectOnce.Do(func() {
			// The real OS child has already been reaped at this point. Admit
			// a new body decoder before the supervisor's final reset proof.
			select {
			case <-worker.process.Done():
			default:
				t.Error("fixture did not reach the post-reaping boundary")
			}
			go func() {
				defer close(ingressDone)
				s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", body))
			}()
			select {
			case <-body.entered:
			case <-time.After(3 * time.Second):
				t.Error("late telemetry body did not enter its decoder")
			}
		})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var pending *runWorkerPendingError
	if err := s.finishRunWorker(ctx, runID); !errors.As(err, &pending) {
		t.Fatalf("late ingress did not preserve the reset barrier: %v", err)
	}
	if err := worker.process.Wait(); err != nil {
		t.Fatalf("first reset did not gracefully reap its real worker: %v", err)
	}
	releaseOnce.Do(func() { close(body.release) })
	select {
	case <-ingressDone:
	case <-ctx.Done():
		t.Fatal("late ingress did not finish")
	}
	if response.Code != http.StatusNoContent {
		t.Fatalf("late body was not acknowledged: %d %s", response.Code, response.Body)
	}
	if err := s.drainRunEvents(ctx, runID); err != nil {
		t.Fatalf("late valid event was not drained before the retry: %v", err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("late event was lost or duplicated: %d deliveries", delivered.Load())
	}
	err = s.finishRunWorker(ctx, runID)
	if expectedFailure == "" {
		if err != nil {
			t.Fatalf("successful OS worker finalization became a failure during a valid reset retry: %v", err)
		}
	} else {
		var failed *runWorkerFailedError
		if !errors.As(err, &failed) || !strings.Contains(err.Error(), expectedFailure) || errors.As(err, &pending) {
			t.Fatalf("reset retry lost or downgraded the actual finalization failure: %v", err)
		}
	}
	if _, err := s.proveRunReset(runID); err != nil {
		t.Fatalf("reset retry did not restore the normal resource proof: %v", err)
	}
	if err := s.runWorkerAdmissionReady("next-run"); err != nil {
		t.Fatalf("confirmed reset still blocked the next Run: %v", err)
	}
}
