package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// A FIFO stands in for an already-started local filesystem read which the
// kernel has not completed. The first writer owns history.mu while actually
// blocked in os.Open; a separate canceled cleanup waiter must not wait for
// that unrelated operation. No running syscall is assumed cancelable here.
func TestHistoryLockDoesNotRetainCanceledCleanupWaiter(t *testing.T) {
	for _, boundary := range []string{"finish", "stop-all-http"} {
		t.Run(boundary, func(t *testing.T) {
			s, _ := newContainerTestServer(t, nil)
			workerControllerFixture(t, s)
			release := blockHistoryFenceReadForTest(t, s, "blocked-filesystem-run")
			defer release()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			var endpoint *httptest.Server
			if boundary == "finish" {
				go func() { finished <- s.finishRunWorker(ctx, "cancelable-finish-run") }()
				// The unrelated OS read is still held; allow the cleanup goroutine
				// to reach that lock before delivering its own cancellation.
				time.Sleep(20 * time.Millisecond)
			} else {
				serverDone := make(chan struct{})
				endpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(serverDone); s.handleNodes(w, r) }))
				t.Cleanup(endpoint.Close)
				request, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				go func() {
					response, err := endpoint.Client().Do(request)
					if response != nil {
						_ = response.Body.Close()
					}
					finished <- err
				}()
				deadline := time.Now().Add(time.Second)
				for {
					s.mu.RLock()
					fenced := s.fencingAll
					s.mu.RUnlock()
					if fenced {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("HTTP cleanup did not reach its state fence")
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
				if err := <-finished; !errors.Is(err, context.Canceled) {
					t.Fatalf("HTTP client did not cancel: %v", err)
				}
				finished = make(chan error, 1)
				go func() { <-serverDone; finished <- nil }()
			}
			cancel()
			observed := false
			select {
			case err := <-finished:
				observed = true
				if boundary == "finish" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cleanup lost cancellation: %v", err)
				}
				if boundary == "finish" {
					var pending *runWorkerPendingError
					if !errors.As(err, &pending) {
						t.Fatalf("canceled finalization was not retained as pending: %v", err)
					}
				}
			case <-time.After(200 * time.Millisecond):
				t.Errorf("canceled %s cleanup still waits behind an unrelated filesystem read holding history.mu", boundary)
			}
			release()
			if !observed {
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Fatal("cleanup did not leave after the actual filesystem operation was released")
				}
			}
			if endpoint != nil {
				endpoint.Close()
			}
			if boundary == "finish" {
				if err := s.finishRunWorker(t.Context(), "cancelable-finish-run"); err != nil {
					t.Fatalf("later finalization did not recover after history writer completed: %v", err)
				}
				if proof, err := s.proveRunReset("cancelable-finish-run"); err != nil || !proof.WorkerReaped {
					t.Fatalf("later finalization did not establish reset proof: %+v %v", proof, err)
				}
			}
		})
	}
}

func TestRunDrainHistoryLockHonorsHTTPDeadlineAndCancellation(t *testing.T) {
	for _, boundary := range []string{"caller-cancel", "drain-deadline"} {
		t.Run(boundary, func(t *testing.T) {
			s, _ := newContainerTestServer(t, nil)
			s.config.RunWorkerFactory = agentRunProcessFactory()
			workerControllerFixture(t, s)
			release := blockHistoryFenceReadForTest(t, s, "run-drain-blocked-writer")
			defer release()
			serverDone := make(chan struct{})
			var serverDoneOnce sync.Once
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer serverDoneOnce.Do(func() { close(serverDone) })
				s.handleRunAction(w, r)
			}))
			t.Cleanup(endpoint.Close)
			const run = "http-cancelable-drain-run"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			timeout := "3s"
			if boundary == "drain-deadline" {
				timeout = "20ms"
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL+"/api/v1/runs/"+run+"/drain?timeout="+timeout, nil)
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				status int
				err    error
				body   model.RunDrainError
			}
			clientDone := make(chan result, 1)
			go func() {
				response, err := endpoint.Client().Do(request)
				got := result{err: err}
				if response != nil {
					got.status = response.StatusCode
					_ = json.NewDecoder(response.Body).Decode(&got.body)
					_ = response.Body.Close()
				}
				clientDone <- got
			}()
			deadline := time.Now().Add(time.Second)
			for {
				s.mu.RLock()
				fence := s.runFences[run]
				s.mu.RUnlock()
				if fence == ^uint64(0) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("HTTP drain did not establish its generation fence")
				}
				time.Sleep(time.Millisecond)
			}
			observed := false
			if boundary == "caller-cancel" {
				cancel()
				if got := <-clientDone; !errors.Is(got.err, context.Canceled) {
					t.Fatalf("HTTP caller did not cancel: %+v", got)
				}
				select {
				case <-serverDone:
					observed = true
				case <-time.After(200 * time.Millisecond):
					t.Error("Run drain HTTP handler retained caller cancellation behind history writer")
				}
			} else {
				select {
				case got := <-clientDone:
					observed = true
					if got.err != nil || got.status != http.StatusServiceUnavailable || got.body.Code != model.RunWorkerPendingCode || !got.body.WorkerPending {
						t.Fatalf("expired Run drain did not preserve pending ownership: %+v", got)
					}
				case <-time.After(200 * time.Millisecond):
					t.Error("Run drain HTTP handler ignored its own deadline behind history writer")
				}
			}
			release()
			if !observed {
				select {
				case <-serverDone:
				case <-time.After(2 * time.Second):
					t.Fatal("Run drain HTTP handler did not leave after filesystem release")
				}
			}
			response, err := endpoint.Client().Post(endpoint.URL+"/api/v1/runs/"+run+"/drain?timeout=3s", "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				t.Fatalf("later HTTP drain did not recover: %s", response.Status)
			}
		})
	}
}

type historyResponseClose struct {
	io.ReadCloser
	done chan struct{}
}

func (r historyResponseClose) Close() error {
	err := r.ReadCloser.Close()
	close(r.done)
	return err
}

type historyResponseTransport struct {
	http.RoundTripper
	done chan struct{}
}

func (r historyResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := r.RoundTripper.RoundTrip(request)
	if err == nil {
		response.Body = historyResponseClose{ReadCloser: response.Body, done: r.done}
	}
	return response, err
}

func TestResetReclaimDoesNotRetainCanceledHistoryWaiter(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	release := blockHistoryFenceReadForTest(t, s, "blocked-reclaim-filesystem-run")
	defer release()
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(controller.Close)
	s.config.ControllerURL = controller.URL
	acknowledged := make(chan struct{})
	s.client = &http.Client{Transport: historyResponseTransport{RoundTripper: controller.Client().Transport, done: acknowledged}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- s.prepareRunReset(ctx, "canceled-reclaim-run") }()
	select {
	case <-acknowledged:
	case <-time.After(time.Second):
		t.Fatal("reset did not receive the actual heartbeat HTTP acknowledgement")
	}
	// The successful heartbeat has completed postData; the only remaining
	// reset preparation step is reclaimHistory's unrelated state writer.
	time.Sleep(20 * time.Millisecond)
	cancel()
	observed := false
	select {
	case err := <-finished:
		observed = true
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reset reclaim lost cancellation: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("canceled reset reclaim still waits behind an unrelated filesystem read holding history.mu")
	}
	release()
	if !observed {
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Fatal("reset did not finish after filesystem release")
		}
	}
}

func blockHistoryFenceReadForTest(t *testing.T, s *Server, runID string) func() {
	t.Helper()
	path := s.history.path("fences", runID)
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	held, heldDone := make(chan struct{}), make(chan error, 1)
	go func() {
		s.history.mu.Lock()
		defer s.history.mu.Unlock()
		close(held)
		heldDone <- s.history.saveFence(runID, ^uint64(0))
	}()
	<-held
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				t.Error(err)
				return
			}
			err = json.NewEncoder(file).Encode(uint64(0))
			_ = file.Close()
			if err != nil {
				t.Error(err)
			}
			if err := <-heldDone; err != nil {
				t.Error(err)
			}
		})
	}
	return release
}
