package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAgentRunReportsShutdownCleanupFailure(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "clean-exit", true: "cleanup-failure"}[failCleanup], func(t *testing.T) {
			settings := map[string]string{"HANG": "wait"}
			if failCleanup {
				settings["FAIL"] = "rm"
			}
			s, path := newContainerTestServer(t, settings)
			s.config.Listen = "127.0.0.1:0"
			registered := make(chan struct{}, 1)
			s.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if strings.HasSuffix(request.URL.Path, "/register") {
					select {
					case registered <- struct{}{}:
					default:
					}
				}
				return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			select {
			case <-registered:
			case err := <-done:
				t.Fatalf("Agent failed before serving: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("Agent did not start")
			}
			if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"}); err != nil {
				t.Fatal(err)
			}
			waitDockerCall(t, path, "wait")
			cancel()
			select {
			case err := <-done:
				if failCleanup {
					if err == nil || !strings.Contains(err.Error(), "Agent shutdown incomplete") || !strings.Contains(err.Error(), "daemon unavailable during rm") {
						t.Fatalf("shutdown returned success or lost the cleanup failure: %v", err)
					}
				} else if err != nil {
					t.Fatalf("clean shutdown returned an error: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Agent did not finish shutdown")
			}
			waitDockerCall(t, path, "rm")
		})
	}
}

func TestWaitStoppedReportsTimeoutAndRetainsPendingCleanup(t *testing.T) {
	proc := &process{node: model.Node{ID: "pending", State: model.NodeStopping}, done: make(chan struct{})}
	s := &Server{processes: map[string]*process{"pending": proc}}
	if err := s.waitStopped(20 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "pending") {
		t.Fatalf("missing shutdown timeout and peer identity: %v", err)
	}
	// A timeout must not mark the peer removed or discard its eventual result.
	if proc.exited {
		t.Fatal("timed-out cleanup was marked complete")
	}
	s.finishProcess("pending", proc, nil, nil)
	if err := s.waitStopped(time.Second); err != nil {
		t.Fatalf("eventual cleanup did not recover: %v", err)
	}
}

func TestWaitStoppedReportsEveryCompletedCleanupFailure(t *testing.T) {
	first, second := errors.New("first cleanup"), errors.New("second cleanup")
	s := &Server{processes: map[string]*process{
		"first":  {node: model.Node{ID: "first"}, exited: true, cleanupErr: first},
		"second": {node: model.Node{ID: "second"}, exited: true, cleanupErr: second},
	}}
	err := s.waitStopped(time.Second)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("shutdown lost a cleanup failure: %v", err)
	}
}

func TestWaitStoppedAllowsCleanupRetryToRecover(t *testing.T) {
	proc := &process{node: model.Node{ID: "retry", State: model.NodeStopping}, done: make(chan struct{}), cleanupErr: errors.New("previous attempt failed")}
	s := &Server{processes: map[string]*process{"retry": proc}}
	done := make(chan error, 1)
	go func() { done <- s.waitStopped(time.Second) }()
	select {
	case err := <-done:
		t.Fatalf("returned before the active cleanup retry completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	s.finishProcess("retry", proc, nil, nil)
	if err := <-done; err != nil {
		t.Fatalf("retained a stale failure after cleanup recovered: %v", err)
	}
}
