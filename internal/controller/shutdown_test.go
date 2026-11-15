package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestControllerShutdownWaitsForCleanupAndFinalState(t *testing.T) {
	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCleanup) }) }
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/") {
			close(cleanupEntered)
			<-releaseCleanup
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "agent"}})
	}))
	defer agent.Close()
	defer release()
	server := newLifecycleTestController(t)
	if _, err := server.state.registerAgent(model.Agent{ID: "agent", URL: agent.URL}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.serve(ctx, listener) }()
	raw := []byte("name: shutdown\njobShutdownTimeout: 2s\nphases:\n  - action: wait\n    duration: 1m\n")
	experiment, err := server.StartScenario(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	// Submission is asynchronous. This test exercises shutdown of an active
	// Run, after its clock and first phase have actually started.
	queueAwait(t, server, experiment.ID, func(run model.Experiment) bool {
		return run.State == "running" && run.Phase == 1 && !run.StartedAt.IsZero()
	})
	// Hold an SSE request open; BaseContext cancellation must drain it too.
	client := &http.Client{Timeout: 2 * time.Second}
	streamRequest, _ := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/api/v1/stream", nil)
	authenticateRequest(t, server, streamRequest)
	response, err := client.Do(streamRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	cancel()
	select {
	case <-cleanupEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel the running experiment")
	}
	select {
	case err := <-done:
		t.Fatalf("Controller returned before Agent cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := server.StartScenario(context.Background(), raw); err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("accepted a new run during shutdown: %v", err)
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Controller failed to finish after cleanup, including the open SSE request")
	}
	data, err := os.ReadFile(filepath.Join(server.config.DataDir, currentRunsDirectory, experiment.ID, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var final model.Experiment
	if err := json.Unmarshal(data, &final); err != nil || final.State != "canceled" || final.FinishedAt.IsZero() {
		t.Fatalf("final experiment = %+v, decode error = %v", final, err)
	}
}

func TestControllerShutdownCancelsQueuedExperimentBeforeItsClockStarts(t *testing.T) {
	server := newLifecycleTestController(t)
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "agent"}})
	}))
	defer agent.Close()
	if _, err := server.state.registerAgent(model.Agent{ID: "agent", URL: agent.URL}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done, finished := make(chan error, 1), make(chan struct{})
	go func() { defer close(finished); done <- server.serve(ctx, listener) }()
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("queued shutdown did not finish its actual work owners")
		}
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Controller did not finish startup: %d", response.StatusCode)
	}
	head := queueTestEnqueue(t, server, "analysis")
	queueTestAwait(t, head.entered, "previous work cleanup")
	defer head.once.Do(func() { close(head.release) })
	run, err := server.StartScenario(t.Context(), []byte("name: queued-shutdown\nphases:\n  - {action: wait, duration: 1m}\n"))
	if err != nil {
		t.Fatal(err)
	}
	queueAwait(t, server, run.ID, func(run model.Experiment) bool { return run.State == "queued" && run.StartedAt.IsZero() })
	cancel()
	head.once.Do(func() { close(head.release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not cancel the queued experiment")
	}
	data, err := os.ReadFile(filepath.Join(server.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
	if err != nil {
		t.Fatal(err)
	}
	var final model.Experiment
	if err := json.Unmarshal(data, &final); err != nil || final.State != "canceled" || !final.StartedAt.IsZero() || final.FinishedAt.IsZero() {
		t.Fatalf("queued experiment started or lost its cancellation: state=%s started=%v finished=%v error=%v", final.State, final.StartedAt, final.FinishedAt, err)
	}
}

func TestControllerShutdownCancelsIncompleteScenarioUpload(t *testing.T) {
	server := newLifecycleTestController(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.serve(ctx, listener) }()
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = fmt.Fprintf(connection, "POST /api/v1/experiments HTTP/1.1\r\nHost: localhost\r\nCookie: %s\r\nX-KPL-Request: dashboard\r\nContent-Length: 1000\r\nExpect: 100-continue\r\n\r\n", loginCookie(t, server).String())
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil || !strings.Contains(line, "100 Continue") {
		t.Fatalf("handler did not begin reading the incomplete upload: %q %v", line, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown waited for the body-read timeout instead of canceling the upload")
	}
	if len(server.state.experiments) != 0 {
		t.Fatal("incomplete upload started an experiment during shutdown")
	}
}

func TestControllerRejectsInvalidDataDirectoryBeforeListening(t *testing.T) {
	path := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(path, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	server := New(ServerConfig{Listen: "127.0.0.1:0", DataDir: path, User: "admin", Password: "secret"}, nil)
	if err := server.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "data directory") {
		t.Fatalf("Run accepted an unusable data directory: %v", err)
	}
}
