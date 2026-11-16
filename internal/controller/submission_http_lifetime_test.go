package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestLostSubmissionResponseKeepsQueuedOSRunAndClock(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f := newControllerWorkerFixture(t)
	s := f.server
	active, err := s.StartScenario(ctx, []byte(queueLongScenario))
	if err != nil {
		t.Fatal(err)
	}
	queueAwait(t, s, active.ID, func(run model.Experiment) bool { return run.State == "running" && !run.StartedAt.IsZero() })
	handler := s.apiTestHandler(ctx)
	accepted := make(chan model.Experiment, 1)
	var dropped atomic.Bool
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/experiments" && r.Header.Get("Idempotency-Key") == "response-loss" && !dropped.Swap(true) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			var run model.Experiment
			if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &run) != nil {
				t.Errorf("submission did not reach committed acceptance: %d", response.Code)
			}
			accepted <- run
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	payload, err := json.Marshal(map[string]any{"scenario": queueShortScenario, "repetitions": 1})
	if err != nil {
		t.Fatal(err)
	}
	post := func() (*http.Response, error) {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint.URL+"/api/v1/experiments", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "response-loss")
		return endpoint.Client().Do(request)
	}
	if response, err := post(); err == nil {
		_ = response.Body.Close()
		t.Fatal("fixture did not lose the accepted HTTP response")
	}
	queued := <-accepted
	queueAwait(t, s, queued.ID, func(run model.Experiment) bool { return run.State == "queued" && run.StartedAt.IsZero() })
	response, err := post()
	if err != nil {
		t.Fatal(err)
	}
	var replay model.Experiment
	err = json.NewDecoder(response.Body).Decode(&replay)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusAccepted || replay.ID != queued.ID || replay.ExecutionID != queued.ExecutionID || replay.Seed != queued.Seed || !replay.StartedAt.IsZero() {
		t.Fatalf("lost response replay changed its queued execution: original=%+v replay=%+v err=%v", queued, replay, err)
	}
	s.state.mu.RLock()
	count := len(s.state.experiments)
	s.state.mu.RUnlock()
	if count != 2 {
		t.Fatalf("replay admitted another experiment: %d", count)
	}
	stop, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint.URL+"/api/v1/experiments/"+active.ID+"/stop", nil)
	if err != nil {
		t.Fatal(err)
	}
	stop.Header.Set("X-KPL-Execution", active.ExecutionID)
	response, err = endpoint.Client().Do(stop)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("active predecessor stop was rejected: %d", response.StatusCode)
	}
	waitWorkerRuns(t, s)
	s.state.mu.RLock()
	previous, finished := s.state.experiments[active.ID], s.state.experiments[queued.ID]
	s.state.mu.RUnlock()
	if previous.State != "canceled" || finished.State != "completed" || finished.StartedAt.Before(previous.FinishedAt) || finished.StartedAt.IsZero() {
		t.Fatalf("queued execution did not retain FIFO isolation after client loss: previous=%+v next=%+v", previous, finished)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) != 2 {
		t.Fatalf("accepted replay changed worker count: %d", len(f.commands))
	}
	for _, command := range f.commands {
		if command.ProcessState == nil {
			t.Fatal("FIFO advanced before the actual OS child was reaped")
		}
	}
}
