package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestDeferredAgentAdmissionRetriesPinnedRequest(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "rolling-upgrade"}[legacy], func(t *testing.T) {
			var attempts atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req model.CreateNodeRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				if req.ID != "node" || req.Generation != 3 || req.Seed != 42 {
					t.Errorf("retry changed request: %+v", req)
				}
				if attempts.Add(1) == 1 {
					w.Header().Set("Retry-After", "0")
					if !legacy {
						w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
					}
					writeError(w, 503, "Agent telemetry backlog is full; retry Peer admission after delivery recovers")
					return
				}
				writeJSON(w, 201, model.Node{ID: req.ID, State: model.NodeStarting})
			}))
			defer endpoint.Close()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if _, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 2}); err != nil {
				t.Fatal(err)
			}
			agent, ok := s.tryReserveAgent("node")
			if !ok {
				t.Fatal("reserve")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			start := time.Now()
			err := s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run", Generation: 3, Seed: 42}, agent, "a", nil)
			if err != nil || attempts.Load() != 2 || time.Since(start) < 250*time.Millisecond {
				t.Fatalf("retry failed or spun: attempts=%d err=%v", attempts.Load(), err)
			}
			if s.state.agents["a"].ActiveNodes != 1 || s.state.nodes["node"].AgentID != "a" {
				t.Fatal("retry leaked reservation or changed pinned placement")
			}
		})
	}
}

func TestDeferredAgentAdmissionReroutesWithoutWaitingOnBusyAgent(t *testing.T) {
	var busyCalls, availableCalls atomic.Int32
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		busyCalls.Add(1)
		w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
		w.Header().Set("Retry-After", "10")
		writeError(w, 503, "backlog full")
	}))
	defer busy.Close()
	available := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		availableCalls.Add(1)
		writeJSON(w, 201, model.Node{ID: "node", State: model.NodeStarting})
	}))
	defer available.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for id, url := range map[string]string{"a": busy.URL, "b": available.URL} {
		if _, err := s.state.registerAgent(model.Agent{ID: id, URL: url, Capacity: 2}); err != nil {
			t.Fatal(err)
		}
	}
	agent, ok := s.tryReserveAgent("node")
	if !ok || agent.ID != "a" {
		t.Fatal("fixture reservation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run"}, agent, "", nil); err != nil {
		t.Fatal(err)
	}
	if busyCalls.Load() != 1 || availableCalls.Load() != 1 || s.state.nodes["node"].AgentID != "b" || s.state.agents["a"].ActiveNodes != 0 {
		t.Fatal("busy Agent was retried or its reservation leaked")
	}
	if agent, ok := s.tryReserveAgent("another"); !ok || agent.ID != "b" {
		t.Fatal("fleet placement ignored Agent cooldown")
	}
}

func TestDeferredAgentAdmissionWaitIsCanceledAndReleasesCapacity(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
		w.Header().Set("Retry-After", "30")
		writeError(w, 503, "busy")
	}))
	defer endpoint.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 2}); err != nil {
		t.Fatal(err)
	}
	agent, _ := s.tryReserveAgent("node")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run"}, agent, "a", nil)
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 || len(s.state.reservations) != 0 || s.state.agents["a"].ActiveNodes != 0 {
		t.Fatalf("cancellation failed/leaked capacity: %v calls=%d", err, calls.Load())
	}
}

func TestAmbiguousAgent503DoesNotRepeatPeerCreation(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		writeError(w, 503, "upstream unavailable after dispatch")
	}))
	defer endpoint.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 2}); err != nil {
		t.Fatal(err)
	}
	agent, _ := s.tryReserveAgent("node")
	err := s.createReservedNode(context.Background(), model.CreateNodeRequest{ID: "node", RunID: "run"}, agent, "a", nil)
	if err == nil || !strings.Contains(err.Error(), "503") || calls.Load() != 1 || len(s.state.reservations) != 0 {
		t.Fatalf("ambiguous create retried: %v calls=%d", err, calls.Load())
	}
}

func TestAdmissionRetryDelayHasSafeBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for value, want := range map[string]time.Duration{"": time.Second, "bad": time.Second, "-1": time.Second, "0": 250 * time.Millisecond, "3": 3 * time.Second, "99999": 30 * time.Second, now.Add(5 * time.Second).Format(http.TimeFormat): 5 * time.Second, now.Add(-time.Minute).Format(http.TimeFormat): 250 * time.Millisecond} {
		if got := admissionRetryDelay(value, now); got != want {
			t.Errorf("%q=%v want %v", value, got, want)
		}
	}
}
