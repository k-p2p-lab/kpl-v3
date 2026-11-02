package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCreateRechecksAgentAfterProfileAdmissionWait(t *testing.T) {
	for _, change := range []string{"disabled", "backpressure", "offline", "instance", "address"} {
		t.Run(change, func(t *testing.T) {
			var oldCalls, alternateCalls atomic.Int32
			oldEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				oldCalls.Add(1)
				writeJSON(w, http.StatusCreated, model.Node{ID: "node", State: model.NodeStarting})
			}))
			defer oldEndpoint.Close()
			alternateEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				alternateCalls.Add(1)
				writeJSON(w, http.StatusCreated, model.Node{ID: "node", State: model.NodeStarting})
			}))
			defer alternateEndpoint.Close()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			for id, endpoint := range map[string]string{"a": oldEndpoint.URL, "b": alternateEndpoint.URL} {
				if _, err := s.state.registerAgent(model.Agent{ID: id, URL: endpoint, Capacity: 2}); err != nil {
					t.Fatal(err)
				}
			}
			agent, ok := s.tryReserveAgentWithPlacement("node", "a", nil)
			if !ok {
				t.Fatal("initial reservation failed")
			}
			profiles := newRunProfileState()
			if err := profiles.gate.Acquire(t.Context(), profileWriteWeight); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			waiting := &phaseWaitObservationContext{Context: ctx, waiting: make(chan struct{})}
			ctx = context.WithValue(waiting, profileRuntimeKey{}, profiles)
			done := make(chan error, 1)
			go func() {
				done <- s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers"}, agent, "", nil)
			}()
			select {
			case <-waiting.waiting:
			case <-waiting.Context.Done():
				profiles.gate.Release(profileWriteWeight)
				t.Fatal("create did not reach the profile admission barrier")
			}
			// The initial availability check has passed, but no request has
			// been dispatched: a schedule still owns the admission barrier.
			wantAgent := "b"
			switch change {
			case "disabled":
				setEnabledTest(t, s.state, "a", false)
			case "backpressure":
				s.state.mu.Lock()
				s.state.agentAdmissionUntil["a"] = time.Now().Add(time.Hour)
				s.state.mu.Unlock()
			case "offline":
				s.state.mu.Lock()
				current := s.state.agents["a"]
				current.State = model.AgentOffline
				s.state.agents["a"] = current
				s.state.mu.Unlock()
			case "instance", "address":
				s.state.mu.Lock()
				current := s.state.agents["a"]
				current.URL = alternateEndpoint.URL
				if change == "instance" {
					current.StartedAt = current.StartedAt.Add(time.Second)
				}
				s.state.agents["a"] = current
				s.state.mu.Unlock()
				wantAgent = "a"
			}
			profiles.gate.Release(profileWriteWeight)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if oldCalls.Load() != 0 || alternateCalls.Load() != 1 {
				t.Fatalf("dispatched after %s during admission wait: old=%d alternate=%d", change, oldCalls.Load(), alternateCalls.Load())
			}
			if s.state.agents["a"].ActiveNodes+s.state.agents["b"].ActiveNodes != 1 || s.state.agents[wantAgent].ActiveNodes != 1 || s.state.reservations["node"] != wantAgent || s.state.nodes["node"].AgentID != wantAgent {
				t.Fatal("admission did not transfer its reservation and node to the available Agent")
			}
			if !profiles.gate.TryAcquire(profileWriteWeight) {
				t.Fatal("rerouting leaked the profile admission gate")
			}
			profiles.gate.Release(profileWriteWeight)
		})
	}
}
