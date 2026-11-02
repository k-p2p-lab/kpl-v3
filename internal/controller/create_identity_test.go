package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestLegacyCreateAcknowledgementRejectsConflictingIdentity(t *testing.T) {
	for _, field := range []string{"node", "run", "agent", "group", "generation"} {
		t.Run(field, func(t *testing.T) {
			s := newLifecycleTestController(t)
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var request model.CreateNodeRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				node := model.Node{ID: request.ID, RunID: request.RunID, AgentID: "a", Group: request.Group, Generation: request.Generation, State: model.NodeStarting}
				switch field {
				case "node":
					node.ID = "another-node"
				case "run":
					node.RunID = "another-run"
				case "agent":
					node.AgentID = "another-agent"
				case "group":
					node.Group = "another-group"
				case "generation":
					node.Generation++
				}
				writeJSON(w, http.StatusCreated, node)
			}))
			defer endpoint.Close()
			agent := reserveReplayAgent(t, s, endpoint.URL, false)
			request := model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2}
			err := s.createReservedNode(t.Context(), request, agent, "a", nil)
			var uncertain *uncertainNodeCreateError
			if !errors.As(err, &uncertain) {
				t.Errorf("conflicting legacy acknowledgement was not uncertain: %v", err)
			}
			s.state.mu.RLock()
			if len(s.state.nodes) != 0 || s.state.reservations["node"] != "a" || s.state.uncertainCreates["node"].runID != "run" {
				t.Errorf("conflicting acknowledgement changed identity or released its pending slot: nodes=%+v reservations=%+v uncertain=%+v", s.state.nodes, s.state.reservations, s.state.uncertainCreates)
			}
			s.state.mu.RUnlock()
			if calls.Load() != 1 {
				t.Errorf("conflicting acknowledgement retried: calls=%d", calls.Load())
			}
		})
	}
}

func TestLegacyCreateAcknowledgementPreservesMissingIdentityFallbacks(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "id-only", true: "empty-legacy-record"}[empty], func(t *testing.T) {
			s := newLifecycleTestController(t)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				node := model.Node{}
				if !empty {
					node.ID = "node"
				}
				writeJSON(w, http.StatusCreated, node)
			}))
			defer endpoint.Close()
			agent := reserveReplayAgent(t, s, endpoint.URL, false)
			request := model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2}
			if err := s.createReservedNode(t.Context(), request, agent, "a", nil); err != nil {
				t.Fatal(err)
			}
			s.state.mu.RLock()
			node := s.state.nodes["node"]
			s.state.mu.RUnlock()
			if node.ID != request.ID || node.RunID != request.RunID || node.Group != request.Group || node.Generation != request.Generation || node.AgentID != "a" {
				t.Fatalf("legacy fallback changed: %+v", node)
			}
		})
	}
}

func TestTimestampLessCreateAcknowledgementKeepsObservedHeartbeat(t *testing.T) {
	s := newLifecycleTestController(t)
	observed := make(chan model.Node, 1)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.CreateNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		s.state.mu.RLock()
		agent := s.state.agents["a"]
		s.state.mu.RUnlock()
		agent.LastSeen = time.Now().UTC()
		node := model.Node{ID: request.ID, RunID: request.RunID, AgentID: "a", Group: request.Group, Generation: request.Generation, State: model.NodeReady, LastSeen: agent.LastSeen, Metadata: map[string]string{"profileRevision": "4", "readyEvidence": "confirmed"}}
		if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}}); err != nil {
			t.Error(err)
			return
		}
		s.state.mu.RLock()
		observed <- s.state.nodes[request.ID]
		s.state.mu.RUnlock()
		writeJSON(w, http.StatusCreated, model.Node{ID: request.ID})
	}))
	defer endpoint.Close()
	agent := reserveReplayAgent(t, s, endpoint.URL, false)
	request := model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2}
	if err := s.createReservedNode(t.Context(), request, agent, "a", nil); err != nil {
		t.Fatal(err)
	}
	var want model.Node
	select {
	case want = <-observed:
	default:
		t.Fatal("heartbeat fixture did not publish its observation")
	}
	s.state.mu.RLock()
	got := s.state.nodes[request.ID]
	s.state.mu.RUnlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("timestamp-less creation ACK replaced a confirmed heartbeat: got=%+v want=%+v", got, want)
	}
}
