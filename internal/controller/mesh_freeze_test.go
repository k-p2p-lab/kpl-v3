package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func meshFreezeNode(id string) model.Node {
	return model.Node{ID: id, PeerID: "peer-" + id, RunID: "run", Generation: 2, AgentID: "agent", Group: "workers", Role: "worker", Type: "full", State: model.NodeReady,
		Metadata: map[string]string{"pubsubRouter": "gossipsub", "pubsubEnabled": "true", "meshFreezeEnabled": "true"}}
}

func TestMeshFreezeDispatchUsesRunGenerationAndSelectorIntersection(t *testing.T) {
	var mu sync.Mutex
	var called []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.MeshFreezeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if r.Method != http.MethodPost || request.RunID != "run" || request.Generation != 2 || r.Header.Get("Authorization") != "Bearer test" {
			t.Errorf("unexpected method/scope/auth: %s %+v", r.Method, request)
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"), "/mesh-freeze")
		mu.Lock()
		called = append(called, id)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: id, PeerID: "peer-" + id, Frozen: true})
	}))
	defer api.Close()
	server := New(ServerConfig{DataDir: t.TempDir(), Token: "test"}, nil)
	server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
	for _, id := range []string{"one", "two", "other-group", "other-run", "old-generation", "stopped"} {
		node := meshFreezeNode(id)
		switch id {
		case "other-group":
			node.Group = "other"
		case "other-run":
			node.RunID = "other"
		case "old-generation":
			node.Generation = 1
		case "stopped":
			node.State = model.NodeStopped
		}
		server.state.nodes[id] = node
	}
	for _, phase := range []scenario.Phase{
		{Action: "mesh-freeze", Group: "workers"},
		{Action: "mesh-freeze", NodeIDs: []string{"one"}},
		{Action: "mesh-freeze", PeerIDs: []string{"peer-one"}},
		{Action: "mesh-freeze", Group: "workers", NodeIDs: []string{"one"}, PeerIDs: []string{"peer-one"}, Role: "worker", NodeType: "full"},
	} {
		called = nil
		if err := server.runPhase(context.Background(), "run", 2, phase, nil, nil, time.Second); err != nil {
			t.Fatal(err)
		}
		slices.Sort(called)
		want := []string{"one"}
		if len(phase.NodeIDs) == 0 && len(phase.PeerIDs) == 0 {
			want = []string{"one", "two"}
		}
		if !slices.Equal(called, want) {
			t.Fatalf("phase %+v called %v, want %v", phase, called, want)
		}
	}
}

func TestMeshFreezePreflightRejectsAllBeforeDispatch(t *testing.T) {
	cases := []struct {
		name   string
		phase  scenario.Phase
		modify func(*model.Node, *model.Agent)
	}{
		{name: "unknown", phase: scenario.Phase{NodeIDs: []string{"one", "missing"}}},
		{name: "unknown peer", phase: scenario.Phase{PeerIDs: []string{"peer-one", "missing"}}},
		{name: "empty selector", phase: scenario.Phase{}},
		{name: "empty group", phase: scenario.Phase{Group: "missing"}},
		{name: "intersection mismatch", phase: scenario.Phase{Group: "workers", NodeIDs: []string{"one", "two"}, PeerIDs: []string{"peer-one"}}},
		{name: "wrong run", phase: scenario.Phase{NodeIDs: []string{"one", "two"}}, modify: func(n *model.Node, _ *model.Agent) { n.RunID = "other" }},
		{name: "wrong generation", phase: scenario.Phase{NodeIDs: []string{"one", "two"}}, modify: func(n *model.Node, _ *model.Agent) { n.Generation = 1 }},
		{name: "not ready", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { n.State = model.NodeStarting }},
		{name: "stopped explicit", phase: scenario.Phase{NodeIDs: []string{"one", "two"}}, modify: func(n *model.Node, _ *model.Agent) { n.State = model.NodeStopped }},
		{name: "no peer ID", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { n.PeerID = "" }},
		{name: "wrong router", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { n.Metadata["pubsubRouter"] = "floodsub" }},
		{name: "pubsub disabled", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { n.Metadata["pubsubEnabled"] = "false" }},
		{name: "capability disabled", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { n.Metadata["meshFreezeEnabled"] = "false" }},
		{name: "capability unknown", phase: scenario.Phase{Group: "workers"}, modify: func(n *model.Node, _ *model.Agent) { delete(n.Metadata, "meshFreezeEnabled") }},
		{name: "offline", phase: scenario.Phase{Group: "workers"}, modify: func(_ *model.Node, a *model.Agent) { a.State = model.AgentOffline }},
		{name: "stale", phase: scenario.Phase{Group: "workers"}, modify: func(_ *model.Node, a *model.Agent) { a.LastSeen = time.Now().Add(-time.Hour) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer api.Close()
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agent := model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
			node := meshFreezeNode("two")
			if tc.modify != nil {
				tc.modify(&node, &agent)
			}
			server.state.agents["agent"] = agent
			server.state.nodes["one"] = meshFreezeNode("one")
			server.state.nodes["two"] = node
			if err := server.runMeshFreeze(context.Background(), "run", 2, tc.phase); err == nil {
				t.Fatal("invalid target was accepted")
			}
			if calls.Load() != 0 {
				t.Fatalf("preflight dispatched %d requests", calls.Load())
			}
		})
	}
}

func TestMeshFreezeRejectsFalseAcknowledgementAndReportsFailure(t *testing.T) {
	for _, response := range []model.MeshFreezeResponse{
		{NodeID: "one", PeerID: "peer-one", Frozen: false},
		{NodeID: "other", PeerID: "peer-one", Frozen: true},
		{NodeID: "one", PeerID: "other", Frozen: true},
	} {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(response) }))
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
		server.state.nodes["one"] = meshFreezeNode("one")
		err := server.runMeshFreeze(context.Background(), "run", 2, scenario.Phase{Group: "workers"})
		api.Close()
		if err == nil || !strings.Contains(err.Error(), "0/1") || !strings.Contains(err.Error(), "invalid mesh-freeze acknowledgement") {
			t.Fatalf("response %+v error = %v", response, err)
		}
	}
}

func TestMeshFreezeCancellationAndTimeout(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "timeout"}[deadline], func(t *testing.T) {
			started := make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request model.MeshFreezeRequest
				_ = json.NewDecoder(r.Body).Decode(&request)
				close(started)
				<-r.Context().Done()
			}))
			defer api.Close()
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
			server.state.nodes["one"] = meshFreezeNode("one")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			phase := scenario.Phase{Group: "workers", Timeout: "5s"}
			want := context.Canceled
			if deadline {
				phase.Timeout = "100ms"
				want = context.DeadlineExceeded
			}
			done := make(chan error, 1)
			go func() { done <- server.runMeshFreeze(ctx, "run", 2, phase) }()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("freeze request did not start")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("error = %v, want %v", err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("freeze ignored cancellation")
			}
		})
	}
}

func TestMeshFreezeTimingAllowance(t *testing.T) {
	spec, err := scenario.Parse([]byte("version: 3\nname: freeze\nphases: [{action: mesh-freeze, group: workers, repeat: 2}]"))
	if err != nil {
		t.Fatal(err)
	}
	if got := newTimingPlan(spec).duration; got != 60 {
		t.Fatalf("freeze duration = %v, want 60", got)
	}
}

func TestMeshFreezeUsesPhaseDeadlineInsteadOfOrdinaryClientTimeout(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: "one", PeerID: "peer-one", Frozen: true})
	}))
	defer api.Close()
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.client.Timeout = time.Millisecond
	server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
	server.state.nodes["one"] = meshFreezeNode("one")
	if err := server.runMeshFreeze(context.Background(), "run", 2, scenario.Phase{Group: "workers", Timeout: "2s"}); err != nil {
		t.Fatal(err)
	}
	if server.client.Timeout != time.Millisecond {
		t.Fatal("freeze changed the shared HTTP client's timeout")
	}
}
