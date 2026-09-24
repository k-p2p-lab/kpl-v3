package controller

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestConcurrentReservationsShareAgentCapacity(t *testing.T) {
	server := newLifecycleTestController(t)
	for id, capacity := range map[string]int{"a": 7, "b": 11, "c": 5} {
		if _, err := server.state.registerAgent(model.Agent{ID: id, URL: "http://" + id, Capacity: capacity}); err != nil {
			t.Fatal(err)
		}
	}
	server.state.agents["offline"] = model.Agent{ID: "offline", Capacity: 100, State: model.AgentOffline}
	var workers sync.WaitGroup
	var accepted atomic.Int32
	for i := 0; i < 100; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, ok := server.tryReserveAgent(fmt.Sprintf("run-%d-node-%d", i%4, i)); ok {
				accepted.Add(1)
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 10; i++ {
			for _, id := range []string{"a", "b", "c"} {
				if err := server.state.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: id}}); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	workers.Wait()
	if accepted.Load() != 23 || len(server.state.reservations) != 23 {
		t.Fatalf("accepted=%d reservations=%d, want total capacity 23", accepted.Load(), len(server.state.reservations))
	}
	for _, id := range []string{"a", "b", "c"} {
		agent := server.state.agents[id]
		if agent.ActiveNodes != agent.Capacity {
			t.Fatalf("agent %s capacity was miscounted: %+v", id, agent)
		}
	}
}

func TestPlacementBalancesCapacityAndRejectsStaleLeases(t *testing.T) {
	server := newLifecycleTestController(t)
	server.state.agents["a"] = model.Agent{ID: "a", State: model.AgentOnline, Capacity: 10}
	server.state.agents["b"] = model.Agent{ID: "b", State: model.AgentOnline, Capacity: 20}
	server.state.agents["stale"] = model.Agent{ID: "stale", State: model.AgentOnline, Capacity: 1000, LastSeen: time.Now().Add(-2 * agentStaleAfter)}
	for i := 0; i < 9; i++ {
		if _, ok := server.tryReserveAgent(fmt.Sprintf("node-%d", i)); !ok {
			t.Fatal("reservation failed despite capacity")
		}
	}
	if server.state.agents["a"].ActiveNodes != 3 || server.state.agents["b"].ActiveNodes != 6 || server.state.agents["stale"].ActiveNodes != 0 {
		t.Fatalf("unexpected weighted distribution: %+v", server.state.agents)
	}
	if _, ok := server.tryReserveAgentWithPlacement("pinned", "stale", nil); ok {
		t.Fatal("explicit placement accepted a stale lease")
	}
	for i := 0; i < 10; i++ {
		agent, ok := server.tryReserveAgentWithPlacement(fmt.Sprintf("random-%d", i), "", rand.New(rand.NewSource(int64(i))))
		if !ok || agent.ID == "stale" {
			t.Fatalf("random placement selected stale Agent: %+v %t", agent, ok)
		}
		batch, err := server.selectBatchAgent(context.Background(), rand.New(rand.NewSource(int64(i))))
		if err != nil || batch == "stale" {
			t.Fatalf("single-agent placement selected stale Agent: %s %v", batch, err)
		}
	}
}

func TestHeartbeatPreservesReportedCleanupOccupancyAndPendingReservations(t *testing.T) {
	server := newLifecycleTestController(t)
	if _, err := server.state.registerAgent(model.Agent{ID: "a", URL: "http://a", Capacity: 4}); err != nil {
		t.Fatal(err)
	}
	server.tryReserveAgent("observed")
	server.tryReserveAgent("pending")
	if err := server.state.heartbeat(model.AgentHeartbeat{
		Agent: model.Agent{ID: "a", ActiveNodes: 3},
		Nodes: []model.Node{{ID: "observed", State: model.NodeStopped}},
	}); err != nil {
		t.Fatal(err)
	}
	if agent := server.state.agents["a"]; agent.ActiveNodes != 4 {
		t.Fatalf("occupied + pending capacity = %d, want 4", agent.ActiveNodes)
	}
	if _, ok := server.tryReserveAgent("extra"); ok {
		t.Fatal("admitted a peer over physical capacity")
	}
	if err := server.state.heartbeat(model.AgentHeartbeat{
		Agent: model.Agent{ID: "a"},
		Nodes: []model.Node{{ID: "observed", State: model.NodeStopped}, {ID: "pending", State: model.NodeStopped}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := server.tryReserveAgent("after-cleanup"); !ok {
		t.Fatal("Agent-confirmed free capacity remained unavailable")
	}
}

func TestOlderHeartbeatCannotEraseNewerSnapshot(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	if _, err := s.registerAgent(model.Agent{ID: "a", URL: "http://a", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	newer := model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: started, LastSeen: started.Add(3 * time.Second), ActiveNodes: 2}, Nodes: []model.Node{{ID: "one", State: model.NodeReady}, {ID: "two", State: model.NodeReady}}}
	if err := s.heartbeat(newer); err != nil {
		t.Fatal(err)
	}
	received := s.agents["a"].LastSeen
	older := model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: started, LastSeen: started.Add(2 * time.Second), ActiveNodes: 1}, Nodes: []model.Node{{ID: "one", State: model.NodeReady}}}
	if err := s.heartbeat(older); err != nil {
		t.Fatal(err)
	}
	if s.nodes["two"].State != model.NodeReady || s.agents["a"].ActiveNodes != 2 || !s.agents["a"].LastSeen.Equal(received) {
		t.Fatalf("old snapshot changed current state: agent=%+v nodes=%+v", s.agents["a"], s.nodes)
	}
}

func TestRegistrationSnapshotFencesQueuedOlderHeartbeat(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	registered := model.Agent{ID: "a", URL: "http://a", StartedAt: started, LastSeen: started.Add(3 * time.Second), ActiveNodes: 3}
	if _, err := s.registerAgent(registered); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: started, LastSeen: started.Add(time.Second), ActiveNodes: 1}}); err != nil {
		t.Fatal(err)
	}
	if s.agents["a"].ActiveNodes != 3 {
		t.Fatal("a queued heartbeat erased registration's newer physical occupancy")
	}
}

func TestAgentMetricsURLSurvivesSparseSameInstanceReports(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	metricsURL := "http://worker.example:9091/metrics"
	if _, err := s.registerAgent(model.Agent{ID: "a", URL: "http://a", MetricsURL: metricsURL, StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: started, LastSeen: started.Add(time.Second)}}); err != nil {
		t.Fatal(err)
	}
	if got := s.agents["a"].MetricsURL; got != metricsURL {
		t.Fatalf("heartbeat metrics URL=%q, want %q", got, metricsURL)
	}
	if _, err := s.registerAgent(model.Agent{ID: "a", URL: "http://a", StartedAt: started}); err != nil {
		t.Fatal(err)
	}
	if got := s.agents["a"].MetricsURL; got != metricsURL {
		t.Fatalf("same-instance registration metrics URL=%q, want %q", got, metricsURL)
	}
}

func TestAgentReRegistrationPreservesReservationsAndFencesOldInstance(t *testing.T) {
	server := newLifecycleTestController(t)
	s := server.state
	started := time.Now().Add(-time.Hour)
	agent := model.Agent{ID: "a", URL: "http://old-agent", Capacity: 2, StartedAt: started}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	server.tryReserveAgent("pending")
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	if s.agents["a"].ActiveNodes != 1 || s.reservations["pending"] != "a" {
		t.Fatal("same-instance re-registration lost pending capacity")
	}
	replacement := agent
	replacement.StartedAt = started.Add(time.Minute)
	replacement.URL = "http://new-agent"
	if _, err := s.registerAgent(replacement); err == nil {
		t.Fatal("duplicate live Agent ID was accepted")
	}
	s.mu.Lock()
	old := s.agents["a"]
	old.State = model.AgentOffline
	s.agents["a"] = old
	s.nodes["old-node"] = model.Node{ID: "old-node", AgentID: "a", State: model.NodeReady}
	s.mu.Unlock()
	if _, err := s.registerAgent(replacement); err != nil {
		t.Fatal(err)
	}
	if len(s.reservations) != 0 || s.nodes["old-node"].State != model.NodeFailed {
		t.Fatal("replacement retained old instance reservations/nodes")
	}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{{ID: "old-node", State: model.NodeReady}}}); err == nil {
		t.Fatal("old Agent heartbeat was accepted")
	}
	if _, err := s.registerAgent(agent); err == nil {
		t.Fatal("retired Agent registration replaced the new instance")
	}
	if server.recordCreatedNode(model.CreateNodeRequest{ID: "late-create"}, "a", model.Node{ID: "late-create", State: model.NodeReady}, started) {
		t.Fatal("old instance create response was accepted")
	}
	if _, exists := s.nodes["late-create"]; exists || s.agents["a"].URL != replacement.URL {
		t.Fatal("retired instance changed current registry")
	}
}

func TestHeartbeatRejectsDuplicateAndForeignNodesAtomically(t *testing.T) {
	s := newState(t.TempDir())
	for _, id := range []string{"a", "b"} {
		if _, err := s.registerAgent(model.Agent{ID: id, URL: "http://" + id}); err != nil {
			t.Fatal(err)
		}
	}
	s.nodes["owned"] = model.Node{ID: "owned", AgentID: "a", State: model.NodeReady}
	for _, nodes := range [][]model.Node{{{ID: "owned"}}, {{ID: "same"}, {ID: "same"}}} {
		if err := s.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "b", URL: "http://bad"}, Nodes: nodes}); err == nil {
			t.Fatal("accepted invalid heartbeat ownership")
		}
		if s.nodes["owned"].AgentID != "a" || s.agents["b"].URL != "http://b" {
			t.Fatal("rejected heartbeat partially changed state")
		}
	}
}
