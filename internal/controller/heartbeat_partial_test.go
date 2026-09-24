package controller

import (
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPartialHeartbeatChunksPreserveOmittedNodesAndReservations(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: started, Capacity: 10}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	s.nodes["existing"] = model.Node{ID: "existing", AgentID: agent.ID, State: model.NodeReady}
	s.reservations["short-lived"] = agent.ID
	agent.LastSeen, agent.ActiveNodes = started.Add(time.Minute), 3
	for _, node := range []model.Node{
		{ID: "first", State: model.NodeReady},
		{ID: "short-lived", State: model.NodeStopped, Metadata: map[string]string{"stoppedAt": "confirmed"}},
		{ID: "second", State: model.NodeReady},
	} {
		if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
			t.Fatal(err)
		}
		if s.nodes["existing"].State != model.NodeReady || s.nodes["first"].State != model.NodeReady {
			t.Fatal("omission from a partial chunk stopped a live peer")
		}
		if node.ID == "first" && s.reservations["short-lived"] != agent.ID {
			t.Fatal("omission from a partial chunk released an unconfirmed reservation")
		}
	}
	if s.nodes["second"].State != model.NodeReady || s.nodes["short-lived"].Metadata["stoppedAt"] != "confirmed" {
		t.Fatal("same-timestamp later chunks were ignored")
	}
	if len(s.reservations) != 0 || s.agents[agent.ID].ActiveNodes != 3 {
		t.Fatalf("partial occupancy is wrong: reservations=%v agent=%+v", s.reservations, s.agents[agent.ID])
	}
	// Explicit full reports keep the existing reconciliation behavior.
	agent.LastSeen = agent.LastSeen.Add(time.Second)
	agent.ActiveNodes = 1
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{{ID: "first", State: model.NodeReady}}}); err != nil {
		t.Fatal(err)
	}
	if s.nodes["existing"].State != model.NodeStopped || s.nodes["second"].State != model.NodeStopped {
		t.Fatal("full inventory no longer reconciles missing peers")
	}
}

func TestStalePartialHeartbeatAcknowledgesOnlyImmutableTerminalNodes(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: started, Capacity: 10}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	s.reservations["short-lived"] = agent.ID
	agent.LastSeen, agent.ActiveNodes = started.Add(4*time.Minute), 1
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{{ID: "live", State: model.NodeReady}}}); err != nil {
		t.Fatal(err)
	}
	previous := s.agents[agent.ID]
	older := agent
	older.LastSeen, older.ActiveNodes, older.Capacity = started.Add(3*time.Minute), 20, 99
	if err := s.heartbeat(model.AgentHeartbeat{Agent: older, Partial: true, Nodes: []model.Node{
		{ID: "live", State: model.NodeFailed, Error: "old failure"},
		{ID: "short-lived", State: model.NodeStopped, Metadata: map[string]string{"stoppedAt": "confirmed"}},
		{ID: "old-live", State: model.NodeReady},
	}}); err != nil {
		t.Fatal(err)
	}
	if s.nodes["live"].State != model.NodeReady || s.nodes["old-live"].ID != "" {
		t.Fatal("an older partial chunk replaced newer active inventory")
	}
	if s.nodes["short-lived"].Metadata["stoppedAt"] != "confirmed" || len(s.reservations) != 0 {
		t.Fatal("crossed status lost an unseen terminal peer or leaked its reservation")
	}
	got := s.agents[agent.ID]
	if got.ActiveNodes != 1 || got.Capacity != previous.Capacity || !got.LastSeen.Equal(previous.LastSeen) || !s.agentSnapshots[agent.ID].Equal(agent.LastSeen) {
		t.Fatalf("stale partial replaced current occupancy, metadata or lease: %+v", got)
	}
	// A delayed earlier full inventory also cannot revive the successful exit.
	older.LastSeen = started.Add(2 * time.Minute)
	if err := s.heartbeat(model.AgentHeartbeat{Agent: older, Nodes: []model.Node{{ID: "short-lived", State: model.NodeReady}}}); err != nil {
		t.Fatal(err)
	}
	if s.nodes["short-lived"].State != model.NodeStopped || s.nodes["live"].State != model.NodeReady {
		t.Fatal("old full status undid an acknowledged terminal report")
	}
}

func TestPartialHeartbeatDoesNotCountReservedInventoryTwice(t *testing.T) {
	s := newState(t.TempDir())
	agent := model.Agent{ID: "agent", URL: "http://agent", Capacity: 2}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	s.nodes["creating"] = model.Node{ID: "creating", AgentID: agent.ID, State: model.NodeStarting}
	s.reservations["creating"] = agent.ID
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if s.agents[agent.ID].ActiveNodes != 1 || len(s.reservations) != 1 {
		t.Fatalf("unconfirmed creation was counted twice or released: %+v", s.agents[agent.ID])
	}
}

func TestPartialHeartbeatCannotCrossAgentInstanceFence(t *testing.T) {
	s := newState(t.TempDir())
	started := time.Now().Add(-time.Hour)
	agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: started}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	agent.StartedAt = started.Add(-time.Hour)
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Partial: true, Nodes: []model.Node{{ID: "old", State: model.NodeStopped}}}); err == nil {
		t.Fatal("old Agent instance was allowed to acknowledge terminal history")
	}
	if len(s.nodes) != 0 {
		t.Fatal("rejected old instance mutated inventory")
	}
}
