package controller

import (
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"testing"
	"time"
)

func TestAgentRetiredHistoryPreservesAcknowledgedFailuresAndLateEvidence(t *testing.T) {
	s := newState(t.TempDir())
	now := time.Now().UTC()
	agent := model.Agent{ID: "agent", URL: "http://agent:8090", StartedAt: now.Add(-time.Hour), LastSeen: now}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	failed := model.Node{ID: "failed", RunID: "run", State: model.NodeFailed, Error: "peer exited 1", LastSeen: now, Metadata: map[string]string{"cleanupComplete": "true"}}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{failed}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	late := model.Node{ID: "late-failed", RunID: "run", State: model.NodeReady, LastSeen: now}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{late}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	agent.LastSeen = now.Add(time.Second)
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent}); err != nil {
		t.Fatal(err)
	}
	if got := s.nodes[failed.ID]; got.State != model.NodeFailed || got.Error != "peer exited 1" {
		t.Fatalf("retirement rewrote failed result: %+v", got)
	}
	// A full status crossed an older chunk; immutable cleaned failure still counts.
	failed.ID = "late-failed"
	agent.LastSeen = now
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{failed}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if s.nodes[failed.ID].State != model.NodeFailed {
		t.Fatal("late terminal evidence lost")
	}
}
