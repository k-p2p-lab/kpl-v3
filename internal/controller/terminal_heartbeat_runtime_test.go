package controller

import (
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestConfirmedFailedPeerCannotRegressDuringLateHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name, state, cleanup string
		stale                bool
	}{
		{"stale-stopped", model.NodeStopped, "true", true},
		{"fresh-stopping", model.NodeStopping, "false", false},
		{"fresh-stopped", model.NodeStopped, "true", false},
		{"fresh-failed-without-cause", model.NodeFailed, "true", false},
		{"delayed-cleanup-failure", model.NodeFailed, "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newState(t.TempDir())
			now := time.Now().UTC()
			agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: now.Add(-time.Hour), LastSeen: now}
			if _, err := s.registerAgent(agent); err != nil {
				t.Fatal(err)
			}
			failed := model.Node{ID: "peer", RunID: "run", State: model.NodeFailed, Error: "peer exited with status 1", LastSeen: now, Metadata: map[string]string{"cleanupComplete": "true", "exitEvidence": "confirmed"}}
			if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{failed}, Partial: true}); err != nil {
				t.Fatal(err)
			}
			confirmed := s.nodes[failed.ID]
			agent.LastSeen = now.Add(time.Second)
			if tc.stale {
				agent.LastSeen = now.Add(-time.Second)
			}
			late := model.Node{ID: failed.ID, RunID: failed.RunID, State: tc.state, LastSeen: agent.LastSeen, Metadata: map[string]string{"cleanupComplete": tc.cleanup}}
			if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{late}, Partial: true}); err != nil {
				t.Fatal(err)
			}
			if got := s.nodes[failed.ID]; !reflect.DeepEqual(got, confirmed) {
				t.Fatalf("confirmed failure was rewritten by a late report: got=%+v want=%+v", got, confirmed)
			}
		})
	}
}

func TestPendingCleanupFailureCanStillRecoverNormally(t *testing.T) {
	s := newState(t.TempDir())
	now := time.Now().UTC()
	agent := model.Agent{ID: "agent", URL: "http://agent", StartedAt: now.Add(-time.Hour), LastSeen: now}
	if _, err := s.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	failed := model.Node{ID: "peer", RunID: "run", State: model.NodeFailed, Error: "Docker remove timeout", LastSeen: now, Metadata: map[string]string{"cleanupComplete": "false"}}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{failed}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	agent.LastSeen = now.Add(time.Second)
	stopped := model.Node{ID: failed.ID, RunID: failed.RunID, State: model.NodeStopped, LastSeen: agent.LastSeen, Metadata: map[string]string{"cleanupComplete": "true"}}
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{stopped}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if got := s.nodes[failed.ID]; got.State != model.NodeStopped || got.Error != "" || got.Metadata["cleanupComplete"] != "true" {
		t.Fatalf("temporary cleanup failure became permanent: %+v", got)
	}
}
