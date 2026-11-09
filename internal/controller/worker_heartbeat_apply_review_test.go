package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestHeartbeatRetirementViewCanExceedPositiveCacheCapacity(t *testing.T) {
	s, _ := historicalHeartbeatFixture(t, t.TempDir())
	const count = closedRunCacheEntries + 44
	nodes := make([]model.Node, count)
	for i := range nodes {
		nodes[i] = model.Node{ID: fmt.Sprintf("old-peer-%d", i), RunID: fmt.Sprintf("old-run-%d", i)}
	}
	lookups := 0
	s.state.retiredLookup = func(string) (bool, error) {
		lookups++
		if lookups > count*2 {
			return false, fmt.Errorf("positive cache eviction repeatedly invalidated the retirement view")
		}
		return true, nil
	}
	retired, revision, err := s.state.heartbeatRetirements(nodes)
	if err != nil || lookups != count || len(retired) != count || revision != s.state.retirementRevision || len(s.state.closedRuns) > closedRunCacheEntries {
		t.Fatalf("bounded positive cache prevented a finite complete retirement view: nodes=%d lookups=%d cached=%d revision=%d err=%v", len(retired), lookups, len(s.state.closedRuns), revision, err)
	}
	for _, node := range nodes {
		if !retired[node.RunID] {
			t.Fatalf("cache eviction lost retirement evidence for %s", node.RunID)
		}
	}
}

func TestPreparedColdHeartbeatRevalidatesStateBeforeRemovingHotEvidence(t *testing.T) {
	for _, change := range []string{"agent-instance", "peer-owner", "peer-run", "reservation-owner"} {
		t.Run(change, func(t *testing.T) {
			s, agent := historicalHeartbeatFixture(t, t.TempDir())
			node := historicalFixtureNode("prepared-cold-owner")
			if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
				t.Fatal(err)
			}
			late := node
			late.State, late.Metadata = model.NodeReady, nil
			h := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{late}, Partial: true}
			retired, revision, err := s.state.heartbeatRetirements(h.Nodes)
			if err != nil {
				t.Fatal(err)
			}
			historical, err := s.state.prepareHistoricalHeartbeat(h, retired)
			if err != nil || historical[node.ID].ID != node.ID {
				t.Fatalf("fixture omitted immutable cold evidence: %v %v", historical, err)
			}
			s.state.mu.Lock()
			switch change {
			case "agent-instance":
				agent.StartedAt = agent.StartedAt.Add(time.Hour)
				s.state.agents[agent.ID] = agent
			case "peer-owner":
				node.AgentID = "different-agent"
				s.state.setNodeLocked(node)
			case "peer-run":
				node.RunID = "different-run"
				s.state.setNodeLocked(node)
			case "reservation-owner":
				s.state.reservations[node.ID] = "different-agent"
			}
			before, existed := s.state.nodes[node.ID]
			beforeAgent := s.state.agents[agent.ID]
			beforeReservation := s.state.reservations[node.ID]
			beforeRevision := s.state.nodeRevision
			s.state.mu.Unlock()
			if err := s.state.applyPreparedHeartbeat(h, historical, retired, revision); err == nil {
				t.Fatal("prepared heartbeat bypassed the state change after cold I/O")
			}
			s.state.mu.RLock()
			after, exists := s.state.nodes[node.ID]
			unchanged := exists == existed && reflect.DeepEqual(before, after) && reflect.DeepEqual(beforeAgent, s.state.agents[agent.ID]) && beforeReservation == s.state.reservations[node.ID] && beforeRevision == s.state.nodeRevision
			s.state.mu.RUnlock()
			if !unchanged {
				t.Fatal("rejected prepared heartbeat changed hot ownership, admission, or Agent state")
			}
		})
	}
}
