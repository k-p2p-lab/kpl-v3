package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPreparedHeartbeatCannotRecreateRetiredRunAfterHistoryIO(t *testing.T) {
	for _, mode := range []struct {
		state string
		evict bool
	}{{model.NodeStopped, false}, {model.NodeStopped, true}, {model.NodeReady, false}, {model.NodeStarting, true}, {model.NodeStopping, false}, {model.NodeFailed, true}} {
		t.Run(fmt.Sprintf("%s/evicted=%t", mode.state, mode.evict), func(t *testing.T) {
			s, agent := historicalHeartbeatFixture(t, t.TempDir())
			const id = "run-heartbeat-retirement"
			run, _ := resultFixture(t, s, id, "failed", time.Now().UTC())
			run.CleanupState = "running"
			s.state.experiments[id] = run
			node := historicalFixtureNode(id + "-worker-1")
			node.RunID = id
			h := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}
			if err := s.state.heartbeat(h); err != nil {
				t.Fatal(err)
			}
			retired, revision, err := s.state.heartbeatRetirements(h.Nodes)
			if err != nil || retired[id] {
				t.Fatalf("fixture already retired: %v %v", retired, err)
			}
			historical, err := s.state.prepareHistoricalHeartbeat(h, retired)
			if err != nil {
				t.Fatal(err)
			}
			// Pause at the real heartbeat's disk-I/O boundary. The retirement
			// commits while this heartbeat still holds an earlier lookup result.
			if err := s.retireRunRuntime(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			if mode.evict {
				s.state.mu.Lock()
				for i := range closedRunCacheEntries + 1 {
					s.state.cacheRetiredRunLocked(fmt.Sprintf("other-retired-%d", i))
				}
				s.state.mu.Unlock()
			}
			h.Nodes[0].State = mode.state
			if mode.state != model.NodeStopped {
				h.Nodes[0].Metadata = nil
			}
			if err := s.state.applyPreparedHeartbeat(h, historical, retired, revision); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyRetiredRuntime(id); err != nil {
				t.Fatal(err)
			}
			s.state.mu.RLock()
			nodes, reports, versions := len(s.state.nodes), len(s.state.nodeReportTimes), len(s.state.nodeVersions)
			s.state.mu.RUnlock()
			if nodes != 0 || reports != 0 || versions != 0 {
				t.Fatalf("late heartbeat recreated retired indexes: nodes=%d reports=%d versions=%d", nodes, reports, versions)
			}
		})
	}
}

func TestRetiredHeartbeatPreservesCurrentRunAndPhysicalOccupancy(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	const retiredID = "run-retired-mixed-heartbeat"
	resultFixture(t, s, retiredID, "completed", time.Now().UTC())
	if err := s.retireRunRuntime(t.Context(), retiredID); err != nil {
		t.Fatal(err)
	}
	late := historicalFixtureNode(retiredID + "-late-unknown")
	late.RunID, late.State, late.Metadata = retiredID, model.NodeReady, nil
	current := model.Node{ID: "current-ready", RunID: "run-current", AgentID: agent.ID, State: model.NodeReady}
	agent.ActiveNodes = 2 // Physical occupancy must remain visible even for a late report.
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{late, current}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.verifyRetiredRuntime(retiredID); err != nil {
		t.Fatal(err)
	}
	if len(s.state.nodes) != 1 || s.state.nodes[current.ID].State != model.NodeReady || len(s.state.nodeReportTimes) != 1 || len(s.state.nodeVersions) != 1 || s.state.agents[agent.ID].ActiveNodes != 2 {
		t.Fatalf("late Run report changed current inventory or hid physical occupancy: nodes=%v agent=%+v", s.state.nodes, s.state.agents[agent.ID])
	}
}

func TestRetiredHeartbeatCannotEraseUnconfirmedObligations(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	const id = "run-retired-unconfirmed"
	node := historicalFixtureNode(id + "-pending")
	node.RunID, node.State, node.Metadata = id, model.NodeFailed, map[string]string{"cleanupComplete": "false"}
	s.state.setNodeLocked(node)
	s.state.reservations[node.ID] = agent.ID
	s.state.uncertainCreates = map[string]uncertainCreate{node.ID: {runID: id}}
	path := filepath.Join(s.config.DataDir, retiredRuntimeDirectory)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	err = writeRetiredRuntimeFence(directory, id)
	directory.Close()
	if err != nil {
		t.Fatal(err)
	}
	late := node
	late.State = model.NodeReady
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{late}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if s.state.nodes[node.ID].State != model.NodeFailed || s.state.reservations[node.ID] != agent.ID || len(s.state.uncertainCreates) != 1 {
		t.Fatal("retirement marker or late live state erased unconfirmed cleanup")
	}
	terminal := node
	terminal.State, terminal.Metadata = model.NodeStopped, map[string]string{"cleanupComplete": "true"}
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{terminal}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if !confirmedHistoricalNode(s.state.nodes[node.ID]) || len(s.state.reservations) != 0 || len(s.state.uncertainCreates) != 0 {
		t.Fatal("retired lookup prevented authoritative terminal evidence from resolving cleanup")
	}
}
