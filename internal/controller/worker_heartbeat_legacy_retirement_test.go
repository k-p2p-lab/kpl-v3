package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestLegacyPreparedHeartbeatKeepsRunIdentityAcrossRetirement(t *testing.T) {
	for _, state := range []string{model.NodeStopped, model.NodeReady, model.NodeStarting} {
		t.Run(state, func(t *testing.T) {
			s, agent := historicalHeartbeatFixture(t, t.TempDir())
			const id = "run-legacy-heartbeat-retirement"
			run, _ := resultFixture(t, s, id, "failed", time.Now().UTC())
			run.CleanupState = "running"
			s.state.experiments[id] = run
			node := historicalFixtureNode(id + "-worker-1")
			node.RunID = id
			if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
				t.Fatal(err)
			}
			legacy := node
			legacy.RunID, legacy.State = "", state
			if state != model.NodeStopped {
				legacy.Metadata = nil
			}
			original := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{legacy}, Partial: true}
			h := s.state.normalizeHeartbeatRunIDs(original)
			if h.Nodes[0].RunID != id || original.Nodes[0].RunID != "" {
				t.Fatalf("normalization lost known ownership or mutated the original report: prepared=%q original=%q", h.Nodes[0].RunID, original.Nodes[0].RunID)
			}
			retired, revision, err := s.state.heartbeatRetirements(h.Nodes)
			if err != nil || retired[id] {
				t.Fatalf("fixture already retired: %v %v", retired, err)
			}
			historical, err := s.state.prepareHistoricalHeartbeat(h, retired)
			if err != nil {
				t.Fatal(err)
			}
			// The old report is prepared before the live map disappears. Run
			// identity must survive the disk-I/O boundary and cache eviction.
			if err := s.retireRunRuntime(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			s.state.mu.Lock()
			for i := range closedRunCacheEntries + 1 {
				s.state.cacheRetiredRunLocked(fmt.Sprintf("legacy-other-retired-%d", i))
			}
			s.state.mu.Unlock()
			if err := s.state.applyPreparedHeartbeat(h, historical, retired, revision); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyRetiredRuntime(id); err != nil {
				t.Fatal(err)
			}
			s.state.mu.RLock()
			nodes, reports, versions, active := len(s.state.nodes), len(s.state.nodeReportTimes), len(s.state.nodeVersions), len(s.state.activeNodeIDs)
			s.state.mu.RUnlock()
			if nodes != 0 || reports != 0 || versions != 0 || active != 0 {
				t.Fatalf("late legacy report recreated orphan runtime indexes: nodes=%d reports=%d versions=%d active=%d", nodes, reports, versions, active)
			}
			if original.Nodes[0].RunID != "" {
				t.Fatal("applying the prepared report mutated the original legacy payload")
			}
		})
	}
}

func TestHeartbeatRunIDNormalizationPreservesExplicitOwnership(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	known := historicalFixtureNode("known-legacy-node")
	s.state.setNodeLocked(known)
	original := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{
		{ID: known.ID},
		{ID: "unknown-legacy-node"},
		{ID: known.ID, RunID: "explicit-other-run"},
	}, Partial: true}
	normalized := s.state.normalizeHeartbeatRunIDs(original)
	if normalized.Nodes[0].RunID != known.RunID || normalized.Nodes[1].RunID != "" || normalized.Nodes[2].RunID != "explicit-other-run" {
		t.Fatalf("normalization guessed unknown ownership or rewrote an explicit Run ID: %+v", normalized.Nodes)
	}
	if original.Nodes[0].RunID != "" || original.Nodes[1].RunID != "" || original.Nodes[2].RunID != "explicit-other-run" {
		t.Fatalf("normalization changed caller-owned input: %+v", original.Nodes)
	}
}
