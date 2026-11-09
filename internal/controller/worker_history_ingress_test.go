package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func historicalHeartbeatFixture(t *testing.T, directory string) (*Server, model.Agent) {
	t.Helper()
	s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
	a := model.Agent{ID: "history-agent", URL: "http://history.invalid", StartedAt: time.Now().Add(-time.Hour).UTC(), RunDrain: true, RunProcesses: true, Capacity: 100, ActiveNodes: 1}
	if _, err := s.state.registerAgent(a); err != nil {
		t.Fatal(err)
	}
	return s, a
}

func historicalFixtureNode(id string) model.Node {
	return model.Node{ID: id, RunID: "legacy-run", AgentID: "history-agent", Generation: 2, Group: "workers", Role: "worker", Type: "full", Profile: "profile", PeerID: "peer-" + id,
		State: model.NodeStopped, StartedAt: time.Unix(1700000000, 0).UTC(), LastSeen: time.Unix(1700000100, 0).UTC(),
		Metadata:       map[string]string{"cleanupComplete": "true", "topicsJSON": `["topic"]`, "diagnostic": strings.Repeat("original ", 40)},
		ConnectedPeers: []string{"original-connection"}, MeshPeers: map[string][]string{"topic": {"original-mesh"}}, PeerScores: map[string]float64{"original-score": -1.25}}
}

func TestHistoricalHeartbeatKeepsLegacyNodesOffTheLiveHeapAndPreservesReads(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	s.state.experiments["active-run"] = model.Experiment{ID: "active-run", State: "running"}
	active := model.Node{ID: "active-peer", RunID: "active-run", State: model.NodeReady, AgentID: agent.ID}
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{active}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	const count = 2048
	for first := 0; first < count; first += 128 {
		var nodes []model.Node
		for i := first; i < first+128; i++ {
			nodes = append(nodes, historicalFixtureNode(fmt.Sprintf("historical-%05d", i)))
		}
		if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: nodes, Partial: true}); err != nil {
			t.Fatal(err)
		}
		if len(s.state.nodes) != 1 || len(s.state.nodeReportTimes) != 1 || len(s.state.nodeVersions) > 1 {
			t.Fatalf("legacy history accumulated in live indexes: nodes=%d reports=%d versions=%d", len(s.state.nodes), len(s.state.nodeReportTimes), len(s.state.nodeVersions))
		}
	}
	if retired, err := s.isRuntimeRetired("legacy-run"); err != nil || retired {
		t.Fatalf("per-Peer history invented a Run cleanup proof: retired=%v err=%v", retired, err)
	}
	metrics := gatherTestMetrics(t, s.state)
	requireMetricValue(t, metrics, "kpl_nodes", map[string]string{"run_id": "legacy-run", "agent_id": agent.ID, "group": "workers", "role": "worker", "node_type": "full", "state": "stopped"}, count)
	for _, path := range []string{"/api/v1/nodes", "/api/v1/snapshot", "/api/v1/network"} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		switch path {
		case "/api/v1/nodes":
			s.handleNodes(response, request)
		case "/api/v1/snapshot":
			s.handleSnapshot(response, request)
		case "/api/v1/network":
			s.handleNetwork(response, request)
		}
		if response.Code != 200 {
			t.Fatalf("%s: HTTP %d: %s", path, response.Code, response.Body.String())
		}
		var nodes []model.Node
		if path == "/api/v1/nodes" {
			if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
				t.Fatal(err)
			}
		} else {
			var snapshot model.Snapshot
			if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
				t.Fatal(err)
			}
			nodes = snapshot.Nodes
			if snapshot.GeneratedAt.IsZero() {
				t.Fatal("streamed snapshot lost its original metadata")
			}
		}
		if len(nodes) != count+1 || nodes[0].ID != active.ID || !reflect.DeepEqual(nodes[1], historicalFixtureNode("historical-00000")) {
			t.Fatalf("%s lost sorted historical records or original fields: count=%d", path, len(nodes))
		}
	}
	// A delayed live report must not resurrect an ID whose terminal evidence
	// was already acknowledged and persisted outside the live state map.
	replayed := historicalFixtureNode("historical-00000")
	replayed.State = model.NodeReady
	replayed.Metadata = nil
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{replayed}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if len(s.state.nodes) != 1 || s.state.agents[agent.ID].ActiveNodes != 1 {
		t.Fatal("historical replay revived a retired Peer or changed physical occupancy")
	}
	// Every API/scrape read remains a disk projection rather than a warm cache.
	if len(s.state.nodes) != 1 {
		t.Fatal("explicit inventory warmed the whole history into control RAM")
	}
	restarted, _ := historicalHeartbeatFixture(t, s.config.DataDir)
	var restored int
	if err := restarted.state.walkHistoricalInventory(t.Context(), nil, time.Now().UTC(), func(model.Node) error { restored++; return nil }); err != nil || restored != count {
		t.Fatalf("Controller restart lost historical diagnostics: count=%d err=%v", restored, err)
	}
}

func TestHistoricalHeartbeatProtectsActiveRunsPendingCleanupAndOwnership(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	s.state.experiments["active-run"] = model.Experiment{ID: "active-run", State: "running"}
	active := historicalFixtureNode("active-terminal")
	active.RunID = "active-run"
	pending := historicalFixtureNode("unconfirmed-terminal")
	pending.Metadata = map[string]string{"cleanupComplete": "false"}
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{active, pending}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if len(s.state.nodes) != 2 {
		t.Fatal("active Run or unconfirmed cleanup evidence was discarded")
	}
	wrong := active
	wrong.RunID = "different-run"
	newNode := historicalFixtureNode("must-not-partially-commit")
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{newNode, wrong}, Partial: true}); err == nil {
		t.Fatal("same-Agent Peer changed its Run ownership")
	}
	page, _, _, err := s.state.terminalHistory.page(t.Context(), "", time.Now().UTC(), 128)
	if err != nil || len(page) != 0 || s.state.nodes[active.ID].RunID != "active-run" {
		t.Fatalf("rejected heartbeat partially changed disk/live state: %v %v", page, err)
	}
	legacy := active
	legacy.RunID = ""
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{legacy}, Partial: true}); err != nil || s.state.nodes[active.ID].RunID != "active-run" {
		t.Fatalf("omitted legacy Run ID erased known ownership: %v", err)
	}
}

func TestHistoricalInventoryReleasesStorageBeforeSlowReadersAndHonorsDeletion(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	first := historicalFixtureNode("first")
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{first}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	asOf := time.Now().UTC()
	visited := 0
	err := s.state.walkHistoricalInventory(t.Context(), nil, asOf, func(model.Node) error {
		visited++
		finished := make(chan error, 1)
		go func() {
			next := historicalFixtureNode("later")
			finished <- s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{next}, Partial: true})
		}()
		select {
		case err := <-finished:
			return err
		case <-time.After(3 * time.Second):
			return errors.New("HTTP reader retained a disk transaction or history mutex")
		}
	})
	if err != nil || visited != 1 {
		t.Fatalf("point-in-time streamed inventory: visited=%d err=%v", visited, err)
	}
	s.state.persistMu.Lock()
	err = s.markResultDeletedLocked("legacy-run")
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	visited = 0
	if err := s.state.walkHistoricalInventory(t.Context(), nil, time.Now().UTC(), func(model.Node) error { visited++; return nil }); err != nil || visited != 0 {
		t.Fatalf("explicitly deleted Run reappeared in historical inventory: count=%d err=%v", visited, err)
	}
	metrics := gatherTestMetrics(t, s.state)
	if _, exists := metrics[metricTestKey("kpl_nodes", map[string]string{"run_id": "legacy-run", "agent_id": agent.ID, "group": "workers", "role": "worker", "node_type": "full", "state": "stopped"})]; exists {
		t.Fatal("deleted historical Node metrics reappeared")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.state.walkHistoricalInventory(ctx, nil, time.Now().UTC(), func(model.Node) error { t.Fatal("canceled reader emitted data"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled historical read: %v", err)
	}
}

func TestHistoricalHeartbeatStorageFailureCannotAcknowledgeOrWarmLiveState(t *testing.T) {
	s, agent := historicalHeartbeatFixture(t, t.TempDir())
	if err := os.Mkdir(filepath.Join(s.config.DataDir, terminalHistoryFile), 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{historicalFixtureNode("terminal")}, Partial: true}); err == nil {
		t.Fatal("history storage failure was acknowledged")
	}
	if len(s.state.nodes) != 0 || len(s.state.nodeReportTimes) != 0 {
		t.Fatal("failed historical storage retained a partial in-memory fallback")
	}
}
