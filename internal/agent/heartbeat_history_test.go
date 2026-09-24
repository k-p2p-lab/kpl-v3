package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestChurnHeartbeatUpdatesStoppedPeersWithoutDashboard(t *testing.T) {
	now := time.Now().UTC()
	agent := topologyStatusServer()
	agent.config.ID = "history-agent"
	agent.config.AdvertiseURL = "http://history-agent:8090"
	agent.config.Capacity = 200
	agent.startedAt = now.Add(-time.Hour)
	agent.processes = make(map[string]*process)
	const count = 10000
	for i := range count {
		node := topologyStatusAt(now)
		node.ID = fmt.Sprintf("run-20260907T114254Z-0123456789abcdef0123456789abcdef-delay-high-%05d", i)
		node.RunID, node.AgentID, node.Group = "churn-run", agent.config.ID, "delay-high"
		node.State, node.Type, node.Role, node.Profile = model.NodeStopped, "full", "worker", "delay-high"
		node.StartedAt = now.Add(-time.Minute)
		// Retained overlays, scores and resolved configuration made old full
		// heartbeats grow with every expired Peer, beyond the 10 MiB endpoint.
		node.Metadata = map[string]string{
			"runtime": "docker", "stoppedAt": now.Format(time.RFC3339Nano),
			"topics": "kpl/prysm/beacon_block", "topicsJSON": `["kpl/prysm/beacon_block"]`,
			"pubsubEnabled": "true", "topicMode": "subscribe",
			"networkRequested": strings.Repeat("x", 2048),
		}
		agent.processes[node.ID] = &process{node: node, exited: true}
	}
	ready := topologyStatusAt(now)
	ready.ID, ready.RunID = "still-ready", "churn-run"
	agent.processes[ready.ID] = &process{node: ready, apiURL: "http://peer:18000"}

	controllerServer := authenticatedTestController(t.TempDir())
	controllerHTTP := httptest.NewServer(controllerServer.Handler(context.Background()))
	defer controllerHTTP.Close()
	agent.config.Token = controllerTestToken
	agent.config.ControllerURL = controllerHTTP.URL
	agent.client = controllerHTTP.Client()
	if err := agent.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Seed a Controller's previous view; no browser/SSE subscriber is present.
	old := model.AgentHeartbeat{Agent: agent.snapshot().Agent}
	for _, proc := range agent.processes {
		old.Nodes = append(old.Nodes, model.Node{ID: proc.node.ID, RunID: "churn-run", State: model.NodeStarting})
	}
	if err := agent.postJSON(context.Background(), "/api/v1/agents/heartbeat", old, nil); err != nil {
		t.Fatal(err)
	}
	if err := agent.heartbeat(context.Background()); err != nil {
		t.Fatalf("termination snapshot rejected after churn: %v", err)
	}
	response, err := controllerHTTP.Client().Do(controllerReadRequest(t, controllerServer.Handler(context.Background()), controllerHTTP.URL+"/api/v1/snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var snapshot model.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	stopped := 0
	for _, node := range snapshot.Nodes {
		if node.ID == ready.ID {
			if node.State != model.NodeReady || !reflect.DeepEqual(node.PeerScores, ready.PeerScores) {
				t.Fatalf("active Peer detail was lost: %+v", node)
			}
			continue
		}
		if node.State != model.NodeStopped || node.Metadata["stoppedAt"] == "" {
			t.Fatalf("fresh dashboard would show expired Peer as an issue: %+v", node)
		}
		stopped++
	}
	if stopped != count || snapshot.Agents[0].ActiveNodes != 1 {
		t.Fatalf("ended Peers or capacity remained stale: stopped=%d agents=%+v", stopped, snapshot.Agents)
	}
	data, err := json.Marshal(agent.snapshot())
	if err != nil || len(data) >= 10<<20 {
		t.Fatalf("compacted heartbeat bytes=%d err=%v", len(data), err)
	}
	t.Logf("10,000 stopped Peers plus an active Peer: heartbeat %d bytes", len(data))
	// Inspection of a retained Agent record remains available independently of
	// the compact synchronization projection.
	for _, node := range agent.nodes() {
		if node.State == model.NodeStopped && len(node.Metadata["networkRequested"]) != 2048 {
			t.Fatal("heartbeat mutated retained diagnostics")
		}
	}
}

func TestOversizedHeartbeatCannotPartiallyUpdatePeerStates(t *testing.T) {
	server := authenticatedTestController(t.TempDir())
	handler := server.Handler(context.Background())
	request := func(path string, input any) *httptest.ResponseRecorder {
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
		req.Header.Set("Authorization", "Bearer "+controllerTestToken)
		handler.ServeHTTP(response, req)
		return response
	}
	agent := model.Agent{ID: "agent", URL: "http://agent", Capacity: 2}
	if got := request("/api/v1/agents/register", agent); got.Code != http.StatusCreated {
		t.Fatal(got.Body.String())
	}
	if got := request("/api/v1/agents/heartbeat", model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{{ID: "one", State: model.NodeReady}}}); got.Code != http.StatusNoContent {
		t.Fatal(got.Body.String())
	}
	oversized := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{
		{ID: "one", State: model.NodeStopped},
		{ID: "two", Metadata: map[string]string{"large": strings.Repeat("x", 10<<20)}},
	}}
	if got := request("/api/v1/agents/heartbeat", oversized); got.Code != http.StatusBadRequest {
		t.Fatalf("oversized heartbeat status=%d", got.Code)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, controllerReadRequest(t, handler, "/api/v1/nodes"))
	var nodes []model.Node
	if err := json.NewDecoder(response.Body).Decode(&nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].State != model.NodeReady {
		t.Fatalf("partial heartbeat altered state: %+v", nodes)
	}
}

func TestHeartbeatCompactsOnlyConfirmedSuccessfulExits(t *testing.T) {
	for _, state := range []string{model.NodeStarting, model.NodeReady, model.NodeStopping, model.NodeStopped, model.NodeFailed} {
		for _, exited := range []bool{false, true} {
			for _, cleanupFailed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/exited=%t/cleanupFailed=%t", state, exited, cleanupFailed), func(t *testing.T) {
					proc := &process{node: topologyStatusAt(time.Now()), exited: exited}
					proc.node.State = state
					proc.node.Metadata = map[string]string{"runtime": "docker", "stoppedAt": "confirmed", "networkRequested": "detail"}
					if cleanupFailed {
						proc.cleanupErr = errors.New("container removal failed")
						proc.node.Error = proc.cleanupErr.Error()
					}
					expected := cloneNodeStatus(proc.node)
					got := heartbeatNodeStatus(proc)
					if exited && !cleanupFailed && state == model.NodeStopped {
						if len(got.ConnectedPeers)+len(got.RoutingPeers)+len(got.MeshPeers)+len(got.PeerScores)+len(got.Addresses)+len(got.TopicPeers) != 0 || !got.OverlayObservedAt.IsZero() {
							t.Fatalf("completed exit retained live overlay: %+v", got)
						}
						if got.Metadata["stoppedAt"] != "confirmed" || got.Metadata["runtime"] != "docker" || got.Metadata["networkRequested"] != "" {
							t.Fatalf("incorrect terminal metadata: %+v", got.Metadata)
						}
					} else if !reflect.DeepEqual(got, expected) {
						t.Fatalf("unconfirmed/failed process lost diagnostic data: %+v", got)
					}
					got.Metadata["runtime"] = "mutated"
					if !reflect.DeepEqual(proc.node, expected) {
						t.Fatal("snapshot changed retained status")
					}
				})
			}
		}
	}
}
