package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func topologyTestNodes(now time.Time) []model.Node {
	return []model.Node{
		{ID: "a", RunID: "run", AgentID: "agent-a", PeerID: "peer-a", State: model.NodeReady, LastSeen: now, OverlayObservedAt: now,
			ConnectedPeers: []string{"peer-b", "peer-b", "unknown"}, RoutingPeers: []string{"peer-b"}, MeshPeers: map[string][]string{"one": {"peer-b", "peer-b"}, "two": {"peer-b"}}},
		{ID: "b", RunID: "run", AgentID: "agent-b", PeerID: "peer-b", State: model.NodeReady, LastSeen: now, OverlayObservedAt: now,
			ConnectedPeers: []string{"peer-a"}, MeshPeers: map[string][]string{"one": {"peer-a"}}},
	}
}

func TestTopologySeparatesProtocolsTopicsAndReporters(t *testing.T) {
	now := time.Now()
	nodes := topologyTestNodes(now)
	edges := networkEdgesAt(nodes, nil, now)
	want := []model.Edge{
		{Source: "a", Target: "b", Protocol: "gossipsub", Topic: "one", ReportedBy: []string{"a", "b"}},
		{Source: "a", Target: "b", Protocol: "gossipsub", Topic: "two", ReportedBy: []string{"a"}},
		{Source: "a", Target: "b", Protocol: "kademlia", ReportedBy: []string{"a"}},
		{Source: "a", Target: "b", Protocol: "transport", ReportedBy: []string{"a", "b"}},
	}
	if !reflect.DeepEqual(edges, want) {
		t.Fatalf("protocol/topic evidence lost:\ngot  %+v\nwant %+v", edges, want)
	}
	// A routing relationship need not have a current TCP connection.
	nodes[0].ConnectedPeers, nodes[1].ConnectedPeers = nil, nil
	if got := networkEdgesAt(nodes, nil, now); len(got) != 3 {
		t.Fatalf("overlays depended on transport: %+v", got)
	}
	// Reordering heartbeat node slices must not move links in the output.
	nodes[0], nodes[1] = nodes[1], nodes[0]
	if got := networkEdgesAt(nodes, nil, now); !reflect.DeepEqual(got, want[:3]) {
		t.Fatalf("unstable ordering: %+v", got)
	}
}

func TestTopologyAuthoritativeEmptyPrunesWithoutEventReplay(t *testing.T) {
	now := time.Now()
	nodes := topologyTestNodes(now)
	nodes[0].MeshPeers = map[string][]string{}
	edges := networkEdgesAt(nodes, nil, now)
	for _, edge := range edges {
		if edge.Protocol == "gossipsub" && (edge.Topic != "one" || !reflect.DeepEqual(edge.ReportedBy, []string{"b"})) {
			t.Fatalf("old mesh membership survived: %+v", edge)
		}
	}
	nodes[1].MeshPeers = nil
	for _, edge := range networkEdgesAt(nodes, nil, now) {
		if edge.Protocol == "gossipsub" {
			t.Fatalf("empty reports retained mesh: %+v", edge)
		}
	}
}

func TestTopologyDoesNotInferOverlaysForLegacyPeers(t *testing.T) {
	now := time.Now()
	nodes := topologyTestNodes(now)
	for i := range nodes {
		nodes[i].OverlayObservedAt = time.Time{}
	}
	edges := networkEdgesAt(nodes, nil, now)
	if len(edges) != 1 || edges[0].Protocol != "transport" {
		t.Fatalf("invented legacy overlays: %+v", edges)
	}
}

func TestTopologyRejectsStoppedStaleOfflineForeignAndAmbiguousEndpoints(t *testing.T) {
	now := time.Now()
	agents := map[string]model.Agent{
		"agent-a": {State: model.AgentOnline, LastSeen: now}, "agent-b": {State: model.AgentOnline, LastSeen: now},
	}
	for _, state := range []string{model.NodeStarting, model.NodeStopping, model.NodeStopped, model.NodeFailed} {
		nodes := topologyTestNodes(now)
		nodes[1].State = state
		if edges := networkEdgesAt(nodes, agents, now); len(edges) != 0 {
			t.Fatalf("%s endpoint has edges: %+v", state, edges)
		}
	}
	nodes := topologyTestNodes(now)
	nodes[1].LastSeen = now.Add(-agentStaleAfter - time.Second)
	if edges := networkEdgesAt(nodes, agents, now); len(edges) != 0 {
		t.Fatalf("fresh Agent kept stale Peer edges: %+v", edges)
	}
	nodes = topologyTestNodes(now)
	agents["agent-b"] = model.Agent{State: model.AgentOnline, LastSeen: now.Add(-time.Minute)}
	if edges := networkEdgesAt(nodes, agents, now); len(edges) != 0 {
		t.Fatalf("offline Agent retained edges: %+v", edges)
	}
	nodes[1].RunID = "other-run"
	if edges := networkEdgesAt(nodes, nil, now); len(edges) != 0 {
		t.Fatalf("cross-run relation accepted: %+v", edges)
	}
	nodes = topologyTestNodes(now)
	alias := nodes[1]
	alias.ID = "same-peer-alias"
	nodes = append(nodes, alias)
	if edges := networkEdgesAt(nodes, nil, now); len(edges) != 0 {
		t.Fatalf("ambiguous Peer ID was resolved: %+v", edges)
	}
}

func TestNetworkAPIUsesStatusSnapshotsBeyondRecentEvents(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now()
	for _, node := range topologyTestNodes(now) {
		s.state.nodes[node.ID] = node
		s.state.agents[node.AgentID] = model.Agent{ID: node.AgentID, State: model.AgentOnline, LastSeen: now}
	}
	// No replayed GRAFT is necessary; even contradictory/truncated old events
	// cannot overwrite current protocol snapshots.
	for i := 0; i < recentEventLimit; i++ {
		s.state.events = append(s.state.events, model.TraceEvent{NodeID: "a", Type: "prune", Topic: "one", RemotePeerID: "peer-b"})
	}
	recorder := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/network", nil))
	var network struct {
		Edges []model.Edge `json:"edges"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &network); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || !reflect.DeepEqual(network.Edges, networkEdgesAt(topologyTestNodes(now), nil, now)) {
		t.Fatalf("network API lost current overlays: %d %s", recorder.Code, recorder.Body)
	}
}

func TestTopologyFreshnessUsesRelativeAgentAgeDespiteClockOffset(t *testing.T) {
	for _, offset := range []time.Duration{-time.Hour, time.Hour} {
		s := newState(t.TempDir())
		base := time.Now()
		for index, age := range []time.Duration{time.Second, agentStaleAfter + time.Second} {
			agentTime := base.Add(offset + time.Duration(index)*time.Second)
			nodes := topologyTestNodes(agentTime)
			for i := range nodes {
				nodes[i].AgentID = "agent"
				nodes[i].LastSeen = agentTime.Add(-age)
			}
			if _, exists := s.agents["agent"]; !exists {
				if _, err := s.registerAgent(model.Agent{ID: "agent", URL: "http://agent"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "agent", LastSeen: agentTime}, Nodes: nodes}); err != nil {
				t.Fatal(err)
			}
			snapshot := s.snapshot()
			if age < agentStaleAfter && len(snapshot.Edges) != 4 {
				t.Fatalf("offset %s hid fresh edges: %+v", offset, snapshot.Edges)
			}
			if age > agentStaleAfter && len(snapshot.Edges) != 0 {
				t.Fatalf("offset %s retained stale edges: %+v", offset, snapshot.Edges)
			}
			if !s.nodes["a"].LastSeen.Equal(nodes[0].LastSeen) {
				t.Fatal("normalization altered original status timestamp")
			}
		}
	}
}
