package agent

import (
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func topologyStatusServer() *Server {
	return &Server{processes: map[string]*process{
		"node": {apiURL: "http://peer:18000", node: model.Node{ID: "node", State: model.NodeStarting, Metadata: map[string]string{"owned": "agent"}}},
	}}
}

func topologyStatusAt(observed time.Time) model.Node {
	return model.Node{
		ID: "node", State: model.NodeReady, PeerID: "self", LastSeen: observed,
		Addresses: []string{"address"}, ConnectedPeers: []string{"transport"},
		RoutingPeers: []string{"routing"}, MeshPeers: map[string][]string{"topic": {"mesh"}},
		OverlayObservedAt: observed,
		TopicPeers:        map[string]int{"topic": 1}, PeerScores: map[string]float64{"mesh": 1.5},
	}
}

func TestTopologyStatusOwnsIncomingAndOutgoingCollections(t *testing.T) {
	server := topologyStatusServer()
	update := topologyStatusAt(time.Unix(100, 0))
	if err := server.updateNode(update); err != nil {
		t.Fatal(err)
	}
	expected := server.nodes()[0]
	update.Addresses[0] = "changed"
	update.ConnectedPeers[0] = "changed"
	update.RoutingPeers[0] = "changed"
	update.MeshPeers["topic"][0] = "changed"
	update.MeshPeers["new"] = []string{"changed"}
	update.TopicPeers["topic"] = 9
	update.PeerScores["mesh"] = 9
	for _, output := range []model.Node{server.nodes()[0], server.snapshot().Nodes[0]} {
		if !reflect.DeepEqual(output, expected) {
			t.Fatalf("incoming collections changed stored status: %+v", output)
		}
		output.Addresses[0] = "outgoing"
		output.ConnectedPeers[0] = "outgoing"
		output.RoutingPeers[0] = "outgoing"
		output.MeshPeers["topic"][0] = "outgoing"
		output.TopicPeers["topic"] = 8
		output.PeerScores["mesh"] = 8
		output.Metadata["owned"] = "outgoing"
	}
	if got := server.snapshot().Nodes[0]; !reflect.DeepEqual(got, expected) {
		t.Fatalf("outgoing collections changed stored status: %+v", got)
	}
}

func TestTopologyStatusRejectsCrossedReportsUsingOnlyPeerClock(t *testing.T) {
	for _, sourceTime := range []time.Time{time.Unix(1, 0), time.Now().Add(24 * time.Hour)} {
		server := topologyStatusServer()
		if err := server.updateNode(topologyStatusAt(sourceTime)); err != nil {
			t.Fatal(err)
		}
		newer := topologyStatusAt(sourceTime.Add(time.Second))
		newer.RoutingPeers = []string{"new-routing"}
		if err := server.updateNode(newer); err != nil {
			t.Fatal(err)
		}
		accepted := server.nodes()[0]
		if accepted.RoutingPeers[0] != "new-routing" || !accepted.OverlayObservedAt.Equal(newer.OverlayObservedAt) {
			t.Fatalf("Agent receipt clock rejected a newer Peer report: %+v", accepted)
		}
		if accepted.LastSeen.Before(time.Now().Add(-time.Minute)) || accepted.LastSeen.After(time.Now().Add(time.Minute)) {
			t.Fatalf("Agent LastSeen was replaced by Peer clock: %v", accepted.LastSeen)
		}
		for _, staleTime := range []time.Time{sourceTime, newer.LastSeen, {}} {
			stale := topologyStatusAt(staleTime)
			stale.State = model.NodeStarting
			stale.Error = "stale error"
			if err := server.updateNode(stale); err != nil {
				t.Fatal(err)
			}
			if got := server.nodes()[0]; !reflect.DeepEqual(got, accepted) {
				t.Fatalf("crossed/retried/undated report replaced accepted status: %+v", got)
			}
		}
	}
}

func TestTopologyStatusClearsOnlyAuthoritativeNewOverlay(t *testing.T) {
	server := topologyStatusServer()
	observed := time.Unix(100, 0)
	if err := server.updateNode(topologyStatusAt(observed)); err != nil {
		t.Fatal(err)
	}
	for index, overlayTime := range []time.Time{observed.Add(-time.Second), observed, {}} {
		update := model.Node{ID: "node", State: model.NodeReady, LastSeen: observed.Add(time.Duration(index+1) * time.Second), OverlayObservedAt: overlayTime}
		if err := server.updateNode(update); err != nil {
			t.Fatal(err)
		}
		got := server.nodes()[0]
		if len(got.RoutingPeers) != 1 || len(got.MeshPeers["topic"]) != 1 || !got.OverlayObservedAt.Equal(observed) {
			t.Fatalf("stale or unobserved empty overlay cleared known topology: %+v", got)
		}
	}
	update := model.Node{ID: "node", State: model.NodeReady, LastSeen: observed.Add(4 * time.Second), OverlayObservedAt: observed.Add(4 * time.Second)}
	if err := server.updateNode(update); err != nil {
		t.Fatal(err)
	}
	if got := server.snapshot().Nodes[0]; len(got.RoutingPeers) != 0 || len(got.MeshPeers) != 0 || !got.OverlayObservedAt.Equal(update.OverlayObservedAt) {
		t.Fatalf("new authoritative empty snapshot did not clear overlay: %+v", got)
	}
}

func TestTopologyStatusCannotReviveStoppingOrExitedProcess(t *testing.T) {
	for _, state := range []string{model.NodeStopping, model.NodeStopped, model.NodeFailed, "exited"} {
		server := topologyStatusServer()
		proc := server.processes["node"]
		proc.node = topologyStatusAt(time.Unix(100, 0))
		if state == "exited" {
			proc.exited = true
		} else {
			proc.node.State = state
		}
		before := cloneNodeStatus(proc.node)
		if err := server.updateNode(topologyStatusAt(time.Unix(200, 0))); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(proc.node, before) {
			t.Fatalf("late topology revived %s process: %+v", state, proc.node)
		}
	}
}
