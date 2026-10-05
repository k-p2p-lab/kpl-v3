package model

import (
	"slices"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func topologyCommandTestID(t *testing.T, value string) string {
	t.Helper()
	hash, err := multihash.Sum([]byte(value), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return peer.ID(hash).String()
}

func TestTopologyCommandValidatesFullNeighborSet(t *testing.T) {
	id, other := topologyCommandTestID(t, "one"), topologyCommandTestID(t, "two")
	valid := TopologyRequest{RunID: "run", Generation: 2, TopologyID: "plan", Stage: "prepare", Topic: "topic",
		Neighbors: []TopologyPeer{{PeerID: id, Addresses: []string{"/ip4/127.0.0.1/tcp/20000/p2p/" + id}}}}
	infos, err := valid.PeerInfos()
	if err != nil || len(infos) != 1 || infos[0].Addrs[0].String() != "/ip4/127.0.0.1/tcp/20000" {
		t.Fatalf("valid transport destination not normalized: %+v %v", infos, err)
	}
	for _, name := range []string{"run", "id", "stage", "topic", "duplicate", "bad peer", "no addresses", "bad address", "wrong destination", "no transport", "many neighbors"} {
		t.Run(name, func(t *testing.T) {
			r := valid
			r.Neighbors = slices.Clone(valid.Neighbors)
			switch name {
			case "run":
				r.RunID = " "
			case "id":
				r.TopologyID = strings.Repeat("x", 257)
			case "stage":
				r.Stage = "freeze"
			case "topic":
				r.Topic = " "
			case "duplicate":
				r.Neighbors = append(r.Neighbors, r.Neighbors[0])
			case "bad peer":
				r.Neighbors[0].PeerID = "not-a-peer"
			case "no addresses":
				r.Neighbors[0].Addresses = nil
			case "bad address":
				r.Neighbors[0].Addresses = []string{"not-a-multiaddr"}
			case "wrong destination":
				r.Neighbors[0].Addresses = []string{"/ip4/127.0.0.1/tcp/20000/p2p/" + other}
			case "no transport":
				r.Neighbors[0].Addresses = []string{"/p2p/" + id}
			case "many neighbors":
				r.Neighbors = make([]TopologyPeer, MaxTopologyNeighbors+1)
			}
			if _, err := r.PeerInfos(); err == nil {
				t.Fatal("invalid topology request accepted")
			}
		})
	}
}

func TestTopologyAcknowledgementMatchesExactSortedPlan(t *testing.T) {
	id, one, two := topologyCommandTestID(t, "node"), topologyCommandTestID(t, "one"), topologyCommandTestID(t, "two")
	request := TopologyRequest{RunID: "run", TopologyID: "plan", Stage: "apply", Topic: "topic", Neighbors: []TopologyPeer{{PeerID: one}, {PeerID: two}}}
	valid := TopologyResponse{NodeID: "node", PeerID: id, TopologyID: "plan", Stage: "apply", Topic: "topic", Frozen: true, Neighbors: []string{one, two}}
	slices.Sort(valid.Neighbors)
	if err := valid.Validate(request, "node", id); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"node", "peer", "plan", "stage", "topic", "not frozen", "different neighbors", "unsorted"} {
		t.Run(name, func(t *testing.T) {
			response := valid
			response.Neighbors = slices.Clone(valid.Neighbors)
			switch name {
			case "node":
				response.NodeID = "other"
			case "peer":
				response.PeerID = one
			case "plan":
				response.TopologyID = "other"
			case "stage":
				response.Stage = "prepare"
			case "topic":
				response.Topic = "other"
			case "not frozen":
				response.Frozen = false
			case "different neighbors":
				response.Neighbors[0] = id
			case "unsorted":
				slices.Reverse(response.Neighbors)
			}
			if err := response.Validate(request, "node", id); err == nil {
				t.Fatal("incorrect acknowledgement accepted")
			}
		})
	}
}
