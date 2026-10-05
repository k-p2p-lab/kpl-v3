package model

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	MaxTopologyRequestBytes = 1 << 20
	MaxTopologyNeighbors    = MaxTopologyNodes - 1
	TopologyCommandTimeout  = 30 * time.Second
)

// TopologyRequest addresses one Peer incarnation and one Controller-generated
// plan. Prepare establishes links; apply installs and freezes the prepared mesh.
type TopologyRequest struct {
	RunID      string         `json:"runId"`
	Generation uint64         `json:"generation"`
	TopologyID string         `json:"topologyId"`
	Stage      string         `json:"stage"`
	Topic      string         `json:"topic"`
	Neighbors  []TopologyPeer `json:"neighbors"`
}

type TopologyPeer struct {
	PeerID    string   `json:"peerId"`
	Addresses []string `json:"addresses"`
}

type TopologyResponse struct {
	NodeID     string   `json:"nodeId"`
	PeerID     string   `json:"peerId"`
	TopologyID string   `json:"topologyId"`
	Stage      string   `json:"stage"`
	Topic      string   `json:"topic"`
	Frozen     bool     `json:"frozen"`
	Neighbors  []string `json:"neighbors"`
}

// PeerInfos validates the entire command before callers perform network I/O.
// Destination /p2p suffixes are checked and removed from transport addresses.
func (r TopologyRequest) PeerInfos() ([]peer.AddrInfo, error) {
	if strings.TrimSpace(r.RunID) == "" || len(r.RunID) > 256 {
		return nil, fmt.Errorf("runId is required and must not exceed 256 bytes")
	}
	if strings.TrimSpace(r.TopologyID) == "" || len(r.TopologyID) > 256 {
		return nil, fmt.Errorf("topologyId is required and must not exceed 256 bytes")
	}
	if r.Stage != "prepare" && r.Stage != "apply" {
		return nil, fmt.Errorf("topology stage must be prepare or apply")
	}
	if strings.TrimSpace(r.Topic) == "" || len(r.Topic) > 1024 {
		return nil, fmt.Errorf("topology topic is required and must not exceed 1024 bytes")
	}
	if len(r.Neighbors) > MaxTopologyNeighbors {
		return nil, fmt.Errorf("topology exceeds %d neighbors per peer", MaxTopologyNeighbors)
	}
	infos := make([]peer.AddrInfo, 0, len(r.Neighbors))
	seen := make(map[peer.ID]bool, len(r.Neighbors))
	for i, neighbor := range r.Neighbors {
		id, err := peer.Decode(neighbor.PeerID)
		if err != nil {
			return nil, fmt.Errorf("topology neighbor %d has an invalid peer ID: %w", i, err)
		}
		if seen[id] {
			return nil, fmt.Errorf("duplicate topology neighbor %s", id)
		}
		seen[id] = true
		if len(neighbor.Addresses) == 0 || len(neighbor.Addresses) > 32 {
			return nil, fmt.Errorf("topology neighbor %s requires 1 to 32 transport addresses", id)
		}
		info := peer.AddrInfo{ID: id}
		for _, raw := range neighbor.Addresses {
			if len(raw) > 2048 {
				return nil, fmt.Errorf("topology address exceeds 2048 bytes")
			}
			address, err := multiaddr.NewMultiaddr(raw)
			if err != nil {
				return nil, fmt.Errorf("invalid topology address for %s: %w", id, err)
			}
			transport, destination := peer.SplitAddr(address)
			if destination != "" && destination != id {
				return nil, fmt.Errorf("topology address destination does not match neighbor %s", id)
			}
			if transport == nil {
				return nil, fmt.Errorf("topology neighbor %s has no transport address", id)
			}
			info.Addrs = append(info.Addrs, transport)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Validate rejects acknowledgements for another command, Peer, or neighbor set.
func (r TopologyResponse) Validate(request TopologyRequest, nodeID, expectedPeerID string) error {
	if r.NodeID != nodeID || r.PeerID == "" || expectedPeerID != "" && r.PeerID != expectedPeerID ||
		r.TopologyID != request.TopologyID || r.Stage != request.Stage || r.Topic != request.Topic || request.Stage == "apply" && !r.Frozen {
		return fmt.Errorf("topology acknowledgement does not match the requested target and plan")
	}
	if _, err := peer.Decode(r.PeerID); err != nil {
		return fmt.Errorf("invalid topology acknowledgement peer ID: %w", err)
	}
	if !slices.IsSorted(r.Neighbors) {
		return fmt.Errorf("topology acknowledgement neighbors are not sorted")
	}
	want := make([]string, 0, len(request.Neighbors))
	for _, neighbor := range request.Neighbors {
		id, err := peer.Decode(neighbor.PeerID)
		if err != nil {
			return fmt.Errorf("invalid topology request peer ID: %w", err)
		}
		want = append(want, id.String())
	}
	got := append([]string{}, r.Neighbors...)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) {
		return fmt.Errorf("topology acknowledgement has a different neighbor set")
	}
	return nil
}
