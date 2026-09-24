package controller

import (
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type topologyPeerKey struct{ runID, peerID string }
type topologyEdgeKey struct{ source, target, protocol, topic string }

func topologyReportTime(received, agentReported, peerSeen time.Time) time.Time {
	if agentReported.IsZero() || peerSeen.IsZero() {
		return received
	}
	age := agentReported.Sub(peerSeen)
	if age < 0 {
		age = 0
	}
	return received.Add(-age)
}

// Edges are projections of current Peer reports, never reconstructions from
// the bounded event feed. A routing-table member is not interchangeable with
// a transport connection or a topic's accepted GossipSub mesh membership.
// The caller normalizes node LastSeen on a copy into Controller clock time.
func networkEdgesAt(nodes []model.Node, agents map[string]model.Agent, now time.Time) []model.Edge {
	peerToNode := make(map[topologyPeerKey]string)
	ready := make(map[string]bool)
	for _, node := range nodes {
		if node.PeerID == "" || node.State != model.NodeReady {
			continue
		}
		if agents != nil && !agentIsOnline(agents[node.AgentID], now) {
			continue
		}
		// Fresh Agent heartbeats alone must not keep a dead Peer report alive.
		if !node.LastSeen.IsZero() && now.Sub(node.LastSeen) > agentStaleAfter {
			continue
		}
		key := topologyPeerKey{node.RunID, node.PeerID}
		if _, duplicate := peerToNode[key]; duplicate {
			// Ambiguous identities cannot be assigned a trustworthy endpoint.
			peerToNode[key] = ""
		} else {
			peerToNode[key] = node.ID
		}
		ready[node.ID] = true
	}
	reports := make(map[topologyEdgeKey]map[string]struct{})
	add := func(node model.Node, peerID, protocol, topic string) {
		if peerToNode[topologyPeerKey{node.RunID, node.PeerID}] != node.ID {
			return
		}
		target := peerToNode[topologyPeerKey{node.RunID, peerID}]
		if target == "" || target == node.ID {
			return
		}
		a, b := node.ID, target
		if a > b {
			a, b = b, a
		}
		key := topologyEdgeKey{a, b, protocol, topic}
		if reports[key] == nil {
			reports[key] = make(map[string]struct{})
		}
		reports[key][node.ID] = struct{}{}
	}
	for _, node := range nodes {
		if !ready[node.ID] {
			continue
		}
		for _, peerID := range node.ConnectedPeers {
			add(node, peerID, "transport", "")
		}
		// The timestamp distinguishes a known empty snapshot from older Peers
		// that do not implement overlay reporting. Never relabel TCP as DHT.
		if node.OverlayObservedAt.IsZero() {
			continue
		}
		for _, peerID := range node.RoutingPeers {
			add(node, peerID, "kademlia", "")
		}
		for topic, peers := range node.MeshPeers {
			if topic == "" {
				continue
			}
			for _, peerID := range peers {
				add(node, peerID, "gossipsub", topic)
			}
		}
	}
	edges := make([]model.Edge, 0, len(reports))
	for key, reporters := range reports {
		edge := model.Edge{Source: key.source, Target: key.target, Protocol: key.protocol, Topic: key.topic}
		for reporter := range reporters {
			edge.ReportedBy = append(edge.ReportedBy, reporter)
		}
		sort.Strings(edge.ReportedBy)
		edges = append(edges, edge)
	}
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.Target != b.Target {
			return a.Target < b.Target
		}
		if a.Protocol != b.Protocol {
			return a.Protocol < b.Protocol
		}
		return a.Topic < b.Topic
	})
	return edges
}

func networkEdges(nodes []model.Node) []model.Edge {
	return networkEdgesAt(nodes, nil, time.Now())
}
