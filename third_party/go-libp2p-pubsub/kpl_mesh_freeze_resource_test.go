package pubsub

import (
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

// Disconnect must make the upstream positive-score retention decision before
// dropping mesh accounting. Otherwise a healthy peer's P1 disappears and a new
// P3b penalty can turn its discarded score into retained negative history.
func TestKPLMeshFreezeDisconnectPreservesScoreRetention(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		mode := "ordinary"
		if frozen {
			mode = "frozen"
		}
		for _, positive := range []bool{false, true} {
			scoreName := "nonpositive"
			if positive {
				scoreName = "positive"
			}
			for _, path := range []string{"router-remove", "dead-peer"} {
				t.Run(mode+"/"+scoreName+"/"+path, func(t *testing.T) {
					topic, pid := "score-retention", peer.ID("departing-peer")
					params := &PeerScoreParams{
						Topics: map[string]*TopicScoreParams{topic: {
							TopicWeight: 1, TimeInMeshQuantum: time.Second, TimeInMeshWeight: 1, TimeInMeshCap: 100,
							MeshMessageDeliveriesThreshold: 5, MeshMessageDeliveriesWeight: -1, MeshFailurePenaltyWeight: -2,
						}},
						AppSpecificScore: func(peer.ID) float64 { return 0 }, RetainScore: time.Minute,
					}
					scorer := newPeerScore(params)
					meshTime := time.Second
					if positive {
						meshTime = 30 * time.Second
					}
					stats := &topicStats{inMesh: true, meshTime: meshTime, meshMessageDeliveriesActive: true, meshMessageDeliveries: 2}
					scorer.peerStats[pid] = &peerStats{topics: map[string]*topicStats{topic: stats}, connected: true}
					if got := scorer.Score(pid); (got > 0) != positive {
						t.Fatalf("invalid fixture score: %v", got)
					}

					manager, err := connmgr.NewConnManager(5, 10)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = manager.Close() })
					tags := newTagTracer(manager)
					tags.Graft(pid, topic)
					p := &PubSub{
						host:   newHopwaveHosts(t, 1)[0],
						peers:  map[peer.ID]*rpcQueue{pid: newRpcQueue(1)},
						topics: map[string]map[peer.ID]struct{}{topic: {pid: {}}}, myRelays: map[string]int{topic: 1},
						peerDeadPend: map[peer.ID]struct{}{pid: {}},
						tracer:       &pubsubTracer{raw: []RawTracer{scorer, tags}},
					}
					gs := &GossipSubRouter{
						p: p, score: scorer, tagTracer: tags, tracer: p.tracer, meshFrozen: frozen,
						mesh:             map[string]map[peer.ID]struct{}{topic: {pid: {}}},
						frozenMeshActive: map[string]map[peer.ID]bool{topic: {pid: true}},
						peers:            map[peer.ID]protocol.ID{pid: GossipSubID_v11},
					}
					p.rt = gs
					if path == "dead-peer" {
						p.handleDeadPeers()
					} else {
						gs.RemovePeer(pid)
					}

					retained := scorer.peerStats[pid]
					if positive {
						if retained != nil {
							t.Errorf("positive disconnected peer acquired retained history: score=%v P3b=%v", scorer.Score(pid), stats.meshFailurePenalty)
						}
					} else if retained == nil || retained.connected || stats.inMesh || stats.meshFailurePenalty != 9 {
						t.Errorf("nonpositive disconnect must retain one mesh failure: retained=%+v topic=%+v", retained, stats)
					}
					if frozen {
						if manager.IsProtected(pid, topicTag(topic)) || gs.frozenMeshActive[topic][pid] {
							t.Error("disconnected pinned peer retained active connection protection")
						}
						if _, ok := gs.mesh[topic][pid]; !ok {
							t.Error("disconnect removed pinned logical membership")
						}
					}
				})
			}
		}
	}
}

// A pinned identity may reconnect with a different protocol. Only a negotiated
// mesh-capable protocol can restore its active mesh accounting; FloodSub peers
// still count once as ordinary subscribers and must not acquire mesh scores.
func TestKPLMeshFreezeReconnectRequiresMeshProtocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		proto  protocol.ID
		active bool
	}{
		{"gossipsub", GossipSubID_v11, true},
		{"floodsub", FloodSubID, false},
		{"custom-with-mesh", protocol.ID("/custom-mesh/1.0.0"), true},
		{"custom-without-mesh", protocol.ID("/custom-no-mesh/1.0.0"), false},
	} {
		for _, order := range []string{"protocol-first", "subscription-first"} {
			t.Run(tc.name+"/"+order, func(t *testing.T) {
				topic, pid := "reconnected-topic", peer.ID("pinned-peer")
				h := newHopwaveHosts(t, 1)[0]
				scorer := newPeerScore(&PeerScoreParams{
					Topics:           map[string]*TopicScoreParams{topic: {TopicWeight: 1, TimeInMeshQuantum: time.Second}},
					AppSpecificScore: func(peer.ID) float64 { return 0 },
				})
				scorer.host = h
				stats := &topicStats{}
				scorer.peerStats[pid] = &peerStats{topics: map[string]*topicStats{topic: stats}}
				manager, err := connmgr.NewConnManager(5, 10)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = manager.Close() })
				tags := newTagTracer(manager)
				p := &PubSub{
					host: h, peers: map[peer.ID]*rpcQueue{pid: newRpcQueue(1)},
					topics: make(map[string]map[peer.ID]struct{}), myRelays: map[string]int{topic: 1},
					tracer: &pubsubTracer{raw: []RawTracer{scorer, tags}},
				}
				gs := &GossipSubRouter{
					p: p, score: scorer, tagTracer: tags, tracer: p.tracer, meshFrozen: true,
					mesh:             map[string]map[peer.ID]struct{}{topic: {pid: {}}},
					frozenMeshActive: map[string]map[peer.ID]bool{topic: {pid: false}},
					peers:            make(map[peer.ID]protocol.ID), outbound: make(map[peer.ID]bool),
					params: DefaultGossipSubParams(),
					feature: func(f GossipSubFeature, proto protocol.ID) bool {
						return (f == GossipSubFeatureMesh && proto == "/custom-mesh/1.0.0") || GossipSubDefaultFeatures(f, proto)
					},
				}
				p.rt = gs
				subscribe := func() {
					yes := true
					p.handleIncomingRPC(&RPC{from: pid, RPC: pb.RPC{
						Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic, Subscribe: &yes}},
					}})
				}
				if order == "protocol-first" {
					gs.AddPeer(pid, tc.proto)
					subscribe()
				} else {
					subscribe()
					gs.AddPeer(pid, tc.proto)
				}
				if active := gs.meshPeerActive(topic, pid); active != tc.active {
					t.Errorf("active=%v for protocol %s, want %v", active, tc.proto, tc.active)
				}
				if gs.frozenMeshActive[topic][pid] != tc.active || stats.inMesh != tc.active || manager.IsProtected(pid, topicTag(topic)) != tc.active {
					t.Errorf("wrong restored mesh accounting: active=%v scoreInMesh=%v protected=%v", gs.frozenMeshActive[topic][pid], stats.inMesh, manager.IsProtected(pid, topicTag(topic)))
				}
				if gs.EnoughPeers(topic, 2) {
					t.Error("one subscribed peer was counted twice toward the requested peer count")
				}
				if _, pinned := gs.mesh[topic][pid]; !pinned {
					t.Error("protocol negotiation changed frozen logical membership")
				}
			})
		}
	}
}
