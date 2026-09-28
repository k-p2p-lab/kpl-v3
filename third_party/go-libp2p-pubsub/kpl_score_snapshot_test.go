package pubsub

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"testing"
	"time"
)

func TestKPLTimedInspectionPreservesInternalPenaltyState(t *testing.T) {
	params := &PeerScoreParams{Topics: map[string]*TopicScoreParams{"t": {TopicWeight: 1, TimeInMeshQuantum: time.Second, MeshMessageDeliveriesThreshold: 5, MeshMessageDeliveriesWeight: -1, MeshFailurePenaltyWeight: -2}}, AppSpecificScore: func(peer.ID) float64 { return 0 }}
	scorer := newPeerScore(params)
	scorer.peerStats[peer.ID("p")] = &peerStats{topics: map[string]*topicStats{"t": {inMesh: false, meshMessageDeliveriesActive: true, meshMessageDeliveries: 2, meshFailurePenalty: 7}}}
	type result struct {
		at     time.Time
		scores map[peer.ID]*PeerScoreSnapshot
	}
	results := make(chan result, 2)
	scorer.inspectTimed = func(at time.Time, scores map[peer.ID]*PeerScoreSnapshot) { results <- result{at, scores} }
	scorer.inspectScores()
	first := <-results
	topic := first.scores[peer.ID("p")].Topics["t"]
	if first.at.IsZero() || topic.InMesh || !topic.MeshMessageDeliveriesActive || topic.MeshFailurePenalty != 7 || first.scores[peer.ID("p")].Score != -23 {
		t.Fatalf("lost retained penalties: %+v", first)
	}
	delete(scorer.peerStats, peer.ID("p"))
	scorer.inspectScores()
	empty := <-results
	if !empty.at.After(first.at) || len(empty.scores) != 0 {
		t.Fatal("empty snapshot lost order")
	}
}
