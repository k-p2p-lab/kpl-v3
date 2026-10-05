package pubsub

import (
	"reflect"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// This test and its fixture use only v0.13.1 fields and APIs, so they can also
// run unchanged against pristine upstream to check observation-only parity.
func TestKPLScoreAuditLegacyInspectionParity(t *testing.T) {
	for _, mode := range []string{"disabled", "simple", "extended"} {
		t.Run(mode, func(t *testing.T) {
			scorer := newScoreAuditScorer()
			results := make(chan float64, 1)
			switch mode {
			case "simple":
				scorer.inspect = func(scores map[peer.ID]float64) { results <- scores["p"] }
			case "extended":
				scorer.inspectEx = func(scores map[peer.ID]*PeerScoreSnapshot) { results <- scores["p"].Score }
			}
			check := func(want float64) {
				t.Helper()
				if got := scorer.Score("p"); got != want {
					t.Fatalf("score=%v want=%v", got, want)
				}
				scorer.inspectScores()
				if mode != "disabled" {
					select {
					case got := <-results:
						if got != want {
							t.Fatalf("inspected score=%v want=%v", got, want)
						}
					case <-time.After(time.Second):
						t.Fatal("inspector did not return")
					}
				}
				if got := scorer.Score("p"); got != want {
					t.Fatalf("inspection changed score: got=%v want=%v", got, want)
				}
			}
			check(-78) // Includes every weighted component and retained P3/P3b.
			scorer.Prune("p", "a")
			check(-117) // Lose P1 and retain one additional delivery deficit.
			scorer.RemovePeer("p")
			check(-133) // Retained disconnect state additionally resets P2.
			if got := scorer.peerStats["p"].topics["a"].meshFailurePenalty; got != 16 {
				t.Fatalf("inspection/lifecycle duplicated the sticky penalty: %v", got)
			}
			// Cap only the topic subtotal; application, IP, and behavior terms
			// must still be applied afterward.
			scorer.params.TopicScoreCap = 12
			for _, params := range scorer.params.Topics {
				params.MeshMessageDeliveriesWeight = 0
				params.MeshFailurePenaltyWeight = 0
				params.InvalidMessageDeliveriesWeight = 0
			}
			scorer.peerStats["p"].topics["a"].inMesh = true
			scorer.peerStats["p"].topics["a"].firstMessageDeliveries = 4
			check(-10) // min(3 + 16, 12) + 8 - 18 - 12.
		})
	}
}

func newScoreAuditScorer() *peerScore {
	params := &PeerScoreParams{
		Topics: map[string]*TopicScoreParams{
			"a": {TopicWeight: 2, TimeInMeshQuantum: time.Second, TimeInMeshWeight: .5, TimeInMeshCap: 3,
				FirstMessageDeliveriesWeight: 2, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesThreshold: 5,
				MeshFailurePenaltyWeight: -2, InvalidMessageDeliveriesWeight: -3},
			"b": {TopicWeight: 1, TimeInMeshQuantum: time.Second, TimeInMeshWeight: 1, TimeInMeshCap: 10,
				MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesThreshold: 3, MeshFailurePenaltyWeight: -1},
		},
		AppSpecificScore: func(peer.ID) float64 { return 2 }, AppSpecificWeight: 4,
		IPColocationFactorWeight: -2, IPColocationFactorThreshold: 1,
		BehaviourPenaltyWeight: -3, BehaviourPenaltyThreshold: 1, RetainScore: time.Hour,
	}
	scorer := newPeerScore(params)
	scorer.peerStats["p"] = &peerStats{
		connected: true, ips: []string{"192.0.2.1"}, behaviourPenalty: 3,
		topics: map[string]*topicStats{
			"a": {inMesh: true, meshTime: 5500 * time.Millisecond, firstMessageDeliveries: 4,
				meshMessageDeliveriesActive: true, meshMessageDeliveries: 2, meshFailurePenalty: 7, invalidMessageDeliveries: 2},
			"b": {meshMessageDeliveriesActive: true, meshMessageDeliveries: 1, meshFailurePenalty: 1},
		},
	}
	scorer.peerIPs["192.0.2.1"] = map[peer.ID]struct{}{"p": {}, "q": {}, "r": {}, "s": {}}
	return scorer
}

func TestKPLScoreAuditTimedSnapshotOwnershipAndAsyncCallback(t *testing.T) {
	scorer := newScoreAuditScorer()
	type observation struct {
		at     time.Time
		scores map[peer.ID]*PeerScoreSnapshot
	}
	results := make(chan observation, 2)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	scorer.inspectTimed = func(at time.Time, scores map[peer.ID]*PeerScoreSnapshot) {
		// A callback must be able to re-enter the scorer without its lock being
		// held, and blocking this callback must not stop later observations.
		_ = scorer.Score("p")
		if scores["p"].Topics["a"].MeshFailurePenalty == 7 {
			started <- struct{}{}
			<-release
		}
		results <- observation{at, scores}
	}
	returned := make(chan struct{})
	before := time.Now()
	go func() { scorer.inspectScores(); close(returned) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback could not re-enter scorer")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("slow callback blocked score inspection")
	}
	scorer.Lock()
	scorer.peerStats["p"].topics["a"].meshFailurePenalty = 8
	scorer.Unlock()
	scorer.inspectScores()
	var second observation
	select {
	case second = <-results:
	case <-time.After(time.Second):
		t.Fatal("blocked callback prevented later inspection")
	}
	if second.at.Before(before) || second.at.After(time.Now()) || second.scores["p"].Score != -82 {
		t.Fatalf("timed snapshot changed sampling or score: %+v", second)
	}
	second.scores["p"].Topics["a"].MeshFailurePenalty = 999
	delete(second.scores["p"].Topics, "b")
	if got := scorer.Score("p"); got != -82 {
		t.Fatalf("callback mutation changed scorer state: %v", got)
	}
	// Release the older callback without closing the channel twice at cleanup.
	release <- struct{}{}
	select {
	case first := <-results:
		if !second.at.After(first.at) || first.scores["p"].Topics["a"].MeshFailurePenalty != 7 || len(first.scores["p"].Topics) != 2 {
			t.Fatalf("snapshot aliasing or unordered sampling timestamps: first=%+v second=%+v", first, second)
		}
	case <-time.After(time.Second):
		t.Fatal("older callback did not complete")
	}
}

func TestKPLScoreAuditInspectorOptionsDoNotActivateOrReplaceScoring(t *testing.T) {
	callbacks := []any{
		PeerScoreInspectFn(func(map[peer.ID]float64) {}),
		ExtendedPeerScoreInspectFn(func(map[peer.ID]*PeerScoreSnapshot) {}),
		TimedPeerScoreInspectFn(func(time.Time, map[peer.ID]*PeerScoreSnapshot) {}),
	}
	for _, first := range callbacks {
		p := &PubSub{rt: &GossipSubRouter{}}
		if err := WithPeerScoreInspect(first, time.Second)(p); err == nil {
			t.Fatal("inspection activated disabled scoring")
		}
		for _, duplicate := range callbacks {
			scorer := newScoreAuditScorer()
			p.rt = &GossipSubRouter{score: scorer}
			if err := WithPeerScoreInspect(first, time.Second)(p); err != nil {
				t.Fatal(err)
			}
			before := []uintptr{reflect.ValueOf(scorer.inspect).Pointer(), reflect.ValueOf(scorer.inspectEx).Pointer(), reflect.ValueOf(scorer.inspectTimed).Pointer()}
			if err := WithPeerScoreInspect(duplicate, 2*time.Second)(p); err == nil {
				t.Fatal("duplicate inspector accepted")
			}
			after := []uintptr{reflect.ValueOf(scorer.inspect).Pointer(), reflect.ValueOf(scorer.inspectEx).Pointer(), reflect.ValueOf(scorer.inspectTimed).Pointer()}
			if !reflect.DeepEqual(before, after) || scorer.inspectPeriod != time.Second || scorer.Score("p") != -78 {
				t.Fatal("rejected inspector mutated installed callback, period, or score")
			}
		}
	}
}
