package peer

import (
	"io"
	"log/slog"
	"math"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestDetailedPeerScoreWeightsPrunedActivationAndCap(t *testing.T) {
	config := model.PeerScoreConfig{AppSpecificWeight: 4, IPColocationFactorWeight: -2, BehaviourPenaltyWeight: -3, BehaviourPenaltyThreshold: 1, Topics: map[string]model.TopicScoreConfig{
		"a": {TopicWeight: 2, TimeInMeshQuantum: "1s", TimeInMeshWeight: .5, TimeInMeshCap: 3, FirstMessageDeliveriesWeight: 2, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesThreshold: 5, MeshFailurePenaltyWeight: -2, InvalidMessageDeliveriesWeight: -3},
		"b": {TopicWeight: 1, TimeInMeshQuantum: "1s", TimeInMeshWeight: 1, TimeInMeshCap: 10, MeshMessageDeliveriesWeight: -1, MeshMessageDeliveriesThreshold: 3, MeshFailurePenaltyWeight: -1},
	}}
	snapshot := &pubsub.PeerScoreSnapshot{Score: -78, AppSpecificScore: 2, IPColocationFactor: 9, BehaviourPenalty: 3, Topics: map[string]*pubsub.TopicScoreSnapshot{
		"a": {InMesh: true, TimeInMesh: 5500 * time.Millisecond, FirstMessageDeliveries: 4, MeshMessageDeliveriesActive: true, MeshMessageDeliveries: 2, MeshFailurePenalty: 7, InvalidMessageDeliveries: 2},
		// A pruned peer can still have active P3. TimeInMesh alone cannot tell this.
		"b": {InMesh: false, MeshMessageDeliveriesActive: true, MeshMessageDeliveries: 1, MeshFailurePenalty: 1},
	}}
	got, ok := weightedPeerScore(config, snapshot)
	want := map[string]float64{"p1": 3, "p2": 16, "p3": -22, "p3b": -29, "p4": -24, "p5": 8, "p6": -18, "p7": -12, "topicCap": 0, "total": -78}
	if !ok {
		t.Fatal("valid snapshot rejected")
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s=%v want=%v", key, got[key], value)
		}
	}
	config.TopicScoreCap = 12
	config.Topics = map[string]model.TopicScoreConfig{"a": {TopicWeight: 1, TimeInMeshQuantum: "1s", TimeInMeshWeight: 1, TimeInMeshCap: 100, FirstMessageDeliveriesWeight: 2}}
	snapshot = &pubsub.PeerScoreSnapshot{Score: 20, AppSpecificScore: 2, Topics: map[string]*pubsub.TopicScoreSnapshot{"a": {InMesh: true, TimeInMesh: 10 * time.Second, FirstMessageDeliveries: 5}}}
	got, ok = weightedPeerScore(config, snapshot)
	if !ok || got["topicCap"] != -8 {
		t.Fatalf("cap: %v", got)
	}
	sum := 0.0
	for key, value := range got {
		if key != "total" {
			sum += value
		}
	}
	if sum != got["total"] {
		t.Fatalf("components do not reconcile: %v", got)
	}
	snapshot.Score = math.Inf(1)
	if _, ok := weightedPeerScore(config, snapshot); ok {
		t.Fatal("non-finite score accepted")
	}
}

func TestDetailedPeerScoresBoundedEventsAndOrderedEmptySnapshots(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &Server{config: model.PeerProcessConfig{NodeConfig: model.NodeConfig{GossipSub: model.GossipSubConfig{Score: &model.PeerScoreConfig{AppSpecificWeight: 1}, ScoreInspectInterval: "1s"}}}, logger: logger, telemetry: newTelemetry(model.Node{ID: "observer", RunID: "run"}, "", "", logger)}
	now := time.Now()
	scores := make(map[corepeer.ID]*pubsub.PeerScoreSnapshot)
	for i := 0; i < 500; i++ {
		scores[corepeer.ID(string(rune(i+1)))] = &pubsub.PeerScoreSnapshot{Score: 2, AppSpecificScore: 2}
	}
	s.recordDetailedPeerScores(now, scores)
	if len(s.telemetry.events) != 1 || len(s.peerScores) != 500 || s.scoreSample.Components["p5"].Count != 500 || s.scoreSample.Components["p5"].Mean != 2 {
		t.Fatal("unbounded or incorrect component measurement")
	}
	first := s.scoreSample
	s.recordDetailedPeerScores(now.Add(time.Second), nil)
	s.recordDetailedPeerScores(now, scores)
	if len(s.peerScores) != 0 || s.scoreSample.Components["total"].Count != 0 || len(s.telemetry.events) != 2 {
		t.Fatal("older callback restored cleared scores")
	}
	if first.Components["p5"].Count != 500 {
		t.Fatal("published sample mutated")
	}
	event := <-s.telemetry.events
	if event.Type != "peer_score" || event.ScoreSample == nil || !event.ScoreSample.Valid() {
		t.Fatal("component event not persisted")
	}
}
