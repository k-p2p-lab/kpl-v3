package peer

import (
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Use the actual mesh activation flag and sticky P3b counter. Deriving them
// from TimeInMesh or the capped total is incorrect after prune/disconnection.
func weightedPeerScore(config model.PeerScoreConfig, snapshot *pubsub.PeerScoreSnapshot) (map[string]float64, bool) {
	if snapshot == nil {
		return nil, false
	}
	values := make(map[string]float64, len(model.ScoreComponentKeys))
	for _, key := range model.ScoreComponentKeys {
		values[key] = 0
	}
	for topic, observed := range snapshot.Topics {
		params, ok := config.Topics[topic]
		if !ok || observed == nil {
			continue
		}
		quantum, _ := time.ParseDuration(params.TimeInMeshQuantum)
		if observed.InMesh && quantum > 0 {
			values["p1"] += min(float64(observed.TimeInMesh/quantum), params.TimeInMeshCap) * params.TimeInMeshWeight * params.TopicWeight
		}
		values["p2"] += observed.FirstMessageDeliveries * params.FirstMessageDeliveriesWeight * params.TopicWeight
		if observed.MeshMessageDeliveriesActive && observed.MeshMessageDeliveries < params.MeshMessageDeliveriesThreshold {
			deficit := params.MeshMessageDeliveriesThreshold - observed.MeshMessageDeliveries
			values["p3"] += deficit * deficit * params.MeshMessageDeliveriesWeight * params.TopicWeight
		}
		values["p3b"] += observed.MeshFailurePenalty * params.MeshFailurePenaltyWeight * params.TopicWeight
		values["p4"] += observed.InvalidMessageDeliveries * observed.InvalidMessageDeliveries * params.InvalidMessageDeliveriesWeight * params.TopicWeight
	}
	topicSum := values["p1"] + values["p2"] + values["p3"] + values["p3b"] + values["p4"]
	if config.TopicScoreCap > 0 && topicSum > config.TopicScoreCap {
		values["topicCap"] = config.TopicScoreCap - topicSum
	}
	values["p5"] = snapshot.AppSpecificScore * config.AppSpecificWeight
	// IPColocationFactor already includes the threshold and quadratic penalty.
	values["p6"] = snapshot.IPColocationFactor * config.IPColocationFactorWeight
	if snapshot.BehaviourPenalty > config.BehaviourPenaltyThreshold {
		excess := snapshot.BehaviourPenalty - config.BehaviourPenaltyThreshold
		values["p7"] = excess * excess * config.BehaviourPenaltyWeight
	}
	values["total"] = snapshot.Score
	for _, value := range values {
		if !model.FiniteScore(value) {
			return nil, false
		}
	}
	return values, true
}

func (s *Server) recordDetailedPeerScores(sampledAt time.Time, scores map[peer.ID]*pubsub.PeerScoreSnapshot) {
	config := s.config.NodeConfig.GossipSub
	if config.Score == nil {
		return
	}
	interval, err := time.ParseDuration(config.ScoreInspectInterval)
	if err != nil || interval <= 0 {
		return
	}
	sample := &model.PeerScoreSample{ObservedAt: s.telemetry.adjustTimestamp(sampledAt), IntervalSeconds: interval.Seconds(), Components: make(map[string]model.ScoreStatistic)}
	for _, key := range model.ScoreComponentKeys {
		sample.Components[key] = model.ScoreStatistic{}
	}
	totals := make(map[string]float64, len(scores))
	for id, score := range scores {
		if score != nil && model.FiniteScore(score.Score) {
			totals[id.String()] = score.Score
		}
		values, valid := weightedPeerScore(*config.Score, score)
		if !valid {
			continue
		}
		for key, value := range values {
			statistic := sample.Components[key]
			statistic.Add(value)
			sample.Components[key] = statistic
		}
	}
	s.scoreMu.Lock()
	defer s.scoreMu.Unlock()
	if !sampledAt.After(s.scoreObservedAt) {
		return
	}
	// Keep the measured Controller time even when its offset moves backwards.
	// The Agent uses this process-local inspection sequence for ordering, so a
	// clock correction cannot retain an obsolete score (or hide an empty one).
	sample.Sequence = 1
	if s.scoreSample != nil {
		sample.Sequence = s.scoreSample.Sequence + 1
	}
	s.scoreObservedAt, s.peerScores, s.scoreSample = sampledAt, totals, sample
	// Emit only one bounded event per observer per inspection. The full matrix
	// would scale with every observer-peer pair and distort busy experiments.
	s.telemetry.emitObserved(func(controllerClockReading) (model.TraceEvent, bool) {
		return model.TraceEvent{Type: "peer_score", ScoreSample: sample}, true
	})
}
