package peer

import (
	"fmt"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"net"
	"time"
)

func peerScoreOptions(config model.PeerScoreConfig) (*pubsub.PeerScoreParams, *pubsub.PeerScoreThresholds, error) {
	if !config.IsEnabled() {
		return nil, nil, fmt.Errorf("peer scoring is disabled; set score.enabled to true")
	}
	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		return nil, nil, fmt.Errorf("peer score: %w", err)
	}
	appDefault := 0.0
	appPeers := make(map[peer.ID]float64)
	if config.AppSpecificScore != nil {
		appDefault = config.AppSpecificScore.Default
		for raw, value := range config.AppSpecificScore.Peers {
			id, err := peer.Decode(raw)
			if err != nil {
				return nil, nil, fmt.Errorf("parse application score peer %q: %w", raw, err)
			}
			appPeers[id] = value
		}
	}
	whitelist := make([]*net.IPNet, 0, len(config.IPColocationFactorWhitelist))
	for _, raw := range config.IPColocationFactorWhitelist {
		_, network, err := net.ParseCIDR(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("parse peer score whitelist %q: %w", raw, err)
		}
		whitelist = append(whitelist, network)
	}
	decayInterval, err := parseOptionalDuration(config.DecayInterval)
	if err != nil {
		return nil, nil, fmt.Errorf("parse peer score decayInterval: %w", err)
	}
	retainScore, err := parseOptionalDuration(config.RetainScore)
	if err != nil {
		return nil, nil, fmt.Errorf("parse peer score retainScore: %w", err)
	}
	seenTTL, err := parseOptionalDuration(config.SeenMessageTTL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse peer score seenMessageTTL: %w", err)
	}
	topics := make(map[string]*pubsub.TopicScoreParams, len(config.Topics))
	for topic, input := range config.Topics {
		timeInMeshQuantum, err := parseOptionalDuration(input.TimeInMeshQuantum)
		if err != nil {
			return nil, nil, fmt.Errorf("parse score topic %s timeInMeshQuantum: %w", topic, err)
		}
		activation, err := parseOptionalDuration(input.MeshMessageDeliveriesActivation)
		if err != nil {
			return nil, nil, fmt.Errorf("parse score topic %s meshMessageDeliveriesActivation: %w", topic, err)
		}
		window, err := parseOptionalDuration(input.MeshMessageDeliveriesWindow)
		if err != nil {
			return nil, nil, fmt.Errorf("parse score topic %s meshMessageDeliveriesWindow: %w", topic, err)
		}
		topics[topic] = &pubsub.TopicScoreParams{
			SkipAtomicValidation: input.SkipAtomicValidation,
			TopicWeight:          input.TopicWeight,
			TimeInMeshWeight:     input.TimeInMeshWeight, TimeInMeshQuantum: timeInMeshQuantum, TimeInMeshCap: input.TimeInMeshCap,
			FirstMessageDeliveriesWeight: input.FirstMessageDeliveriesWeight, FirstMessageDeliveriesDecay: input.FirstMessageDeliveriesDecay, FirstMessageDeliveriesCap: input.FirstMessageDeliveriesCap,
			MeshMessageDeliveriesWeight: input.MeshMessageDeliveriesWeight, MeshMessageDeliveriesDecay: input.MeshMessageDeliveriesDecay,
			MeshMessageDeliveriesThreshold: input.MeshMessageDeliveriesThreshold, MeshMessageDeliveriesCap: input.MeshMessageDeliveriesCap,
			MeshMessageDeliveriesActivation: activation, MeshMessageDeliveriesWindow: window,
			MeshFailurePenaltyWeight: input.MeshFailurePenaltyWeight, MeshFailurePenaltyDecay: input.MeshFailurePenaltyDecay,
			InvalidMessageDeliveriesWeight: input.InvalidMessageDeliveriesWeight, InvalidMessageDeliveriesDecay: input.InvalidMessageDeliveriesDecay,
		}
	}
	score := &pubsub.PeerScoreParams{
		SkipAtomicValidation: config.SkipAtomicValidation,
		Topics:               topics,
		TopicScoreCap:        config.TopicScoreCap,
		AppSpecificScore: func(id peer.ID) float64 {
			if score, ok := appPeers[id]; ok {
				return score
			}
			return appDefault
		},
		AppSpecificWeight:           config.AppSpecificWeight,
		IPColocationFactorWeight:    config.IPColocationFactorWeight,
		IPColocationFactorThreshold: config.IPColocationFactorThreshold,
		IPColocationFactorWhitelist: whitelist,
		BehaviourPenaltyWeight:      config.BehaviourPenaltyWeight,
		BehaviourPenaltyThreshold:   config.BehaviourPenaltyThreshold,
		BehaviourPenaltyDecay:       config.BehaviourPenaltyDecay,
		DecayInterval:               decayInterval,
		DecayToZero:                 config.DecayToZero,
		RetainScore:                 retainScore,
		SeenMsgTTL:                  seenTTL,
	}
	t := config.Thresholds
	thresholds := &pubsub.PeerScoreThresholds{
		SkipAtomicValidation:        t.SkipAtomicValidation,
		GossipThreshold:             t.GossipThreshold,
		PublishThreshold:            t.PublishThreshold,
		GraylistThreshold:           t.GraylistThreshold,
		AcceptPXThreshold:           t.AcceptPXThreshold,
		OpportunisticGraftThreshold: t.OpportunisticGraftThreshold,
	}
	return score, thresholds, nil
}

func parseOptionalDuration(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	return time.ParseDuration(value)
}
