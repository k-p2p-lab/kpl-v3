package peer

import (
	"fmt"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"time"
)

func gossipSubOptions(config model.GossipSubConfig, tracer pubsub.EventTracer) ([]pubsub.Option, error) {
	options, err := gossipExtendedOptions(config)
	if err != nil {
		return nil, err
	}
	options = append(options, pubsub.WithEventTracer(tracer))
	if config.PeerOutboundQueueSize != nil {
		options = append(options, pubsub.WithPeerOutboundQueueSize(*config.PeerOutboundQueueSize))
	}
	if config.MaxMessageSize != nil {
		options = append(options, pubsub.WithMaxMessageSize(*config.MaxMessageSize))
	}
	if config.ValidateQueueSize != nil {
		options = append(options, pubsub.WithValidateQueueSize(*config.ValidateQueueSize))
	}
	if config.ValidateThrottle != nil {
		options = append(options, pubsub.WithValidateThrottle(*config.ValidateThrottle))
	}
	if config.ValidateWorkers != nil {
		options = append(options, pubsub.WithValidateWorkers(*config.ValidateWorkers))
	}
	if config.SeenMessagesTTL != "" {
		ttl, err := time.ParseDuration(config.SeenMessagesTTL)
		if err != nil {
			return nil, fmt.Errorf("parse seen messages TTL: %w", err)
		}
		options = append(options, pubsub.WithSeenMessagesTTL(ttl))
	}
	switch config.SignaturePolicy {
	case "strict-sign":
		options = append(options, pubsub.WithMessageSignaturePolicy(pubsub.StrictSign))
	case "strict-no-sign":
		options = append(options, pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign))
	case "lax-sign":
		options = append(options, pubsub.WithMessageSignaturePolicy(pubsub.LaxSign))
	case "lax-no-sign":
		options = append(options, pubsub.WithMessageSignaturePolicy(pubsub.LaxNoSign))
	}
	if config.Router != "gossipsub" {
		return options, nil
	}
	if config.FloodPublish != nil {
		options = append(options, pubsub.WithFloodPublish(*config.FloodPublish))
	}
	if config.PeerExchange != nil {
		options = append(options, pubsub.WithPeerExchange(*config.PeerExchange))
	}
	params := pubsub.DefaultGossipSubParams()
	p := config.Params
	params.D = *p.D
	params.Dlo = *p.DLow
	params.Dhi = *p.DHigh
	params.Dscore = *p.DScore
	params.Dout = *p.DOut
	params.Dlazy = *p.DLazy
	params.HistoryLength = *p.HistoryLength
	params.HistoryGossip = *p.HistoryGossip
	params.GossipFactor = *p.GossipFactor
	params.GossipRetransmission = *p.GossipRetransmission
	params.SlowHeartbeatWarning = *p.SlowHeartbeatWarning
	params.PrunePeers = *p.PrunePeers
	params.Connectors = *p.Connectors
	params.MaxPendingConnections = *p.MaxPendingConnections
	params.DirectConnectTicks = *p.DirectConnectTicks
	params.OpportunisticGraftTicks = *p.OpportunisticGraftTicks
	params.OpportunisticGraftPeers = *p.OpportunisticGraftPeers
	params.MaxIHaveLength = *p.MaxIHaveLength
	params.MaxIHaveMessages = *p.MaxIHaveMessages
	params.MaxIDontWantLength = *p.MaxIDontWantLength
	params.MaxIDontWantMessages = *p.MaxIDontWantMessages
	params.IDontWantMessageThreshold = *p.IDontWantMessageThreshold
	params.IDontWantMessageTTL = *p.IDontWantMessageTTL

	durations := []struct {
		name   string
		value  string
		target *time.Duration
	}{
		{"heartbeatInitialDelay", p.HeartbeatInitialDelay, &params.HeartbeatInitialDelay},
		{"heartbeatInterval", p.HeartbeatInterval, &params.HeartbeatInterval},
		{"fanoutTTL", p.FanoutTTL, &params.FanoutTTL},
		{"pruneBackoff", p.PruneBackoff, &params.PruneBackoff},
		{"unsubscribeBackoff", p.UnsubscribeBackoff, &params.UnsubscribeBackoff},
		{"connectionTimeout", p.ConnectionTimeout, &params.ConnectionTimeout},
		{"directConnectInitialDelay", p.DirectConnectInitialDelay, &params.DirectConnectInitialDelay},
		{"graftFloodThreshold", p.GraftFloodThreshold, &params.GraftFloodThreshold},
		{"iWantFollowupTime", p.IWantFollowupTime, &params.IWantFollowupTime},
	}
	for _, item := range durations {
		value, err := time.ParseDuration(item.value)
		if err != nil {
			return nil, fmt.Errorf("parse gossipsub %s: %w", item.name, err)
		}
		*item.target = value
	}

	options = append(options, pubsub.WithGossipSubParams(params))
	if config.Score != nil && config.Score.IsEnabled() {
		score, thresholds, err := peerScoreOptions(*config.Score)
		if err != nil {
			return nil, err
		}
		options = append(options, pubsub.WithPeerScore(score, thresholds))
	}
	return options, nil
}
