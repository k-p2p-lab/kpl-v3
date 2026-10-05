package model

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// NodeConfig is the complete, per-node protocol configuration. The flat fields
// are retained for v3 preview scenario compatibility; new scenarios should use
// the nested libp2p, kademlia, and gossipsub sections.
type NodeConfig struct {
	Type         string          `json:"type,omitempty" yaml:"type,omitempty"`
	Topics       []string        `json:"topics,omitempty" yaml:"topics,omitempty"`
	D            int             `json:"d,omitempty" yaml:"d,omitempty"`
	DLow         int             `json:"dLow,omitempty" yaml:"dLow,omitempty"`
	DHigh        int             `json:"dHigh,omitempty" yaml:"dHigh,omitempty"`
	DOut         int             `json:"dOut,omitempty" yaml:"dOut,omitempty"`
	DLazy        int             `json:"dLazy,omitempty" yaml:"dLazy,omitempty"`
	Heartbeat    string          `json:"heartbeat,omitempty" yaml:"heartbeat,omitempty"`
	ConnectLimit int             `json:"connectLimit,omitempty" yaml:"connectLimit,omitempty"`
	Libp2p       Libp2pConfig    `json:"libp2p,omitempty" yaml:"libp2p,omitempty"`
	Kademlia     KademliaConfig  `json:"kademlia,omitempty" yaml:"kademlia,omitempty"`
	GossipSub    GossipSubConfig `json:"gossipsub,omitempty" yaml:"gossipsub,omitempty"`
	Network      NetworkConfig   `json:"network,omitempty" yaml:"network,omitempty"`
}

type Libp2pConfig struct {
	UserAgent         string                   `json:"userAgent,omitempty" yaml:"userAgent,omitempty"`
	NATPortMap        *bool                    `json:"natPortMap,omitempty" yaml:"natPortMap,omitempty"`
	Relay             *bool                    `json:"relay,omitempty" yaml:"relay,omitempty"`
	RelayService      *bool                    `json:"relayService,omitempty" yaml:"relayService,omitempty"`
	ConnectionLimit   *int                     `json:"connectionLimit,omitempty" yaml:"connectionLimit,omitempty"`
	ConnectionManager *ConnectionManagerConfig `json:"connectionManager,omitempty" yaml:"connectionManager,omitempty"`
	DialTimeout       string                   `json:"dialTimeout,omitempty" yaml:"dialTimeout,omitempty"`
}

type ConnectionManagerConfig struct {
	LowWater    int    `json:"lowWater" yaml:"lowWater"`
	HighWater   int    `json:"highWater" yaml:"highWater"`
	GracePeriod string `json:"gracePeriod" yaml:"gracePeriod"`
}

func boolPointer(value bool) *bool { return &value }
func intPointer(value int) *int    { return &value }

func applyV2WorkerGossipDefaults(config *NodeConfig) {
	config.GossipSub.Params.DLow = intPointer(5)
	config.GossipSub.Params.DScore = intPointer(3)
	config.GossipSub.Params.MaxIHaveLength = intPointer(5500)
	config.GossipSub.Params.HeartbeatInitialDelay = "1s"
}

// BuiltInNodeConfig returns useful behavior presets. Any preset can be further
// overridden by a scenario profile or an inline node block.
func BuiltInNodeConfig(kind string) (NodeConfig, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	config := NodeConfig{Type: kind}
	switch kind {
	case "boot":
		config.Kademlia.Mode = "server"
		config.GossipSub.Enabled = boolPointer(false)
		config.GossipSub.Params.DScore = intPointer(3)
	case "full", "worker":
		config.Type = "full"
		config.Kademlia.Mode = "server"
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
	case "light":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(32)
		applyV2WorkerGossipDefaults(&config)
	case "publisher":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
		config.GossipSub.Subscribe = boolPointer(false)
		config.GossipSub.TopicMode = "publish"
	case "subscriber", "observer":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
		config.GossipSub.AllowPublish = boolPointer(false)
	case "relay":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
		config.GossipSub.TopicMode = "relay"
		config.GossipSub.AllowPublish = boolPointer(false)
	case "flood":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(55)
		config.GossipSub.Router = "floodsub"
	case "random":
		config.Kademlia.Mode = "client"
		config.Libp2p.ConnectionLimit = intPointer(55)
		config.GossipSub.Router = "randomsub"
		config.GossipSub.RandomDegree = intPointer(6)
		config.GossipSub.RandomNetworkSize = intPointer(100)
	case "dht-only":
		config.Kademlia.Mode = "server"
		config.Libp2p.ConnectionLimit = intPointer(55)
		config.GossipSub.Enabled = boolPointer(false)
	case "gossip-only":
		config.Kademlia.Enabled = boolPointer(false)
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
	case "non-gossip", "mesh-only":
		config.Kademlia.Mode = "server"
		config.Libp2p.ConnectionLimit = intPointer(55)
		applyV2WorkerGossipDefaults(&config)
		zeroInt := 0
		zeroFloat := 0.0
		config.GossipSub.Params.HistoryGossip = &zeroInt
		config.GossipSub.Params.GossipFactor = &zeroFloat
	default:
		return NodeConfig{}, false
	}
	return config.WithDefaults(), true
}

// Merge overlays explicitly set values on top of a base configuration.
func (c NodeConfig) Merge(overlay NodeConfig) NodeConfig {
	overlay.promoteLegacy()
	if overlay.GossipSub.Subscribe != nil && overlay.GossipSub.TopicMode == "" {
		if *overlay.GossipSub.Subscribe {
			overlay.GossipSub.TopicMode = "subscribe"
		} else {
			overlay.GossipSub.TopicMode = "publish"
		}
	}
	if overlay.Kademlia.ProtocolID != "" && overlay.Kademlia.ProtocolPrefix == "" {
		c.Kademlia.ProtocolPrefix = ""
		if overlay.Kademlia.ProtocolExtension == "" {
			c.Kademlia.ProtocolExtension = ""
		}
	}
	if overlay.Kademlia.ProtocolPrefix != "" && overlay.Kademlia.ProtocolID == "" {
		c.Kademlia.ProtocolID = ""
	}
	if (overlay.Kademlia.BootstrapSource == "none" || overlay.Kademlia.BootstrapSource == "controller") && overlay.Kademlia.BootstrapPeers == nil {
		c.Kademlia.BootstrapPeers = nil
	}
	// Switching network delay/shaper modes replaces the inherited alternative.
	// Explicitly supplying both in one overlay remains invalid during Validate.
	if overlay.Network.Delay != "" && overlay.Network.DelayDistribution == nil {
		c.Network.DelayDistribution = nil
	}
	if overlay.Network.DelayDistribution != nil && overlay.Network.Delay == "" {
		c.Network.Delay = ""
	}
	if overlay.Network.TBF != nil && overlay.Network.RateMbps == nil {
		c.Network.RateMbps = nil
	}
	if overlay.Network.RateMbps != nil && overlay.Network.TBF == nil {
		c.Network.TBF = nil
	}
	base := reflect.ValueOf(&c).Elem()
	mergeNonZero(base, reflect.ValueOf(overlay))
	return c
}

func mergeNonZero(dst, src reflect.Value) {
	for i := 0; i < src.NumField(); i++ {
		s, d := src.Field(i), dst.Field(i)
		switch s.Kind() {
		case reflect.Struct:
			mergeNonZero(d, s)
		case reflect.Pointer, reflect.Interface:
			if !s.IsNil() {
				d.Set(s)
			}
		case reflect.Slice, reflect.Map:
			if !s.IsNil() {
				d.Set(s)
			}
		default:
			if !s.IsZero() {
				d.Set(s)
			}
		}
	}
}

func (c *NodeConfig) promoteLegacy() {
	if len(c.GossipSub.Topics) == 0 && len(c.Topics) > 0 {
		c.GossipSub.Topics = append([]string(nil), c.Topics...)
	}
	p := &c.GossipSub.Params
	if c.D != 0 && p.D == nil {
		p.D = intPointer(c.D)
	}
	if c.DLow != 0 && p.DLow == nil {
		p.DLow = intPointer(c.DLow)
	}
	if c.DHigh != 0 && p.DHigh == nil {
		p.DHigh = intPointer(c.DHigh)
	}
	if c.DOut != 0 && p.DOut == nil {
		p.DOut = intPointer(c.DOut)
	}
	if c.DLazy != 0 && p.DLazy == nil {
		p.DLazy = intPointer(c.DLazy)
	}
	if c.Heartbeat != "" && p.HeartbeatInterval == "" {
		p.HeartbeatInterval = c.Heartbeat
	}
	if c.ConnectLimit != 0 && c.Libp2p.ConnectionLimit == nil {
		c.Libp2p.ConnectionLimit = intPointer(c.ConnectLimit)
	}
}

func (c NodeConfig) WithDefaults() NodeConfig {
	c.promoteLegacy()
	if c.Type == "" {
		c.Type = "full"
	}
	if c.GossipSub.Enabled == nil {
		c.GossipSub.Enabled = boolPointer(true)
	}
	if c.GossipSub.Router == "" {
		c.GossipSub.Router = "gossipsub"
	}
	if c.GossipSub.TopicMode == "" {
		if c.GossipSub.Subscribe != nil && !*c.GossipSub.Subscribe {
			c.GossipSub.TopicMode = "publish"
		} else {
			c.GossipSub.TopicMode = "subscribe"
		}
	}
	if c.GossipSub.Subscribe == nil {
		c.GossipSub.Subscribe = boolPointer(true)
	}
	c.GossipSub.Subscribe = boolPointer(c.GossipSub.TopicMode == "subscribe")
	if c.GossipSub.AllowPublish == nil {
		c.GossipSub.AllowPublish = boolPointer(true)
	}
	if c.GossipSub.Score != nil {
		score := c.GossipSub.Score.WithDefaults()
		c.GossipSub.Score = &score
	}
	if c.GossipSub.Score != nil && c.GossipSub.Score.IsEnabled() && c.GossipSub.ScoreInspectInterval == "" {
		c.GossipSub.ScoreInspectInterval = "1s"
	}
	if len(c.GossipSub.Topics) == 0 {
		c.GossipSub.Topics = []string{"kpl/default"}
	}
	if c.Kademlia.Enabled == nil {
		c.Kademlia.Enabled = boolPointer(true)
	}
	if c.Kademlia.Mode == "" {
		c.Kademlia.Mode = "server"
	}
	if c.Kademlia.ProtocolPrefix == "" && c.Kademlia.ProtocolID == "" {
		c.Kademlia.ProtocolPrefix = "/k-p2p-lab/v3"
	}
	if c.Kademlia.BucketSize == nil {
		c.Kademlia.BucketSize = intPointer(20)
	}
	if c.Kademlia.BootstrapTimeout == "" {
		c.Kademlia.BootstrapTimeout = "60s"
	}
	if c.Kademlia.BootstrapRetryInterval == "" {
		c.Kademlia.BootstrapRetryInterval = "1s"
	}

	// These are the v2/libp2p defaults and remain explicit in manifests.
	p := &c.GossipSub.Params
	if p.D == nil {
		p.D = intPointer(6)
	}
	if p.DLow == nil {
		p.DLow = intPointer(5)
	}
	if p.DHigh == nil {
		p.DHigh = intPointer(12)
	}
	if p.DScore == nil {
		p.DScore = intPointer(4)
	}
	if p.DOut == nil {
		p.DOut = intPointer(2)
	}
	if p.DLazy == nil {
		p.DLazy = intPointer(6)
	}
	if p.HistoryLength == nil {
		p.HistoryLength = intPointer(5)
	}
	if p.HistoryGossip == nil {
		p.HistoryGossip = intPointer(3)
	}
	if p.GossipFactor == nil {
		value := 0.25
		p.GossipFactor = &value
	}
	if p.GossipRetransmission == nil {
		p.GossipRetransmission = intPointer(3)
	}
	if p.HeartbeatInitialDelay == "" {
		p.HeartbeatInitialDelay = "100ms"
	}
	if p.HeartbeatInterval == "" {
		p.HeartbeatInterval = "1s"
	}
	if p.SlowHeartbeatWarning == nil {
		value := 0.1
		p.SlowHeartbeatWarning = &value
	}
	if p.FanoutTTL == "" {
		p.FanoutTTL = "1m"
	}
	if p.PrunePeers == nil {
		p.PrunePeers = intPointer(16)
	}
	if p.PruneBackoff == "" {
		p.PruneBackoff = "1m"
	}
	if p.UnsubscribeBackoff == "" {
		p.UnsubscribeBackoff = "10s"
	}
	if p.Connectors == nil {
		p.Connectors = intPointer(8)
	}
	if p.MaxPendingConnections == nil {
		p.MaxPendingConnections = intPointer(128)
	}
	if p.ConnectionTimeout == "" {
		p.ConnectionTimeout = "30s"
	}
	if p.DirectConnectTicks == nil {
		value := uint64(300)
		p.DirectConnectTicks = &value
	}
	if p.DirectConnectInitialDelay == "" {
		p.DirectConnectInitialDelay = "1s"
	}
	if p.OpportunisticGraftTicks == nil {
		value := uint64(60)
		p.OpportunisticGraftTicks = &value
	}
	if p.OpportunisticGraftPeers == nil {
		p.OpportunisticGraftPeers = intPointer(2)
	}
	if p.GraftFloodThreshold == "" {
		p.GraftFloodThreshold = "10s"
	}
	if p.MaxIHaveLength == nil {
		p.MaxIHaveLength = intPointer(5000)
	}
	if p.MaxIHaveMessages == nil {
		p.MaxIHaveMessages = intPointer(10)
	}
	if p.MaxIDontWantLength == nil {
		p.MaxIDontWantLength = intPointer(10)
	}
	if p.MaxIDontWantMessages == nil {
		p.MaxIDontWantMessages = intPointer(1000)
	}
	if p.IWantFollowupTime == "" {
		p.IWantFollowupTime = "3s"
	}
	if p.IDontWantMessageThreshold == nil {
		p.IDontWantMessageThreshold = intPointer(1 << 10)
	}
	if p.IDontWantMessageTTL == nil {
		p.IDontWantMessageTTL = intPointer(3)
	}
	if p.HopwaveFactor == nil {
		value := float64(1)
		p.HopwaveFactor = &value
	}
	if p.HopwaveInterval == nil {
		p.HopwaveInterval = intPointer(1)
	}

	c.Topics = append([]string(nil), c.GossipSub.Topics...)
	c.D, c.DLow, c.DHigh = *p.D, *p.DLow, *p.DHigh
	c.DOut, c.DLazy, c.Heartbeat = *p.DOut, *p.DLazy, p.HeartbeatInterval
	if c.Libp2p.ConnectionLimit != nil {
		c.ConnectLimit = *c.Libp2p.ConnectionLimit
	}
	return c
}

func (c NodeConfig) Validate() error {
	c = c.WithDefaults()
	if c.GossipSub.MeshFreeze != nil && *c.GossipSub.MeshFreeze && (c.GossipSub.Enabled == nil || !*c.GossipSub.Enabled) {
		return fmt.Errorf("meshFreeze requires enabled GossipSub")
	}
	if err := c.Network.Validate(); err != nil {
		return fmt.Errorf("network: %w", err)
	}
	if c.Type == "" {
		return fmt.Errorf("node type is required")
	}
	if c.Libp2p.ConnectionLimit != nil && *c.Libp2p.ConnectionLimit < 0 {
		return fmt.Errorf("libp2p connectionLimit cannot be negative")
	}
	if c.Kademlia.Enabled != nil && *c.Kademlia.Enabled {
		if err := c.Kademlia.Validate(); err != nil {
			return fmt.Errorf("kademlia: %w", err)
		}
		if c.Kademlia.ProtocolID != "" && c.Kademlia.ProtocolPrefix != "" {
			return fmt.Errorf("kademlia protocolId and protocolPrefix are mutually exclusive")
		}
		if c.Kademlia.ProtocolID != "" && c.Kademlia.ProtocolExtension != "" {
			return fmt.Errorf("kademlia protocolExtension cannot be combined with protocolId")
		}
		switch c.Kademlia.Mode {
		case "auto", "auto-server", "client", "server":
		default:
			return fmt.Errorf("invalid kademlia mode %q", c.Kademlia.Mode)
		}
		if c.Kademlia.BucketSize != nil && *c.Kademlia.BucketSize <= 0 {
			return fmt.Errorf("kademlia bucketSize must be positive")
		}
		if c.Kademlia.Concurrency != nil && *c.Kademlia.Concurrency <= 0 {
			return fmt.Errorf("kademlia concurrency must be positive")
		}
		if c.Kademlia.Resiliency != nil && *c.Kademlia.Resiliency < 0 {
			return fmt.Errorf("kademlia resiliency cannot be negative")
		}
		if c.Kademlia.LookupCheckConcurrency != nil && *c.Kademlia.LookupCheckConcurrency <= 0 {
			return fmt.Errorf("kademlia lookupCheckConcurrency must be positive")
		}
		if c.Kademlia.OptimisticProvideJobsPoolSize != nil && *c.Kademlia.OptimisticProvideJobsPoolSize <= 0 {
			return fmt.Errorf("kademlia optimisticProvideJobsPoolSize must be positive")
		}
	}
	if c.GossipSub.Enabled != nil && *c.GossipSub.Enabled {
		if err := c.GossipSub.ValidateExtended(); err != nil {
			return fmt.Errorf("gossipsub: %w", err)
		}
		if d := c.GossipSub.Discovery; d != nil && d.Mode == "routing" {
			if c.Kademlia.Enabled == nil || !*c.Kademlia.Enabled || (c.Kademlia.DisableProviders != nil && *c.Kademlia.DisableProviders) || (c.Kademlia.ProviderStore != nil && c.Kademlia.ProviderStore.Mode == "null") {
				return fmt.Errorf("routing discovery requires enabled Kademlia providers and provider storage")
			}
		}
		if c.GossipSub.Score != nil && c.GossipSub.Score.IsEnabled() {
			for topic := range c.GossipSub.Score.Topics {
				if !pubsubContains(c.GossipSub.Topics, topic) {
					return fmt.Errorf("gossipsub score refers to unjoined topic %q", topic)
				}
			}
		}
		if len(c.GossipSub.Topics) == 0 {
			return fmt.Errorf("gossipsub topics cannot be empty")
		}
		switch c.GossipSub.Router {
		case "gossipsub", "floodsub", "randomsub":
		default:
			return fmt.Errorf("invalid pubsub router %q", c.GossipSub.Router)
		}
		switch c.GossipSub.TopicMode {
		case "subscribe", "relay", "publish":
		default:
			return fmt.Errorf("invalid topicMode %q", c.GossipSub.TopicMode)
		}
		if c.GossipSub.Router == "randomsub" {
			if c.GossipSub.RandomDegree == nil || *c.GossipSub.RandomDegree <= 0 {
				return fmt.Errorf("randomsub requires a positive randomDegree")
			}
			if c.GossipSub.RandomNetworkSize == nil || *c.GossipSub.RandomNetworkSize <= 0 {
				return fmt.Errorf("randomsub requires a positive randomNetworkSize")
			}
		}
		if c.GossipSub.Router != "gossipsub" {
			if c.GossipSub.Score != nil && c.GossipSub.Score.IsEnabled() {
				return fmt.Errorf("gossipsub score requires the gossipsub router")
			}
			if c.GossipSub.FloodPublish != nil || c.GossipSub.PeerExchange != nil {
				return fmt.Errorf("floodPublish and peerExchange require the gossipsub router")
			}
		}
		for name, value := range map[string]*int{
			"peerOutboundQueueSize": c.GossipSub.PeerOutboundQueueSize,
			"validateQueueSize":     c.GossipSub.ValidateQueueSize,
			"validateWorkers":       c.GossipSub.ValidateWorkers,
		} {
			if value != nil && *value <= 0 {
				return fmt.Errorf("gossipsub %s must be positive", name)
			}
		}
		for name, value := range map[string]*int{
			"maxMessageSize":         c.GossipSub.MaxMessageSize,
			"subscriptionBufferSize": c.GossipSub.SubscriptionBufferSize,
			"validateThrottle":       c.GossipSub.ValidateThrottle,
		} {
			if value != nil && *value < 0 {
				return fmt.Errorf("gossipsub %s cannot be negative", name)
			}
		}
		if c.GossipSub.Router == "gossipsub" {
			p := c.GossipSub.Params
			if *p.DLow > *p.D || *p.D > *p.DHigh {
				return fmt.Errorf("gossipsub degree must satisfy dLow <= d <= dHigh")
			}
			if *p.D <= 0 || *p.DLow < 0 || *p.DHigh <= 0 || *p.DScore < 0 || *p.DScore > *p.D || *p.DLazy < 0 {
				return fmt.Errorf("gossipsub degrees must be non-negative, with positive d/dHigh and dScore <= d")
			}
			if *p.DOut < 0 || *p.DOut >= *p.DLow || *p.DOut > *p.D/2 {
				return fmt.Errorf("gossipsub dOut must be < dLow and <= d/2")
			}
			if *p.HistoryLength <= 0 || *p.HistoryGossip < 0 || *p.HistoryGossip > *p.HistoryLength {
				return fmt.Errorf("gossipsub history requires 0 <= historyGossip <= historyLength")
			}
			if *p.DirectConnectTicks == 0 || *p.OpportunisticGraftTicks == 0 {
				return fmt.Errorf("gossipsub directConnectTicks and opportunisticGraftTicks must be positive")
			}
			if !validNumber(*p.GossipFactor) || *p.GossipFactor < 0 || *p.GossipFactor > 1 {
				return fmt.Errorf("gossipsub gossipFactor must be finite and in [0, 1]")
			}
			if !validNumber(*p.HopwaveFactor) || *p.HopwaveFactor < 0 || *p.HopwaveFactor > 1 {
				return fmt.Errorf("gossipsub hopwaveFactor must be finite and in [0, 1]")
			}
			if *p.HopwaveInterval < 1 || *p.HopwaveInterval > 2147483647 {
				return fmt.Errorf("gossipsub hopwaveInterval must be in [1, 2147483647]")
			}
			if !validNumber(*p.SlowHeartbeatWarning) || *p.SlowHeartbeatWarning < 0 {
				return fmt.Errorf("gossipsub slowHeartbeatWarning must be finite and non-negative")
			}
			for name, value := range map[string]int{
				"gossipRetransmission":      *p.GossipRetransmission,
				"prunePeers":                *p.PrunePeers,
				"connectors":                *p.Connectors,
				"maxPendingConnections":     *p.MaxPendingConnections,
				"opportunisticGraftPeers":   *p.OpportunisticGraftPeers,
				"maxIHaveLength":            *p.MaxIHaveLength,
				"maxIHaveMessages":          *p.MaxIHaveMessages,
				"maxIDontWantLength":        *p.MaxIDontWantLength,
				"maxIDontWantMessages":      *p.MaxIDontWantMessages,
				"iDontWantMessageThreshold": *p.IDontWantMessageThreshold,
				"iDontWantMessageTTL":       *p.IDontWantMessageTTL,
			} {
				if value < 0 {
					return fmt.Errorf("gossipsub %s cannot be negative", name)
				}
			}
		}
		switch c.GossipSub.SignaturePolicy {
		case "", "strict-sign", "strict-no-sign", "lax-sign", "lax-no-sign":
		default:
			return fmt.Errorf("invalid signaturePolicy %q", c.GossipSub.SignaturePolicy)
		}
		if c.GossipSub.Score != nil {
			if err := c.GossipSub.Score.validate(); err != nil {
				return fmt.Errorf("gossipsub score: %w", err)
			}
		}
	}
	positiveDurations := map[string]bool{
		"libp2p.dialTimeout":                  true,
		"kademlia.routingTableRefreshPeriod":  true,
		"kademlia.routingTableRefreshTimeout": true,
		"kademlia.bootstrapTimeout":           true,
		"kademlia.bootstrapRetryInterval":     true,
	}
	durations := map[string]string{
		"libp2p.dialTimeout":                    c.Libp2p.DialTimeout,
		"kademlia.routingTableLatencyTolerance": c.Kademlia.RoutingTableLatencyTolerance,
		"kademlia.routingTableRefreshPeriod":    c.Kademlia.RoutingTableRefreshPeriod,
		"kademlia.routingTableRefreshTimeout":   c.Kademlia.RoutingTableRefreshTimeout,
		"kademlia.maxRecordAge":                 c.Kademlia.MaxRecordAge,
		"kademlia.bootstrapTimeout":             c.Kademlia.BootstrapTimeout,
		"kademlia.bootstrapRetryInterval":       c.Kademlia.BootstrapRetryInterval,
		"gossipsub.seenMessagesTTL":             c.GossipSub.SeenMessagesTTL,
		"gossipsub.scoreInspectInterval":        c.GossipSub.ScoreInspectInterval,
	}
	if c.GossipSub.Enabled != nil && *c.GossipSub.Enabled && c.GossipSub.Router == "gossipsub" {
		positiveDurations["gossipsub.heartbeatInterval"] = true
		for name, value := range map[string]string{
			"gossipsub.heartbeatInitialDelay":     c.GossipSub.Params.HeartbeatInitialDelay,
			"gossipsub.heartbeatInterval":         c.GossipSub.Params.HeartbeatInterval,
			"gossipsub.fanoutTTL":                 c.GossipSub.Params.FanoutTTL,
			"gossipsub.pruneBackoff":              c.GossipSub.Params.PruneBackoff,
			"gossipsub.unsubscribeBackoff":        c.GossipSub.Params.UnsubscribeBackoff,
			"gossipsub.connectionTimeout":         c.GossipSub.Params.ConnectionTimeout,
			"gossipsub.directConnectInitialDelay": c.GossipSub.Params.DirectConnectInitialDelay,
			"gossipsub.graftFloodThreshold":       c.GossipSub.Params.GraftFloodThreshold,
			"gossipsub.iWantFollowupTime":         c.GossipSub.Params.IWantFollowupTime,
		} {
			durations[name] = value
		}
	}
	for name, value := range durations {
		if value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if duration < 0 {
				return fmt.Errorf("%s cannot be negative", name)
			}
			if duration == 0 && positiveDurations[name] {
				return fmt.Errorf("%s must be positive", name)
			}
		}
	}
	if cm := c.Libp2p.ConnectionManager; cm != nil {
		if cm.LowWater < 0 || cm.HighWater <= 0 || cm.LowWater > cm.HighWater {
			return fmt.Errorf("connectionManager requires 0 <= lowWater <= highWater")
		}
		if cm.GracePeriod != "" {
			gracePeriod, err := time.ParseDuration(cm.GracePeriod)
			if err != nil {
				return fmt.Errorf("connectionManager.gracePeriod: %w", err)
			}
			if gracePeriod < 0 {
				return fmt.Errorf("connectionManager.gracePeriod cannot be negative")
			}
		}
	}
	return nil
}

func validNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validDecay(value float64) bool {
	return validNumber(value) && value > 0 && value < 1
}

func finitePositive(value float64) bool {
	return validNumber(value) && value > 0
}

func (c NodeConfig) PublishAllowed() bool {
	c = c.WithDefaults()
	return c.GossipSub.Enabled != nil && *c.GossipSub.Enabled && c.GossipSub.AllowPublish != nil && *c.GossipSub.AllowPublish
}
