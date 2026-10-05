package model

type GossipSubConfig struct {
	MessageID              string                          `json:"messageId,omitempty" yaml:"messageId,omitempty"`
	TopicOptions           map[string]PubSubTopicConfig    `json:"topicOptions,omitempty" yaml:"topicOptions,omitempty"`
	PeerFilter             *PubSubPeerFilterConfig         `json:"peerFilter,omitempty" yaml:"peerFilter,omitempty"`
	DirectPeers            []string                        `json:"directPeers,omitempty" yaml:"directPeers,omitempty"`
	PeerGater              *PeerGaterConfig                `json:"peerGater,omitempty" yaml:"peerGater,omitempty"`
	SubscriptionFilter     *PubSubSubscriptionFilterConfig `json:"subscriptionFilter,omitempty" yaml:"subscriptionFilter,omitempty"`
	SeenMessagesStrategy   string                          `json:"seenMessagesStrategy,omitempty" yaml:"seenMessagesStrategy,omitempty"`
	Protocols              []PubSubProtocolConfig          `json:"protocols,omitempty" yaml:"protocols,omitempty"`
	ProtocolMatch          string                          `json:"protocolMatch,omitempty" yaml:"protocolMatch,omitempty"`
	Blacklist              *PubSubBlacklistConfig          `json:"blacklist,omitempty" yaml:"blacklist,omitempty"`
	DefaultValidators      []PubSubValidatorConfig         `json:"defaultValidators,omitempty" yaml:"defaultValidators,omitempty"`
	RPCInspector           *PubSubRPCInspectorConfig       `json:"rpcInspector,omitempty" yaml:"rpcInspector,omitempty"`
	Discovery              *PubSubDiscoveryConfig          `json:"discovery,omitempty" yaml:"discovery,omitempty"`
	Author                 *PubSubAuthorConfig             `json:"author,omitempty" yaml:"author,omitempty"`
	Publish                *PubSubPublishConfig            `json:"publish,omitempty" yaml:"publish,omitempty"`
	Enabled                *bool                           `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Router                 string                          `json:"router,omitempty" yaml:"router,omitempty"`
	Hopwave                *bool                           `json:"hopwave,omitempty" yaml:"hopwave,omitempty"`
	MeshFreeze             *bool                           `json:"meshFreeze,omitempty" yaml:"meshFreeze,omitempty"`
	TopicMode              string                          `json:"topicMode,omitempty" yaml:"topicMode,omitempty"`
	RandomDegree           *int                            `json:"randomDegree,omitempty" yaml:"randomDegree,omitempty"`
	RandomNetworkSize      *int                            `json:"randomNetworkSize,omitempty" yaml:"randomNetworkSize,omitempty"`
	Subscribe              *bool                           `json:"subscribe,omitempty" yaml:"subscribe,omitempty"`
	AllowPublish           *bool                           `json:"allowPublish,omitempty" yaml:"allowPublish,omitempty"`
	Topics                 []string                        `json:"topics,omitempty" yaml:"topics,omitempty"`
	Params                 GossipSubParamsConfig           `json:"params,omitempty" yaml:"params,omitempty"`
	Score                  *PeerScoreConfig                `json:"score,omitempty" yaml:"score,omitempty"`
	ScoreInspectInterval   string                          `json:"scoreInspectInterval,omitempty" yaml:"scoreInspectInterval,omitempty"`
	FloodPublish           *bool                           `json:"floodPublish,omitempty" yaml:"floodPublish,omitempty"`
	PeerExchange           *bool                           `json:"peerExchange,omitempty" yaml:"peerExchange,omitempty"`
	MaxMessageSize         *int                            `json:"maxMessageSize,omitempty" yaml:"maxMessageSize,omitempty"`
	PeerOutboundQueueSize  *int                            `json:"peerOutboundQueueSize,omitempty" yaml:"peerOutboundQueueSize,omitempty"`
	SignaturePolicy        string                          `json:"signaturePolicy,omitempty" yaml:"signaturePolicy,omitempty"`
	SeenMessagesTTL        string                          `json:"seenMessagesTTL,omitempty" yaml:"seenMessagesTTL,omitempty"`
	ValidateQueueSize      *int                            `json:"validateQueueSize,omitempty" yaml:"validateQueueSize,omitempty"`
	ValidateThrottle       *int                            `json:"validateThrottle,omitempty" yaml:"validateThrottle,omitempty"`
	ValidateWorkers        *int                            `json:"validateWorkers,omitempty" yaml:"validateWorkers,omitempty"`
	SubscriptionBufferSize *int                            `json:"subscriptionBufferSize,omitempty" yaml:"subscriptionBufferSize,omitempty"`
}

// GossipSubParamsConfig covers every field in GossipSubParams provided by the
// pinned go-libp2p-pubsub version. Pointers distinguish an omitted value from an
// intentional zero, so experiments can override library defaults precisely.
type GossipSubParamsConfig struct {
	D                         *int     `json:"d,omitempty" yaml:"d,omitempty"`
	DLow                      *int     `json:"dLow,omitempty" yaml:"dLow,omitempty"`
	DHigh                     *int     `json:"dHigh,omitempty" yaml:"dHigh,omitempty"`
	DScore                    *int     `json:"dScore,omitempty" yaml:"dScore,omitempty"`
	DOut                      *int     `json:"dOut,omitempty" yaml:"dOut,omitempty"`
	DLazy                     *int     `json:"dLazy,omitempty" yaml:"dLazy,omitempty"`
	HistoryLength             *int     `json:"historyLength,omitempty" yaml:"historyLength,omitempty"`
	HistoryGossip             *int     `json:"historyGossip,omitempty" yaml:"historyGossip,omitempty"`
	GossipFactor              *float64 `json:"gossipFactor,omitempty" yaml:"gossipFactor,omitempty"`
	GossipRetransmission      *int     `json:"gossipRetransmission,omitempty" yaml:"gossipRetransmission,omitempty"`
	HeartbeatInitialDelay     string   `json:"heartbeatInitialDelay,omitempty" yaml:"heartbeatInitialDelay,omitempty"`
	HeartbeatInterval         string   `json:"heartbeatInterval,omitempty" yaml:"heartbeatInterval,omitempty"`
	SlowHeartbeatWarning      *float64 `json:"slowHeartbeatWarning,omitempty" yaml:"slowHeartbeatWarning,omitempty"`
	FanoutTTL                 string   `json:"fanoutTTL,omitempty" yaml:"fanoutTTL,omitempty"`
	PrunePeers                *int     `json:"prunePeers,omitempty" yaml:"prunePeers,omitempty"`
	PruneBackoff              string   `json:"pruneBackoff,omitempty" yaml:"pruneBackoff,omitempty"`
	UnsubscribeBackoff        string   `json:"unsubscribeBackoff,omitempty" yaml:"unsubscribeBackoff,omitempty"`
	Connectors                *int     `json:"connectors,omitempty" yaml:"connectors,omitempty"`
	MaxPendingConnections     *int     `json:"maxPendingConnections,omitempty" yaml:"maxPendingConnections,omitempty"`
	ConnectionTimeout         string   `json:"connectionTimeout,omitempty" yaml:"connectionTimeout,omitempty"`
	DirectConnectTicks        *uint64  `json:"directConnectTicks,omitempty" yaml:"directConnectTicks,omitempty"`
	DirectConnectInitialDelay string   `json:"directConnectInitialDelay,omitempty" yaml:"directConnectInitialDelay,omitempty"`
	OpportunisticGraftTicks   *uint64  `json:"opportunisticGraftTicks,omitempty" yaml:"opportunisticGraftTicks,omitempty"`
	OpportunisticGraftPeers   *int     `json:"opportunisticGraftPeers,omitempty" yaml:"opportunisticGraftPeers,omitempty"`
	GraftFloodThreshold       string   `json:"graftFloodThreshold,omitempty" yaml:"graftFloodThreshold,omitempty"`
	MaxIHaveLength            *int     `json:"maxIHaveLength,omitempty" yaml:"maxIHaveLength,omitempty"`
	MaxIHaveMessages          *int     `json:"maxIHaveMessages,omitempty" yaml:"maxIHaveMessages,omitempty"`
	MaxIDontWantLength        *int     `json:"maxIDontWantLength,omitempty" yaml:"maxIDontWantLength,omitempty"`
	MaxIDontWantMessages      *int     `json:"maxIDontWantMessages,omitempty" yaml:"maxIDontWantMessages,omitempty"`
	IWantFollowupTime         string   `json:"iWantFollowupTime,omitempty" yaml:"iWantFollowupTime,omitempty"`
	IDontWantMessageThreshold *int     `json:"iDontWantMessageThreshold,omitempty" yaml:"iDontWantMessageThreshold,omitempty"`
	IDontWantMessageTTL       *int     `json:"iDontWantMessageTTL,omitempty" yaml:"iDontWantMessageTTL,omitempty"`
	HopwaveFactor             *float64 `json:"hopwaveFactor,omitempty" yaml:"hopwaveFactor,omitempty"`
	HopwaveInterval           *int     `json:"hopwaveInterval,omitempty" yaml:"hopwaveInterval,omitempty"`
}
