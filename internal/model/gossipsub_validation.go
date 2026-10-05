package model

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"time"

	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

// ValidateExtended checks every declarative adapter for PubSub's callback and
// interface options before any peer or network operation is started.
func (c GossipSubConfig) ValidateExtended() error {
	if c.MeshFreeze != nil && *c.MeshFreeze && c.Router != "" && c.Router != "gossipsub" {
		return fmt.Errorf("meshFreeze requires the gossipsub router")
	}
	if c.Hopwave != nil && *c.Hopwave && c.Router != "" && c.Router != "gossipsub" {
		return fmt.Errorf("hopwave requires the gossipsub router")
	}
	if err := validateMessageID(c.MessageID); err != nil {
		return err
	}
	switch c.SeenMessagesStrategy {
	case "", "first-seen", "last-seen":
	default:
		return fmt.Errorf("invalid seenMessagesStrategy %q", c.SeenMessagesStrategy)
	}
	switch c.ProtocolMatch {
	case "", "exact", "prefix":
	default:
		return fmt.Errorf("invalid protocolMatch %q", c.ProtocolMatch)
	}
	if c.ProtocolMatch == "prefix" && len(c.Protocols) == 0 {
		return fmt.Errorf("protocolMatch prefix requires explicit protocols")
	}
	if c.Router != "" && c.Router != "gossipsub" && (len(c.DirectPeers) != 0 || c.PeerGater != nil) {
		return fmt.Errorf("directPeers and peerGater require the gossipsub router")
	}
	if c.Router == "randomsub" && len(c.Protocols) != 0 {
		return fmt.Errorf("custom protocols are unavailable for randomsub")
	}
	seenProtocols := make(map[string]bool)
	for _, p := range c.Protocols {
		if !strings.HasPrefix(p.ID, "/") || strings.TrimSpace(p.ID) != p.ID || seenProtocols[p.ID] {
			return fmt.Errorf("protocol IDs must be unique, nonempty, slash-prefixed strings")
		}
		seenProtocols[p.ID] = true
		if c.Router == "floodsub" && len(p.Features) != 0 {
			return fmt.Errorf("floodsub protocols cannot enable GossipSub features")
		}
		seen := make(map[string]bool)
		for _, f := range p.Features {
			switch f {
			case "none", "mesh", "px", "idontwant":
			default:
				return fmt.Errorf("unknown GossipSub protocol feature %q", f)
			}
			if seen[f] {
				return fmt.Errorf("duplicate protocol feature %q", f)
			}
			seen[f] = true
		}
		if seen["none"] && len(seen) > 1 {
			return fmt.Errorf("protocol feature none cannot be combined with capabilities")
		}
		if (seen["px"] || seen["idontwant"]) && !seen["mesh"] {
			return fmt.Errorf("protocol px/idontwant features require mesh")
		}
	}
	for _, raw := range c.DirectPeers {
		address, err := multiaddr.NewMultiaddr(raw)
		if err != nil {
			return fmt.Errorf("invalid direct peer multiaddr: %w", err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil || len(info.Addrs) == 0 {
			return fmt.Errorf("directPeers require transport multiaddrs ending in /p2p/<peer ID>")
		}
	}
	if c.PeerFilter != nil {
		if err := validatePeerIDs(append(append([]string{}, c.PeerFilter.Allow...), c.PeerFilter.Deny...)); err != nil {
			return fmt.Errorf("peerFilter: %w", err)
		}
		for topic, rule := range c.PeerFilter.Topics {
			if topic == "" {
				return fmt.Errorf("peerFilter topic cannot be empty")
			}
			if err := validatePeerIDs(append(append([]string{}, rule.Allow...), rule.Deny...)); err != nil {
				return fmt.Errorf("peerFilter topic %q: %w", topic, err)
			}
		}
	}
	if c.Blacklist != nil {
		if err := validatePeerIDs(c.Blacklist.Peers); err != nil {
			return fmt.Errorf("blacklist: %w", err)
		}
		if err := pubsubDuration("blacklist.ttl", c.Blacklist.TTL, true); err != nil {
			return err
		}
	}
	if c.SubscriptionFilter != nil {
		f := c.SubscriptionFilter
		if len(f.Allowlist) != 0 && f.Pattern != "" {
			return fmt.Errorf("subscriptionFilter allowlist and pattern are mutually exclusive")
		}
		var rx *regexp.Regexp
		if f.Pattern != "" {
			var err error
			rx, err = regexp.Compile(f.Pattern)
			if err != nil {
				return fmt.Errorf("subscriptionFilter.pattern: %w", err)
			}
		}
		if f.MaxSubscriptions != nil && *f.MaxSubscriptions <= 0 {
			return fmt.Errorf("subscriptionFilter.maxSubscriptions must be positive")
		}
		for _, topic := range c.Topics {
			if rx != nil && !rx.MatchString(topic) {
				return fmt.Errorf("subscriptionFilter excludes joined topic %q", topic)
			}
			if len(f.Allowlist) > 0 && !pubsubContains(f.Allowlist, topic) {
				return fmt.Errorf("subscriptionFilter excludes joined topic %q", topic)
			}
		}
	}
	for topic, options := range c.TopicOptions {
		if !pubsubContains(c.Topics, topic) {
			return fmt.Errorf("topicOptions refers to unjoined topic %q", topic)
		}
		if err := validateMessageID(options.MessageID); err != nil {
			return fmt.Errorf("topicOptions %q: %w", topic, err)
		}
		if options.SubscriptionBufferSize != nil && *options.SubscriptionBufferSize < 0 {
			return fmt.Errorf("topicOptions subscriptionBufferSize cannot be negative")
		}
		if options.SubscriptionBufferSize != nil && c.TopicMode != "" && c.TopicMode != "subscribe" {
			return fmt.Errorf("topic subscriptionBufferSize requires subscribe topicMode")
		}
		if options.Validator != nil {
			if options.Validator.BasicSeqno && c.SignaturePolicy != "" && c.SignaturePolicy != "strict-sign" {
				return fmt.Errorf("basicSeqno validator requires strict-sign")
			}
			if err := options.Validator.Validate(); err != nil {
				return fmt.Errorf("topicOptions %q validator: %w", topic, err)
			}
		}
	}
	for i, v := range c.DefaultValidators {
		if v.BasicSeqno && c.SignaturePolicy != "" && c.SignaturePolicy != "strict-sign" {
			return fmt.Errorf("basicSeqno validator requires strict-sign")
		}
		if err := v.Validate(); err != nil {
			return fmt.Errorf("defaultValidators[%d]: %w", i, err)
		}
	}
	if c.RPCInspector != nil {
		r := c.RPCInspector
		for name, value := range map[string]*int{"maxMessages": r.MaxMessages, "maxSubscriptions": r.MaxSubscriptions, "maxControlEntries": r.MaxControlEntries, "maxMessageIds": r.MaxMessageIDs, "maxBytes": r.MaxBytes} {
			if value != nil && *value < 0 {
				return fmt.Errorf("rpcInspector.%s cannot be negative", name)
			}
		}
		if err := validatePeerIDs(r.RejectPeers); err != nil {
			return fmt.Errorf("rpcInspector: %w", err)
		}
	}
	if c.PeerGater != nil {
		if err := c.PeerGater.Validate(); err != nil {
			return fmt.Errorf("peerGater: %w", err)
		}
	}
	if c.Discovery != nil {
		if err := c.Discovery.Validate(); err != nil {
			return fmt.Errorf("discovery: %w", err)
		}
	}
	if c.Author != nil {
		if _, _, err := c.Author.Identity(); err != nil {
			return fmt.Errorf("author: %w", err)
		}
		if c.Author.NoAuthor {
			if c.SignaturePolicy != "strict-no-sign" && c.SignaturePolicy != "lax-no-sign" {
				return fmt.Errorf("author.noAuthor requires an unsigned signaturePolicy")
			}
			for _, topic := range c.Topics {
				mode := c.MessageID
				if override := c.TopicOptions[topic].MessageID; override != "" {
					mode = override
				}
				if mode == "" || mode == "default" {
					return fmt.Errorf("author.noAuthor requires a content messageId policy on every topic")
				}
			}
		}
	}
	if c.Publish != nil {
		p := c.Publish
		if p.ReadinessMinPeers != nil && *p.ReadinessMinPeers < 0 {
			return fmt.Errorf("publish.readinessMinPeers cannot be negative")
		}
		if err := pubsubDuration("publish.readinessTimeout", p.ReadinessTimeout, true); err != nil {
			return err
		}
		if p.ReadinessTimeout != "" {
			timeout, _ := time.ParseDuration(p.ReadinessTimeout)
			if timeout > 10*time.Second {
				return fmt.Errorf("publish.readinessTimeout cannot exceed the 10s publish request budget")
			}
		}
		if p.ReadinessTimeout != "" && p.ReadinessMinPeers == nil {
			return fmt.Errorf("publish.readinessTimeout requires readinessMinPeers")
		}
		if p.Author != nil {
			if p.Local != nil && *p.Local {
				return fmt.Errorf("publish.author cannot be combined with local publication")
			}
			if p.Author.NoAuthor || p.Author.PrivateKey == "" {
				return fmt.Errorf("publish.author requires a privateKey and cannot omit author")
			}
			if _, _, err := p.Author.Identity(); err != nil {
				return fmt.Errorf("publish.author: %w", err)
			}
			if c.SignaturePolicy == "strict-no-sign" || c.SignaturePolicy == "lax-no-sign" || c.Author != nil && c.Author.NoAuthor {
				return fmt.Errorf("publish.author requires a signing signaturePolicy")
			}
		}
		if len(p.ValidatorData) != 0 {
			enabled := false
			for _, v := range c.DefaultValidators {
				enabled = enabled || v.UseValidatorData
			}
			for _, topic := range c.Topics {
				if v := c.TopicOptions[topic].Validator; v != nil {
					enabled = enabled || v.UseValidatorData
				}
			}
			if !enabled {
				return fmt.Errorf("publish.validatorData requires a validator with useValidatorData")
			}
			result, ok := p.ValidatorData["result"].(string)
			if !ok || !pubsubContains([]string{"accept", "reject", "ignore"}, result) {
				return fmt.Errorf("publish.validatorData.result must be accept, reject, or ignore")
			}
		}
	}
	return nil
}

func validateMessageID(value string) error {
	switch value {
	case "", "default", "sha256", "topic-sha256":
		return nil
	default:
		return fmt.Errorf("invalid messageId policy %q", value)
	}
}
func validatePeerIDs(values []string) error {
	for _, value := range values {
		if _, err := peer.Decode(value); err != nil {
			return fmt.Errorf("invalid peer ID %q: %w", value, err)
		}
	}
	return nil
}
func pubsubContains(values []string, needle string) bool {
	for _, v := range values {
		if v == needle {
			return true
		}
	}
	return false
}
func pubsubDuration(name, value string, positive bool) error {
	if value == "" {
		return nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 || positive && d == 0 {
		return fmt.Errorf("%s must be a %s duration", name, map[bool]string{true: "positive", false: "non-negative"}[positive])
	}
	return nil
}
func (c PubSubValidatorConfig) Validate() error {
	for name, value := range map[string]string{"result": c.Result, "failureResult": c.FailureResult} {
		if !pubsubContains([]string{"", "accept", "reject", "ignore"}, value) {
			return fmt.Errorf("%s must be accept, reject, or ignore", name)
		}
	}
	if c.FailureResult == "accept" {
		return fmt.Errorf("failureResult must be reject or ignore")
	}
	if c.MinBytes != nil && *c.MinBytes < 0 || c.MaxBytes != nil && *c.MaxBytes < 0 {
		return fmt.Errorf("validator byte limits cannot be negative")
	}
	if c.MinBytes != nil && c.MaxBytes != nil && *c.MinBytes > *c.MaxBytes {
		return fmt.Errorf("validator minBytes exceeds maxBytes")
	}
	if c.Pattern != "" {
		if _, err := regexp.Compile(c.Pattern); err != nil {
			return fmt.Errorf("validator pattern: %w", err)
		}
	}
	switch c.Format {
	case "", "any", "json", "envelope":
	default:
		return fmt.Errorf("validator format must be any, json, or envelope")
	}
	for name, value := range map[string]string{"delay": c.Delay, "timeout": c.Timeout} {
		if err := pubsubDuration("validator."+name, value, false); err != nil {
			return err
		}
	}
	if c.Concurrency != nil && *c.Concurrency <= 0 {
		return fmt.Errorf("validator concurrency must be positive")
	}
	return nil
}

func (c PeerGaterConfig) Validate() error {
	for name, value := range map[string]*float64{"threshold": c.Threshold, "globalDecay": c.GlobalDecay, "sourceDecay": c.SourceDecay, "decayToZero": c.DecayToZero, "duplicateWeight": c.DuplicateWeight, "ignoreWeight": c.IgnoreWeight, "rejectWeight": c.RejectWeight} {
		if value == nil {
			continue
		}
		if !validNumber(*value) || *value <= 0 {
			return fmt.Errorf("%s must be finite and positive", name)
		}
		if (name == "globalDecay" || name == "sourceDecay" || name == "decayToZero") && *value >= 1 {
			return fmt.Errorf("%s must be below 1", name)
		}
		if (name == "ignoreWeight" || name == "rejectWeight") && *value < 1 {
			return fmt.Errorf("%s must be at least 1", name)
		}
	}
	for name, value := range map[string]string{"decayInterval": c.DecayInterval, "quiet": c.Quiet, "retainStats": c.RetainStats} {
		if err := pubsubDuration(name, value, false); err != nil {
			return err
		}
		if value != "" && name != "retainStats" {
			d, _ := time.ParseDuration(value)
			if d < time.Second {
				return fmt.Errorf("%s must be at least 1s", name)
			}
		}
	}
	for topic, weight := range c.TopicDeliveryWeights {
		if topic == "" || !validNumber(weight) || weight <= 0 {
			return fmt.Errorf("topicDeliveryWeights require nonempty topics and finite positive weights")
		}
	}
	return nil
}

func (c PubSubDiscoveryConfig) Validate() error {
	switch c.Mode {
	case "", "controller", "routing", "none":
	default:
		return fmt.Errorf("mode must be controller, routing, or none")
	}
	if c.Mode == "none" && (c.TTL != "" || c.Limit != nil || c.Connector != nil) {
		return fmt.Errorf("disabled discovery cannot configure TTL, limit, or connector")
	}
	if err := pubsubDuration("ttl", c.TTL, true); err != nil {
		return err
	}
	if c.Limit != nil && *c.Limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	if c.Connector == nil {
		return nil
	}
	b := c.Connector
	if b.CacheSize != nil && *b.CacheSize <= 0 {
		return fmt.Errorf("connector.cacheSize must be positive")
	}
	for name, value := range map[string]string{"dialTimeout": b.DialTimeout, "minBackoff": b.MinBackoff, "maxBackoff": b.MaxBackoff, "timeUnit": b.TimeUnit, "offset": b.Offset} {
		if err := pubsubDuration("connector."+name, value, name != "offset"); err != nil {
			return err
		}
	}
	min, max := 10*time.Second, time.Hour
	if b.MinBackoff != "" {
		min, _ = time.ParseDuration(b.MinBackoff)
	}
	if b.MaxBackoff != "" {
		max, _ = time.ParseDuration(b.MaxBackoff)
	}
	if min > max {
		return fmt.Errorf("connector.minBackoff exceeds maxBackoff")
	}
	if b.Base != nil && (!validNumber(*b.Base) || *b.Base <= 1) {
		return fmt.Errorf("connector.base must be finite and greater than 1")
	}
	switch b.Jitter {
	case "", "full", "none":
	default:
		return fmt.Errorf("connector.jitter must be full or none")
	}
	return nil
}

// Identity decodes an explicitly configured author without exposing key material
// in validation errors. A nil key means the host peerstore supplies the identity.
func (c PubSubAuthorConfig) Identity() (libcrypto.PrivKey, peer.ID, error) {
	if c.NoAuthor {
		if c.PeerID != "" || c.PrivateKey != "" {
			return nil, "", fmt.Errorf("noAuthor conflicts with peerId/privateKey")
		}
		return nil, "", nil
	}
	if c.PeerID == "" && c.PrivateKey == "" {
		return nil, "", fmt.Errorf("peerId or privateKey is required")
	}
	var id peer.ID
	if c.PeerID != "" {
		var err error
		id, err = peer.Decode(c.PeerID)
		if err != nil {
			return nil, "", fmt.Errorf("invalid author peerId")
		}
	}
	if c.PrivateKey == "" {
		return nil, id, nil
	}
	data, err := base64.StdEncoding.DecodeString(c.PrivateKey)
	if err != nil {
		return nil, "", fmt.Errorf("privateKey must be base64 libp2p key bytes")
	}
	key, err := libcrypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, "", fmt.Errorf("invalid libp2p privateKey")
	}
	derived, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return nil, "", fmt.Errorf("derive author identity: %w", err)
	}
	if id != "" && id != derived {
		return nil, "", fmt.Errorf("author peerId does not match privateKey")
	}
	return key, derived, nil
}
