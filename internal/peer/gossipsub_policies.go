package peer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

func gossipExtendedOptions(config model.GossipSubConfig) ([]pubsub.Option, error) {
	if err := config.ValidateExtended(); err != nil {
		return nil, err
	}
	var options []pubsub.Option
	if config.MessageID != "" {
		options = append(options, pubsub.WithMessageIdFn(gossipMessageID(config, "")))
	}
	switch config.SeenMessagesStrategy {
	case "first-seen":
		options = append(options, pubsub.WithSeenMessagesStrategy(timecache.Strategy_FirstSeen))
	case "last-seen":
		options = append(options, pubsub.WithSeenMessagesStrategy(timecache.Strategy_LastSeen))
	}
	if config.PeerFilter != nil {
		options = append(options, pubsub.WithPeerFilter(gossipPeerFilter(*config.PeerFilter)))
	}
	if config.SubscriptionFilter != nil {
		options = append(options, pubsub.WithSubscriptionFilter(gossipSubscriptionFilter(*config.SubscriptionFilter)))
	}
	if config.Blacklist != nil {
		ttl, _ := time.ParseDuration(config.Blacklist.TTL)
		blacklist := &gossipBlacklist{ttl: ttl, entries: make(map[peer.ID]time.Time)}
		for _, raw := range config.Blacklist.Peers {
			id, _ := peer.Decode(raw)
			blacklist.Add(id)
		}
		options = append(options, pubsub.WithBlacklist(blacklist))
	}
	if config.ProtocolMatch != "" {
		options = append(options, pubsub.WithProtocolMatchFn(func(expected protocol.ID) func(protocol.ID) bool {
			return func(actual protocol.ID) bool {
				if config.ProtocolMatch == "prefix" {
					return strings.HasPrefix(string(actual), string(expected))
				}
				return actual == expected
			}
		}))
	}
	if config.RPCInspector != nil {
		options = append(options, pubsub.WithAppSpecificRpcInspector(gossipRPCInspector(*config.RPCInspector)))
	}
	for _, value := range config.DefaultValidators {
		validator, opts, err := gossipValidator(value)
		if err != nil {
			return nil, err
		}
		options = append(options, pubsub.WithDefaultValidator(validator, opts...))
	}
	if config.Router == "gossipsub" {
		if len(config.Protocols) > 0 {
			options = append(options, pubsub.WithGossipSubProtocols(gossipProtocolIDs(config), gossipProtocolFeatures(config)))
		}
		if len(config.DirectPeers) > 0 {
			byID := make(map[peer.ID][]multiaddr.Multiaddr)
			for _, raw := range config.DirectPeers {
				address, _ := multiaddr.NewMultiaddr(raw)
				info, _ := peer.AddrInfoFromP2pAddr(address)
				byID[info.ID] = append(byID[info.ID], info.Addrs...)
			}
			ids := make([]peer.ID, 0, len(byID))
			for id := range byID {
				ids = append(ids, id)
			}
			slices.Sort(ids)
			infos := make([]peer.AddrInfo, 0, len(ids))
			for _, id := range ids {
				infos = append(infos, peer.AddrInfo{ID: id, Addrs: byID[id]})
			}
			options = append(options, pubsub.WithDirectPeers(infos))
		}
		if config.PeerGater != nil {
			options = append(options, pubsub.WithPeerGater(gossipPeerGater(*config.PeerGater)))
		}
	}
	return options, nil
}

// Keep this resolver shared by PubSub construction and telemetry fallback so
// raw publish, delivery and duplicate events use the same configured wire ID.
func gossipMessageID(config model.GossipSubConfig, topic string) pubsub.MsgIdFunction {
	mode := config.MessageID
	if override := config.TopicOptions[topic].MessageID; override != "" {
		mode = override
	}
	switch mode {
	case "sha256":
		return func(message *pubsubpb.Message) string {
			digest := sha256.Sum256(message.GetData())
			return string(digest[:])
		}
	case "topic-sha256":
		return func(message *pubsubpb.Message) string {
			hash := sha256.New()
			var length [8]byte
			binary.BigEndian.PutUint64(length[:], uint64(len(message.GetTopic())))
			hash.Write(length[:])
			hash.Write([]byte(message.GetTopic()))
			hash.Write(message.GetData())
			return string(hash.Sum(nil))
		}
	default:
		return pubsub.DefaultMsgIdFn
	}
}
func gossipTopicOptions(config model.GossipSubConfig, topic string) []pubsub.TopicOpt {
	if config.TopicOptions[topic].MessageID == "" {
		return nil
	}
	return []pubsub.TopicOpt{pubsub.WithTopicMessageIdFn(gossipMessageID(config, topic))}
}
func gossipSubscriptionOptions(config model.GossipSubConfig, topic string) []pubsub.SubOpt {
	size := config.SubscriptionBufferSize
	if override := config.TopicOptions[topic].SubscriptionBufferSize; override != nil {
		size = override
	}
	if size == nil {
		return nil
	}
	return []pubsub.SubOpt{pubsub.WithBufferSize(*size)}
}
func gossipProtocolIDs(config model.GossipSubConfig) []protocol.ID {
	ids := make([]protocol.ID, 0, len(config.Protocols))
	for _, p := range config.Protocols {
		ids = append(ids, protocol.ID(p.ID))
	}
	return ids
}
func gossipProtocolFeatures(config model.GossipSubConfig) pubsub.GossipSubFeatureTest {
	protocols := slices.Clone(config.Protocols)
	// Longest prefix wins when protocols with overlapping prefixes coexist.
	slices.SortStableFunc(protocols, func(a, b model.PubSubProtocolConfig) int { return len(b.ID) - len(a.ID) })
	return func(feature pubsub.GossipSubFeature, id protocol.ID) bool {
		for _, p := range protocols {
			if string(id) != p.ID && !(config.ProtocolMatch == "prefix" && strings.HasPrefix(string(id), p.ID)) {
				continue
			}
			if len(p.Features) == 0 {
				return pubsub.GossipSubDefaultFeatures(feature, protocol.ID(p.ID))
			}
			name := ""
			switch feature {
			case pubsub.GossipSubFeatureMesh:
				name = "mesh"
			case pubsub.GossipSubFeaturePX:
				name = "px"
			case pubsub.GossipSubFeatureIdontwant:
				name = "idontwant"
			}
			return name != "" && slices.Contains(p.Features, name)
		}
		return false
	}
}
func gossipPeerFilter(config model.PubSubPeerFilterConfig) pubsub.PeerFilter {
	globalAllow, globalDeny := gossipPeerSet(config.Allow), gossipPeerSet(config.Deny)
	type rule struct{ allow, deny map[peer.ID]bool }
	topics := make(map[string]rule, len(config.Topics))
	for topic, value := range config.Topics {
		topics[topic] = rule{gossipPeerSet(value.Allow), gossipPeerSet(value.Deny)}
	}
	return func(id peer.ID, topic string) bool {
		if globalDeny[id] || len(globalAllow) > 0 && !globalAllow[id] {
			return false
		}
		r := topics[topic]
		return !r.deny[id] && (len(r.allow) == 0 || r.allow[id])
	}
}
func gossipPeerSet(values []string) map[peer.ID]bool {
	result := make(map[peer.ID]bool, len(values))
	for _, value := range values {
		id, err := peer.Decode(value)
		if err == nil {
			result[id] = true
		}
	}
	return result
}
func gossipSubscriptionFilter(config model.PubSubSubscriptionFilterConfig) pubsub.SubscriptionFilter {
	var filter pubsub.SubscriptionFilter
	if len(config.Allowlist) > 0 {
		filter = pubsub.NewAllowlistSubscriptionFilter(config.Allowlist...)
	} else {
		pattern := config.Pattern
		if pattern == "" {
			pattern = "(?s).*"
		}
		filter = pubsub.NewRegexpSubscriptionFilter(regexp.MustCompile(pattern))
	}
	if config.MaxSubscriptions != nil {
		filter = pubsub.WrapLimitSubscriptionFilter(filter, *config.MaxSubscriptions)
	}
	return filter
}

// Lazy expiry avoids leaking a time-cache cleanup goroutine after a peer stops.
// PubSub may call Add/Contains from callbacks, so the adapter owns its lock.
type gossipBlacklist struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[peer.ID]time.Time
}

func (b *gossipBlacklist) Add(id peer.ID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	at, exists := b.entries[id]
	if exists && (b.ttl == 0 || now.Before(at)) {
		return false
	}
	if b.ttl > 0 {
		b.entries[id] = now.Add(b.ttl)
	} else {
		b.entries[id] = time.Time{}
	}
	return true
}
func (b *gossipBlacklist) Contains(id peer.ID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	at, exists := b.entries[id]
	if exists && b.ttl > 0 && !time.Now().Before(at) {
		delete(b.entries, id)
		return false
	}
	return exists
}

func gossipPeerGater(config model.PeerGaterConfig) *pubsub.PeerGaterParams {
	p := pubsub.DefaultPeerGaterParams()
	for target, value := range map[*float64]*float64{&p.Threshold: config.Threshold, &p.GlobalDecay: config.GlobalDecay, &p.SourceDecay: config.SourceDecay, &p.DecayToZero: config.DecayToZero, &p.DuplicateWeight: config.DuplicateWeight, &p.IgnoreWeight: config.IgnoreWeight, &p.RejectWeight: config.RejectWeight} {
		if value != nil {
			*target = *value
		}
	}
	for target, value := range map[*time.Duration]string{&p.DecayInterval: config.DecayInterval, &p.RetainStats: config.RetainStats, &p.Quiet: config.Quiet} {
		if value != "" {
			*target, _ = time.ParseDuration(value)
		}
	}
	p.TopicDeliveryWeights = maps.Clone(config.TopicDeliveryWeights)
	return p
}

func gossipValidator(config model.PubSubValidatorConfig) (pubsub.ValidatorEx, []pubsub.ValidatorOpt, error) {
	if err := config.Validate(); err != nil {
		return nil, nil, err
	}
	var rx *regexp.Regexp
	if config.Pattern != "" {
		rx = regexp.MustCompile(config.Pattern)
	}
	delay, _ := time.ParseDuration(config.Delay)
	failure := gossipValidationResult(config.FailureResult, pubsub.ValidationReject)
	result := gossipValidationResult(config.Result, pubsub.ValidationAccept)
	var sequenceValidator pubsub.ValidatorEx
	if config.BasicSeqno {
		sequenceValidator = pubsub.NewBasicSeqnoValidator(&gossipSequenceStore{values: make(map[peer.ID][]byte)})
	}
	validator := func(ctx context.Context, _ peer.ID, message *pubsub.Message) pubsub.ValidationResult {
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return pubsub.ValidationIgnore
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			return pubsub.ValidationIgnore
		}
		if message == nil || message.Message == nil {
			return failure
		}
		if config.MinBytes != nil && len(message.Data) < *config.MinBytes || config.MaxBytes != nil && len(message.Data) > *config.MaxBytes {
			return failure
		}
		if rx != nil && !rx.Match(message.Data) {
			return failure
		}
		switch config.Format {
		case "json":
			if !json.Valid(message.Data) {
				return failure
			}
		case "envelope":
			var value envelope
			if json.Unmarshal(message.Data, &value) != nil || value.ID == "" || value.RunID == "" || value.Publisher == "" || value.SentAt <= 0 {
				return failure
			}
		}
		decision := result
		if config.UseValidatorData && message.ValidatorData != nil {
			data, ok := message.ValidatorData.(map[string]any)
			if !ok {
				return failure
			}
			value, ok := data["result"].(string)
			if !ok {
				return failure
			}
			decision = gossipValidationResult(value, failure)
		}
		if decision == pubsub.ValidationAccept && sequenceValidator != nil {
			// The upstream helper expects an eight-byte nonce. Reject malformed
			// signed messages before its binary decoder can panic.
			if len(message.GetSeqno()) != 8 || len(message.GetFrom()) == 0 {
				return failure
			}
			return sequenceValidator(ctx, message.ReceivedFrom, message)
		}
		return decision
	}
	var options []pubsub.ValidatorOpt
	if config.Timeout != "" {
		timeout, _ := time.ParseDuration(config.Timeout)
		options = append(options, pubsub.WithValidatorTimeout(timeout))
	}
	if config.Concurrency != nil {
		options = append(options, pubsub.WithValidatorConcurrency(*config.Concurrency))
	}
	if config.Inline != nil {
		options = append(options, pubsub.WithValidatorInline(*config.Inline))
	}
	return validator, options, nil
}
func gossipValidationResult(value string, fallback pubsub.ValidationResult) pubsub.ValidationResult {
	switch value {
	case "accept":
		return pubsub.ValidationAccept
	case "reject":
		return pubsub.ValidationReject
	case "ignore":
		return pubsub.ValidationIgnore
	default:
		return fallback
	}
}
func registerGossipValidators(ps *pubsub.PubSub, config model.GossipSubConfig) error {
	for _, topic := range config.Topics {
		value := config.TopicOptions[topic].Validator
		if value == nil {
			continue
		}
		validator, opts, err := gossipValidator(*value)
		if err != nil {
			return err
		}
		if err := ps.RegisterTopicValidator(topic, validator, opts...); err != nil {
			return fmt.Errorf("register validator for %s: %w", topic, err)
		}
	}
	return nil
}
func gossipRPCInspector(config model.PubSubRPCInspectorConfig) func(peer.ID, *pubsub.RPC) error {
	rejected := gossipPeerSet(config.RejectPeers)
	return func(id peer.ID, rpc *pubsub.RPC) error {
		if rejected[id] {
			return fmt.Errorf("RPC sender rejected by policy")
		}
		if rpc == nil {
			return fmt.Errorf("nil RPC")
		}
		controlEntries, messageIDs := 0, 0
		if ctl := rpc.GetControl(); ctl != nil {
			controlEntries = len(ctl.Ihave) + len(ctl.Iwant) + len(ctl.Graft) + len(ctl.Prune) + len(ctl.Idontwant)
			for _, v := range ctl.Ihave {
				messageIDs += len(v.GetMessageIDs())
			}
			for _, v := range ctl.Iwant {
				messageIDs += len(v.GetMessageIDs())
			}
			for _, v := range ctl.Idontwant {
				messageIDs += len(v.GetMessageIDs())
			}
		}
		for _, limit := range []struct {
			name   string
			max    *int
			actual int
		}{{"messages", config.MaxMessages, len(rpc.Publish)}, {"subscriptions", config.MaxSubscriptions, len(rpc.Subscriptions)}, {"control entries", config.MaxControlEntries, controlEntries}, {"message IDs", config.MaxMessageIDs, messageIDs}, {"bytes", config.MaxBytes, rpc.Size()}} {
			if limit.max != nil && limit.actual > *limit.max {
				return fmt.Errorf("RPC %s exceed configured maximum", limit.name)
			}
		}
		return nil
	}
}

// Host-scoped options run before constructing PubSub, which reads the selected
// signing key from the host peerstore. KPL's transport identity remains unchanged.
func gossipAuthorOptions(config model.GossipSubConfig, h host.Host) ([]pubsub.Option, error) {
	if config.Author == nil {
		return nil, nil
	}
	value := *config.Author
	key, id, err := value.Identity()
	if err != nil {
		return nil, err
	}
	if value.NoAuthor {
		return []pubsub.Option{pubsub.WithNoAuthor()}, nil
	}
	if key != nil {
		if err := h.Peerstore().AddPrivKey(id, key); err != nil {
			return nil, err
		}
		if err := h.Peerstore().AddPubKey(id, key.GetPublic()); err != nil {
			return nil, err
		}
	}
	if config.SignaturePolicy != "strict-no-sign" && config.SignaturePolicy != "lax-no-sign" && h.Peerstore().PrivKey(id) == nil {
		return nil, fmt.Errorf("author.peerId requires its privateKey when it differs from the peer's transport identity")
	}
	return []pubsub.Option{pubsub.WithMessageAuthor(id)}, nil
}

// Readiness is deliberately handled by the caller before preparing the payload
// timestamp. These options alter the publication after that readiness boundary.
func gossipPublishOptions(config model.GossipSubConfig) ([]pubsub.PubOpt, error) {
	if config.Publish == nil {
		return nil, nil
	}
	value := config.Publish
	var options []pubsub.PubOpt
	if value.Local != nil {
		options = append(options, pubsub.WithLocalPublication(*value.Local))
	}
	if len(value.ValidatorData) != 0 {
		options = append(options, pubsub.WithValidatorData(maps.Clone(value.ValidatorData)))
	}
	if value.Author != nil {
		key, id, err := value.Author.Identity()
		if err != nil {
			return nil, err
		}
		if key == nil || value.Author.NoAuthor {
			return nil, fmt.Errorf("publish.author requires a privateKey")
		}
		options = append(options, pubsub.WithSecretKeyAndPeerId(key, id))
	}
	return options, nil
}

// Sequence metadata lasts for the lifetime of this configured validator.
type gossipSequenceStore struct {
	mu     sync.RWMutex
	values map[peer.ID][]byte
}

func (s *gossipSequenceStore) Get(_ context.Context, id peer.ID) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.values[id]), nil
}
func (s *gossipSequenceStore) Put(_ context.Context, id peer.ID, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[id] = slices.Clone(value)
	return nil
}
