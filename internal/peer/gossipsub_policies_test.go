package peer

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func policyPubSub(t *testing.T, config model.GossipSubConfig, extra ...pubsub.Option) (*pubsub.PubSub, host.Host) {
	t.Helper()
	h := newConfigTestHost(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	options, err := gossipSubOptions(config, discardEventTracer{})
	if err != nil {
		t.Fatal(err)
	}
	author, err := gossipAuthorOptions(config, h)
	if err != nil {
		t.Fatal(err)
	}
	options = append(options, author...)
	options = append(options, extra...)
	ps, err := pubsub.NewGossipSub(ctx, h, options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := registerGossipValidators(ps, config); err != nil {
		t.Fatal(err)
	}
	return ps, h
}
func policyTopic(t *testing.T, ps *pubsub.PubSub, config model.GossipSubConfig, name string) (*pubsub.Topic, *pubsub.Subscription) {
	t.Helper()
	topic, err := ps.Join(name, gossipTopicOptions(config, name)...)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := topic.Subscribe(gossipSubscriptionOptions(config, name)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Cancel(); _ = topic.Close() })
	return topic, sub
}
func nextPolicyMessage(t *testing.T, sub *pubsub.Subscription) *pubsub.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	message, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func TestGossipContentIDsAndAnonymousAuthorReachRealSubscriptions(t *testing.T) {
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"a", "b"}, MessageID: "sha256", SignaturePolicy: "strict-no-sign", Author: &model.PubSubAuthorConfig{NoAuthor: true}, TopicOptions: map[string]model.PubSubTopicConfig{"b": {MessageID: "topic-sha256", SubscriptionBufferSize: nil}}}}.WithDefaults().GossipSub
	one := 1
	config.TopicOptions["b"] = model.PubSubTopicConfig{MessageID: "topic-sha256", SubscriptionBufferSize: &one}
	ps, _ := policyPubSub(t, config)
	for _, name := range config.Topics {
		topic, sub := policyTopic(t, ps, config, name)
		payload := []byte("identical bytes")
		if err := topic.Publish(t.Context(), payload); err != nil {
			t.Fatal(err)
		}
		message := nextPolicyMessage(t, sub)
		if len(message.From) != 0 || len(message.Seqno) != 0 || len(message.Signature) != 0 {
			t.Fatalf("anonymous message includes author information: %+v", message.Message)
		}
		if want := gossipMessageID(config, name)(message.Message); message.ID != want {
			t.Fatalf("topic %q did not use configured ID", name)
		}
		if name == "b" && message.ID == gossipMessageID(config, "a")(message.Message) {
			t.Fatal("per-topic ID override ignored")
		}
	}
}
func TestGossipValidatorsAffectActualPublishAdmission(t *testing.T) {
	inline := true
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic"}, DefaultValidators: []model.PubSubValidatorConfig{{Format: "json", Inline: &inline}}, TopicOptions: map[string]model.PubSubTopicConfig{"topic": {Validator: &model.PubSubValidatorConfig{Pattern: `"allowed":true`, Inline: &inline}}}}}.WithDefaults().GossipSub
	ps, _ := policyPubSub(t, config)
	topic, sub := policyTopic(t, ps, config, "topic")
	for _, payload := range []string{`not json`, `{"allowed":false}`} {
		if err := topic.Publish(t.Context(), []byte(payload)); err == nil {
			t.Fatalf("validator accepted %s", payload)
		}
	}
	payload := []byte(`{"allowed":true}`)
	if err := topic.Publish(t.Context(), payload); err != nil {
		t.Fatal(err)
	}
	if message := nextPolicyMessage(t, sub); string(message.Data) != string(payload) {
		t.Fatalf("rejected publication reached consumer: %s", message.Data)
	}
}
func TestGossipValidatorTimeoutAndDataPolicy(t *testing.T) {
	validator, _, err := gossipValidator(model.PubSubValidatorConfig{Delay: "1s", FailureResult: "ignore"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	if result := validator(ctx, "", &pubsub.Message{Message: &pb.Message{Data: []byte("payload")}}); result != pubsub.ValidationIgnore || time.Since(started) > 100*time.Millisecond {
		t.Fatalf("validator ignored cancellation: %v", result)
	}
	validator, _, err = gossipValidator(model.PubSubValidatorConfig{UseValidatorData: true})
	if err != nil {
		t.Fatal(err)
	}
	message := &pubsub.Message{Message: &pb.Message{Data: []byte("payload")}, ValidatorData: map[string]any{"result": "reject"}}
	if result := validator(t.Context(), "", message); result != pubsub.ValidationReject {
		t.Fatalf("validator data not used: %v", result)
	}
	message.ValidatorData = nil
	if result := validator(t.Context(), "", message); result != pubsub.ValidationAccept {
		t.Fatal("wire messages without local data did not use configured result")
	}
}
func TestGossipPublishOptionsUseVirtualSigningIdentityAndValidatorData(t *testing.T) {
	key, _, err := libcrypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := libcrypto.MarshalPrivateKey(key)
	id, _ := peer.IDFromPrivateKey(key)
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic"}, DefaultValidators: []model.PubSubValidatorConfig{{UseValidatorData: true}}, Publish: &model.PubSubPublishConfig{Author: &model.PubSubAuthorConfig{PrivateKey: base64.StdEncoding.EncodeToString(wire)}, ValidatorData: map[string]any{"result": "accept"}}}}.WithDefaults().GossipSub
	ps, h := policyPubSub(t, config)
	topic, sub := policyTopic(t, ps, config, "topic")
	options, err := gossipPublishOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := topic.Publish(t.Context(), []byte("virtual publisher"), options...); err != nil {
		t.Fatal(err)
	}
	message := nextPolicyMessage(t, sub)
	if message.GetFrom() != id || message.ReceivedFrom != h.ID() || len(message.Signature) == 0 {
		t.Fatalf("custom identity not applied: from=%s received=%s", message.GetFrom(), message.ReceivedFrom)
	}
	config.Publish.ValidatorData["result"] = "reject"
	options, err = gossipPublishOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := topic.Publish(t.Context(), []byte("rejected by data"), options...); err == nil {
		t.Fatal("WithValidatorData did not affect validation")
	}
}
func TestGossipAuthorLoadsTheSelectedPeerstoreSigningKey(t *testing.T) {
	key, _, err := libcrypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := libcrypto.MarshalPrivateKey(key)
	id, _ := peer.IDFromPrivateKey(key)
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic"}, Author: &model.PubSubAuthorConfig{PrivateKey: base64.StdEncoding.EncodeToString(wire)}}}.WithDefaults().GossipSub
	ps, h := policyPubSub(t, config)
	topic, sub := policyTopic(t, ps, config, "topic")
	if err := topic.Publish(t.Context(), []byte("authored")); err != nil {
		t.Fatal(err)
	}
	message := nextPolicyMessage(t, sub)
	if message.GetFrom() != id || id == h.ID() || !key.Equals(h.Peerstore().PrivKey(id)) {
		t.Fatal("configured message author was not used independently of transport identity")
	}
}
func TestGossipFilterPoliciesAndRPCInspection(t *testing.T) {
	first := newConfigTestHost(t).ID()
	second := newConfigTestHost(t).ID()
	filter := gossipPeerFilter(model.PubSubPeerFilterConfig{Allow: []string{first.String(), second.String()}, Topics: map[string]model.PubSubPeerRule{"private": {Deny: []string{second.String()}}}})
	if !filter(first, "private") || filter(second, "private") || !filter(second, "public") {
		t.Fatal("peer filter did not combine global and per-topic policy")
	}
	one := 1
	subFilter := gossipSubscriptionFilter(model.PubSubSubscriptionFilterConfig{Pattern: "^allowed$", MaxSubscriptions: &one})
	if !subFilter.CanSubscribe("allowed") || subFilter.CanSubscribe("other") {
		t.Fatal("subscription pattern not applied")
	}
	allowed := "allowed"
	opts := []*pb.RPC_SubOpts{{Topicid: &allowed}, {Topicid: &allowed}}
	if _, err := subFilter.FilterIncomingSubscriptions(first, opts); !errors.Is(err, pubsub.ErrTooManySubscriptions) {
		t.Fatalf("subscription limit did not reject oversized RPC: %v", err)
	}
	inspect := gossipRPCInspector(model.PubSubRPCInspectorConfig{MaxMessageIDs: &one, RejectPeers: []string{second.String()}})
	rpc := &pubsub.RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{MessageIDs: []string{"a", "b"}}}}}}
	if err := inspect(first, rpc); err == nil {
		t.Fatal("logical message IDs bypassed RPC cap")
	}
	if err := inspect(second, &pubsub.RPC{}); err == nil {
		t.Fatal("denied RPC peer bypassed inspector")
	}
	if err := inspect(first, &pubsub.RPC{}); err != nil {
		t.Fatal(err)
	}
}
func TestGossipRouterReceivesGaterDirectPeersAndCustomProtocols(t *testing.T) {
	remote := newConfigTestHost(t)
	threshold := 0.17
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{DirectPeers: []string{"/ip4/192.0.2.1/tcp/4001/p2p/" + remote.ID().String()}, PeerGater: &model.PeerGaterConfig{Threshold: &threshold}, Protocols: []model.PubSubProtocolConfig{{ID: "/test/gossip/1", Features: []string{"mesh", "px"}}}, ProtocolMatch: "prefix", SeenMessagesStrategy: "last-seen"}}.WithDefaults().GossipSub
	ps, h := policyPubSub(t, config)
	if !slices.Contains(h.Mux().Protocols(), protocol.ID("/test/gossip/1")) {
		t.Fatalf("custom protocol absent from host: %v", h.Mux().Protocols())
	}
	router := reflect.ValueOf(ps).Elem().FieldByName("rt").Elem().Elem()
	if router.FieldByName("direct").Len() != 1 || router.FieldByName("gate").IsNil() {
		t.Fatal("direct peers or peer gater option ignored")
	}
	if actual := router.FieldByName("gate").Elem().FieldByName("params").Elem().FieldByName("Threshold").Float(); actual != threshold {
		t.Fatalf("peer gater threshold=%v", actual)
	}
	if state := reflect.ValueOf(ps).Elem().FieldByName("seenMsgStrategy").Uint(); state != 1 {
		t.Fatalf("last-seen strategy=%d", state)
	}
	feature := gossipProtocolFeatures(config)
	if !feature(pubsub.GossipSubFeatureMesh, "/test/gossip/1/custom") || !feature(pubsub.GossipSubFeaturePX, "/test/gossip/1") || feature(pubsub.GossipSubFeatureIdontwant, "/test/gossip/1") {
		t.Fatal("protocol feature policy ignored")
	}
}
func TestGossipBlacklistExpiresWithoutBackgroundWorkers(t *testing.T) {
	id := newConfigTestHost(t).ID()
	blacklist := &gossipBlacklist{ttl: time.Millisecond, entries: make(map[peer.ID]time.Time)}
	if !blacklist.Add(id) || !blacklist.Contains(id) || blacklist.Add(id) {
		t.Fatal("initial blacklist admission incorrect")
	}
	blacklist.mu.Lock()
	blacklist.entries[id] = time.Now().Add(-time.Second)
	blacklist.mu.Unlock()
	if blacklist.Contains(id) || !blacklist.Add(id) {
		t.Fatal("expired blacklist entry did not recover")
	}
}

type policyDiscovery struct{ advertisements chan discovery.Options }

func (d *policyDiscovery) Advertise(_ context.Context, _ string, opts ...discovery.Option) (time.Duration, error) {
	var options discovery.Options
	if err := options.Apply(opts...); err != nil {
		return 0, err
	}
	select {
	case d.advertisements <- options:
	default:
	}
	return time.Hour, nil
}
func (d *policyDiscovery) FindPeers(_ context.Context, _ string, opts ...discovery.Option) (<-chan peer.AddrInfo, error) {
	channel := make(chan peer.AddrInfo)
	close(channel)
	return channel, nil
}
func TestGossipDiscoveryPassesTTLAndLimitToProvider(t *testing.T) {
	limit := 7
	config := model.PubSubDiscoveryConfig{Mode: "routing", TTL: "20m", Limit: &limit, Connector: &model.PubSubDiscoveryConnectorConfig{Jitter: "none", MinBackoff: "1s", MaxBackoff: "1m"}}
	options, err := gossipDiscoverOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	provider := &policyDiscovery{advertisements: make(chan discovery.Options, 1)}
	gossip := model.NodeConfig{}.WithDefaults().GossipSub
	ps, _ := policyPubSub(t, gossip, pubsub.WithDiscovery(provider, options...))
	policyTopic(t, ps, gossip, gossip.Topics[0])
	select {
	case got := <-provider.advertisements:
		if got.Ttl != 20*time.Minute || got.Limit != 7 {
			t.Fatalf("discovery options=%+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("discovery provider never advertised")
	}
}

func TestGossipBasicSequenceValidatorRejectsReplayAndMalformedNonce(t *testing.T) {
	validator, _, err := gossipValidator(model.PubSubValidatorConfig{BasicSeqno: true})
	if err != nil {
		t.Fatal(err)
	}
	id := newConfigTestHost(t).ID()
	nonce := make([]byte, 8)
	binary.BigEndian.PutUint64(nonce, 10)
	message := &pubsub.Message{Message: &pb.Message{From: []byte(id), Seqno: nonce}}
	if got := validator(t.Context(), id, message); got != pubsub.ValidationAccept {
		t.Fatalf("initial nonce rejected: %v", got)
	}
	if got := validator(t.Context(), id, message); got != pubsub.ValidationIgnore {
		t.Fatalf("replayed nonce not ignored: %v", got)
	}
	binary.BigEndian.PutUint64(nonce, 11)
	if got := validator(t.Context(), id, message); got != pubsub.ValidationAccept {
		t.Fatalf("new nonce rejected: %v", got)
	}
	message.Seqno = []byte{1}
	if got := validator(t.Context(), id, message); got != pubsub.ValidationReject {
		t.Fatalf("malformed nonce not rejected: %v", got)
	}
}
func TestGossipLocalPublicationOptionReachesSubscription(t *testing.T) {
	local := true
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Publish: &model.PubSubPublishConfig{Local: &local}}}.WithDefaults().GossipSub
	ps, _ := policyPubSub(t, config)
	topic, sub := policyTopic(t, ps, config, config.Topics[0])
	options, err := gossipPublishOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := topic.Publish(t.Context(), []byte("local"), options...); err != nil {
		t.Fatal(err)
	}
	if message := nextPolicyMessage(t, sub); !message.Local {
		t.Fatal("local publication flag did not reach actual PubSub message")
	}
}
func TestGossipValidatorTimeoutAppliesToActualPublication(t *testing.T) {
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{DefaultValidators: []model.PubSubValidatorConfig{{Delay: "1s", Timeout: "10ms"}}}}.WithDefaults().GossipSub
	ps, _ := policyPubSub(t, config)
	topic, _ := policyTopic(t, ps, config, config.Topics[0])
	started := time.Now()
	if err := topic.Publish(t.Context(), []byte("timeout")); err == nil {
		t.Fatal("timed out validator accepted publication")
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("validator timeout was ignored")
	}
}
