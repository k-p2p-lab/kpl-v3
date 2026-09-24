package peer

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	corehost "github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
)

type discardEventTracer struct{}

func TestHostOptionsPreserveV2TCPNoiseYamuxStack(t *testing.T) {
	options, err := hostOptions(model.NodeConfig{}.WithDefaults())
	if err != nil {
		t.Fatal(err)
	}
	var config libp2p.Config
	if err := config.Apply(options...); err != nil {
		t.Fatal(err)
	}
	if len(config.Transports) != 1 || len(config.SecurityTransports) != 1 || config.SecurityTransports[0].ID != noise.ID || len(config.Muxers) != 1 || config.Muxers[0].ID != yamux.ID {
		t.Fatalf("v2 transport stack was changed: transports=%d security=%v muxers=%v", len(config.Transports), config.SecurityTransports, config.Muxers)
	}
	createHost := func() corehost.Host {
		h, err := libp2p.New(append(options, libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	first, second := createHost(), createHost()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := first.Connect(ctx, corepeer.AddrInfo{ID: second.ID(), Addrs: second.Addrs()}); err != nil {
		t.Fatal(err)
	}
	connections := first.Network().ConnsToPeer(second.ID())
	if len(connections) == 0 {
		t.Fatal("hosts did not connect")
	}
	state := connections[0].ConnState()
	if state.Security != noise.ID || state.StreamMultiplexer != yamux.ID || state.Transport != "tcp" {
		t.Fatalf("negotiated unexpected stack: %+v", state)
	}
}

func (discardEventTracer) Trace(*pubsubpb.TraceEvent) {}

func newConfigTestHost(t *testing.T) corehost.Host {
	t.Helper()
	host, err := libp2p.New(libp2p.NoListenAddrs, libp2p.DisableMetrics())
	if err != nil {
		t.Fatalf("create test libp2p host: %v", err)
	}
	t.Cleanup(func() { _ = host.Close() })
	return host
}

func hasProtocol(protocols []protocol.ID, target protocol.ID) bool {
	for _, candidate := range protocols {
		if candidate == target {
			return true
		}
	}
	return false
}

func gossipSubParams(t *testing.T, instance *pubsub.PubSub) reflect.Value {
	t.Helper()
	router := reflect.ValueOf(instance).Elem().FieldByName("rt")
	if !router.IsValid() || router.IsNil() {
		t.Fatal("pubsub router is unavailable")
	}
	router = router.Elem()
	if router.Kind() == reflect.Pointer {
		router = router.Elem()
	}
	params := router.FieldByName("params")
	if !params.IsValid() {
		t.Fatalf("router %s does not expose GossipSub params", router.Type())
	}
	return params
}

func TestGossipSubOptionsPreserveResolvedDefaultsAndZeros(t *testing.T) {
	config, ok := model.BuiltInNodeConfig("non-gossip")
	if !ok {
		t.Fatal("non-gossip built-in profile is missing")
	}
	zero := 0
	config = config.Merge(model.NodeConfig{
		GossipSub: model.GossipSubConfig{
			Params: model.GossipSubParamsConfig{DOut: &zero},
		},
	}).WithDefaults()
	options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
	if err != nil {
		t.Fatalf("build gossipsub options: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	instance, err := pubsub.NewGossipSub(ctx, newConfigTestHost(t), options...)
	if err != nil {
		t.Fatalf("create gossipsub: %v", err)
	}
	params := gossipSubParams(t, instance)

	if got := params.FieldByName("HistoryGossip").Int(); got != 0 {
		t.Fatalf("HistoryGossip = %d, want explicit zero", got)
	}
	if got := params.FieldByName("GossipFactor").Float(); got != 0 {
		t.Fatalf("GossipFactor = %v, want explicit zero", got)
	}
	if got := params.FieldByName("Dout").Int(); got != 0 {
		t.Fatalf("Dout = %d, want explicit zero", got)
	}
	if got := params.FieldByName("Dlo").Int(); got != 5 {
		t.Fatalf("Dlo = %d, want v2 worker default 5", got)
	}
	if got := params.FieldByName("Dscore").Int(); got != 3 {
		t.Fatalf("Dscore = %d, want v2 worker default 3", got)
	}
	if got := params.FieldByName("MaxIHaveLength").Int(); got != 5500 {
		t.Fatalf("MaxIHaveLength = %d, want v2 worker default 5500", got)
	}
	if got := time.Duration(params.FieldByName("HeartbeatInitialDelay").Int()); got != time.Second {
		t.Fatalf("HeartbeatInitialDelay = %s, want 1s", got)
	}
}

func TestDHTOptionsDistinguishPrefixAndProtocolOverride(t *testing.T) {
	tests := []struct {
		name   string
		config model.KademliaConfig
		want   protocol.ID
	}{
		{
			name: "prefix receives kad suffix",
			config: model.KademliaConfig{
				Mode:           "server",
				ProtocolPrefix: "/k-p2p-lab/v3-test",
			},
			want: "/k-p2p-lab/v3-test/kad/1.0.0",
		},
		{
			name: "complete protocol id is not modified",
			config: model.KademliaConfig{
				Mode:       "server",
				ProtocolID: "/k-p2p-lab/custom-kad/9.9.9",
			},
			want: "/k-p2p-lab/custom-kad/9.9.9",
		},
		{
			name: "extension is inserted before kad suffix",
			config: model.KademliaConfig{
				Mode:              "server",
				ProtocolPrefix:    "/k-p2p-lab/v3-test",
				ProtocolExtension: "/private",
			},
			want: "/k-p2p-lab/v3-test/private/kad/1.0.0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options, err := dhtOptions(test.config)
			if err != nil {
				t.Fatalf("build DHT options: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			host := newConfigTestHost(t)
			instance, err := dht.New(ctx, host, options...)
			if err != nil {
				t.Fatalf("create DHT: %v", err)
			}
			defer instance.Close()
			if protocols := host.Mux().Protocols(); !hasProtocol(protocols, test.want) {
				t.Fatalf("DHT protocols = %v, want %q", protocols, test.want)
			}
		})
	}
}

func TestPubSubRouterOptionsConstructRequestedRouter(t *testing.T) {
	tests := []struct {
		kind         string
		wantProtocol protocol.ID
	}{
		{kind: "full", wantProtocol: pubsub.GossipSubID_v12},
		{kind: "flood", wantProtocol: pubsub.FloodSubID},
		{kind: "random", wantProtocol: pubsub.RandomSubID},
	}

	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			config, ok := model.BuiltInNodeConfig(test.kind)
			if !ok {
				t.Fatalf("built-in profile %q is missing", test.kind)
			}
			options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
			if err != nil {
				t.Fatalf("build %s options: %v", config.GossipSub.Router, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			host := newConfigTestHost(t)
			switch config.GossipSub.Router {
			case "gossipsub":
				_, err = pubsub.NewGossipSub(ctx, host, options...)
			case "floodsub":
				_, err = pubsub.NewFloodSub(ctx, host, options...)
			case "randomsub":
				_, err = pubsub.NewRandomSub(ctx, host, *config.GossipSub.RandomNetworkSize, options...)
			default:
				t.Fatalf("unexpected router %q", config.GossipSub.Router)
			}
			if err != nil {
				t.Fatalf("construct %s: %v", config.GossipSub.Router, err)
			}
			if protocols := host.Mux().Protocols(); !hasProtocol(protocols, test.wantProtocol) {
				t.Fatalf("host protocols = %v, want %q", protocols, test.wantProtocol)
			}
		})
	}
}

func TestStartPubSubSeparatesRandomNetworkSizeAndDegree(t *testing.T) {
	config, ok := model.BuiltInNodeConfig("random")
	if !ok {
		t.Fatal("random built-in profile is missing")
	}
	degree, networkSize := 7, 144
	config.GossipSub.RandomDegree = &degree
	config.GossipSub.RandomNetworkSize = &networkSize
	config.GossipSub.TopicMode = "publish"

	oldDegree := pubsub.RandomSubD
	t.Cleanup(func() { pubsub.RandomSubD = oldDegree })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := &Server{
		config:    model.PeerProcessConfig{NodeConfig: config},
		host:      newConfigTestHost(t),
		topics:    make(map[string]*pubsub.Topic),
		telemetry: newTelemetry(model.Node{}, "", "", nil),
	}
	if err := server.startPubSub(ctx); err != nil {
		t.Fatalf("start randomsub: %v", err)
	}
	if pubsub.RandomSubD != degree {
		t.Fatalf("randomsub degree = %d, want %d", pubsub.RandomSubD, degree)
	}
	router := reflect.ValueOf(server.pubsub).Elem().FieldByName("rt").Elem()
	if router.Kind() == reflect.Pointer {
		router = router.Elem()
	}
	if got := int(router.FieldByName("size").Int()); got != networkSize {
		t.Fatalf("randomsub network size = %d, want %d", got, networkSize)
	}
}

func TestHostOptionsApplyDialTimeout(t *testing.T) {
	config := model.NodeConfig{Libp2p: model.Libp2pConfig{DialTimeout: "750ms"}}.WithDefaults()
	options, err := hostOptions(config)
	if err != nil {
		t.Fatalf("build host options: %v", err)
	}
	var applied libp2p.Config
	if err := applied.Apply(options...); err != nil {
		t.Fatalf("apply host options: %v", err)
	}
	if applied.DialTimeout != 750*time.Millisecond {
		t.Fatalf("libp2p dial timeout = %s, want 750ms", applied.DialTimeout)
	}
}

func TestConnectionLimitDoesNotImplicitlyEnableSoftConnectionManager(t *testing.T) {
	limit := 55
	config := model.NodeConfig{Libp2p: model.Libp2pConfig{ConnectionLimit: &limit}}.WithDefaults()
	options, err := hostOptions(config)
	if err != nil {
		t.Fatalf("build host options: %v", err)
	}
	var applied libp2p.Config
	if err := applied.Apply(options...); err != nil {
		t.Fatalf("apply host options: %v", err)
	}
	if applied.ConnManager != nil {
		t.Fatal("hard connectionLimit unexpectedly installed a soft connection manager")
	}
}

func TestPeerScoreThresholdSkipValidationAndSnapshot(t *testing.T) {
	_, thresholds, err := peerScoreOptions(model.PeerScoreConfig{Enabled: scoreEnabled(),
		SkipAtomicValidation: true,
		Thresholds: model.PeerScoreThresholdsConfig{
			SkipAtomicValidation: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !thresholds.SkipAtomicValidation {
		t.Fatal("threshold skipAtomicValidation was not mapped")
	}

	server := &Server{}
	peerID := corepeer.ID("peer-a")
	server.recordPeerScores(map[corepeer.ID]float64{peerID: 1.25})
	server.scoreMu.RLock()
	score := server.peerScores[peerID.String()]
	server.scoreMu.RUnlock()
	if score != 1.25 {
		t.Fatalf("peer score snapshot = %v, want 1.25", score)
	}
}

func TestPeerScoreConfigurationPassesModelAndRouterValidation(t *testing.T) {
	score := model.PeerScoreConfig{Enabled: scoreEnabled(),
		SkipAtomicValidation: true,
		DecayInterval:        "1s",
		DecayToZero:          0.01,
		Thresholds: model.PeerScoreThresholdsConfig{
			SkipAtomicValidation: true,
		},
		Topics: map[string]model.TopicScoreConfig{
			"kpl/default": {
				SkipAtomicValidation:           true,
				InvalidMessageDeliveriesWeight: -1,
				InvalidMessageDeliveriesDecay:  0.5,
			},
		},
	}
	config, ok := model.BuiltInNodeConfig("full")
	if !ok {
		t.Fatal("full preset is missing")
	}
	config.GossipSub.Score = &score
	config = config.WithDefaults()
	if err := config.Validate(); err != nil {
		t.Fatalf("model rejected valid peer score: %v", err)
	}
	options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := pubsub.NewGossipSub(ctx, newConfigTestHost(t), options...); err != nil {
		t.Fatalf("router rejected model-validated peer score: %v", err)
	}
}

func TestPubSubOptionsPreserveSupportedZeroLimits(t *testing.T) {
	zero := 0
	config, ok := model.BuiltInNodeConfig("full")
	if !ok {
		t.Fatal("full preset is missing")
	}
	config.GossipSub.MaxMessageSize = &zero
	config.GossipSub.ValidateThrottle = &zero
	if err := config.Validate(); err != nil {
		t.Fatalf("model rejected supported zero limits: %v", err)
	}
	options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	instance, err := pubsub.NewGossipSub(ctx, newConfigTestHost(t), options...)
	if err != nil {
		t.Fatalf("construct gossipsub with zero limits: %v", err)
	}
	value := reflect.ValueOf(instance).Elem()
	if got := int(value.FieldByName("maxMessageSize").Int()); got != 0 {
		t.Fatalf("maxMessageSize = %d, want 0", got)
	}
	validation := value.FieldByName("val")
	if validation.Kind() == reflect.Pointer {
		validation = validation.Elem()
	}
	if got := validation.FieldByName("validateThrottle").Cap(); got != 0 {
		t.Fatalf("validateThrottle capacity = %d, want 0", got)
	}
}

func TestPeerScoreOptionsRequireAppSpecificScore(t *testing.T) {
	_, _, err := peerScoreOptions(model.PeerScoreConfig{Enabled: scoreEnabled(), AppSpecificWeight: 1})
	if err == nil {
		t.Fatal("non-zero appSpecificWeight without appSpecificScore was silently accepted")
	}
}
