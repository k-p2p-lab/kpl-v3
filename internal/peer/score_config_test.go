package peer

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corehost "github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestApplicationScoreUsesImmutableExplicitPeerOverrides(t *testing.T) {
	specific, unknown := newConfigTestHost(t), newConfigTestHost(t)
	policy := &model.AppSpecificScoreConfig{Default: 3, Peers: map[string]float64{specific.ID().String(): 0}}
	score, _, err := peerScoreOptions(model.PeerScoreConfig{Enabled: scoreEnabled(), SkipAtomicValidation: true, AppSpecificWeight: -2, AppSpecificScore: policy})
	if err != nil {
		t.Fatal(err)
	}
	policy.Default = 9
	policy.Peers[specific.ID().String()] = 8
	if got := score.AppSpecificScore(specific.ID()); got != 0 {
		t.Fatalf("explicit zero score = %g", got)
	}
	if got := score.AppSpecificScore(unknown.ID()); got != 3 {
		t.Fatalf("default score = %g", got)
	}
	if score.AppSpecificWeight != -2 {
		t.Fatalf("negative application weight = %g", score.AppSpecificWeight)
	}
	for _, bad := range []model.PeerScoreConfig{
		{Enabled: scoreEnabled(), SkipAtomicValidation: true, DecayInterval: "0s"},
		{Enabled: scoreEnabled(), SkipAtomicValidation: true, BehaviourPenaltyDecay: math.NaN()},
		{Enabled: scoreEnabled(), SkipAtomicValidation: true, AppSpecificScore: &model.AppSpecificScoreConfig{Default: math.Inf(1)}},
	} {
		if _, _, err := peerScoreOptions(bad); err == nil {
			t.Fatal("runtime accepted invalid scoring parameters")
		}
	}
}

// Exercise score inspection after actual GRAFT and after the decay worker has
// ticked. Router construction alone misses selective-validation defaults that
// otherwise panic in NewTicker(0) or the mesh-time division by zero.
func TestActualMeshApplicationScoreWithOmittedSelectiveGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	createHost := func() corehost.Host {
		t.Helper()
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	firstHost, secondHost := createHost(), createHost()
	create := func(id string, h corehost.Host, policy *model.AppSpecificScoreConfig) *Server {
		t.Helper()
		config := model.NodeConfig{GossipSub: model.GossipSubConfig{
			Topics: []string{"score/topic"}, ScoreInspectInterval: "20ms",
			Params: model.GossipSubParamsConfig{HeartbeatInitialDelay: "10ms", HeartbeatInterval: "100ms"},
			Score: &model.PeerScoreConfig{Enabled: scoreEnabled(),
				SkipAtomicValidation: true, AppSpecificWeight: 1.5, AppSpecificScore: policy,
				Topics: map[string]model.TopicScoreConfig{"score/topic": {SkipAtomicValidation: true, TopicWeight: 1}},
			},
		}}.WithDefaults()
		if err := config.Validate(); err != nil {
			t.Fatalf("score config: %v", err)
		}
		server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: id, RunID: "score-test"}, NodeConfig: config}, host: h,
			topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{node: model.Node{ID: id, RunID: "score-test"}, events: make(chan model.TraceEvent)},
		}
		t.Cleanup(server.Close)
		if err := server.startPubSub(ctx); err != nil {
			t.Fatalf("start scoring peer: %v", err)
		}
		return server
	}
	first := create("first", firstHost, &model.AppSpecificScoreConfig{Default: 2, Peers: map[string]float64{secondHost.ID().String(): 4}})
	second := create("second", secondHost, &model.AppSpecificScoreConfig{Default: 2})
	if err := firstHost.Connect(ctx, corepeer.AddrInfo{ID: secondHost.ID(), Addrs: secondHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		firstMesh, _ := first.mesh.snapshot()
		secondMesh, _ := second.mesh.snapshot()
		first.scoreMu.RLock()
		firstScore, firstObserved := first.peerScores[secondHost.ID().String()]
		first.scoreMu.RUnlock()
		second.scoreMu.RLock()
		secondScore, secondObserved := second.peerScores[firstHost.ID().String()]
		second.scoreMu.RUnlock()
		if len(firstMesh["score/topic"]) == 1 && len(secondMesh["score/topic"]) == 1 && firstObserved && secondObserved && firstScore == 6 && secondScore == 3 && time.Since(start) > 1200*time.Millisecond {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("actual mesh scores did not converge: mesh=%v/%v scores=%v(%v)/%v(%v)", firstMesh, secondMesh, firstScore, firstObserved, secondScore, secondObserved)
		}
	}
}

func scoreEnabled() *bool { enabled := true; return &enabled }

func TestScoreIsDisabledWithoutExplicitOptIn(t *testing.T) {
	disabled := false
	for _, policy := range []*model.PeerScoreConfig{
		nil,
		{},
		{AppSpecificWeight: 2},
		{Enabled: &disabled, AppSpecificWeight: 2},
	} {
		config := model.NodeConfig{GossipSub: model.GossipSubConfig{Score: policy, ScoreInspectInterval: "10ms"}}.WithDefaults()
		if err := config.Validate(); err != nil {
			t.Fatalf("inactive score configuration rejected: %v", err)
		}
		options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		instance, err := pubsub.NewGossipSub(ctx, newConfigTestHost(t), options...)
		if err != nil {
			cancel()
			t.Fatalf("disabled score could not construct router: %v", err)
		}
		router := reflect.ValueOf(instance).Elem().FieldByName("rt").Elem().Elem()
		if !router.FieldByName("score").IsNil() {
			cancel()
			t.Fatal("score block without enabled:true activated peer scoring")
		}
		cancel()
	}
}
