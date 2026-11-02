package peer

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func attackBool(value bool) *bool        { return &value }
func attackFloat(value float64) *float64 { return &value }
func attackInt(value int) *int           { return &value }
func waitAttackCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("attack condition did not converge")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func attackTestChain(t *testing.T, configure func([]host.Host) model.NodeConfig) ([]*Server, *pubsub.Subscription, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	hosts := make([]host.Host, 3)
	servers := make([]*Server, 3)
	t.Cleanup(func() {
		cancel()
		for _, s := range servers {
			if s != nil && s.pubsub != nil {
				drain, stop := context.WithTimeout(context.Background(), time.Second)
				_ = s.pubsub.WaitInboundMessagePolicy(drain)
				stop()
				s.Close()
			} else if s != nil && s.host != nil {
				s.host.Close()
			}
		}
	})
	for i := range hosts {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
		servers[i] = &Server{host: h}
	}
	middle := configure(hosts)
	for i := range hosts {
		cfg, _ := model.BuiltInNodeConfig("gossip-only")
		cfg.GossipSub.Topics = []string{"blocks", "control"}
		if middle.GossipSub.MessageID != "" {
			cfg.GossipSub.MessageID = middle.GossipSub.MessageID
		}
		if middle.GossipSub.SignaturePolicy != "" {
			cfg.GossipSub.SignaturePolicy = middle.GossipSub.SignaturePolicy
			cfg.GossipSub.Author = middle.GossipSub.Author
		}
		if i == 1 {
			cfg = cfg.Merge(middle)
		}
		for j, other := range hosts {
			if j == i-1 || j == i+1 {
				cfg.GossipSub.DirectPeers = append(cfg.GossipSub.DirectPeers, fmt.Sprintf("%s/p2p/%s", other.Addrs()[0], other.ID()))
			}
		}
		cfg = cfg.WithDefaults()
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		node := model.Node{ID: fmt.Sprintf("peer-%d", i), RunID: "run", Generation: 2, Role: "worker", Group: "workers"}
		server := servers[i]
		server.config = model.PeerProcessConfig{Node: node, NodeConfig: cfg, Seed: 42}
		server.topics = make(map[string]*pubsub.Topic)
		server.peerScores = map[string]float64{}
		server.telemetry = newTelemetry(node, "", "", slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err := server.startPubSub(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := hosts[i].Connect(ctx, corepeer.AddrInfo{ID: hosts[i+1].ID(), Addrs: hosts[i+1].Addrs()}); err != nil {
			t.Fatal(err)
		}
	}
	waitAttackCondition(t, func() bool {
		return len(servers[0].pubsub.ListPeers("blocks")) == 1 && len(servers[1].pubsub.ListPeers("blocks")) == 2 && len(servers[2].pubsub.ListPeers("blocks")) == 1
	})
	sub, err := servers[2].topics["blocks"].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Cancel)
	return servers, sub, cancel
}
func expectAttackMessage(t *testing.T, sub *pubsub.Subscription, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	msg, err := sub.Next(ctx)
	if err != nil || string(msg.Data) != want {
		t.Fatalf("message=%v err=%v want=%s", msg, err, want)
	}
}
func expectAttackSilence(t *testing.T, sub *pubsub.Subscription) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	msg, err := sub.Next(ctx)
	if err == nil {
		t.Fatalf("suppressed message delivered: %s", msg.Data)
	}
}
func publishAttackMessage(t *testing.T, s *Server, data string) {
	t.Helper()
	if err := s.topics["blocks"].Publish(t.Context(), []byte(data)); err != nil {
		t.Fatal(err)
	}
}
func TestAttackSinkKeepsConnectionsAndRuntimeIdentity(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true), Probability: attackFloat(1)}}
	})
	publishAttackMessage(t, servers[0], "blocked")
	expectAttackSilence(t, sub)
	if servers[1].attacks.snapshot()["sink"]["dropped"] != 1 || len(servers[1].pubsub.ListPeers("blocks")) != 2 {
		t.Fatal("sink disrupted topic membership or did not record drop")
	}
	original := servers[1].host.ID()
	request := model.ProfileUpdateRequest{RunID: "run", Generation: 2, Revision: 1, Stage: "prepare", Set: model.RuntimeProfilePatch{Sink: &model.ProbabilityAttack{Probability: attackFloat(0)}}}
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	publishAttackMessage(t, servers[0], "still blocked during prepare")
	expectAttackSilence(t, sub)
	request.Stage = "apply"
	response, err := servers[1].executeProfileUpdate(t.Context(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.ValidateProfileAcknowledgement(request, response); err != nil {
		t.Fatal(err)
	}
	publishAttackMessage(t, servers[0], "passes at zero")
	expectAttackMessage(t, sub, "passes at zero")
	if servers[1].host.ID() != original {
		t.Fatal("policy update replaced peer identity")
	}
	request.Revision = 2
	request.Stage = "prepare"
	request.Set.Sink = &model.ProbabilityAttack{Enabled: attackBool(false), Probability: attackFloat(1)}
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	request.Stage = "apply"
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	publishAttackMessage(t, servers[0], "passes while disabled")
	expectAttackMessage(t, sub, "passes while disabled")
}
func TestAttackSinkExcludesLocalPublicationAndDeduplicates(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true)}, GossipSub: model.GossipSubConfig{MessageID: "sha256"}}
	})
	publishAttackMessage(t, servers[1], "local publication")
	expectAttackMessage(t, sub, "local publication")
	for range 2 {
		publishAttackMessage(t, servers[0], "same message")
	}
	expectAttackSilence(t, sub)
	if counts := servers[1].attacks.snapshot()["sink"]; counts["considered"] != 1 || counts["dropped"] != 1 {
		t.Fatalf("duplicates became new probability samples: %v", counts)
	}
}
func TestAttackCensorshipUsesSignedOriginalAuthor(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func(hosts []host.Host) model.NodeConfig {
		return model.NodeConfig{Censorship: &model.CensorshipConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Topics: []string{"blocks"}, Publishers: &model.AttackSelector{PeerIDs: []string{hosts[0].ID().String()}}}}
	})
	publishAttackMessage(t, servers[0], `{"publisher":"pretend-someone-else"}`)
	expectAttackSilence(t, sub)
	key, _, err := libcrypto.GenerateEd25519Key(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	author, err := corepeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"publisher":"peer-0"}`
	if err := servers[0].topics["blocks"].Publish(t.Context(), []byte(payload), pubsub.WithSecretKeyAndPeerId(key, author)); err != nil {
		t.Fatal(err)
	}
	expectAttackMessage(t, sub, payload)
	if servers[1].attacks.snapshot()["censorship"]["dropped"] != 1 {
		t.Fatal("censorship used payload header instead of signed author")
	}
	control, err := servers[2].topics["control"].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer control.Cancel()
	if err := servers[0].topics["control"].Publish(t.Context(), []byte("unattacked topic")); err != nil {
		t.Fatal(err)
	}
	expectAttackMessage(t, control, "unattacked topic")
}
func TestAttackMessageDelayBoundedOverflowAndCancellation(t *testing.T) {
	servers, sub, cancel := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{MessageDelay: &model.MessageDelayConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Delay: "1m", MaxPendingMessages: attackInt(1), MaxPendingBytes: attackInt(1024)}}
	})
	publishAttackMessage(t, servers[0], "first")
	waitAttackCondition(t, func() bool { return servers[1].attacks.snapshot()["messageDelay"]["queued"] == 1 })
	publishAttackMessage(t, servers[0], "overflow")
	waitAttackCondition(t, func() bool { return servers[1].attacks.snapshot()["messageDelay"]["overflow"] == 1 })
	if len(servers[1].pubsub.ListPeers("blocks")) != 2 {
		t.Fatal("delay blocked router processing")
	}
	expectAttackSilence(t, sub)
	cancel()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := servers[1].pubsub.WaitInboundMessagePolicy(ctx); err != nil {
		t.Fatal(err)
	}
	if counts := servers[1].attacks.snapshot()["messageDelay"]; counts["canceled"] != 1 || counts["dropped"] != 2 {
		t.Fatalf("pending data lost at shutdown: %v", counts)
	}
}
func TestAttackMessageDelayReleasesToNormalValidation(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{MessageDelay: &model.MessageDelayConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Delay: "200ms"}}
	})
	start := time.Now()
	publishAttackMessage(t, servers[0], "delayed")
	expectAttackMessage(t, sub, "delayed")
	if time.Since(start) < 190*time.Millisecond {
		t.Fatal("message was not delayed")
	}
	if counts := servers[1].attacks.snapshot()["messageDelay"]; counts["queued"] != 1 || counts["released"] != 1 {
		t.Fatalf("wrong delay counts: %v", counts)
	}
}
func TestAttackProbabilityIsStableAndMessageScoped(t *testing.T) {
	selected := 0
	for i := 0; i < 10000; i++ {
		id := fmt.Sprint(i)
		first := attackSelected(42, "worker", "sink", "blocks", id, "", 0.3)
		if first != attackSelected(42, "worker", "sink", "blocks", id, "", 0.3) {
			t.Fatal("unstable decision")
		}
		if first {
			selected++
		}
	}
	if selected < 2700 || selected > 3300 {
		t.Fatalf("unexpected sampling distribution: %d", selected)
	}
	if attackSelected(42, "worker", "sink", "blocks", "id", "", 0) || !attackSelected(42, "worker", "sink", "blocks", "id", "", 1) {
		t.Fatal("wrong probability boundary")
	}
}

func TestAttackStatusOmitsUnsupportedDormantRuntime(t *testing.T) {
	server := &Server{config: model.PeerProcessConfig{NodeConfig: model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(false)}, GossipSub: model.GossipSubConfig{Enabled: attackBool(false)}}.WithDefaults()}}
	if snapshot := server.profileStatusSnapshot(server.config.NodeConfig); snapshot.HasAttacks() {
		t.Fatal("inactive unsupported attack leaked into recoverable runtime snapshot")
	}
}

func TestAttackUnsignedDormantPolicyCanActivate(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(false)}, GossipSub: model.GossipSubConfig{MessageID: "sha256", SignaturePolicy: "strict-no-sign", Author: &model.PubSubAuthorConfig{NoAuthor: true}}}
	})
	publishAttackMessage(t, servers[0], "unsigned normal")
	expectAttackMessage(t, sub, "unsigned normal")
	updated := servers[1].attacks.policy.Load().config
	updated = updated.Merge(model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true)}}).WithDefaults()
	servers[1].attacks.update(updated, 1)
	publishAttackMessage(t, servers[0], "unsigned sink")
	expectAttackSilence(t, sub)
	if servers[1].attacks.snapshot()["sink"]["dropped"] != 1 {
		t.Fatal("unsigned data bypassed active policy")
	}
}
func TestAttackControlGeneratorsReachRealRemoteRPCs(t *testing.T) {
	servers, _, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{GraftFlood: &model.GraftFloodConfig{ControlAttackConfig: model.ControlAttackConfig{Enabled: attackBool(true), Topics: []string{"blocks"}, Interval: "20ms"}}, IHaveSpam: &model.IHaveSpamConfig{ControlAttackConfig: model.ControlAttackConfig{Enabled: attackBool(true), Topics: []string{"blocks"}, Interval: "20ms"}, IDsPerRPC: attackInt(3)}}
	})
	runtime := servers[1].attacks
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{}, 2)
	go func() { defer func() { done <- struct{}{} }(); runtime.graftLoop(ctx) }()
	go func() { defer func() { done <- struct{}{} }(); runtime.ihaveLoop(ctx) }()
	defer func() { cancel(); <-done; <-done }()
	seen := map[string]bool{}
	timeout := time.After(3 * time.Second)
	for !seen["recv_graft"] || !seen["recv_ihave"] {
		select {
		case event := <-servers[0].telemetry.events:
			seen[event.Type] = true
		case <-timeout:
			t.Fatalf("generated controls did not reach remote: %v", seen)
		}
	}
	waitAttackCondition(t, func() bool {
		counts := runtime.snapshot()
		return counts["graftFlood"]["queuedRPCs"] > 0 && counts["ihaveSpam"]["queuedRPCs"] > 0
	})
}
func TestAttackConnectionAcquiresOnlySelectedNewTransport(t *testing.T) {
	servers, _, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: &model.AttackSelector{Groups: []string{"victims"}}, TargetConnections: attackInt(1), RetryInterval: "20ms"}}
	})
	target, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	runtime := servers[1].attacks
	runtime.registry.Store(newAttackRegistry([]model.AttackTarget{{NodeID: "target", Group: "victims", PeerID: target.ID().String(), Addresses: []string{target.Addrs()[0].String()}, Ready: true}}, time.Now()))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); runtime.connectionLoop(ctx) }()
	defer func() { cancel(); <-done }()
	waitAttackCondition(t, func() bool { return runtime.snapshot()["connectionAttack"]["dialSucceeded"] == 1 })
	if len(servers[1].host.Network().ConnsToPeer(target.ID())) == 0 {
		t.Fatal("targeted dial was not a real connection")
	}
}
func TestAttackIdentityAmbiguityAndStaleEvidence(t *testing.T) {
	config := model.NodeConfig{Censorship: &model.CensorshipConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Publishers: &model.AttackSelector{Groups: []string{"victims"}}}}.WithDefaults()
	rows := []model.AttackTarget{{NodeID: "one", Group: "victims", PeerID: "transport", AuthorPeerID: "shared"}, {NodeID: "two", Group: "others", PeerID: "transport", AuthorPeerID: "shared"}}
	if validateAttackPublisherTargets(config, rows) == nil {
		t.Fatal("partially selected shared signing identity accepted")
	}
	if validateAttackPublisherTargets(config, []model.AttackTarget{{NodeID: "anonymous", Group: "victims"}}) == nil {
		t.Fatal("anonymous selected author accepted")
	}
	runtime := &attackRuntime{}
	runtime.registry.Store(newAttackRegistry(rows, time.Now()))
	if _, known := runtime.matchTransport(config.Censorship.Publishers, "transport"); known {
		t.Fatal("ambiguous transport treated as known")
	}
	if _, known := runtime.matchPublisher(config.Censorship.Publishers, "shared"); known {
		t.Fatal("ambiguous author treated as known")
	}
	runtime.registry.Store(newAttackRegistry(rows[:1], time.Now().Add(-10*time.Second)))
	if _, known := runtime.matchPublisher(config.Censorship.Publishers, "shared"); known {
		t.Fatal("stale identity evidence used")
	}
}

func TestAttackProbabilityUsesActualWireIDOnRelay(t *testing.T) {
	servers, sub, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{Sink: &model.ProbabilityAttack{Enabled: attackBool(true), Probability: attackFloat(0.3)}, GossipSub: model.GossipSubConfig{MessageID: "sha256"}}
	})
	dropped := uint64(0)
	for i := 0; i < 32; i++ {
		data := fmt.Sprintf("sample-%d", i)
		digest := sha256.Sum256([]byte(data))
		drop := attackSelected(42, "peer-1", "sink", "blocks", string(digest[:]), "", 0.3)
		publishAttackMessage(t, servers[0], data)
		if drop {
			dropped++
			want := dropped
			waitAttackCondition(t, func() bool { return servers[1].attacks.snapshot()["sink"]["dropped"] == want })
		} else {
			expectAttackMessage(t, sub, data)
		}
	}
	expectAttackSilence(t, sub)
	if counts := servers[1].attacks.snapshot()["sink"]; counts["considered"] != 32 || counts["dropped"] != dropped {
		t.Fatalf("wire decisions differ from configured samples: %v expected=%d", counts, dropped)
	}
}

func TestAttackWithholdingTraceIdentifiesRequester(t *testing.T) {
	servers, _, _ := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{IWantWithholding: &model.IWantWithholdingConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}}}
	})
	topic := "blocks"
	msg := &pubsub.Message{ID: "cached", ReceivedFrom: servers[0].host.ID(), Message: &pb.Message{Topic: &topic, From: []byte(servers[0].host.ID()), Data: []byte("data")}}
	if servers[1].attacks.allowIWant(servers[2].host.ID(), msg) {
		t.Fatal("response was not withheld")
	}
	timeout := time.After(time.Second)
	for {
		select {
		case event := <-servers[1].telemetry.events:
			if event.Type == "attack_decision" {
				if event.RemotePeerID != servers[2].host.ID().String() {
					t.Fatal("withholding attributed to cached sender instead of requester")
				}
				return
			}
		case <-timeout:
			t.Fatal("missing withholding decision trace")
		}
	}
}
