package peer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestAttackDelayedCancellationKeepsSelectionRevision(t *testing.T) {
	servers, _, cancel := attackTestChain(t, func([]host.Host) model.NodeConfig {
		return model.NodeConfig{MessageDelay: &model.MessageDelayConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Delay: "1m"}}
	})
	publishAttackMessage(t, servers[0], "queued under revision zero")
	waitAttackCondition(t, func() bool { return servers[1].attacks.snapshot()["messageDelay"]["queued"] == 1 })
	request := model.ProfileUpdateRequest{RunID: "run", Generation: 2, Revision: 1, Stage: "prepare", Set: model.RuntimeProfilePatch{MessageDelay: &model.ProbabilityAttack{Enabled: attackBool(false)}}}
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	request.Stage = "apply"
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	drain, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := servers[1].pubsub.WaitInboundMessagePolicy(drain); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-servers[1].telemetry.events:
			if event.Type == "attack_decision" && event.Fields["attack"] == "messageDelay" && event.Fields["decision"] == "canceled" {
				if fmt.Sprint(event.Fields["revision"]) != "0" {
					t.Fatalf("cancellation relabeled with later policy revision: %+v", event.Fields)
				}
				return
			}
		case <-drain.Done():
			t.Fatal("delay cancellation trace absent")
		}
	}
}

func TestAttackForwardingPreservesLocalReceptionAndRuntimeIdentity(t *testing.T) {
	testAttackForwardingRuntime(t, false)
}

func TestAttackForwardingUnsignedContentMessageIDs(t *testing.T) {
	testAttackForwardingRuntime(t, true)
}

func testAttackForwardingRuntime(t *testing.T, unsigned bool) {
	t.Helper()
	servers, downstream, _ := attackTestChain(t, func(h []host.Host) model.NodeConfig {
		config := model.NodeConfig{Forwarding: &model.ForwardingConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}, Targets: &model.AttackSelector{PeerIDs: []string{h[2].ID().String()}}}}
		if unsigned {
			config.GossipSub.MessageID = "sha256"
			config.GossipSub.SignaturePolicy = "strict-no-sign"
			config.GossipSub.Author = &model.PubSubAuthorConfig{NoAuthor: true}
		}
		return config
	})
	middle, err := servers[1].topics["blocks"].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer middle.Cancel()
	publishAttackMessage(t, servers[0], "accepted locally")
	expectAttackMessage(t, middle, "accepted locally")
	expectAttackSilence(t, downstream)
	publishAttackMessage(t, servers[1], "local publication exempt")
	expectAttackMessage(t, middle, "local publication exempt")
	expectAttackMessage(t, downstream, "local publication exempt")
	id := servers[1].host.ID()
	request := model.ProfileUpdateRequest{RunID: "run", Generation: 2, Revision: 1, Stage: "prepare", Set: model.RuntimeProfilePatch{Forwarding: &model.ProbabilityAttack{Enabled: attackBool(false)}}}
	if _, err := servers[1].executeProfileUpdate(t.Context(), request, nil); err != nil {
		t.Fatal(err)
	}
	request.Stage = "apply"
	ack, err := servers[1].executeProfileUpdate(t.Context(), request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := model.ValidateProfileAcknowledgement(request, ack); err != nil {
		t.Fatal(err)
	}
	publishAttackMessage(t, servers[0], "forwarding restored")
	expectAttackMessage(t, downstream, "forwarding restored")
	if id != servers[1].host.ID() || servers[1].attacks.snapshot()["forwarding"]["publishWithheld"] != 1 {
		t.Fatal("identity or suppression counter changed")
	}
}

func TestAttackInvalidSignatureReachesRemoteValidation(t *testing.T) {
	servers, downstream, _ := attackTestChain(t, func([]host.Host) model.NodeConfig { return model.NodeConfig{} })
	result, err := servers[1].pubsub.SendExperimentalInvalidMessage(t.Context(), servers[0].host.ID(), "blocks", "invalid-signature", 64, 1)
	if err != nil || result.Queued != 1 {
		t.Fatalf("injection: %+v %v", result, err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case evt := <-servers[0].telemetry.events:
			if evt.Type == "pubsub_reject" && evt.RemotePeerID == servers[1].host.ID().String() {
				if evt.Fields["reason"] != pubsub.RejectInvalidSignature {
					t.Fatalf("wrong rejection: %+v", evt)
				}
				expectAttackSilence(t, downstream)
				return
			}
		case <-deadline:
			t.Fatal("remote signature rejection not observed")
		}
	}
}

func TestAttackInvalidGeneratorJoinsRuntimeShutdown(t *testing.T) {
	servers, _, _ := attackTestChain(t, func(h []host.Host) model.NodeConfig {
		return model.NodeConfig{InvalidMessage: &model.InvalidMessageConfig{ControlAttackConfig: model.ControlAttackConfig{Enabled: attackBool(true), Topics: []string{"blocks"}, Targets: &model.AttackSelector{PeerIDs: []string{h[0].ID().String()}}, Interval: "20ms"}, MessagesPerPeer: attackInt(2)}}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- servers[1].attacks.run(ctx) }()
	joined := false
	defer func() {
		cancel()
		if joined {
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("attack runtime did not stop")
		}
	}()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case event := <-servers[0].telemetry.events:
			if event.Type == "pubsub_reject" && event.RemotePeerID == servers[1].host.ID().String() && event.Fields["reason"] == pubsub.RejectInvalidSignature {
				waitAttackCondition(t, func() bool { return servers[1].attacks.snapshot()["invalidMessage"]["queuedRPCs"] > 0 })
				cancel()
				select {
				case err := <-done:
					joined = true
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("generator blocked runtime shutdown")
				}
				before := servers[1].attacks.snapshot()["invalidMessage"]["generatedMessages"]
				time.Sleep(50 * time.Millisecond)
				if before == 0 || servers[1].attacks.snapshot()["invalidMessage"]["generatedMessages"] != before {
					t.Fatal("generator continued after runtime shutdown")
				}
				return
			}
		case <-deadline:
			t.Fatal("configured invalid generator did not reach remote validation")
		}
	}
}

func TestAttackMalformedProtobufResetsRemoteStreamAndRecovers(t *testing.T) {
	servers, downstream, _ := attackTestChain(t, func([]host.Host) model.NodeConfig { return model.NodeConfig{} })
	result, err := servers[1].pubsub.SendExperimentalInvalidMessage(t.Context(), servers[2].host.ID(), "blocks", "malformed-protobuf", 1, 1)
	if err != nil || result.Queued != 1 {
		t.Fatalf("injection: %+v %v", result, err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case evt := <-servers[1].telemetry.events:
			// The receiver resets the malformed inbound stream. The sender
			// observes that reset on its outbound PubSub transport.
			if evt.Type == "remove_peer" && evt.RemotePeerID == servers[2].host.ID().String() {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				// A PubSub stream reset does not itself reset the underlying
				// connection or promise a bounded subscription refresh. Exercise
				// normal transport recovery explicitly in this isolated fixture.
				if err := servers[1].host.Network().ClosePeer(servers[2].host.ID()); err != nil {
					t.Fatal(err)
				}
				if err := servers[1].host.Connect(ctx, corepeer.AddrInfo{ID: servers[2].host.ID(), Addrs: servers[2].host.Addrs()}); err != nil {
					t.Fatal(err)
				}
				for len(servers[1].pubsub.ListPeers("blocks")) != 2 {
					select {
					case <-ctx.Done():
						t.Fatal("PubSub did not recover after explicit transport reconnect")
					case <-time.After(10 * time.Millisecond):
					}
				}
				publishAttackMessage(t, servers[1], "healthy after stream reset")
				expectAttackMessage(t, downstream, "healthy after stream reset")
				return
			}
		case <-deadline:
			t.Fatal("malformed RPC did not reset remote stream")
		}
	}
}
