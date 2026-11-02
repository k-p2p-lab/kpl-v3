package pubsub

import (
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestKPLInvalidSignatureProducesObservedP4OnScoringReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hosts := newHopwaveHosts(t, 2)
	observed := make(chan *PeerScoreSnapshot, 1)
	params := &PeerScoreParams{Topics: map[string]*TopicScoreParams{"blocks": {TopicWeight: 1, TimeInMeshQuantum: time.Second, InvalidMessageDeliveriesWeight: -1, InvalidMessageDeliveriesDecay: .999}}, AppSpecificScore: func(peer.ID) float64 { return 0 }, DecayInterval: time.Second, DecayToZero: .01}
	receiver, err := NewGossipSub(ctx, hosts[1], WithPeerScore(params, &PeerScoreThresholds{GossipThreshold: -10, PublishThreshold: -20, GraylistThreshold: -100}), WithPeerScoreInspect(ExtendedPeerScoreInspectFn(func(scores map[peer.ID]*PeerScoreSnapshot) {
		if snapshot := scores[hosts[0].ID()]; snapshot != nil {
			select {
			case observed <- snapshot:
			default:
			}
		}
	}), 10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	attacker, err := NewGossipSub(ctx, hosts[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []*PubSub{attacker, receiver} {
		sub, err := p.Subscribe("blocks")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Cancel()
	}
	if err := hosts[0].Connect(ctx, peer.AddrInfo{ID: hosts[1].ID(), Addrs: hosts[1].Addrs()}); err != nil {
		t.Fatal(err)
	}
	for len(attacker.ListPeers("blocks")) != 1 || len(receiver.ListPeers("blocks")) != 1 {
		select {
		case <-ctx.Done():
			t.Fatal("scoring fixture did not connect")
		case <-time.After(5 * time.Millisecond):
		}
	}
	result, err := attacker.SendExperimentalInvalidMessage(ctx, hosts[1].ID(), "blocks", "invalid-signature", 64, 1)
	if err != nil || result.Queued != 1 {
		t.Fatalf("injection: %+v %v", result, err)
	}
	for {
		select {
		case snapshot := <-observed:
			if topic := snapshot.Topics["blocks"]; topic != nil && topic.InvalidMessageDeliveries > 0 {
				if snapshot.Score >= 0 || snapshot.AppSpecificScore != 0 {
					t.Fatalf("unexpected P4 total: %+v", snapshot)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("receiver did not observe P4 for invalid wire signature")
		}
	}
}

func TestKPLDelayedMessagesRespectInlineValidationWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := getDefaultHosts(t, 1)[0]
	queued := make(chan struct{}, 4)
	p, err := NewGossipSub(ctx, h, WithValidateWorkers(1), WithMessageSignaturePolicy(LaxNoSign), WithInboundMessagePolicy(func(*Message) InboundMessageDecision {
		return InboundMessageDecision{Delay: 20 * time.Millisecond}
	}, InboundMessagePolicyLimits{MaxPendingMessages: 4, MaxPendingBytes: 1024, Observe: func(_ *Message, event string) {
		if event == "queued" {
			queued <- struct{}{}
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	defer close(release)
	if err := p.RegisterTopicValidator("blocks", func(context.Context, peer.ID, *Message) bool {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return true
	}, WithValidatorInline(true)); err != nil {
		t.Fatal(err)
	}
	topic := "blocks"
	for i := byte(0); i < 4; i++ {
		p.val.Push("remote", &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte{i}, Data: []byte{1}}, ReceivedFrom: "remote"})
	}
	for i := 0; i < 4; i++ {
		select {
		case <-queued:
		case <-time.After(time.Second):
			t.Fatal("delay queue did not admit fixture")
		}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("inline validator did not start")
	}
	select {
	case <-started:
		t.Fatal("delayed messages bypassed the single inline validation worker")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestKPLForwardFilterPreservesCacheLocalAndOtherRecipients(t *testing.T) {
	h := getDefaultHosts(t, 1)[0]
	p, gs, _ := controlPolicyTestPubSub(t)
	p.host = h
	p.idGen = newMsgIdGenerator()
	gs.mcache.SetMsgIdFn(p.idGen.ID)
	topic := "blocks"
	remote := &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte("remote")}, ReceivedFrom: "upstream"}
	local := &Message{Message: &pb.Message{Topic: &topic, From: []byte(h.ID()), Seqno: []byte("local")}, ReceivedFrom: h.ID()}
	gs.mcache.Put(remote)
	gs.mcache.Put(local)
	var paths []string
	gs.forwardFilter = func(id peer.ID, msg *Message, path string) bool {
		if msg != remote {
			t.Fatal("local publication entered filter")
		}
		paths = append(paths, path)
		return id != "victim"
	}
	rpc := &RPC{RPC: pb.RPC{Publish: []*pb.Message{remote.Message, local.Message}, Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{remote.ID, local.ID, "bogus"}}}, Graft: []*pb.ControlGraft{{TopicID: &topic}}}}}
	filtered := gs.filterForwardRPC("victim", rpc)
	if len(filtered.Publish) != 1 || filtered.Publish[0] != local.Message || len(filtered.Control.Ihave[0].MessageIDs) != 2 || len(filtered.Control.Graft) != 1 {
		t.Fatalf("wrong filtered RPC: %+v", filtered)
	}
	if len(rpc.Publish) != 2 || len(rpc.Control.Ihave[0].MessageIDs) != 3 {
		t.Fatal("shared recipient RPC mutated")
	}
	other := gs.filterForwardRPC("other", rpc)
	if len(other.Publish) != 2 || len(other.Control.Ihave[0].MessageIDs) != 3 {
		t.Fatal("other recipient affected")
	}
	if retained, ok := gs.mcache.Get(remote.ID); !ok || retained != remote {
		t.Fatal("cache lost remote message")
	}
	if len(paths) != 4 {
		t.Fatalf("expected eager and gossip opportunities, got %v", paths)
	}
	gs.forwardFilterActive = func() bool { return false }
	if gs.filterForwardRPC("victim", rpc) != rpc {
		t.Fatal("disabled policy changed normal RPC")
	}
}

func TestKPLForwardFilterCoversIWANTPiggybackAndFragments(t *testing.T) {
	p, gs, _ := controlPolicyTestPubSub(t)
	p.host = newHopwaveHosts(t, 1)[0]
	p.idGen = newMsgIdGenerator()
	p.peerFilter = func(peer.ID, string) bool { return true }
	p.maxMessageSize = 220
	q := newRpcQueue(64)
	p.peers["target"] = q
	gs.params = DefaultGossipSubParams()
	gs.peerhave = make(map[peer.ID]int)
	gs.peerdontwant = make(map[peer.ID]int)
	gs.unwanted = make(map[peer.ID]map[checksum]int)
	gs.mcache.SetMsgIdFn(p.idGen.ID)
	topic := "blocks"
	gs.mesh = map[string]map[peer.ID]struct{}{topic: {"target": {}}}
	gs.control = map[peer.ID]*pb.ControlMessage{"target": {Graft: []*pb.ControlGraft{{TopicID: &topic}}}}
	var mids []string
	for i := byte(0); i < 10; i++ {
		data := make([]byte, 100)
		data[0] = i
		msg := &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte{i}, Data: data}, ReceivedFrom: "upstream"}
		gs.mcache.Put(msg)
		mids = append(mids, msg.ID)
	}
	advertised := append(append([]string(nil), mids...), "bogus-not-cached")
	gs.gossip = map[peer.ID][]*pb.ControlIHave{"target": {{TopicID: &topic, MessageIDs: advertised}}}
	gs.forwardFilter = func(_ peer.ID, msg *Message, _ string) bool { return msg.Data[0]%2 == 0 }
	gs.HandleRPC(&RPC{from: "target", RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: mids}}}}})
	dataCount, advertisedCount, grafts, fragments := 0, 0, 0, 0
	for {
		q.queueMu.Lock()
		n := q.queue.Len()
		q.queueMu.Unlock()
		if n == 0 {
			break
		}
		out, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		fragments++
		if out.Size() > p.maxMessageSize {
			t.Fatal("oversized fragment escaped")
		}
		for _, msg := range out.Publish {
			if msg.Data[0]%2 != 0 {
				t.Fatal("IWANT bypassed forwarding suppression")
			}
			dataCount++
		}
		for _, entry := range out.GetControl().GetIhave() {
			for _, mid := range entry.MessageIDs {
				if msg, ok := gs.mcache.Get(mid); ok && msg.Data[0]%2 != 0 {
					t.Fatal("piggybacked IHAVE bypassed suppression")
				}
				advertisedCount++
			}
		}
		grafts += len(out.GetControl().GetGraft())
	}
	if fragments < 2 || dataCount != 5 || advertisedCount != 6 || grafts != 1 {
		t.Fatalf("fragments=%d data=%d ads=%d grafts=%d", fragments, dataCount, advertisedCount, grafts)
	}
	for _, mid := range mids {
		if _, ok := gs.mcache.Get(mid); !ok {
			t.Fatal("cached response removed")
		}
	}
	if len(advertised) != 11 {
		t.Fatal("shared gossip list mutated")
	}
}

func TestKPLInvalidMessageUsesBoundedQueueAndBrokenSignature(t *testing.T) {
	p, gs, q := controlPolicyTestPubSub(t)
	p.host = getDefaultHosts(t, 1)[0]
	ctx := context.Background()
	result, err := p.SendExperimentalInvalidMessage(ctx, "target", "blocks", "invalid-signature", 32, 1)
	if err != nil || result.Queued != 1 {
		t.Fatalf("queue: %+v %v", result, err)
	}
	result, err = p.SendExperimentalInvalidMessage(ctx, "target", "blocks", "invalid-signature", 32, 2)
	if err != nil || result.Dropped != 1 {
		t.Fatalf("full queue: %+v %v", result, err)
	}
	rpc, err := q.Pop(ctx)
	if err != nil || len(rpc.Publish) != 1 {
		t.Fatal("missing invalid publication")
	}
	msg := rpc.Publish[0]
	if err := verifyMessageSignature(msg); err == nil {
		t.Fatal("corrupted signature still valid")
	}
	if _, exists := gs.mcache.Get(string(msg.From) + string(msg.Seqno)); exists {
		t.Fatal("invalid message entered local cache")
	}
	result, err = p.SendExperimentalInvalidMessage(ctx, "target", "blocks", "malformed-protobuf", 32, 3)
	if err != nil || result.Queued != 1 {
		t.Fatalf("malformed queue: %+v %v", result, err)
	}
	rpc, _ = q.Pop(ctx)
	if !rpc.experimentalMalformed || len(rpc.Publish) != 0 {
		t.Fatal("malformed wire marker lost")
	}
	gs.meshFrozen = true
	result, err = p.SendExperimentalInvalidMessage(ctx, "absent", "blocks", "malformed-protobuf", 32, 4)
	if err != nil || result.Skipped != 1 {
		t.Fatalf("missing peer: %+v %v", result, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.SendExperimentalInvalidMessage(canceled, "target", "blocks", "invalid-signature", 32, 5); err == nil {
		t.Fatal("canceled injection accepted")
	}
	if _, err := p.SendExperimentalInvalidMessage(ctx, "target", "blocks", "valid", 32, 6); err == nil {
		t.Fatal("unsupported mode accepted")
	}
}
