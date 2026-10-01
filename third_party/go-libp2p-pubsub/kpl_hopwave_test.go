package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func newHopwaveHosts(t *testing.T, count int) []host.Host {
	t.Helper()
	hosts := make([]host.Host, count)
	for i := range hosts {
		h, err := libp2p.New(
			libp2p.NoTransports,
			libp2p.Transport(tcp.NewTCPTransport),
			libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
			libp2p.Security(noise.ID, noise.New),
			libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
			libp2p.DisableMetrics(),
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.Close() })
		hosts[i] = h
	}
	return hosts
}

func TestKPLHopwaveForwardingPreservesInboundMessage(t *testing.T) {
	for _, hops := range []int32{0, 1, -1, math.MaxInt32} {
		m := &pb.Message{Data: []byte("payload"), HopCount: &hops, PropaType: pb.PropagationType_LAZY_PULL.Enum()}
		before, _ := m.Marshal()
		out := hopwaveMessage(m, pb.PropagationType_EAGER_PUSH)
		if out == m || out.PropaType == m.PropaType || out.GetPropaType() != pb.PropagationType_EAGER_PUSH {
			t.Fatal("outgoing metadata shares the inbound message")
		}
		if hops >= 0 && hops < math.MaxInt32 {
			if out.HopCount == m.HopCount || out.GetHopCount() != hops+1 {
				t.Fatalf("hop %d was not incremented exactly once", hops)
			}
		} else if out.HopCount != nil {
			t.Fatal("invalid or overflowing hop count became known")
		}
		after, _ := m.Marshal()
		if !bytes.Equal(before, after) {
			t.Fatal("forwarding mutated the delivered/cached message")
		}
		encoded, err := out.Marshal()
		var decoded pb.Message
		if err != nil || decoded.Unmarshal(encoded) != nil || decoded.GetPropaType() != out.GetPropaType() || (decoded.HopCount == nil) != (out.HopCount == nil) || decoded.GetHopCount() != out.GetHopCount() {
			t.Fatal("Hopwave fields failed protobuf round trip")
		}
	}
	legacy := &pb.Message{Data: []byte("legacy")}
	if out := hopwaveMessage(legacy, pb.PropagationType_LAZY_PULL); out.HopCount != nil {
		t.Fatal("missing hop count was synthesized from a legacy message")
	}
}

func TestKPLHopwaveSignaturesExcludeOnlyMutableMetadata(t *testing.T) {
	key, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(key.GetPublic())
	if err != nil {
		t.Fatal(err)
	}
	topic, zero := "topic", int32(0)
	m := &pb.Message{From: []byte(id), Seqno: []byte("sequence"), Topic: &topic, Data: []byte("payload"), HopCount: &zero, PropaType: pb.PropagationType_EAGER_PUSH.Enum()}
	if err := signMessage(id, key, m); err != nil {
		t.Fatal(err)
	}
	forwarded := hopwaveMessage(hopwaveMessage(m, pb.PropagationType_EAGER_PUSH), pb.PropagationType_LAZY_PULL)
	if err := verifyMessageSignature(forwarded); err != nil {
		t.Fatalf("relay metadata invalidated signature: %v", err)
	}
	legacy := *m
	legacy.PropaType, legacy.HopCount = nil, nil
	if err := verifyMessageSignature(&legacy); err != nil {
		t.Fatalf("base message signature changed: %v", err)
	}
	forwarded.Data = []byte("tampered payload")
	if err := verifyMessageSignature(forwarded); err == nil {
		t.Fatal("metadata exclusion allowed payload tampering")
	}
}

func TestKPLHopwaveIWantRepliesDoNotAccumulateHopsOrMutateQueuedPush(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		h := newHopwaveHosts(t, 1)[0]
		remote, topic, hops := peer.ID("remote"), "topic", int32(2)
		queue := newRpcQueue(4)
		p := &PubSub{host: h, hopwave: enabled, maxMessageSize: DefaultMaxMessageSize,
			idGen: newMsgIdGenerator(), topics: map[string]map[peer.ID]struct{}{topic: {remote: {}}},
			peers: map[peer.ID]*rpcQueue{remote: queue}, peerFilter: func(peer.ID, string) bool { return true }}
		gs := &GossipSubRouter{p: p, params: DefaultGossipSubParams(), feature: GossipSubDefaultFeatures, mcache: NewMessageCache(3, 5),
			mesh: map[string]map[peer.ID]struct{}{topic: {remote: {}}}, peers: map[peer.ID]protocol.ID{remote: GossipSubID_v12}}
		msg := &Message{Message: &pb.Message{Topic: &topic, Data: []byte("payload"), From: []byte("author"), Seqno: []byte("seq"), HopCount: &hops, PropaType: pb.PropagationType_LAZY_PULL.Enum()}, ReceivedFrom: "previous"}
		before, _ := msg.Message.Marshal()
		gs.Publish(msg)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		out, err := queue.Pop(ctx)
		cancel()
		if err != nil || len(out.Publish) != 1 {
			t.Fatalf("eager transmission: %v", err)
		}
		pushBefore, _ := out.Publish[0].Marshal()
		control := &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{DefaultMsgIdFn(msg.Message)}}}}
		for i := 0; i < 2; i++ {
			pull := gs.handleIWant(remote, control)
			if len(pull) != 1 || pull[0].GetPropaType() != pb.PropagationType_LAZY_PULL || pull[0].GetHopCount() != 2+boolInt(enabled) {
				t.Fatalf("IWANT response %d accumulated or lost hops: %+v", i, pull)
			}
		}
		pushAfter, _ := out.Publish[0].Marshal()
		after, _ := msg.Message.Marshal()
		if !bytes.Equal(before, after) || !bytes.Equal(pushBefore, pushAfter) {
			t.Fatal("pull response changed the cache, subscriber or queued eager RPC")
		}
		if enabled && (out.Publish[0].GetPropaType() != pb.PropagationType_EAGER_PUSH || out.Publish[0].GetHopCount() != 3) {
			t.Fatal("eager forwarding did not reset propagation type")
		}
	}
}

func boolInt(value bool) int32 {
	if value {
		return 1
	}
	return 0
}

type hopwaveTraceCollector struct{ events chan *pb.TraceEvent }

func (tr hopwaveTraceCollector) Trace(event *pb.TraceEvent) {
	if event.GetType() == pb.TraceEvent_DELIVER_MESSAGE {
		tr.events <- event
	}
}

func TestKPLHopwaveSignedPropagationAcrossTwoHops(t *testing.T) {
	for _, mode := range []string{"default", "metadata", "waves"} {
		enabled := mode != "default"
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		hosts := newHopwaveHosts(t, 3)
		psubs := make([]*PubSub, 3)
		topics := make([]*Topic, 3)
		subs := make([]*Subscription, 3)
		traces := make([]hopwaveTraceCollector, 3)
		for i, h := range hosts {
			traces[i] = hopwaveTraceCollector{make(chan *pb.TraceEvent, 4)}
			options := []Option{WithEventTracer(traces[i])}
			if mode == "waves" {
				params := DefaultGossipSubParams()
				params.HopwaveFactor, params.HopwaveInterval = 0.5, 2
				options = append(options, WithGossipSubParams(params), WithHopwavePublish(true))
			} else if enabled {
				options = append(options, WithHopwave())
			}
			psubs[i] = getGossipsub(ctx, h, options...)
			var err error
			topics[i], err = psubs[i].Join("hopwave-test")
			if err != nil {
				t.Fatal(err)
			}
			subs[i], err = topics[i].Subscribe()
			if err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		connect(t, hosts[1], hosts[2])
		for i, ps := range psubs {
			neighbors := 1
			if i == 1 {
				neighbors = 2
			}
			for {
				ready := make(chan bool, 1)
				select {
				case ps.eval <- func() { ready <- len(ps.rt.(*GossipSubRouter).mesh["hopwave-test"]) == neighbors }:
				case <-ctx.Done():
					t.Fatal("mesh did not form")
				}
				select {
				case ok := <-ready:
					if ok {
						goto joined
					}
				case <-ctx.Done():
					t.Fatal("mesh check timed out")
				}
				time.Sleep(10 * time.Millisecond)
			}
		joined:
		}
		if err := topics[0].Publish(ctx, []byte("signed-hopwave")); err != nil {
			t.Fatal(err)
		}
		for i, sub := range subs {
			expected := int32(i)
			if mode == "waves" {
				expected %= 2
			}
			msg, err := sub.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if enabled {
				if msg.HopCount == nil || msg.GetHopCount() != expected || msg.GetPropaType() != pb.PropagationType_EAGER_PUSH {
					t.Fatalf("subscriber %d: %+v", i, msg.Message)
				}
			} else if msg.HopCount != nil || msg.PropaType != nil {
				t.Fatal("default GossipSub emitted Hopwave metadata")
			}
			select {
			case event := <-traces[i].events:
				if enabled && (event.DeliverMessage.HopCount == nil || event.DeliverMessage.GetHopCount() != expected || event.DeliverMessage.GetPropaType() != pb.TraceEvent_EAGER_PUSH) {
					t.Fatalf("trace changed the arrival hop count at subscriber %d", i)
				}
			case <-ctx.Done():
				t.Fatal("delivery trace missing")
			}
		}
		cancel()
	}
}

func TestKPLHopwaveRejectsOtherRouters(t *testing.T) {
	if err := WithHopwave()(&PubSub{rt: &FloodSubRouter{}}); err == nil {
		t.Fatal("Hopwave was silently accepted by FloodSub")
	}
	if err := WithHopwavePublish(true)(&PubSub{rt: &FloodSubRouter{}}); err == nil {
		t.Fatal("Hopwave publication was silently accepted by FloodSub")
	}
}

func TestKPLHopwavePeriodicForwardingAndPullRecovery(t *testing.T) {
	h := newHopwaveHosts(t, 1)[0]
	for _, tc := range []struct {
		name     string
		hops     *int32
		factor   float64
		interval int
		want     int
		outHops  int32
	}{
		{"fraction", int32ptr(0), 0.5, 3, 5, 1},
		{"before-wave", int32ptr(1), 0.5, 3, 5, 2},
		{"full-wave", int32ptr(2), 0.5, 3, 10, 0},
		{"minimum-one", int32ptr(0), 0.01, 3, 1, 1},
		{"zero-factor", int32ptr(0), 0, 3, 0, 1},
		{"zero-factor-full-wave", int32ptr(2), 0, 3, 10, 0},
		{"all-recipients", int32ptr(0), 1, 3, 10, 1},
		{"interval-one", int32ptr(0), 0.5, 1, 10, 0},
		{"legacy", nil, 0.5, 3, 10, 0},
		{"negative", int32ptr(-1), 0.5, 3, 10, 0},
		{"overflow", int32ptr(math.MaxInt32), 0.5, 3, 10, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			topic, previous, author := "topic", peer.ID("previous"), peer.ID("author")
			candidates := make(map[peer.ID]struct{})
			queues := make(map[peer.ID]*rpcQueue)
			protocols := make(map[peer.ID]protocol.ID)
			for i := 0; i < 10; i++ {
				p := peer.ID(fmt.Sprintf("remote-%d", i))
				candidates[p], queues[p], protocols[p] = struct{}{}, newRpcQueue(4), GossipSubID_v12
			}
			for _, p := range []peer.ID{previous, author, "unwanted"} {
				candidates[p], queues[p], protocols[p] = struct{}{}, newRpcQueue(4), GossipSubID_v12
			}
			p := &PubSub{host: h, hopwave: true, maxMessageSize: DefaultMaxMessageSize, idGen: newMsgIdGenerator(),
				topics: map[string]map[peer.ID]struct{}{topic: candidates}, peers: queues,
				peerFilter: func(peer.ID, string) bool { return true }}
			params := DefaultGossipSubParams()
			params.HopwaveFactor, params.HopwaveInterval = tc.factor, tc.interval
			msg := &Message{Message: &pb.Message{Topic: &topic, Data: []byte("payload"), From: []byte(author), Seqno: []byte("seq"), HopCount: tc.hops, PropaType: pb.PropagationType_LAZY_PULL.Enum()}, ReceivedFrom: previous}
			mid := DefaultMsgIdFn(msg.Message)
			gs := &GossipSubRouter{p: p, hopwavePublish: true, params: params, feature: GossipSubDefaultFeatures,
				mcache: NewMessageCache(3, 5), mesh: map[string]map[peer.ID]struct{}{topic: candidates}, peers: protocols,
				unwanted: map[peer.ID]map[checksum]int{"unwanted": {computeChecksum(mid): 1}}}
			before, _ := msg.Message.Marshal()
			gs.Publish(msg)
			known := tc.hops != nil && *tc.hops >= 0 && *tc.hops < math.MaxInt32
			sent := 0
			for pid, q := range queues {
				if q.queue.Len() == 0 {
					continue
				}
				sent++
				if pid == previous || pid == author || pid == "unwanted" {
					t.Fatal("ineligible recipient affected fractional selection")
				}
				out := q.queue.Pop().Publish[0]
				if (out.HopCount != nil) != known || (known && out.GetHopCount() != tc.outHops) || out.GetPropaType() != pb.PropagationType_EAGER_PUSH {
					t.Fatalf("invalid outgoing wave metadata: %+v", out)
				}
			}
			if sent != tc.want {
				t.Fatalf("sent to %d eligible recipients, want %d", sent, tc.want)
			}
			control := &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{mid}}}}
			for i := 0; i < 2; i++ {
				pull := gs.handleIWant("remote-0", control)
				if len(pull) != 1 || (pull[0].HopCount != nil) != known || (known && pull[0].GetHopCount() != tc.outHops) || pull[0].GetPropaType() != pb.PropagationType_LAZY_PULL {
					t.Fatalf("pull recovery accumulated or lost wave metadata: %+v", pull)
				}
			}
			after, _ := msg.Message.Marshal()
			if !bytes.Equal(before, after) {
				t.Fatal("wave selection/pull recovery changed cached arrival metadata")
			}
		})
	}
}

func int32ptr(value int32) *int32 { return &value }

func TestKPLHopwaveValidatesParametersInEitherOptionOrder(t *testing.T) {
	for _, values := range []struct {
		factor   float64
		interval int
	}{{-0.1, 3}, {1.1, 3}, {math.NaN(), 3}, {math.Inf(1), 3}, {0.5, 0}, {0.5, -1}, {0.5, math.MaxInt32 + 1}} {
		params := DefaultGossipSubParams()
		params.HopwaveFactor, params.HopwaveInterval = values.factor, values.interval
		for _, first := range []bool{false, true} {
			p := &PubSub{rt: &GossipSubRouter{params: DefaultGossipSubParams()}}
			options := []Option{WithGossipSubParams(params), WithHopwavePublish(true)}
			if first {
				options[0], options[1] = options[1], options[0]
			}
			var err error
			for _, option := range options {
				if err = option(p); err != nil {
					break
				}
			}
			if err == nil {
				t.Fatalf("invalid Hopwave settings accepted: %+v", values)
			}
		}
	}
}
