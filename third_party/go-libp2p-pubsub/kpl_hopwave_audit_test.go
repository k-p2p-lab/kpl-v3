package pubsub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// Metadata alone, and either way of configuring full Hopwave forwarding, must
// preserve the original mesh/fanout/flood recipient rules. In particular, score
// and IDONTWANT filtering intentionally differ between these upstream paths.
func TestKPLHopwaveFullForwardingPreservesRecipients(t *testing.T) {
	h := newHopwaveHosts(t, 1)[0]
	for _, route := range []string{"mesh", "fanout", "flood-publication"} {
		for _, mode := range []string{"disabled", "metadata", "factor-one", "interval-one"} {
			t.Run(route+"/"+mode, func(t *testing.T) {
				name := "forwarding-parity"
				ids := []peer.ID{"mesh", "mesh-low", "unwanted", "direct-low", "floodsub", "floodsub-low", "gossip", "gossip-low", "previous", "author"}
				members := map[peer.ID]struct{}{"mesh": {}, "mesh-low": {}, "unwanted": {}, "previous": {}, "author": {}}
				topicPeers := make(map[peer.ID]struct{})
				queues := make(map[peer.ID]*rpcQueue)
				protocols := make(map[peer.ID]protocol.ID)
				score := newPeerScore(&PeerScoreParams{AppSpecificWeight: 1, AppSpecificScore: func(pid peer.ID) float64 {
					if strings.HasSuffix(string(pid), "-low") {
						return -10
					}
					return 0
				}})
				for _, pid := range ids {
					topicPeers[pid], queues[pid], protocols[pid] = struct{}{}, newRpcQueue(8), GossipSubID_v12
					score.peerStats[pid] = &peerStats{}
					if strings.HasPrefix(string(pid), "floodsub") {
						protocols[pid] = FloodSubID
					}
				}
				params := DefaultGossipSubParams()
				params.HopwaveFactor, params.HopwaveInterval = 1, 3
				if mode == "interval-one" {
					params.HopwaveFactor, params.HopwaveInterval = 0, 1
				}
				p := &PubSub{host: h, hopwave: mode != "disabled", maxMessageSize: DefaultMaxMessageSize,
					idGen: newMsgIdGenerator(), peers: queues, topics: map[string]map[peer.ID]struct{}{name: topicPeers}, peerFilter: DefaultPeerFilter}
				gs := &GossipSubRouter{p: p, params: params, feature: GossipSubDefaultFeatures, peers: protocols, score: score,
					mesh: make(map[string]map[peer.ID]struct{}), fanout: make(map[string]map[peer.ID]struct{}), lastpub: make(map[string]int64),
					direct: map[peer.ID]struct{}{"direct-low": {}}, publishThreshold: -1, mcache: NewMessageCache(3, 5),
					hopwavePublish: mode == "factor-one" || mode == "interval-one"}
				from := peer.ID("previous")
				want := []string{"direct-low", "floodsub", "mesh", "mesh-low"}
				switch route {
				case "mesh":
					gs.mesh[name] = members
				case "fanout":
					gs.fanout[name] = members
				case "flood-publication":
					gs.floodPublish, from = true, h.ID()
					want = []string{"direct-low", "floodsub", "gossip", "mesh", "previous", "unwanted"}
				}
				hop := int32(0)
				msg := &Message{Message: &pb.Message{Topic: &name, Data: []byte("payload"), From: []byte("author"), Seqno: []byte{1}, HopCount: &hop, PropaType: pb.PropagationType_LAZY_PULL.Enum()}, ReceivedFrom: from}
				mid := p.idGen.ID(msg)
				gs.unwanted = map[peer.ID]map[checksum]int{"unwanted": {computeChecksum(mid): 1}}
				before, _ := msg.Message.Marshal()
				gs.Publish(msg)
				var recipients []string
				for pid, queue := range queues {
					if queue.queue.Len() == 0 {
						continue
					}
					recipients = append(recipients, string(pid))
					rpc := queue.queue.Pop()
					if len(rpc.Publish) != 1 {
						t.Fatalf("publication count changed for %s: %d", pid, len(rpc.Publish))
					}
					out := rpc.Publish[0]
					if mode == "disabled" {
						after, _ := out.Marshal()
						if !bytes.Equal(before, after) {
							t.Error("disabled Hopwave changed forwarded wire bytes")
						}
					} else if out.GetPropaType() != pb.PropagationType_EAGER_PUSH {
						t.Error("outgoing eager publication retained a lazy label")
					}
				}
				sort.Strings(recipients)
				if !reflect.DeepEqual(recipients, want) {
					t.Errorf("recipients=%v, want upstream recipients=%v", recipients, want)
				}
				after, _ := msg.Message.Marshal()
				if !bytes.Equal(before, after) {
					t.Error("forwarding changed the cached arrival")
				}
			})
		}
	}
}

func TestKPLHopwaveIntervalBoundaryPreservesArrivalAndPullLimits(t *testing.T) {
	for _, interval := range []int{1, 2, math.MaxInt32} {
		t.Run(fmt.Sprintf("interval=%d", interval), func(t *testing.T) {
			name, pid := "boundary", peer.ID("requester")
			hops := int32(interval - 1)
			msg := &Message{Message: &pb.Message{Topic: &name, Data: []byte("payload"), From: []byte("author"), Seqno: []byte{1}, HopCount: &hops, PropaType: pb.PropagationType_EAGER_PUSH.Enum()}}
			params := DefaultGossipSubParams()
			params.HopwaveInterval, params.HopwaveFactor = interval, 0
			p := &PubSub{hopwave: true, peerFilter: DefaultPeerFilter}
			gs := &GossipSubRouter{p: p, params: params, hopwavePublish: true, mcache: NewMessageCache(3, 5)}
			gs.mcache.Put(msg)
			ctl := &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{DefaultMsgIdFn(msg.Message)}}}}
			before, _ := msg.Message.Marshal()
			for i := 0; i <= params.GossipRetransmission; i++ {
				out := gs.handleIWant(pid, ctl)
				if i == params.GossipRetransmission {
					if len(out) != 0 {
						t.Error("Hopwave bypassed the upstream IWANT retransmission limit")
					}
					continue
				}
				if len(out) != 1 || out[0].HopCount == nil || *out[0].HopCount != 0 || out[0].GetPropaType() != pb.PropagationType_LAZY_PULL {
					t.Errorf("boundary pull %d failed to reset the hop counter: %+v", i, out)
				}
			}
			after, _ := msg.Message.Marshal()
			if !bytes.Equal(before, after) {
				t.Error("repeated boundary pulls changed the cached arrival")
			}
		})
	}
}

func TestKPLHopwaveCustomMessageIDIgnoresOnlyMutableMetadata(t *testing.T) {
	hashMessage := func(msg *pb.Message) string {
		wire, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		hash := sha256.Sum256(wire)
		return string(hash[:])
	}
	for _, scope := range []string{"default", "topic"} {
		for _, mode := range []string{"disabled", "metadata", "waves"} {
			t.Run(scope+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				params := DefaultGossipSubParams()
				params.HeartbeatInitialDelay = time.Hour
				params.HopwaveInterval = 2
				opts := []Option{WithGossipSubParams(params)}
				if scope == "default" {
					opts = append(opts, WithMessageIdFn(hashMessage))
				}
				switch mode {
				case "metadata":
					opts = append(opts, WithHopwave())
				case "waves":
					opts = append(opts, WithHopwavePublish(true))
				}
				p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], opts...)
				if err != nil {
					t.Fatal(err)
				}
				name := "custom-message-id"
				if scope == "topic" {
					if _, err := p.Join(name, WithTopicMessageIdFn(hashMessage)); err != nil {
						t.Fatal(err)
					}
				}
				gs := p.rt.(*GossipSubRouter)
				arrival := &pb.Message{Topic: &name, Data: []byte("payload"), From: []byte("author"), Seqno: []byte{1}, Signature: []byte("signature"), Key: []byte("key"), HopCount: int32ptr(0), PropaType: pb.PropagationType_EAGER_PUSH.Enum()}
				canonical := *arrival
				canonical.HopCount, canonical.PropaType = nil, nil
				wantID := hashMessage(&canonical)
				push := gs.hopwaveMessage(arrival, pb.PropagationType_EAGER_PUSH)
				pull := gs.hopwaveMessage(push, pb.PropagationType_LAZY_PULL)
				variants := []*pb.Message{arrival, push, pull}
				for i, msg := range variants {
					before, _ := msg.Marshal()
					id := p.idGen.RawID(msg)
					if mode == "disabled" {
						if id != hashMessage(msg) {
							t.Error("disabled Hopwave changed the caller's message ID function input")
						}
					} else if id != wantID {
						t.Errorf("transmission %d changed the canonical message ID", i)
					}
					after, _ := msg.Marshal()
					if !bytes.Equal(before, after) {
						t.Error("message ID computation mutated shared wire metadata")
					}
				}
				if mode == "disabled" {
					return
				}
				// All ordinary protobuf fields remain visible to custom generators.
				for _, field := range []string{"data", "topic", "from", "seqno", "signature", "key"} {
					changed := *arrival
					switch field {
					case "data":
						changed.Data = []byte("different")
					case "topic":
						other := "different-topic"
						changed.Topic = &other
						if scope == "topic" {
							p.idGen.Set(other, hashMessage)
						}
					case "from":
						changed.From = []byte("different")
					case "seqno":
						changed.Seqno = []byte("different")
					case "signature":
						changed.Signature = []byte("different")
					case "key":
						changed.Key = []byte("different")
					}
					if p.idGen.RawID(&changed) == wantID {
						t.Errorf("ID normalization discarded ordinary field %s", field)
					}
				}
				done := make(chan struct{})
				select {
				case p.eval <- func() {
					defer close(done)
					original := &Message{Message: arrival}
					gs.mcache.Put(original)
					for _, forwarded := range []*pb.Message{push, pull} {
						id := p.idGen.RawID(forwarded)
						ctl := &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{id}}}}
						if replies := gs.handleIWant("remote", ctl); len(replies) != 1 || p.idGen.RawID(replies[0]) != wantID {
							t.Error("forwarded ID could not recover the same cached message through IWANT")
						}
					}
					if !p.markSeen(p.idGen.RawID(arrival)) || p.markSeen(p.idGen.RawID(push)) || p.markSeen(p.idGen.RawID(pull)) {
						t.Error("hop changes caused duplicate messages to be treated as new publications")
					}
				}:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			})
		}
	}
}
