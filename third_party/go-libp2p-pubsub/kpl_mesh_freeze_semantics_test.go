package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestKPLMeshFreezeRejoinReleasesFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params), WithFloodPublish(false))
	if err != nil {
		t.Fatal(err)
	}
	gs := p.rt.(*GossipSubRouter)
	topic, err := p.Join("rejoin-fanout")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	name := topic.String()
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		gs.mesh[name] = map[peer.ID]struct{}{"pinned": {}}
		p.topics[name] = map[peer.ID]struct{}{"pinned": {}, "outside": {}}
		gs.peers["pinned"] = GossipSubID_v11
		gs.peers["outside"] = GossipSubID_v11
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.FreezeMesh(ctx); err != nil {
		t.Fatal(err)
	}
	sub.Cancel()
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		// Publishing while unsubscribed must still use ordinary fanout.
		gs.Publish(&Message{Message: &pb.Message{Topic: &name, Data: []byte("fanout publication")}, ReceivedFrom: p.host.ID()})
		if len(gs.fanout[name]) == 0 || gs.lastpub[name] == 0 {
			t.Error("freezing disabled fanout for an unsubscribed publisher")
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err = topic.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		if _, found := gs.fanout[name]; found {
			t.Error("rejoining a frozen mesh retained fanout alongside the subscribed mesh")
		}
		if _, found := gs.lastpub[name]; found {
			t.Error("rejoining retained the fanout publication timestamp")
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.MeshFreezeSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot.Mesh[name], []peer.ID{"pinned"}) {
		t.Fatalf("fanout peers entered the frozen mesh on rejoin: %+v", snapshot.Mesh)
	}
}

func TestKPLMeshFreezeNewParticipationRetainsGossip(t *testing.T) {
	for _, participation := range []string{"subscription", "relay", "subscription-and-relay"} {
		for _, hopwave := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hopwave=%v", participation, hopwave), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				params := DefaultGossipSubParams()
				params.HeartbeatInitialDelay = time.Hour
				opts := []Option{WithMeshFreeze(), WithGossipSubParams(params), WithFloodPublish(false)}
				if hopwave {
					opts = append(opts, WithHopwave())
				}
				p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], opts...)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.FreezeMesh(ctx); err != nil {
					t.Fatal(err)
				}
				topic, err := p.Join("joined-after-freeze")
				if err != nil {
					t.Fatal(err)
				}
				var stops []func()
				if participation != "relay" {
					sub, err := topic.Subscribe()
					if err != nil {
						t.Fatal(err)
					}
					stops = append(stops, sub.Cancel)
				}
				if participation != "subscription" {
					relayCancel, err := topic.Relay()
					if err != nil {
						t.Fatal(err)
					}
					stops = append(stops, relayCancel)
				}
				name := topic.String()
				gs := p.rt.(*GossipSubRouter)
				queue := newRpcQueue(10)
				drain := func() []*RPC {
					queue.queueMu.Lock()
					defer queue.queueMu.Unlock()
					var messages []*RPC
					for queue.queue.Len() > 0 {
						messages = append(messages, queue.queue.Pop())
					}
					return messages
				}
				_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
					p.topics[name] = map[peer.ID]struct{}{"gossip-peer": {}}
					gs.peers["gossip-peer"] = GossipSubID_v11
					p.peers["gossip-peer"] = queue
					iwant := gs.handleIHave("gossip-peer", &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &name, MessageIDs: []string{"unseen-message"}}}})
					if len(iwant) != 1 || !reflect.DeepEqual(iwant[0].MessageIDs, []string{"unseen-message"}) {
						t.Errorf("new participation lost lazy recovery after freeze: IWANT=%+v", iwant)
					}
					msg := &Message{Message: &pb.Message{Topic: &name, Data: []byte("new topic publication")}, ReceivedFrom: p.host.ID()}
					gs.Publish(msg)
					if len(gs.fanout[name]) != 0 || gs.lastpub[name] != 0 {
						t.Error("locally joined topic created a fanout mesh after freeze")
					}
					for _, rpc := range drain() {
						if len(rpc.GetPublish()) != 0 {
							t.Error("empty frozen mesh eagerly forwarded to a new fanout peer")
						}
					}
					gs.heartbeat()
					var advertised []string
					for _, rpc := range drain() {
						for _, ihave := range rpc.GetControl().GetIhave() {
							if ihave.GetTopicID() == name {
								advertised = append(advertised, ihave.GetMessageIDs()...)
							}
						}
						if len(rpc.GetControl().GetGraft()) != 0 || len(rpc.GetControl().GetPrune()) != 0 {
							t.Error("new participation emitted frozen mesh controls")
						}
					}
					if !reflect.DeepEqual(advertised, []string{p.idGen.ID(msg)}) {
						t.Errorf("newly joined topic failed to advertise its cache exactly once: %q", advertised)
					}
					if len(gs.mesh) != 0 {
						t.Error("joining after freeze created mesh membership")
					}
					return MeshFreezeSnapshot{}
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, stop := range stops {
					stop()
				}
				_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
					if iwant := gs.handleIHave("gossip-peer", &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &name, MessageIDs: []string{"after-leave"}}}}); len(iwant) != 0 {
						t.Error("inactive topic requested a lazy message")
					}
					gs.heartbeat()
					for _, rpc := range drain() {
						if len(rpc.GetControl().GetIhave()) != 0 {
							t.Error("inactive topic continued to advertise cached messages")
						}
					}
					return MeshFreezeSnapshot{}
				})
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestKPLMeshFreezePreservesFanoutUntilHeartbeat(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("frozen=%v", frozen), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			params := DefaultGossipSubParams()
			params.HeartbeatInitialDelay = time.Hour
			p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params), WithFloodPublish(false))
			if err != nil {
				t.Fatal(err)
			}
			if frozen {
				if err := p.FreezeMesh(ctx); err != nil {
					t.Fatal(err)
				}
			}
			gs := p.rt.(*GossipSubRouter)
			_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
				name := "unsubscribed-publisher"
				pid := peer.ID("recently-unsubscribed-peer")
				queue := newRpcQueue(10)
				p.peers[pid] = queue
				gs.peers[pid] = GossipSubID_v11
				// Original GossipSub filters removed remote subscriptions out of
				// fanout at the next heartbeat, not during every publication.
				p.topics[name] = map[peer.ID]struct{}{}
				gs.fanout[name] = map[peer.ID]struct{}{pid: {}}
				gs.Publish(&Message{Message: &pb.Message{Topic: &name, Data: []byte("queued-before-maintenance")}, ReceivedFrom: p.host.ID()})
				queue.queueMu.Lock()
				rpc := queue.queue.Pop()
				queue.queueMu.Unlock()
				if rpc == nil || len(rpc.GetPublish()) != 1 || string(rpc.GetPublish()[0].Data) != "queued-before-maintenance" {
					t.Error("mesh freeze changed the fanout filtering schedule")
				}
				gs.heartbeat()
				if _, present := gs.fanout[name][pid]; present {
					t.Error("heartbeat retained an unsubscribed fanout peer")
				}
				return MeshFreezeSnapshot{}
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestKPLMeshFreezeNewTopicRecoversAcrossNetwork(t *testing.T) {
	for _, hopwave := range []bool{false, true} {
		t.Run(fmt.Sprintf("hopwave=%v", hopwave), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			hosts := newHopwaveHosts(t, 2)
			params := DefaultGossipSubParams()
			params.HeartbeatInitialDelay = 10 * time.Millisecond
			params.HeartbeatInterval = 20 * time.Millisecond
			ps := make([]*PubSub, len(hosts))
			topics := make([]*Topic, len(hosts))
			subs := make([]*Subscription, len(hosts))
			for i := range hosts {
				opts := []Option{WithMeshFreeze(), WithGossipSubParams(params), WithFloodPublish(false)}
				if hopwave {
					opts = append(opts, WithHopwave())
				}
				var err error
				ps[i], err = NewGossipSub(ctx, hosts[i], opts...)
				if err != nil {
					t.Fatal(err)
				}
				if err := ps[i].FreezeMesh(ctx); err != nil {
					t.Fatal(err)
				}
				topics[i], err = ps[i].Join("new-topic-lazy-recovery")
				if err != nil {
					t.Fatal(err)
				}
				subs[i], err = topics[i].Subscribe()
				if err != nil {
					t.Fatal(err)
				}
			}
			connect(t, hosts[0], hosts[1])
			for _, p := range ps {
				for len(p.ListPeers("new-topic-lazy-recovery")) != 1 {
					select {
					case <-ctx.Done():
						t.Fatal("remote topic subscription was not announced")
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			if err := topics[0].Publish(ctx, []byte("recover-without-new-mesh-links")); err != nil {
				t.Fatal(err)
			}
			msg, err := subs[1].Next(ctx)
			if err != nil {
				t.Fatalf("gossip failed to recover a new topic after freeze: %v", err)
			}
			if string(msg.GetData()) != "recover-without-new-mesh-links" {
				t.Fatalf("recovered message: %q", msg.GetData())
			}
			for _, p := range ps {
				snapshot, err := p.MeshFreezeSnapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if !snapshot.Frozen || len(snapshot.Mesh) != 0 {
					t.Fatalf("lazy recovery created mesh links: %+v", snapshot)
				}
			}
		})
	}
}

// An accepted asynchronous validator result is delivered by publishMessage even
// when the final local subscription ended while validation was running.
func TestKPLMeshFreezePreservesValidatedMessageAfterLeave(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(fmt.Sprintf("frozen=%v", frozen), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			params := DefaultGossipSubParams()
			params.HeartbeatInitialDelay = time.Hour
			p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params), WithFloodPublish(false))
			if err != nil {
				t.Fatal(err)
			}
			topic, err := p.Join("leave-during-validation")
			if err != nil {
				t.Fatal(err)
			}
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			name := topic.String()
			gs := p.rt.(*GossipSubRouter)
			queues := map[peer.ID]*rpcQueue{"fanout-peer": newRpcQueue(10), "direct-peer": newRpcQueue(10)}
			_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
				gs.mesh[name] = map[peer.ID]struct{}{"fanout-peer": {}}
				p.topics[name] = map[peer.ID]struct{}{"fanout-peer": {}, "direct-peer": {}}
				gs.direct = map[peer.ID]struct{}{"direct-peer": {}}
				for pid, queue := range queues {
					p.peers[pid] = queue
					gs.peers[pid] = GossipSubID_v11
				}
				return MeshFreezeSnapshot{}
			})
			if err != nil {
				t.Fatal(err)
			}
			if frozen {
				if err := p.FreezeMesh(ctx); err != nil {
					t.Fatal(err)
				}
			}
			msg := &Message{Message: &pb.Message{Topic: &name, Data: []byte("accepted-after-leave"), From: []byte("source"), Seqno: []byte{1}}, ReceivedFrom: "source"}
			before, err := msg.Message.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			sub.Cancel()
			_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
				if gs.meshTopicActive(name) {
					t.Error("local subscription was not removed before validation completion")
				}
				// processLoop calls this same entry point for an already accepted
				// asynchronous validator result received from p.sendMsg.
				p.publishMessage(msg)
				if _, cached := gs.mcache.Get(p.idGen.ID(msg)); !cached {
					t.Error("freezing discarded an accepted in-flight message from the cache")
				}
				for pid, queue := range queues {
					queue.queueMu.Lock()
					publications := 0
					for queue.queue.Len() > 0 {
						rpc := queue.queue.Pop()
						publications += len(rpc.GetPublish())
					}
					queue.queueMu.Unlock()
					if publications != 1 {
						t.Errorf("validated message was not forwarded to %s: publications=%d", pid, publications)
					}
				}
				if frozen {
					if !reflect.DeepEqual(gs.mesh[name], map[peer.ID]struct{}{"fanout-peer": {}}) {
						t.Error("completion after leave changed pinned membership")
					}
				} else if _, found := gs.mesh[name]; found {
					t.Error("ordinary leave retained mesh membership")
				}
				return MeshFreezeSnapshot{}
			})
			if err != nil {
				t.Fatal(err)
			}
			after, err := msg.Message.Marshal()
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Error("late validation completion mutated the shared message")
			}
		})
	}
}
