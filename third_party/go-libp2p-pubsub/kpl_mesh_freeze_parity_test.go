package pubsub

import (
	"context"
	"reflect"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// These checks deliberately use the pre-freeze router API and state. The same
// default-mode cases can run against the previous vendored revision or upstream
// with only a no-op constructor-option shim for the unused opted-in cases.
func TestKPLMeshFreezeUnfrozenParity(t *testing.T) {
	for _, mode := range []string{"default", "opted-in"} {
		t.Run(mode, func(t *testing.T) {
			t.Run("graft-prune-and-backoff", func(t *testing.T) {
				p := newUnfrozenParityRouter(t, mode)
				runUnfrozenParityEval(t, p, func() {
					gs := p.rt.(*GossipSubRouter)
					topic, pid := "control", peer.ID("candidate")
					gs.mesh[topic] = make(map[peer.ID]struct{})
					gs.AddPeer(pid, GossipSubID_v11)
					graft := &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}
					if out := gs.handleGraft(pid, graft); len(out) != 0 || len(gs.mesh[topic]) != 1 {
						t.Errorf("normal GRAFT did not add member: mesh=%v prune=%v", gs.mesh[topic], out)
					}
					gs.handlePrune(pid, &pb.ControlMessage{Prune: []*pb.ControlPrune{{TopicID: &topic}}})
					if len(gs.mesh[topic]) != 0 || !gs.backoff[topic][pid].After(time.Now()) {
						t.Error("normal PRUNE did not remove member and establish backoff")
					}
					if out := gs.handleGraft(pid, graft); len(out) != 1 || len(gs.mesh[topic]) != 0 {
						t.Errorf("GRAFT during backoff was not rejected: mesh=%v prune=%v", gs.mesh[topic], out)
					}
				})
			})
			t.Run("heartbeat-repair-prune-and-cache", func(t *testing.T) {
				p := newUnfrozenParityRouter(t, mode)
				runUnfrozenParityEval(t, p, func() {
					gs := p.rt.(*GossipSubRouter)
					topic := "heartbeat"
					installUnfrozenParityPeers(p, topic, "a", "b", "c", "d")
					gs.params.D, gs.params.Dlo, gs.params.Dhi = 2, 1, 4
					gs.params.Dscore, gs.params.Dout = 0, 0
					gs.mesh[topic] = make(map[peer.ID]struct{})
					gs.heartbeat()
					if len(gs.mesh[topic]) != 2 {
						t.Errorf("heartbeat did not repair low degree: %v", gs.mesh[topic])
					}
					gs.mesh[topic] = map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}}
					gs.heartbeat()
					if len(gs.mesh[topic]) != 2 {
						t.Errorf("heartbeat did not prune high degree: %v", gs.mesh[topic])
					}
					msg := &Message{Message: &pb.Message{Topic: &topic, Data: []byte("expiring")}}
					gs.mcache.Put(msg)
					for i := 0; i <= gs.params.HistoryLength; i++ {
						gs.heartbeat()
					}
					if _, found := gs.mcache.Get(p.idGen.ID(msg)); found {
						t.Error("heartbeat did not expire cached message")
					}
				})
			})
			t.Run("join-leave-and-disconnect", func(t *testing.T) {
				p := newUnfrozenParityRouter(t, mode)
				runUnfrozenParityEval(t, p, func() {
					gs := p.rt.(*GossipSubRouter)
					topic := "lifecycle"
					installUnfrozenParityPeers(p, topic, "a", "b")
					gs.Join(topic)
					if len(gs.mesh[topic]) != 2 {
						t.Errorf("Join did not form mesh: %v", gs.mesh[topic])
					}
					gs.RemovePeer("a")
					if _, exists := gs.mesh[topic]["a"]; exists {
						t.Error("disconnected peer retained in unfrozen mesh")
					}
					gs.Leave(topic)
					if _, exists := gs.mesh[topic]; exists {
						t.Error("Leave retained unfrozen topic")
					}
					if !gs.backoff[topic]["b"].After(time.Now()) {
						t.Error("Leave did not apply unsubscribe backoff")
					}
				})
			})
			t.Run("publish-and-outgoing-controls", func(t *testing.T) {
				p := newUnfrozenParityRouter(t, mode)
				runUnfrozenParityEval(t, p, func() {
					gs := p.rt.(*GossipSubRouter)
					topic := "forwarding"
					installUnfrozenParityPeers(p, topic, "sender", "receiver")
					gs.mesh[topic] = map[peer.ID]struct{}{"sender": {}, "receiver": {}}
					gs.Publish(&Message{Message: &pb.Message{Topic: &topic, Data: []byte("payload")}, ReceivedFrom: "sender"})
					if got := len(p.peers["sender"].queue.normal); got != 0 {
						t.Errorf("published back to sender: %d RPCs", got)
					}
					queued := p.peers["receiver"].queue.normal
					if len(queued) != 1 || len(queued[0].GetPublish()) != 1 || string(queued[0].GetPublish()[0].GetData()) != "payload" {
						t.Errorf("normal mesh forwarding changed: %+v", queued)
					}
					out := rpcWithControl(nil, nil, nil, []*pb.ControlGraft{{TopicID: &topic}}, []*pb.ControlPrune{{TopicID: &topic}}, nil)
					gs.sendRPC("receiver", out, true)
					urgent := p.peers["receiver"].queue.priority
					if len(urgent) != 1 || !reflect.DeepEqual(urgent[0].GetControl(), out.GetControl()) {
						t.Errorf("unfrozen sendRPC stripped mesh controls: %+v", urgent)
					}
					gs.pushControl("receiver", out.Control)
					if ctl := gs.control["receiver"]; ctl == nil || len(ctl.Graft) != 1 || len(ctl.Prune) != 1 {
						t.Errorf("unfrozen control retry discarded: %+v", ctl)
					}
				})
			})
		})
	}
}

func TestKPLMeshFreezeUnfrozenNetworkParity(t *testing.T) {
	for _, mode := range []string{"default", "opted-in"} {
		t.Run(mode, func(t *testing.T) {
			routers := []string{"gossipsub"}
			if mode == "default" {
				routers = append(routers, "floodsub", "randomsub")
			}
			for _, router := range routers {
				t.Run(router, func(t *testing.T) {
					hosts := newHopwaveHosts(t, 2)
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					pubsubs := make([]*PubSub, 2)
					topics := make([]*Topic, 2)
					subs := make([]*Subscription, 2)
					const topic = "unfrozen-network"
					for i, h := range hosts {
						var err error
						switch router {
						case "gossipsub":
							params := DefaultGossipSubParams()
							params.HeartbeatInitialDelay = 10 * time.Millisecond
							params.HeartbeatInterval = 20 * time.Millisecond
							params.PruneBackoff = 10 * time.Millisecond
							params.UnsubscribeBackoff = 10 * time.Millisecond
							opts := []Option{WithGossipSubParams(params)}
							if mode == "opted-in" {
								opts = append(opts, WithMeshFreeze())
							}
							pubsubs[i], err = NewGossipSub(ctx, h, opts...)
						case "floodsub":
							pubsubs[i], err = NewFloodSub(ctx, h)
						case "randomsub":
							pubsubs[i], err = NewRandomSub(ctx, h, 2)
						}
						if err != nil {
							t.Fatal(err)
						}
						topics[i], err = pubsubs[i].Join(topic)
						if err != nil {
							t.Fatal(err)
						}
						subs[i], err = topics[i].Subscribe()
						if err != nil {
							t.Fatal(err)
						}
					}
					waitMembers := func(index, count int) {
						t.Helper()
						p := pubsubs[index]
						for {
							matches := false
							runUnfrozenParityEval(t, p, func() {
								if gs, ok := p.rt.(*GossipSubRouter); ok {
									matches = len(gs.mesh[topic]) == count
								} else {
									matches = len(p.topics[topic]) == count
								}
							})
							if matches {
								return
							}
							select {
							case <-ctx.Done():
								t.Fatalf("peer %d did not reach %d topic members", index, count)
							case <-time.After(10 * time.Millisecond):
							}
						}
					}
					connect(t, hosts[0], hosts[1])
					waitMembers(0, 1)
					waitMembers(1, 1)
					if err := topics[0].Publish(ctx, []byte("before-freeze-command")); err != nil {
						t.Fatal(err)
					}
					msg, err := subs[1].Next(ctx)
					if err != nil || string(msg.GetData()) != "before-freeze-command" {
						t.Fatalf("normal delivery changed: msg=%v err=%v", msg, err)
					}
					subs[1].Cancel()
					waitMembers(0, 0)
					subs[1], err = topics[1].Subscribe()
					if err != nil {
						t.Fatal(err)
					}
					waitMembers(0, 1)
					waitMembers(1, 1)
					if err := hosts[0].Network().ClosePeer(hosts[1].ID()); err != nil {
						t.Fatal(err)
					}
					waitMembers(0, 0)
					waitMembers(1, 0)
				})
			}
		})
	}
}

func newUnfrozenParityRouter(t *testing.T, mode string) *PubSub {
	t.Helper()
	h := newHopwaveHosts(t, 1)[0]
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	opts := []Option{WithGossipSubParams(params)}
	if mode == "opted-in" {
		opts = append(opts, WithMeshFreeze())
	}
	p, err := NewGossipSub(ctx, h, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func installUnfrozenParityPeers(p *PubSub, topic string, ids ...peer.ID) {
	gs := p.rt.(*GossipSubRouter)
	p.topics[topic] = make(map[peer.ID]struct{}, len(ids))
	for _, pid := range ids {
		p.topics[topic][pid] = struct{}{}
		p.peers[pid] = newRpcQueue(64)
		gs.AddPeer(pid, GossipSubID_v11)
	}
}

func runUnfrozenParityEval(t *testing.T, p *PubSub, apply func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	select {
	case p.eval <- func() { defer close(done); apply() }:
	case <-ctx.Done():
		t.Fatal("could not enqueue router observation")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("router observation did not complete")
	}
}
