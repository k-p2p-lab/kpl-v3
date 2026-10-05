package pubsub

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestKPLMeshFreezeRequiresOptInAndGossipSub(t *testing.T) {
	p := &PubSub{rt: &FloodSubRouter{}}
	if err := WithMeshFreeze()(p); !errors.Is(err, ErrMeshFreezeUnsupported) {
		t.Fatalf("other router option: %v", err)
	}
	if err := p.FreezeMesh(context.Background()); !errors.Is(err, ErrMeshFreezeUnsupported) {
		t.Fatalf("other router call: %v", err)
	}
	p.rt = &GossipSubRouter{}
	if err := p.FreezeMesh(context.Background()); !errors.Is(err, ErrMeshFreezeDisabled) {
		t.Fatalf("missing opt-in: %v", err)
	}
}

func TestKPLMeshFreezeAcknowledgementAndCancellation(t *testing.T) {
	for _, operation := range []string{"freeze", "snapshot"} {
		for _, stage := range []string{"before-enqueue", "after-enqueue", "acknowledged", "shutdown"} {
			t.Run(operation+"/"+stage, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				lifetime, stop := context.WithCancel(context.Background())
				defer stop()
				p := &PubSub{ctx: lifetime, eval: make(chan func())}
				gs := &GossipSubRouter{p: p, meshFreezeEnabled: true}
				p.rt = gs
				if stage == "before-enqueue" {
					cancel()
				}
				if stage == "shutdown" {
					stop()
				}
				done := make(chan error, 1)
				go func() {
					if operation == "freeze" {
						done <- p.FreezeMesh(ctx)
					} else {
						_, err := p.MeshFreezeSnapshot(ctx)
						done <- err
					}
				}()
				if stage == "after-enqueue" || stage == "acknowledged" {
					var apply func()
					select {
					case apply = <-p.eval:
					case <-time.After(time.Second):
						t.Fatal("operation did not enter the event loop")
					}
					select {
					case err := <-done:
						t.Fatalf("returned before acknowledgement: %v", err)
					default:
					}
					if stage == "after-enqueue" {
						cancel()
						select {
						case err := <-done:
							if !errors.Is(err, context.Canceled) {
								t.Fatalf("canceled queued operation: %v", err)
							}
						case <-time.After(time.Second):
							t.Fatal("queued operation ignored cancellation")
						}
						apply()
						if gs.meshFrozen {
							t.Fatal("operation canceled before application froze the mesh")
						}
						return
					}
					apply()
				}
				select {
				case err := <-done:
					if stage == "acknowledged" {
						if err != nil || (operation == "freeze" && !gs.meshFrozen) {
							t.Fatalf("acknowledged application: frozen=%v err=%v", gs.meshFrozen, err)
						}
					} else if !errors.Is(err, context.Canceled) {
						t.Fatalf("canceled operation: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("operation did not finish")
				}
			})
		}
	}
}

func TestKPLMeshFreezePinsEveryMutationPathAndKeepsCacheMaintenance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params))
	if err != nil {
		t.Fatal(err)
	}
	gs := p.rt.(*GossipSubRouter)
	topic := "pinned"
	mesh := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}}
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		gs.mesh[topic] = mesh
		p.myRelays[topic] = 1
		p.topics[topic] = map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "candidate": {}}
		for pid := range p.topics[topic] {
			gs.peers[pid] = GossipSubID_v11
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.FreezeMesh(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		// Both automatic repair and excessive-degree pruning would change this
		// mesh without the freeze guard. Cache expiry must still advance.
		msg := &Message{Message: &pb.Message{Topic: &topic, Data: []byte("cached")}}
		gs.mcache.Put(msg)
		mid := p.idGen.ID(msg)
		gs.params.Dlo = 10
		gs.heartbeat()
		gs.params.Dhi = 2
		for i := 0; i <= gs.params.HistoryLength; i++ {
			gs.heartbeat()
		}
		if _, found := gs.mcache.Get(mid); found {
			t.Error("freezing disabled message-cache expiry")
		}
		if prune := gs.handleGraft("candidate", &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}); len(prune) != 0 {
			t.Error("frozen router generated a PRUNE reply")
		}
		gs.handlePrune("a", &pb.ControlMessage{Prune: []*pb.ControlPrune{{TopicID: &topic}}})
		gs.Join("new-topic")
		delete(p.myRelays, topic)
		gs.Leave(topic)
		delete(p.topics[topic], "b")
		gs.RemovePeer("b")
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.MeshFreezeSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]peer.ID{topic: {"a", "b", "c"}}
	if !snapshot.Frozen || !reflect.DeepEqual(snapshot.Mesh, want) || len(snapshot.Active[topic]) != 0 {
		t.Fatalf("pinned mesh changed after maintenance/control/lifecycle: %+v", snapshot)
	}
	if err := p.FreezeMesh(ctx); err != nil {
		t.Fatalf("repeated freeze: %v", err)
	}
	// Returned maps must not expose the router's maps or peer slices.
	snapshot.Mesh[topic][0] = "tampered"
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		p.myRelays[topic] = 1
		gs.Join(topic)
		p.topics[topic]["b"] = struct{}{}
		gs.AddPeer("b", GossipSubID_v11)
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := p.MeshFreezeSnapshot(ctx)
	if err != nil || !reflect.DeepEqual(after.Mesh, want) || !reflect.DeepEqual(after.Active, want) || !after.ObservedAt.After(snapshot.ObservedAt) {
		t.Fatalf("rejoin/reconnect or snapshot ownership: %+v err=%v", after, err)
	}
}

func TestKPLMeshFreezeStripsQueuedAndRetriedControlsWithoutMutatingMessages(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params))
	if err != nil {
		t.Fatal(err)
	}
	gs := p.rt.(*GossipSubRouter)
	topic := "pinned"
	rpc := rpcWithControl([]*pb.Message{{Topic: &topic, Data: []byte("payload")}}, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"id"}}}, nil,
		[]*pb.ControlGraft{{TopicID: &topic}}, []*pb.ControlPrune{{TopicID: &topic}}, nil)
	queue := newRpcQueue(10)
	_ = queue.Push(rpc, false)
	_ = queue.UrgentPush(rpc, false)
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		p.peers["queued"] = queue
		gs.control["retry"] = rpc.Control
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.FreezeMesh(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		out, err := queue.Pop(ctx)
		if err != nil || len(out.GetControl().GetGraft()) != 0 || len(out.GetControl().GetPrune()) != 0 || len(out.GetPublish()) != 1 || len(out.GetControl().GetIhave()) != 1 {
			t.Fatalf("pending control strip damaged RPC: %+v err=%v", out, err)
		}
	}
	_, err = p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		if len(gs.control) != 0 {
			t.Error("retry controls survived freeze")
		}
		gs.pushControl("retry", rpc.Control)
		gs.sendRPC("queued", rpc, false)
		if len(gs.control) != 0 {
			t.Error("frozen router retained new control retries")
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := queue.Pop(ctx)
	if err != nil || len(out.GetControl().GetGraft()) != 0 || len(out.GetControl().GetPrune()) != 0 {
		t.Fatalf("send path emitted frozen mesh controls: %+v err=%v", out, err)
	}
	if len(rpc.GetControl().GetGraft()) != 1 || len(rpc.GetControl().GetPrune()) != 1 {
		t.Fatal("stripping mutated another queued RPC's shared control")
	}
}

func TestKPLMeshFreezeSubscriptionAccountingDoesNotDoublePenalize(t *testing.T) {
	topic, pid := "scored", peer.ID("member")
	params := &PeerScoreParams{
		Topics: map[string]*TopicScoreParams{topic: {TopicWeight: 1, TimeInMeshQuantum: time.Second,
			MeshMessageDeliveriesThreshold: 5, MeshMessageDeliveriesWeight: -1, MeshFailurePenaltyWeight: -2}},
		AppSpecificScore: func(peer.ID) float64 { return 0 },
	}
	scorer := newPeerScore(params)
	stats := &topicStats{inMesh: true, meshMessageDeliveriesActive: true, meshMessageDeliveries: 2}
	scorer.peerStats[pid] = &peerStats{topics: map[string]*topicStats{topic: stats}, connected: true}
	p := &PubSub{
		topics: map[string]map[peer.ID]struct{}{topic: {pid: {}}}, myRelays: map[string]int{topic: 1},
		tracer: &pubsubTracer{raw: []RawTracer{scorer}},
	}
	gs := &GossipSubRouter{
		p: p, score: scorer, tracer: p.tracer, meshFrozen: true,
		feature:          GossipSubDefaultFeatures,
		mesh:             map[string]map[peer.ID]struct{}{topic: {pid: {}}},
		frozenMeshActive: map[string]map[peer.ID]bool{topic: {pid: true}},
		peers:            map[peer.ID]protocol.ID{pid: GossipSubID_v11},
	}
	p.rt = gs
	no, yes := false, true
	unsubscribe := &RPC{from: pid, RPC: pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic, Subscribe: &no}}}}
	p.handleIncomingRPC(unsubscribe)
	p.handleIncomingRPC(unsubscribe)
	gs.Leave(topic)
	gs.RemovePeer(pid)
	if stats.inMesh || stats.meshFailurePenalty != 9 {
		t.Fatalf("unsubscribe/leave/disconnect applied duplicate penalties: %+v", stats)
	}
	scorer.AddPeer(pid, GossipSubID_v11)
	gs.peers[pid] = GossipSubID_v11
	p.handleIncomingRPC(&RPC{from: pid, RPC: pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Topicid: &topic, Subscribe: &yes}}}})
	if !stats.inMesh || stats.meshFailurePenalty != 9 || len(gs.mesh[topic]) != 1 {
		t.Fatalf("resubscription failed to restore pinned score accounting: %+v", stats)
	}
}

func TestKPLMeshFreezeNetworkForwardingAndDisconnectedSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hosts := newHopwaveHosts(t, 3)
	ps := make([]*PubSub, 3)
	topics := make([]*Topic, 3)
	subs := make([]*Subscription, 3)
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = 10 * time.Millisecond
	params.HeartbeatInterval = 20 * time.Millisecond
	for i := range hosts {
		var err error
		ps[i], err = NewGossipSub(ctx, hosts[i], WithMeshFreeze(), WithGossipSubParams(params))
		if err != nil {
			t.Fatal(err)
		}
		topics[i], err = ps[i].Join("fixed-network")
		if err != nil {
			t.Fatal(err)
		}
		subs[i], err = topics[i].Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}
	waitActive := func(index, count int) MeshFreezeSnapshot {
		t.Helper()
		for {
			snapshot, err := ps[index].MeshFreezeSnapshot(ctx)
			if err != nil {
				t.Fatalf("waiting for node %d active=%d: %v", index, count, err)
			}
			if len(snapshot.Active["fixed-network"]) == count {
				return snapshot
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	connect(t, hosts[0], hosts[1])
	for i := 0; i < 2; i++ {
		waitActive(i, 1)
		if err := ps[i].FreezeMesh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	connect(t, hosts[0], hosts[2])
	// Drive multiple maintenance cycles, including incoming GRAFT from the
	// new participant. Only the original pair is pinned on node 0.
	for i := 0; i < 5; i++ {
		_, err := ps[2].evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
			ps[2].rt.(*GossipSubRouter).heartbeat()
			return MeshFreezeSnapshot{}
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	snapshot := waitActive(0, 1)
	if !reflect.DeepEqual(snapshot.Mesh["fixed-network"], []peer.ID{hosts[1].ID()}) {
		t.Fatalf("new participant changed frozen mesh: %+v", snapshot)
	}
	if err := topics[0].Publish(ctx, []byte("frozen-delivery")); err != nil {
		t.Fatal(err)
	}
	msg, err := subs[1].Next(ctx)
	if err != nil || string(msg.GetData()) != "frozen-delivery" {
		t.Fatalf("frozen mesh stopped forwarding: %+v err=%v", msg, err)
	}
	subs[1].Cancel()
	if after := waitActive(0, 0); !reflect.DeepEqual(after.Mesh, snapshot.Mesh) {
		t.Fatal("remote unsubscribe changed pinned membership")
	}
	subs[1], err = topics[1].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	waitActive(0, 1)
	if err := hosts[0].Network().ClosePeer(hosts[1].ID()); err != nil {
		t.Fatal(err)
	}
	if after := waitActive(0, 0); !reflect.DeepEqual(after.Mesh, snapshot.Mesh) {
		t.Fatal("physical disconnect changed pinned membership")
	}
	connect(t, hosts[0], hosts[1])
	if after := waitActive(0, 1); !reflect.DeepEqual(after.Mesh, snapshot.Mesh) {
		t.Fatal("same-peer reconnect changed pinned membership")
	}
}
