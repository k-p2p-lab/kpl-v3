package pubsub

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

func meshPlanPeer(label string) peer.ID {
	digest := sha256.Sum256([]byte(label))
	return peer.ID(append([]byte{0x12, 0x20}, digest[:]...))
}

func meshPlanEval(t *testing.T, p *PubSub, check func()) {
	t.Helper()
	_, err := p.evalMeshFreeze(p.ctx, func() MeshFreezeSnapshot { check(); return MeshFreezeSnapshot{} })
	if err != nil {
		t.Fatal(err)
	}
}

func newMeshPlanFixture(t *testing.T) (*PubSub, string, []peer.ID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], WithMeshFreeze(), WithGossipSubParams(params))
	if err != nil {
		t.Fatal(err)
	}
	name := "planned-mesh"
	ids := []peer.ID{meshPlanPeer("removed"), meshPlanPeer("retained"), meshPlanPeer("added")}
	meshPlanEval(t, p, func() {
		gs := p.rt.(*GossipSubRouter)
		p.myRelays[name] = 1
		p.topics[name] = make(map[peer.ID]struct{})
		for _, pid := range ids {
			p.peers[pid] = newRpcQueue(16)
			gs.peers[pid] = GossipSubID_v12
			p.topics[name][pid] = struct{}{}
		}
		gs.mesh[name] = map[peer.ID]struct{}{ids[0]: {}, ids[1]: {}}
	})
	return p, name, ids
}

type meshPlanTraceFunc func(*pb.TraceEvent)

func (f meshPlanTraceFunc) Trace(event *pb.TraceEvent) { f(event) }

func TestKPLMeshPlanInstallsOnceWithExactAccounting(t *testing.T) {
	p, name, ids := newMeshPlanFixture(t)
	gs := p.rt.(*GossipSubRouter)
	manager, err := connmgr.NewConnManager(5, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	age := time.Now().Add(-time.Minute)
	stats := map[peer.ID]*topicStats{}
	var wireTransitions int
	meshPlanEval(t, p, func() {
		score := newPeerScore(&PeerScoreParams{Topics: map[string]*TopicScoreParams{name: {
			TopicWeight: 1, TimeInMeshQuantum: time.Second, MeshMessageDeliveriesThreshold: 5,
		}}, AppSpecificScore: func(peer.ID) float64 { return 0 }})
		for i, pid := range ids {
			stats[pid] = &topicStats{inMesh: i < 2, graftTime: age, meshTime: time.Minute, meshMessageDeliveriesActive: i < 2, meshMessageDeliveries: 2}
			score.peerStats[pid] = &peerStats{topics: map[string]*topicStats{name: stats[pid]}, connected: true}
		}
		gs.score, gs.tagTracer = score, newTagTracer(manager)
		gs.tagTracer.Graft(ids[0], name)
		gs.tagTracer.Graft(ids[1], name)
		p.tracer.tracer = meshPlanTraceFunc(func(event *pb.TraceEvent) {
			if event.GetType() == pb.TraceEvent_GRAFT || event.GetType() == pb.TraceEvent_PRUNE {
				wireTransitions++
			}
		})
		other := "unchanged-topic"
		p.myRelays[other] = 1
		p.topics[other] = map[peer.ID]struct{}{ids[0]: {}}
		gs.mesh[other] = map[peer.ID]struct{}{ids[0]: {}}
		gs.fanout[name] = map[peer.ID]struct{}{ids[0]: {}}
		gs.lastpub[name] = 42
		ctl := &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &name}}, Prune: []*pb.ControlPrune{{TopicID: &name}}}
		gs.control[ids[2]] = ctl
		_ = p.peers[ids[2]].Push(&RPC{RPC: pb.RPC{Control: ctl, Publish: []*pb.Message{{Topic: &name, Data: []byte("keep queued publication")}}}}, false)
	})
	plan := []peer.ID{ids[1], ids[2]}
	if err := p.ValidateMeshPlan(p.ctx, name, plan); err != nil {
		t.Fatal(err)
	}
	meshPlanEval(t, p, func() {
		if gs.meshFrozen || len(gs.control) != 1 || !stats[ids[0]].inMesh || stats[ids[2]].inMesh {
			t.Error("preflight changed mesh, control or score state")
		}
	})
	if err := p.SetMeshAndFreeze(p.ctx, name, plan); err != nil {
		t.Fatal(err)
	}
	plan[0] = ids[0]
	meshPlanEval(t, p, func() {
		want := map[peer.ID]struct{}{ids[1]: {}, ids[2]: {}}
		if !gs.meshFrozen || !p.meshFrozen.Load() || !reflect.DeepEqual(gs.mesh[name], want) {
			t.Error("installed membership was not copied and frozen")
		}
		if !reflect.DeepEqual(gs.mesh["unchanged-topic"], map[peer.ID]struct{}{ids[0]: {}}) {
			t.Error("installing a plan changed another topic")
		}
		if stats[ids[0]].inMesh || stats[ids[0]].meshFailurePenalty != 9 {
			t.Errorf("removed peer transition: %+v", stats[ids[0]])
		}
		if !stats[ids[1]].inMesh || stats[ids[1]].graftTime != age || stats[ids[1]].meshTime != time.Minute || stats[ids[1]].meshFailurePenalty != 0 {
			t.Errorf("retained peer was pruned or regrafted: %+v", stats[ids[1]])
		}
		if !stats[ids[2]].inMesh || stats[ids[2]].meshTime != 0 || stats[ids[2]].meshMessageDeliveriesActive {
			t.Errorf("added peer transition: %+v", stats[ids[2]])
		}
		if manager.IsProtected(ids[0], topicTag(name)) || !manager.IsProtected(ids[1], topicTag(name)) || !manager.IsProtected(ids[2], topicTag(name)) {
			t.Error("connection-manager protection did not follow membership transitions")
		}
		if wireTransitions != 0 || len(gs.control) != 0 || len(gs.fanout[name]) != 0 || gs.lastpub[name] != 0 {
			t.Error("installation synthesized wire transitions or retained stale control/fanout")
		}
		queue := p.peers[ids[2]]
		queue.queueMu.Lock()
		rpc := queue.queue.Pop()
		queue.queueMu.Unlock()
		if len(rpc.GetPublish()) != 1 || len(rpc.GetControl().GetGraft()) != 0 || len(rpc.GetControl().GetPrune()) != 0 {
			t.Error("freezing failed to purge controls while retaining queued data")
		}
	})
	if err := p.SetMeshAndFreeze(p.ctx, name, []peer.ID{ids[2], ids[1]}); err != nil {
		t.Fatalf("same set in a different order: %v", err)
	}
	if err := p.SetMeshAndFreeze(p.ctx, name, []peer.ID{ids[0]}); !errors.Is(err, ErrMeshFreezeConflict) {
		t.Fatalf("changed frozen plan: %v", err)
	}
	meshPlanEval(t, p, func() {
		if stats[ids[0]].meshFailurePenalty != 9 || stats[ids[1]].graftTime != age {
			t.Error("retry or conflict repeated score transitions")
		}
		delete(p.myRelays, name)
		delete(gs.peers, ids[1])
		delete(p.topics[name], ids[2])
	})
	for _, operation := range []func(context.Context, string, []peer.ID) error{p.ValidateMeshPlan, p.SetMeshAndFreeze} {
		if err := operation(p.ctx, name, []peer.ID{ids[1], ids[2]}); err != nil {
			t.Fatalf("same frozen plan after links disappeared: %v", err)
		}
	}
	snapshot, err := p.MeshFreezeSnapshot(p.ctx)
	if err != nil || len(snapshot.Active[name]) != 0 || len(snapshot.Mesh[name]) != 2 {
		t.Fatalf("disconnected snapshot: %+v err=%v", snapshot, err)
	}
}

func TestKPLMeshPlanRejectsInvalidOrUnreadyWithoutMutation(t *testing.T) {
	for _, problem := range []string{"empty-topic", "duplicate", "self", "malformed", "inactive-local", "disconnected", "no-stream", "unsubscribed", "non-mesh", "direct", "filtered", "blacklisted", "mixed-ready-and-unready", "mixed-invalid-and-unready"} {
		t.Run(problem, func(t *testing.T) {
			p, name, ids := newMeshPlanFixture(t)
			gs := p.rt.(*GossipSubRouter)
			plan, topic, wantErr := []peer.ID{ids[2]}, name, ErrMeshPlanInvalid
			meshPlanEval(t, p, func() {
				switch problem {
				case "empty-topic":
					topic = ""
				case "duplicate":
					plan = []peer.ID{ids[2], ids[2]}
				case "self":
					plan = []peer.ID{p.host.ID()}
				case "malformed":
					plan = []peer.ID{"invalid"}
				case "inactive-local":
					delete(p.myRelays, name)
					wantErr = ErrMeshPlanNotReady
				case "disconnected":
					delete(gs.peers, ids[2])
					wantErr = ErrMeshPlanNotReady
				case "no-stream":
					delete(p.peers, ids[2])
					wantErr = ErrMeshPlanNotReady
				case "unsubscribed":
					delete(p.topics[name], ids[2])
					wantErr = ErrMeshPlanNotReady
				case "non-mesh":
					gs.peers[ids[2]] = FloodSubID
				case "direct":
					gs.direct = map[peer.ID]struct{}{ids[2]: {}}
				case "filtered":
					p.peerFilter = func(peer.ID, string) bool { return false }
				case "blacklisted":
					p.blacklist.Add(ids[2])
				case "mixed-ready-and-unready":
					plan = []peer.ID{ids[2], meshPlanPeer("unready")}
					wantErr = ErrMeshPlanNotReady
				case "mixed-invalid-and-unready":
					plan = []peer.ID{ids[2], meshPlanPeer("unready")}
					gs.direct = map[peer.ID]struct{}{ids[2]: {}}
				}
			})
			for _, operation := range []func(context.Context, string, []peer.ID) error{p.ValidateMeshPlan, p.SetMeshAndFreeze} {
				if err := operation(p.ctx, topic, plan); !errors.Is(err, wantErr) {
					t.Errorf("got %v, want %v", err, wantErr)
				}
			}
			meshPlanEval(t, p, func() {
				if gs.meshFrozen || p.meshFrozen.Load() || !reflect.DeepEqual(gs.mesh[name], map[peer.ID]struct{}{ids[0]: {}, ids[1]: {}}) {
					t.Error("failed plan partially installed membership or froze the router")
				}
			})
		})
	}
}

func TestKPLMeshPlanRequiresOptInAndCancellationBeforeCommit(t *testing.T) {
	p := &PubSub{rt: &FloodSubRouter{}}
	if err := p.SetMeshAndFreeze(context.Background(), "topic", nil); !errors.Is(err, ErrMeshFreezeUnsupported) {
		t.Fatalf("unsupported router: %v", err)
	}
	p.rt = &GossipSubRouter{}
	if err := p.ValidateMeshPlan(context.Background(), "topic", nil); !errors.Is(err, ErrMeshFreezeDisabled) {
		t.Fatalf("missing opt-in: %v", err)
	}
	for _, stage := range []string{"before-queue", "during-validation"} {
		t.Run(stage, func(t *testing.T) {
			p, topic, ids := newMeshPlanFixture(t)
			ctx, cancel := context.WithCancel(p.ctx)
			defer cancel()
			if stage == "before-queue" {
				cancel()
			} else {
				meshPlanEval(t, p, func() {
					p.peerFilter = func(peer.ID, string) bool { cancel(); return true }
				})
			}
			if err := p.SetMeshAndFreeze(ctx, topic, []peer.ID{ids[2]}); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled plan: %v", err)
			}
			meshPlanEval(t, p, func() {
				gs := p.rt.(*GossipSubRouter)
				if gs.meshFrozen || !reflect.DeepEqual(gs.mesh[topic], map[peer.ID]struct{}{ids[0]: {}, ids[1]: {}}) {
					t.Error("cancellation before commit changed the mesh")
				}
			})
		})
	}
}

func TestKPLMeshPlanExistingFreezeAndEmptyPlan(t *testing.T) {
	p, topic, ids := newMeshPlanFixture(t)
	if err := p.FreezeMesh(p.ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.SetMeshAndFreeze(p.ctx, topic, []peer.ID{ids[1], ids[0]}); err != nil {
		t.Fatalf("ordinary freeze with identical membership: %v", err)
	}
	if err := p.SetMeshAndFreeze(p.ctx, "new-topic", nil); !errors.Is(err, ErrMeshFreezeConflict) {
		t.Fatalf("frozen topic set changed: %v", err)
	}
	p, topic, _ = newMeshPlanFixture(t)
	if err := p.SetMeshAndFreeze(p.ctx, topic, nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.MeshFreezeSnapshot(p.ctx)
	members, exists := snapshot.Mesh[topic]
	if err != nil || !snapshot.Frozen || !exists || len(members) != 0 {
		t.Fatalf("empty planned mesh: %+v err=%v", snapshot, err)
	}
}

func TestKPLMeshPlanReciprocalTCPTopologyForwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	hosts := newHopwaveHosts(t, 3)
	ps := make([]*PubSub, 3)
	topics := make([]*Topic, 3)
	subs := make([]*Subscription, 3)
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay, params.HeartbeatInterval = 10*time.Millisecond, 20*time.Millisecond
	for i, h := range hosts {
		var err error
		ps[i], err = NewGossipSub(ctx, h, WithMeshFreeze(), WithFloodPublish(false), WithGossipSubParams(params))
		if err != nil {
			t.Fatal(err)
		}
		topics[i], err = ps[i].Join("planned-line")
		if err != nil {
			t.Fatal(err)
		}
		subs[i], err = topics[i].Subscribe()
		if err != nil {
			t.Fatal(err)
		}
	}
	connect(t, hosts[0], hosts[1])
	connect(t, hosts[0], hosts[2])
	connect(t, hosts[1], hosts[2])
	plans := [][]peer.ID{{hosts[1].ID()}, {hosts[0].ID(), hosts[2].ID()}, {hosts[1].ID()}}
	for i, p := range ps {
		for {
			err := p.ValidateMeshPlan(ctx, "planned-line", plans[i])
			if err == nil {
				break
			}
			if !errors.Is(err, ErrMeshPlanNotReady) {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	for i, p := range ps {
		if err := p.SetMeshAndFreeze(ctx, "planned-line", plans[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := topics[0].Publish(ctx, []byte("through-explicit-line")); err != nil {
		t.Fatal(err)
	}
	msg, err := subs[2].Next(ctx)
	if err != nil || string(msg.GetData()) != "through-explicit-line" {
		t.Fatalf("line topology delivery: %+v err=%v", msg, err)
	}
	for i, p := range ps {
		snapshot, err := p.MeshFreezeSnapshot(ctx)
		sort.Slice(plans[i], func(a, b int) bool { return plans[i][a] < plans[i][b] })
		if err != nil || !snapshot.Frozen || !reflect.DeepEqual(snapshot.Mesh["planned-line"], plans[i]) || !reflect.DeepEqual(snapshot.Active["planned-line"], plans[i]) {
			t.Fatalf("node %d installed topology: %+v err=%v", i, snapshot, err)
		}
	}
}
