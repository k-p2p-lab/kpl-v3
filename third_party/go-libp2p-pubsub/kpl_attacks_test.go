package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestKPLIWantWithholdingPreservesCacheAndOtherResponses(t *testing.T) {
	topic := "blocks"
	cache := NewMessageCache(3, 5)
	cache.SetMsgIdFn(func(msg *Message) string { return msg.ID })
	msg := &Message{ID: "known", Message: &pb.Message{Topic: &topic, Data: []byte("valid cached data")}}
	cache.Put(msg)
	gs := &GossipSubRouter{p: &PubSub{peerFilter: func(peer.ID, string) bool { return true }}, mcache: cache, params: DefaultGossipSubParams()}
	calls := 0
	gs.iwantResponseFilter = func(id peer.ID, actual *Message) bool {
		calls++
		if actual != msg {
			t.Fatal("lost cached message")
		}
		return id != "withheld"
	}
	ctl := &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{"missing", "known", "known"}}}}
	if got := gs.handleIWant("withheld", ctl); len(got) != 0 {
		t.Fatal("withheld response escaped")
	}
	if retained, ok := cache.Get("known"); !ok || retained != msg {
		t.Fatal("withholding changed cache")
	}
	if got := gs.handleIWant("allowed", ctl); len(got) != 1 || string(got[0].Data) != "valid cached data" {
		t.Fatal("unselected requester lost response")
	}
	if calls != 2 {
		t.Fatal("missing IDs were counted as eligible responses")
	}
}
func controlPolicyTestPubSub(t *testing.T) (*PubSub, *GossipSubRouter, *rpcQueue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := &PubSub{ctx: ctx, eval: make(chan func()), maxMessageSize: 1024, peers: map[peer.ID]*rpcQueue{}}
	q := newRpcQueue(1)
	p.peers["target"] = q
	gs := &GossipSubRouter{p: p, tracer: &pubsubTracer{}, mcache: NewMessageCache(3, 5), backoff: map[string]map[peer.ID]time.Time{}}
	p.rt = gs
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case fn := <-p.eval:
				fn()
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
	return p, gs, q
}
func TestKPLExperimentalControlQueuesDropsAndRespectsBackoff(t *testing.T) {
	p, _, q := controlPolicyTestPubSub(t)
	topic := "blocks"
	ctl := &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}
	first, err := p.SendExperimentalGossipSubControl(context.Background(), "target", ctl, false)
	if err != nil || first.Queued != 1 {
		t.Fatalf("enqueue: %+v %v", first, err)
	}
	second, err := p.SendExperimentalGossipSubControl(context.Background(), "target", ctl, false)
	if err != nil || second.Dropped != 1 || second.Queued != 0 {
		t.Fatalf("queue full: %+v %v", second, err)
	}
	queued, err := q.Pop(context.Background())
	if err != nil || queued.GetControl().GetGraft()[0].GetTopicID() != topic {
		t.Fatal("wrong queued control")
	}
	absent, err := p.SendExperimentalGossipSubControl(context.Background(), "absent", ctl, false)
	if err != nil || absent.Skipped != 1 {
		t.Fatalf("missing transport: %+v %v", absent, err)
	}
	_, err = p.evalMeshFreeze(context.Background(), func() MeshFreezeSnapshot {
		p.rt.(*GossipSubRouter).backoff[topic] = map[peer.ID]time.Time{"target": time.Now().Add(time.Hour)}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	skipped, err := p.SendExperimentalGossipSubControl(context.Background(), "target", ctl, true)
	if err != nil || skipped.SuppressedGrafts != 1 || skipped.Queued != 0 {
		t.Fatalf("backoff: %+v %v", skipped, err)
	}
	sent, err := p.SendExperimentalGossipSubControl(context.Background(), "target", ctl, false)
	if err != nil || sent.Queued != 1 {
		t.Fatalf("explicit backoff violation: %+v %v", sent, err)
	}
}
func TestKPLExperimentalBogusIHaveAvoidsCachedIDs(t *testing.T) {
	p, gs, q := controlPolicyTestPubSub(t)
	topic := "blocks"
	_, err := p.evalMeshFreeze(context.Background(), func() MeshFreezeSnapshot {
		gs.mcache.SetMsgIdFn(func(msg *Message) string { return msg.ID })
		gs.mcache.Put(&Message{ID: "cached", Message: &pb.Message{Topic: &topic}})
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		t.Fatal(err)
	}
	ctl := &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"cached", "bogus"}}}}
	result, err := p.SendExperimentalGossipSubControl(context.Background(), "target", ctl, false)
	if err != nil || result.Queued != 1 {
		t.Fatal(result, err)
	}
	rpc, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := rpc.GetControl().GetIhave()[0].GetMessageIDs()
	if ids[0] == "cached" || ids[1] != "bogus" || ctl.Ihave[0].MessageIDs[0] != "cached" {
		t.Fatal("cache collision or caller ownership violated")
	}
}
func TestKPLExperimentalControlCancellationDoesNotApply(t *testing.T) {
	p := &PubSub{ctx: context.Background(), eval: make(chan func()), maxMessageSize: 1024}
	p.rt = &GossipSubRouter{p: p}
	ctx, cancel := context.WithCancel(context.Background())
	topic := "blocks"
	done := make(chan error, 1)
	go func() {
		_, err := p.SendExperimentalGossipSubControl(ctx, "target", &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}, false)
		done <- err
	}()
	apply := <-p.eval
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	apply()
}
