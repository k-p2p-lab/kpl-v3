package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestKPLInboundDrainWaitsForRoutingCallbacks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	p, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0],
		WithInboundMessagePolicy(func(*Message) InboundMessageDecision { return InboundMessageDecision{} }, InboundMessagePolicyLimits{MaxPendingMessages: 1, MaxPendingBytes: 1024}),
		WithGossipSubForwardFilter(func(peer.ID, *Message, string) bool {
			close(entered)
			<-release
			return false
		}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WaitInboundMessagePolicy(context.Background()); err == nil {
		t.Fatal("drain accepted a live PubSub context")
	}
	finished := make(chan struct{})
	select {
	case p.eval <- func() {
		defer close(finished)
		gs := p.rt.(*GossipSubRouter)
		topic := "blocks"
		msg := &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte("one")}, ReceivedFrom: "remote"}
		gs.mcache.Put(msg)
		gs.doSendRPC(rpcWithMessages(msg.Message), "recipient", newRpcQueue(1), false)
	}:
	case <-time.After(time.Second):
		t.Fatal("routing fixture did not start")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("forwarding callback did not start")
	}
	cancel()
	drainCtx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = p.WaitInboundMessagePolicy(drainCtx)
	stop()
	// Release the gate before reporting failure so neither old nor fixed code
	// leaves this event loop behind. The timeout bounds a blocked callback.
	close(release)
	<-finished
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("drain returned before the active routing callback finished: %v", err)
	}
	drainCtx, stop = context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := p.WaitInboundMessagePolicy(drainCtx); err != nil {
		t.Fatalf("drain after callback completion: %v", err)
	}
}

// A validator may finish successfully after PubSub cancellation. Its handoff
// must not retain a global validation slot after the event loop has stopped.
func TestKPLAsyncValidationCancellationReleasesThrottle(t *testing.T) {
	for _, cancelBeforeCompletion := range []bool{false, true} {
		name := "normal-delivery"
		if cancelBeforeCompletion {
			name = "canceled-with-full-delivery-queue"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := &PubSub{ctx: ctx, sendMsg: make(chan *Message, 1)}
			v := newValidation()
			v.p = p
			v.validateThrottle = make(chan struct{}, 1)
			started := make(chan struct{})
			release := make(chan struct{})
			val := &validatorImpl{validateThrottle: make(chan struct{}, 1), validate: func(context.Context, peer.ID, *Message) ValidationResult {
				close(started)
				<-release
				return ValidationAccept
			}}
			msg := &Message{Message: &pb.Message{Data: []byte("accepted")}}
			if cancelBeforeCompletion {
				p.sendMsg <- &Message{Message: &pb.Message{Data: []byte("already queued")}}
			}
			if err := v.validateAfterInboundPolicy([]*validatorImpl{val}, "sender", msg, false); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("async validator did not start")
			}
			if cancelBeforeCompletion {
				cancel()
			}
			close(release)
			deadline := time.Now().Add(time.Second)
			for len(v.validateThrottle) != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if len(v.validateThrottle) != 0 {
				// Release a pre-fix blocked handoff so the regression failure
				// itself does not leave a goroutine behind in the test process.
				<-p.sendMsg
				for len(v.validateThrottle) != 0 && time.Now().Before(deadline.Add(time.Second)) {
					time.Sleep(time.Millisecond)
				}
				t.Error("async validation retained its global throttle after PubSub cancellation")
			}
			if len(val.validateThrottle) != 0 {
				t.Error("async validation retained its topic throttle")
			}
			if !cancelBeforeCompletion {
				select {
				case got := <-p.sendMsg:
					if got != msg {
						t.Fatal("normal validation changed the accepted message")
					}
				default:
					t.Fatal("normal validation did not deliver its accepted message")
				}
			}
		})
	}
}

// A delayed message has already started validation tracking. If its re-entry
// queue is full, it must retire that state without changing the semantics of
// an ordinary queue rejection before signature/seen-ID admission.
func TestKPLDelayedResumeQueuePressureRetiresValidationTracking(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		name := "before-admission"
		if delayed {
			name = "delayed-resume"
		}
		t.Run(name, func(t *testing.T) {
			ids := newMsgIdGenerator()
			tags := &tagTracer{idGen: ids, nearFirst: make(map[string]map[peer.ID]struct{})}
			score := newPeerScore(&PeerScoreParams{})
			score.idGen = ids
			tracer := &pubsubTracer{pid: "local", raw: []RawTracer{tags, score}}
			p := &PubSub{ctx: context.Background(), idGen: ids}
			v := newValidation()
			v.p, v.tracer = p, tracer
			v.validateQ = make(chan *validateReq, 1)
			v.validateQ <- &validateReq{}
			topic := "blocks"
			msg := &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte("queued"), Signature: []byte("force-validation")}, ReceivedFrom: "remote"}
			tracer.ValidateMessage(msg)
			tracer.DuplicateMessage(msg)
			mid := ids.ID(msg)
			if delayed {
				v.resumeAfterInboundPolicy(nil, "remote", msg)
			} else if v.Push("remote", msg) {
				t.Fatal("full validation queue accepted a message")
			}
			record := score.deliveries.records[mid]
			if delayed {
				if len(tags.nearFirst) != 0 {
					t.Error("delayed queue rejection retained connection-tag delivery tracking")
				}
				if record.status != deliveryThrottled || record.peers != nil {
					t.Errorf("delayed queue rejection left score delivery pending: status=%d, peers=%v", record.status, record.peers)
				}
			} else {
				if len(tags.nearFirst[mid]) != 1 || record.status != deliveryUnknown || len(record.peers) != 1 {
					t.Fatal("pre-admission queue rejection retired another in-progress validation")
				}
			}
		})
	}
}
