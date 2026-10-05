package pubsub

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	discimpl "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

type kplAuditAddrBook struct {
	peerstore.AddrBook
	closes atomic.Int32
	closed chan struct{}
	once   sync.Once
}

func (b *kplAuditAddrBook) Close() error {
	b.closes.Add(1)
	err := b.AddrBook.(io.Closer).Close()
	b.once.Do(func() { close(b.closed) })
	return err
}

type kplAuditEventHost struct {
	host.Host
	bus event.Bus
}

func (h *kplAuditEventHost) EventBus() event.Bus { return h.bus }

type kplAuditEventBus struct {
	event.Bus
	attempted    chan struct{}
	once         sync.Once
	subscription event.Subscription
}

func (b *kplAuditEventBus) Subscribe(eventType interface{}, opts ...event.SubscriptionOpt) (event.Subscription, error) {
	types, ok := eventType.([]interface{})
	if !ok {
		types = []interface{}{eventType}
	}
	for _, typ := range types {
		if _, ok := typ.(*event.EvtPeerConnectednessChanged); ok {
			b.once.Do(func() { close(b.attempted) })
			if b.subscription != nil {
				return b.subscription, nil
			}
			return nil, errors.New("address-book event subscription rejected")
		}
	}
	return b.Bus.Subscribe(eventType, opts...)
}

func TestKPLAddrBookSubscriptionFailureStillClosesOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHopwaveHosts(t, 1)[0]
	bus := &kplAuditEventBus{Bus: h.EventBus(), attempted: make(chan struct{})}
	wrapped := &kplAuditEventHost{Host: h, bus: bus}
	var book *kplAuditAddrBook
	p, err := NewGossipSub(ctx, wrapped, func(p *PubSub) error {
		router := p.rt.(*GossipSubRouter)
		book = &kplAuditAddrBook{AddrBook: router.cab, closed: make(chan struct{})}
		router.cab = book
		return nil
	})
	if err != nil || p == nil {
		t.Fatalf("pubsub construction failed before the asynchronous event subscription: %v", err)
	}
	t.Cleanup(func() {
		if book.closes.Load() == 0 {
			_ = book.Close()
		}
	})
	select {
	case <-bus.attempted:
	case <-time.After(time.Second):
		t.Fatal("address-book worker did not attempt event subscription")
	}
	select {
	case <-book.closed:
		t.Fatal("event subscription failure closed an address book still used by the live router")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case <-book.closed:
	case <-time.After(time.Second):
		t.Fatal("event subscription failure retained the owned address book after pubsub shutdown")
	}
	if got := book.closes.Load(); got != 1 {
		t.Fatalf("address book closed %d times, want once", got)
	}
}

type kplAuditClosedSubscription struct {
	out      chan interface{}
	outCalls atomic.Int32
	closes   atomic.Int32
}

func (s *kplAuditClosedSubscription) Name() string { return "closed address-book events" }
func (s *kplAuditClosedSubscription) Close() error { s.closes.Add(1); return nil }
func (s *kplAuditClosedSubscription) Out() <-chan interface{} {
	s.outCalls.Add(1)
	return s.out
}

func TestKPLAddrBookClosedEventStreamWaitsForShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHopwaveHosts(t, 1)[0]
	subscription := &kplAuditClosedSubscription{out: make(chan interface{})}
	close(subscription.out)
	bus := &kplAuditEventBus{Bus: h.EventBus(), attempted: make(chan struct{}), subscription: subscription}
	wrapped := &kplAuditEventHost{Host: h, bus: bus}
	var book *kplAuditAddrBook
	_, err := NewGossipSub(ctx, wrapped, func(p *PubSub) error {
		router := p.rt.(*GossipSubRouter)
		book = &kplAuditAddrBook{AddrBook: router.cab, closed: make(chan struct{})}
		router.cab = book
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if book.closes.Load() == 0 {
			_ = book.Close()
		}
	})
	select {
	case <-bus.attempted:
	case <-time.After(time.Second):
		t.Fatal("address-book worker did not attempt event subscription")
	}
	select {
	case <-book.closed:
		t.Error("closed event stream ended address-book lifetime before pubsub shutdown")
	case <-time.After(20 * time.Millisecond):
	}
	// Repeated source polling after channel closure exposes the original hot
	// loop without relying on global CPU or goroutine measurements.
	if calls := subscription.outCalls.Load(); calls > 2 {
		t.Errorf("closed event stream was repeatedly polled %d times", calls)
	}
	cancel()
	select {
	case <-book.closed:
	case <-time.After(time.Second):
		t.Fatal("closed event stream prevented address-book shutdown")
	}
	if got := book.closes.Load(); got != 1 {
		t.Errorf("address book closed %d times, want once", got)
	}
	if got := subscription.closes.Load(); got != 1 {
		t.Errorf("event subscription closed %d times, want once", got)
	}
}

func TestKPLFailedInitializationPreservesCallerRouterAndHost(t *testing.T) {
	failure := errors.New("constructor rejected configuration")
	for _, stage := range []string{"option", "signature", "discovery", "canceled-option"} {
		t.Run(stage, func(t *testing.T) {
			h := newHopwaveHosts(t, 1)[0]
			rt := DefaultGossipSubRouter(h)
			book := &kplAuditAddrBook{AddrBook: rt.cab, closed: make(chan struct{})}
			rt.cab = book
			t.Cleanup(func() {
				if book.closes.Load() == 0 {
					_ = book.Close()
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var opts []Option
			switch stage {
			case "option":
				opts = []Option{func(*PubSub) error { return failure }}
			case "signature":
				opts = []Option{WithMessageAuthor(peer.ID("unavailable-signing-key"))}
			case "discovery":
				opts = []Option{WithDiscovery(&pubSubDiscovery{}, WithDiscoverConnector(func(host.Host) (*discimpl.BackoffConnector, error) {
					return nil, failure
				}))}
			case "canceled-option":
				opts = []Option{func(*PubSub) error { cancel(); return ctx.Err() }}
			}
			if _, err := NewGossipSubWithRouter(ctx, h, rt, opts...); err == nil {
				t.Fatal("invalid constructor unexpectedly succeeded")
			}
			if got := book.closes.Load(); got != 0 {
				t.Fatalf("failed constructor closed caller-owned router resources %d times", got)
			}
			if h.Peerstore().PrivKey(h.ID()) == nil || len(h.Network().ListenAddresses()) == 0 {
				t.Fatal("failed constructor damaged the caller's host")
			}

			// Reusing the same router and host must work after correcting options.
			retryCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			p, err := NewGossipSubWithRouter(retryCtx, h, rt, WithRawTracer(rt.tagTracer))
			if err != nil {
				t.Fatalf("retry with caller's router: %v", err)
			}
			topic, err := p.Join("retry-after-constructor-failure")
			if err != nil {
				t.Fatal(err)
			}
			sub, err := topic.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			if err := topic.Publish(retryCtx, []byte("caller-resource-still-usable")); err != nil {
				t.Fatal(err)
			}
			message, err := sub.Next(retryCtx)
			if err != nil || string(message.GetData()) != "caller-resource-still-usable" {
				t.Fatalf("retry did not deliver a signed message: message=%v err=%v", message, err)
			}
			stop()
			select {
			case <-book.closed:
			case <-time.After(time.Second):
				t.Fatal("successful attachment failed to close the router address book on cancellation")
			}
			if got := book.closes.Load(); got != 1 {
				t.Fatalf("address book closed %d times, want once after successful attachment", got)
			}
		})
	}
}

func TestKPLDirectReconnectDrainsBacklogAcrossHeartbeats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := &GossipSubRouter{
		p: &PubSub{ctx: ctx}, params: GossipSubParams{DirectConnectTicks: 3}, heartbeatTicks: 3,
		direct: map[peer.ID]struct{}{"one": {}, "two": {}, "three": {}, "four": {}},
		peers:  make(map[peer.ID]protocol.ID), connect: make(chan connectInfo, 1),
	}
	for range len(gs.direct) {
		gs.directConnect()
		if len(gs.connect) != 1 {
			t.Fatal("available reconnect capacity was not used")
		}
		// A full queue must preserve the pending peer and defer remaining ones.
		gs.directConnect()
		pending := <-gs.connect
		if _, duplicate := gs.peers[pending.p]; duplicate {
			t.Fatalf("already connected peer was retried: %s", pending.p)
		}
		gs.peers[pending.p] = GossipSubID_v12
		gs.heartbeatTicks += 3
	}
	gs.directConnect()
	if len(gs.peers) != len(gs.direct) || len(gs.connect) != 0 {
		t.Fatal("bounded reconnect queue lost a direct peer or retried connected peers")
	}
}

func TestKPLHeartbeatPeriodicLoopStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := &GossipSubRouter{
		p:      &PubSub{ctx: ctx, eval: make(chan func())},
		params: GossipSubParams{HeartbeatInitialDelay: 0, HeartbeatInterval: time.Millisecond},
	}
	done := make(chan struct{})
	go func() { defer close(done); gs.heartbeatTimer() }()
	for range 2 {
		select {
		case <-gs.p.eval:
		case <-time.After(time.Second):
			t.Fatal("heartbeat did not reach its periodic schedule")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("periodic heartbeat waited for another event-loop receiver after cancellation")
	}
}
