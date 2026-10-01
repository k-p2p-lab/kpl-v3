package pubsub

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	discimpl "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

type kplTrackedAddrBook struct {
	peerstore.AddrBook
	closed bool
}

func (b *kplTrackedAddrBook) Close() error {
	b.closed = true
	return b.AddrBook.(io.Closer).Close()
}

func TestKPLPubSubInitializationFailureReleasesResources(t *testing.T) {
	failure := errors.New("initialization failed")
	for name, options := range map[string][]Option{
		"option":    {func(*PubSub) error { return failure }},
		"signature": {WithNoAuthor(), WithMessageSigning(true)},
		"discovery": {WithDiscovery(&pubSubDiscovery{}, WithDiscoverConnector(func(host.Host) (*discimpl.BackoffConnector, error) {
			return nil, failure
		}))},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var captured *PubSub
			var book *kplTrackedAddrBook
			capture := func(p *PubSub) error {
				captured = p
				router := p.rt.(*GossipSubRouter)
				book = &kplTrackedAddrBook{AddrBook: router.cab}
				router.cab = book
				return nil
			}
			_, err := NewGossipSub(ctx, newHopwaveHosts(t, 1)[0], append([]Option{capture}, options...)...)
			if err == nil {
				t.Fatal("invalid initialization succeeded")
			}
			if !book.closed {
				t.Error("failed constructor retained its address book")
				_ = book.Close()
			}
			if captured.deadPeerBackoff != nil {
				t.Error("failed constructor started a backoff cleanup loop")
			}
			if captured.seenMessages != nil {
				t.Error("failed constructor started the seen-message cache")
				captured.seenMessages.Done()
			}
		})
	}
}

func TestKPLHeartbeatInitialDelayStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := &GossipSubRouter{
		p:      &PubSub{ctx: ctx, eval: make(chan func())},
		params: GossipSubParams{HeartbeatInitialDelay: time.Hour},
	}
	done := make(chan struct{})
	go func() { defer close(done); gs.heartbeatTimer() }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat retained its initial timer after cancellation")
	}
}

func TestKPLDirectReconnectDoesNotAccumulateBlockedSenders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := &GossipSubRouter{
		p:       &PubSub{ctx: ctx},
		params:  GossipSubParams{DirectConnectTicks: 1},
		direct:  map[peer.ID]struct{}{"remote": {}},
		connect: make(chan connectInfo, 1),
	}
	gs.connect <- connectInfo{p: "busy"}
	before := runtime.NumGoroutine()
	for range 32 {
		gs.directConnect()
	}
	// A full queue must leave retries to a later heartbeat, without spawning
	// another blocked sender for each tick. Allow unrelated runtime activity.
	if extra := runtime.NumGoroutine() - before; extra > 8 {
		t.Errorf("full connection queue retained %d extra goroutines", extra)
	}
	cancel()
	// Also drain old implementations so a failing regression leaves no leaks.
	for range 33 {
		select {
		case <-gs.connect:
		case <-time.After(20 * time.Millisecond):
			return
		}
	}
}

func TestKPLInitialDirectConnectStopsDuringDelayAndQueueWait(t *testing.T) {
	for _, delay := range []time.Duration{time.Hour, 0} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gs := &GossipSubRouter{
				p:       &PubSub{ctx: ctx},
				params:  GossipSubParams{DirectConnectInitialDelay: delay},
				direct:  map[peer.ID]struct{}{"one": {}, "two": {}},
				connect: make(chan connectInfo),
			}
			done := make(chan struct{})
			go func() { defer close(done); gs.initialDirectConnect() }()
			if delay == 0 {
				select {
				case <-gs.connect:
				case <-time.After(time.Second):
					t.Fatal("initial connector did not enqueue its first peer")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("initial connector survived cancellation")
			}
		})
	}
}

func TestKPLDirectReconnectResumesWhenQueueHasCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gs := &GossipSubRouter{
		p:       &PubSub{ctx: ctx},
		params:  GossipSubParams{DirectConnectTicks: 1},
		direct:  map[peer.ID]struct{}{"connected": {}, "missing": {}},
		peers:   map[peer.ID]protocol.ID{"connected": GossipSubID_v12},
		connect: make(chan connectInfo, 1),
	}
	gs.connect <- connectInfo{p: "busy"}
	gs.directConnect()
	if got := <-gs.connect; got.p != "busy" {
		t.Fatalf("overwrote a queued connection: %+v", got)
	}
	gs.directConnect()
	select {
	case got := <-gs.connect:
		if got.p != "missing" {
			t.Fatalf("enqueued wrong peer: %+v", got)
		}
	default:
		t.Fatal("disconnected direct peer was not retried")
	}
	cancel()
	gs.directConnect()
	if len(gs.connect) != 0 {
		t.Fatal("connection queued after cancellation")
	}
}
