package pubsub

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

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
