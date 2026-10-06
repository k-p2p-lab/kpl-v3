package pubsub

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func runtimeParamsTestValue() GossipSubRuntimeParams {
	return GossipSubRuntimeParams{D: 8, Dlo: 5, Dhi: 12, Dscore: 4, Dout: 2, Dlazy: 6, GossipFactor: 0, HopwaveFactor: 0.5, HopwaveInterval: 3}
}

func TestKPLRuntimeParamsAcknowledgementPreservesOtherState(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		p := &PubSub{ctx: ctx, eval: make(chan func())}
		gs := &GossipSubRouter{p: p, params: DefaultGossipSubParams(), meshFrozen: frozen, mesh: map[string]map[peer.ID]struct{}{"topic": {"member": {}}}}
		p.rt = gs
		before := gs.params
		want := runtimeParamsTestValue()
		done := make(chan error, 1)
		go func() { done <- p.SetGossipSubRuntimeParams(ctx, want) }()
		var apply func()
		select {
		case apply = <-p.eval:
		case <-time.After(time.Second):
			t.Fatal("not enqueued")
		}
		select {
		case <-done:
			t.Fatal("returned before application")
		default:
		}
		apply()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if gs.params.D != 8 || gs.params.GossipFactor != 0 || gs.params.HopwaveInterval != 3 || gs.meshFrozen != frozen || len(gs.mesh["topic"]) != 1 {
			t.Fatal("runtime parameters or frozen membership changed incorrectly")
		}
		before.D, before.GossipFactor, before.HopwaveFactor, before.HopwaveInterval = want.D, want.GossipFactor, want.HopwaveFactor, want.HopwaveInterval
		if !reflect.DeepEqual(gs.params, before) {
			t.Fatal("runtime update changed unrelated construction parameters")
		}
		cancel()
	}
}

func TestKPLRuntimeParamsCancellationAndInvalidBounds(t *testing.T) {
	p := &PubSub{ctx: context.Background(), eval: make(chan func())}
	gs := &GossipSubRouter{p: p, params: DefaultGossipSubParams()}
	p.rt = gs
	before := gs.params
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.SetGossipSubRuntimeParams(ctx, runtimeParamsTestValue()) }()
	var apply func()
	select {
	case apply = <-p.eval:
	case <-time.After(time.Second):
		t.Fatal("not enqueued")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	apply()
	if !reflect.DeepEqual(gs.params, before) {
		t.Fatal("canceled callback changed router state")
	}
	bad := runtimeParamsTestValue()
	bad.Dscore = 50
	if err := p.SetGossipSubRuntimeParams(context.Background(), bad); err == nil {
		t.Fatal("invalid pruning bounds accepted")
	}
	p.rt = &FloodSubRouter{}
	if err := p.SetGossipSubRuntimeParams(context.Background(), runtimeParamsTestValue()); err == nil {
		t.Fatal("unsupported router accepted")
	}
}
