package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type heldAttackDrainTrace struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	runtime *attackRuntime
}

func (trace *heldAttackDrainTrace) Trace(event *pb.TraceEvent) {
	if event.GetType() != pb.TraceEvent_JOIN {
		return
	}
	close(trace.entered)
	<-trace.release
	// Model a policy counter settling at the end of a slow router callback.
	trace.runtime.count("forwarding", "publishWithheld", 1)
	close(trace.done)
}

func TestFinalAttackStatsDistinguishPolicyDrainTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := newRunTestServer(t, model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run", Role: "worker"}, NodeConfig: model.NodeConfig{Forwarding: &model.ForwardingConfig{ProbabilityAttack: model.ProbabilityAttack{Enabled: attackBool(true)}}}})
	s.telemetry.events = make(chan model.TraceEvent, 8)
	s.attacks = newAttackRuntime(s)
	s.attacks.flush(false)
	periodic := <-s.telemetry.events
	if _, exists := periodic.Fields["policyDrainComplete"]; exists {
		t.Fatal("periodic statistics claimed a shutdown drain outcome")
	}
	if _, exists := periodic.Fields["drainError"]; exists {
		t.Fatal("periodic statistics included a shutdown drain error")
	}
	trace := &heldAttackDrainTrace{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}), runtime: s.attacks}
	var once sync.Once
	unblock := func() { once.Do(func() { close(trace.release) }) }
	defer unblock()
	p, err := pubsub.NewGossipSub(ctx, s.host,
		pubsub.WithInboundMessagePolicy(func(*pubsub.Message) pubsub.InboundMessageDecision { return pubsub.InboundMessageDecision{} }, pubsub.InboundMessagePolicyLimits{MaxPendingMessages: 1, MaxPendingBytes: 1024}),
		pubsub.WithEventTracer(trace))
	if err != nil {
		t.Fatal(err)
	}
	s.pubsub = p
	topic, err := p.Join("held")
	if err != nil {
		t.Fatal(err)
	}
	subscribed := make(chan struct{})
	go func() {
		defer close(subscribed)
		if sub, err := topic.Subscribe(); err == nil {
			sub.Cancel()
		}
	}()
	select {
	case <-trace.entered:
	case <-time.After(time.Second):
		t.Fatal("router callback did not start")
	}
	// Fill ordinary telemetry capacity; final failure evidence still owns its
	// reserved shutdown slot and is not transformed into a successful drain.
	for i := 0; i < 20; i++ {
		s.telemetry.emit(model.TraceEvent{Type: "deliver"})
	}
	cancel()
	drain, stop := context.WithTimeout(t.Context(), 30*time.Millisecond)
	err = s.finishAttackRuntime(drain)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("blocked callback did not fail the bounded drain: %v", err)
	}
	first := finalAttackSnapshot(t, s.telemetry)
	if first.Fields["policyDrainComplete"] != false || first.Fields["drainError"] == nil {
		t.Errorf("failed drain was not identified as incomplete: %+v", first.Fields)
	}
	if got := first.Fields["counters"].(map[string]map[string]uint64)["forwarding"]["publishWithheld"]; got != 0 {
		t.Fatalf("blocked callback had already settled: %d", got)
	}
	unblock()
	select {
	case <-trace.done:
	case <-time.After(time.Second):
		t.Fatal("router callback did not finish")
	}
	<-subscribed
	drain, stop = context.WithTimeout(t.Context(), time.Second)
	err = s.finishAttackRuntime(drain)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	last := finalAttackSnapshot(t, s.telemetry)
	if last.Fields["policyDrainComplete"] != true || last.Fields["drainError"] != nil {
		t.Errorf("completed drain has incorrect quality: %+v", last.Fields)
	}
	if got := last.Fields["counters"].(map[string]map[string]uint64)["forwarding"]["publishWithheld"]; got != 1 {
		t.Fatalf("completed drain missed the late callback: %d", got)
	}
	if !first.Timestamp.Before(last.Timestamp) {
		t.Error("final snapshots lost observation order")
	}
}

func finalAttackSnapshot(t *testing.T, telemetry *telemetry) model.TraceEvent {
	t.Helper()
	for len(telemetry.events) > 0 {
		event := <-telemetry.events
		if event.Type == "attack_stats" && event.Fields["final"] == true {
			return event
		}
	}
	t.Fatal("final attack snapshot did not reach the reserved telemetry slot")
	return model.TraceEvent{}
}
