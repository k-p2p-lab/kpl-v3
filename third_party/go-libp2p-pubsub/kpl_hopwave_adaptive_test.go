package pubsub

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestKPLAdaptiveHopwavePayloadRanges(t *testing.T) {
	boundary := 10 * 1024
	rules := []HopwavePayloadRange{{MinBytes: boundary, Factor: 0.7}, {MinBytes: 1024, MaxBytes: &boundary, Factor: 0.5}}
	const framing = 32
	configure := WithAdaptiveHopwave(HopwaveAdaptivePolicy{
		PayloadRanges: rules, PayloadSize: func(data []byte) int { return len(data) - framing },
	})
	boundary = 1025
	rules[0].Factor = 0
	for _, adaptiveFirst := range []bool{true, false} {
		gs := &GossipSubRouter{params: DefaultGossipSubParams()}
		p := &PubSub{rt: gs}
		enable := WithHopwavePublish(true)
		options := []Option{enable, configure}
		if adaptiveFirst {
			options = []Option{configure, enable}
		}
		for _, option := range options {
			if err := option(p); err != nil {
				t.Fatal(err)
			}
		}
		gs.params.HopwaveFactor = 0.24 // current fallback, including after a runtime patch
		for _, test := range []struct {
			size int
			want float64
		}{{0, 0.24}, {1023, 0.24}, {1024, 0.5}, {10239, 0.5}, {10240, 0.7}, {10241, 0.7}} {
			d := gs.hopwaveTroughDecision("topic", make([]byte, test.size+framing))
			if got := d.EffectiveFactor; got != test.want {
				t.Errorf("size=%d factor=%v, want %v", test.size, got, test.want)
			}
			if d.PayloadBytes != test.size || d.ConfiguredFactor != 0.24 || d.BaseFactor != test.want ||
				d.RangeMatched != (test.size >= 1024) || d.RetentionValid {
				t.Fatalf("unexpected size-only decision for %d bytes: %+v", test.size, d)
			}
		}
		if gs.hopwaveWindows != nil {
			t.Fatal("size-only policy allocated retention state")
		}
	}
}

func TestKPLAdaptiveHopwaveValidation(t *testing.T) {
	upper := 100
	for _, policy := range []HopwaveAdaptivePolicy{
		{RetentionWindow: time.Second},
		{PayloadRanges: []HopwavePayloadRange{{MinBytes: -1}}},
		{PayloadRanges: []HopwavePayloadRange{{MinBytes: 100, MaxBytes: &upper}}},
		{PayloadRanges: []HopwavePayloadRange{{MaxBytes: &upper}, {MinBytes: 99}}},
		{PayloadRanges: []HopwavePayloadRange{{MinBytes: 20}, {MinBytes: 5}}},
		{PayloadRanges: []HopwavePayloadRange{{Factor: math.NaN()}}},
		{PayloadRanges: []HopwavePayloadRange{{Factor: math.Inf(1)}}},
		{PayloadRanges: []HopwavePayloadRange{{Factor: -0.1}}},
	} {
		if err := WithAdaptiveHopwave(policy)(&PubSub{rt: &GossipSubRouter{}}); err == nil {
			t.Errorf("accepted invalid policy %+v", policy)
		}
	}
	if err := WithAdaptiveHopwave(HopwaveAdaptivePolicy{})(&PubSub{rt: &FloodSubRouter{}}); err == nil {
		t.Error("accepted adaptive Hopwave on an unsupported router")
	}
}

func TestKPLAdaptiveHopwaveRetentionWindows(t *testing.T) {
	ids := func(values ...peer.ID) map[peer.ID]struct{} { return peerListToMap(values) }
	gs := &GossipSubRouter{
		params: DefaultGossipSubParams(), feature: GossipSubDefaultFeatures,
		peers: map[peer.ID]protocol.ID{"p": GossipSubID_v12, "q": GossipSubID_v12, "r": GossipSubID_v12},
		mesh:  map[string]map[peer.ID]struct{}{"replacement": ids("p", "q"), "addition": ids("p", "q"), "pinned": ids("p", "q", "r")},
	}
	gs.p = &PubSub{rt: gs, myRelays: map[string]int{"replacement": 1, "addition": 1, "pinned": 1},
		topics: map[string]map[peer.ID]struct{}{"replacement": ids("p", "q", "r"), "addition": ids("p", "q", "r"), "pinned": ids("p", "q")}}
	policy := WithAdaptiveHopwave(HopwaveAdaptivePolicy{RetentionWindow: time.Minute,
		PayloadSize: func([]byte) int { t.Fatal("size callback used without payload ranges"); return 0 }})
	if err := policy(gs.p); err != nil {
		t.Fatal(err)
	}
	check := func(topic string, want float64) {
		t.Helper()
		d := gs.hopwaveTroughDecision(topic, nil)
		if got := d.EffectiveFactor; math.Abs(got-want) > 1e-12 {
			t.Fatalf("%s F=%v, want %v", topic, got, want)
		}
		if d.PayloadBytes != -1 || d.RangeMatched || d.BaseFactor != gs.params.HopwaveFactor {
			t.Fatalf("unexpected retention-only decision: %+v", d)
		}
	}
	gs.params.HopwaveFactor = 0.95
	start := time.Now()
	gs.observeHopwaveMesh(start)
	check("replacement", 0.95) // no valid R; no cap
	gs.params.HopwaveFactor = 0.5
	delete(gs.mesh["replacement"], "p")
	gs.mesh["replacement"]["r"] = struct{}{} // unchanged cardinality
	gs.mesh["addition"]["r"] = struct{}{}    // all previous neighbors retained
	gs.p.topics["pinned"] = ids("q", "r")    // same pinned IDs, different active neighbors
	gs.observeHopwaveMesh(start.Add(time.Minute - time.Nanosecond))
	check("replacement", 0.5)
	gs.observeHopwaveMesh(start.Add(time.Minute))
	check("replacement", 0.75)
	check("pinned", 0.75)
	check("addition", 0.5)
	clear(gs.mesh["replacement"])
	gs.observeHopwaveMesh(start.Add(90 * time.Second))
	check("replacement", 0.75) // keep the last completed window's value
	gs.observeHopwaveMesh(start.Add(2 * time.Minute))
	check("replacement", 0.9) // all previous neighbors lost
	gs.mesh["replacement"]["p"] = struct{}{}
	gs.params.HopwaveFactor = 0.95
	gs.observeHopwaveMesh(start.Add(3 * time.Minute))
	check("replacement", 0.95) // previous boundary had zero neighbors
	check("addition", 0.9)     // cap only with a valid R
	gs.meshFrozen = true
	gs.Leave("pinned")
	check("pinned", 0.95)
	gs.observeHopwaveMesh(start.Add(4 * time.Minute))
	check("pinned", 0.95) // first heartbeat after rejoining
	other := &GossipSubRouter{params: gs.params}
	if err := policy(&PubSub{rt: other}); err != nil || len(other.hopwaveWindows) != 0 {
		t.Fatalf("routers shared observation state: %v", err)
	}
}

func TestKPLAdaptiveHopwavePublishUsesHeartbeatAndPayloadOnlyAtTrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := DefaultGossipSubParams()
	params.HeartbeatInitialDelay = time.Hour
	params.HopwaveInterval, params.HopwaveFactor = 2, 0.3
	sizeCalls := 0
	var decisions []HopwaveDecision
	p := getGossipsub(ctx, newHopwaveHosts(t, 1)[0], WithGossipSubParams(params), WithHopwavePublish(true),
		WithAdaptiveHopwave(HopwaveAdaptivePolicy{RetentionWindow: time.Minute,
			PayloadRanges:   []HopwavePayloadRange{{MinBytes: 1024, Factor: 0.5}},
			PayloadSize:     func(data []byte) int { sizeCalls++; return len(data) },
			ObserveDecision: func(d HopwaveDecision) { decisions = append(decisions, d) },
		}))
	gs := p.rt.(*GossipSubRouter)
	done := make(chan struct{})
	p.eval <- func() {
		defer close(done)
		topic := "adaptive"
		gs.meshFrozen = true // keep the test mesh stable while exercising real heartbeat sampling
		p.myRelays[topic] = 1
		members := make(map[peer.ID]struct{})
		gs.mesh[topic] = members
		p.topics[topic] = make(map[peer.ID]struct{})
		for i := 0; i < 15; i++ {
			id := peer.ID(fmt.Sprintf("peer-%02d", i))
			members[id] = struct{}{}
			gs.peers[id] = GossipSubID_v12
			p.peers[id] = newRpcQueue(8)
			if i < 10 {
				p.topics[topic][id] = struct{}{}
			}
		}
		gs.heartbeat()
		window, observed := gs.hopwaveWindows[topic]
		if !observed || len(window.neighbors) != 10 {
			t.Errorf("first heartbeat captured %d neighbors, want 10", len(window.neighbors))
			return
		}
		// Cross one actual wall-time boundary without waiting a minute.
		window.start = window.start.Add(-time.Minute)
		gs.hopwaveWindows[topic] = window
		for i := 0; i < 5; i++ {
			delete(p.topics[topic], peer.ID(fmt.Sprintf("peer-%02d", i)))
			p.topics[topic][peer.ID(fmt.Sprintf("peer-%02d", 10+i))] = struct{}{}
		}
		gs.heartbeat()
		for i, test := range []struct {
			size, want int
			hop        *int32
		}{{1024, 7, int32ptr(0)}, {64, 6, int32ptr(0)}, {1024, 10, int32ptr(1)}, {1024, 10, nil}} {
			msg := &Message{Message: &pb.Message{Topic: &topic, From: []byte("author"), Seqno: []byte{byte(i)}, Data: make([]byte, test.size), HopCount: test.hop}, ReceivedFrom: "sender"}
			gs.Publish(msg)
			count := 0
			for id, queue := range p.peers {
				for queue.queue.Len() != 0 {
					rpc := queue.queue.Pop()
					count += len(rpc.Publish)
					if _, active := p.topics[topic][id]; !active && len(rpc.Publish) != 0 {
						t.Errorf("forwarded to inactive pinned peer %v", id)
					}
				}
			}
			if count != test.want {
				t.Errorf("case %d forwarded to %d peers, want %d", i, count, test.want)
			}
			if i < 2 {
				if len(decisions) != i+1 {
					t.Errorf("case %d recorded %d decisions", i, len(decisions))
					return
				}
				d := decisions[i]
				base := params.HopwaveFactor
				if i == 0 {
					base = 0.5
				}
				if d.Topic != topic || d.MessageID != msg.ID || d.MessageID == "" || d.HopCount != 1 ||
					d.EligiblePeers != 10 || d.SelectedPeers != count || d.PayloadBytes != test.size || d.RangeMatched != (i == 0) ||
					!d.RetentionValid || d.Retention != 0.5 || d.ConfiguredFactor != params.HopwaveFactor ||
					d.BaseFactor != base || d.EffectiveFactor != base+(1-base)*(1-d.Retention) {
					t.Errorf("case %d unexpected decision: %+v", i, d)
				}
			} else if len(decisions) != 2 {
				t.Errorf("case %d emitted a decision for a non-Trough publication", i)
			}
		}
		if sizeCalls != 2 {
			t.Errorf("size callback called %d times for two troughs", sizeCalls)
		}
	}
	<-done
}
