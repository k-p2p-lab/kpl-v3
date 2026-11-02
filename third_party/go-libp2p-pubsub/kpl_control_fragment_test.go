package pubsub

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type kplCancelExperimentalSendTracer struct {
	RawTracer
	cancel context.CancelFunc
}

func (tr *kplCancelExperimentalSendTracer) SendRPC(*RPC, peer.ID) { tr.cancel() }

func TestKPLExperimentalControlCancellationDuringAdmission(t *testing.T) {
	for i := 0; i < 32; i++ {
		p, gs, q := controlPolicyTestPubSub(t)
		p.maxMessageSize, q.maxSize = 64, 8
		ctx, cancel := context.WithCancel(context.Background())
		gs.tracer.raw = []RawTracer{&kplCancelExperimentalSendTracer{cancel: cancel}}
		topic := strings.Repeat("t", 48)
		control := &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}, {TopicID: &topic}, {TopicID: &topic}}}
		result, err := p.SendExperimentalGossipSubControl(ctx, "target", control, false)
		cancel()
		if !errors.Is(err, context.Canceled) || result != (ExperimentalControlResult{}) {
			t.Fatalf("iteration %d lost uncertain admission: result=%+v, err=%v", i, result, err)
		}
		// Wait for the already-started callback even if the caller returned on
		// cancellation before it finished. Only its first fragment may enter.
		done := make(chan struct{})
		p.eval <- func() { close(done) }
		<-done
		q.queueMu.Lock()
		queued := q.queue.Len()
		q.queueMu.Unlock()
		if queued != 1 {
			t.Fatalf("iteration %d admitted %d fragments after first-send cancellation", i, queued)
		}
	}
}

func TestKPLExperimentalControlFragmentParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20261102))
	lengths := []int{0, 1, 2, 31, 126, 127, 128, 255, 1024, 16383, 16384}
	limits := []int{1, 2, 3, 4, 7, 31, 127, 128, 129, 255, 256, 257, 1024, 4096, 16383, 16384, 16385}
	for iteration := 0; iteration < 1000; iteration++ {
		topics := []string{"", "blocks", strings.Repeat("t", lengths[rng.Intn(len(lengths))])}
		var topicPointers []*string
		topicPointers = append(topicPointers, nil)
		for i := range topics {
			topicPointers = append(topicPointers, &topics[i])
		}
		control := &pb.ControlMessage{}
		for n := rng.Intn(16); n > 0; n-- {
			control.Graft = append(control.Graft, &pb.ControlGraft{TopicID: topicPointers[rng.Intn(len(topicPointers))]})
		}
		for n := rng.Intn(8); n > 0; n-- {
			entry := &pb.ControlIHave{TopicID: topicPointers[rng.Intn(len(topicPointers))]}
			for ids := rng.Intn(20); ids > 0; ids-- {
				entry.MessageIDs = append(entry.MessageIDs, strings.Repeat("x", lengths[rng.Intn(len(lengths))]))
			}
			control.Ihave = append(control.Ihave, entry)
		}
		before, err := control.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		limit := limits[rng.Intn(len(limits))]
		want := appendOrMergeRPC(nil, limit, RPC{RPC: pb.RPC{Control: control}})
		got := experimentalControlFragments(control, limit, func() bool { return false })
		if len(got) != len(want) {
			t.Fatalf("iteration %d limit %d fragments=%d, upstream=%d", iteration, limit, len(got), len(want))
		}
		for i := range got {
			actual, err := got[i].Marshal()
			if err != nil {
				t.Fatal(err)
			}
			expected, err := want[i].Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, expected) {
				t.Fatalf("iteration %d limit %d fragment %d changed wire bytes: actual size=%d, upstream=%d", iteration, limit, i, len(actual), len(expected))
			}
		}
		after, _ := control.Marshal()
		if !bytes.Equal(before, after) {
			t.Fatalf("iteration %d mutated caller control", iteration)
		}
	}
}

func TestKPLExperimentalControlFragmentCancellation(t *testing.T) {
	topic := "blocks"
	control := &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: make([]string, 65536)}}}
	for i := range control.Ihave[0].MessageIDs {
		control.Ihave[0].MessageIDs[i] = strings.Repeat("f", 64)
	}
	calls := 0
	got := experimentalControlFragments(control, 1<<20, func() bool { calls++; return calls >= 50 })
	if got != nil || calls != 50 {
		t.Fatalf("canceled packing returned fragments=%d after %d checks", len(got), calls)
	}
	if got := experimentalControlFragments(control, 8<<20, func() bool { return true }); got != nil {
		t.Fatal("already canceled packing admitted its fast path")
	}
}

func BenchmarkKPLExperimentalLargeControlFragments(b *testing.B) {
	topic := strings.Repeat("t", 64)
	control := &pb.ControlMessage{}
	for i := 0; i < 65536; i++ {
		control.Graft = append(control.Graft, &pb.ControlGraft{TopicID: &topic})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		experimentalControlFragments(control, 4<<20, func() bool { return false })
	}
}

func FuzzKPLExperimentalControlFragmentParity(f *testing.F) {
	f.Add([]byte{1, 0, 1, 1, 0, 127, 128, 255})
	f.Add([]byte{127, 0, 15, 7, 255, 128, 127, 0})
	f.Add([]byte{255, 63, 0, 7, 31, 255, 1, 128})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 || len(data) > 1024 {
			return
		}
		index := 4
		next := func() byte { value := data[index%len(data)]; index++; return value }
		limit := 1 + int(data[0]) + 256*int(data[1])
		topic := strings.Repeat("t", int(next()))
		topics := []*string{nil, &topic, new(string)}
		control := &pb.ControlMessage{}
		for i := 0; i < int(data[2]&15); i++ {
			control.Graft = append(control.Graft, &pb.ControlGraft{TopicID: topics[int(next())%len(topics)]})
		}
		for i := 0; i < int(data[3]&7); i++ {
			entry := &pb.ControlIHave{TopicID: topics[int(next())%len(topics)]}
			for count := int(next() & 31); count > 0; count-- {
				entry.MessageIDs = append(entry.MessageIDs, strings.Repeat("x", 2*int(next())))
			}
			control.Ihave = append(control.Ihave, entry)
		}
		before, _ := control.Marshal()
		want := appendOrMergeRPC(nil, limit, RPC{RPC: pb.RPC{Control: control}})
		got := experimentalControlFragments(control, limit, func() bool { return false })
		if len(want) != len(got) {
			t.Fatalf("fragment count=%d, upstream=%d, limit=%d", len(got), len(want), limit)
		}
		for i := range got {
			expected, err := want[i].Marshal()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := got[i].Marshal()
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatalf("fragment %d differs from upstream, limit=%d, err=%v", i, limit, err)
			}
		}
		after, _ := control.Marshal()
		if !bytes.Equal(before, after) {
			t.Fatal("fragmentation mutated caller control")
		}
	})
}
