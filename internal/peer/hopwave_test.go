package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestAdaptiveHopwavePayloadSize(t *testing.T) {
	server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "publisher", RunID: "run"}}}
	for _, encoding := range []string{"raw", "envelope"} {
		for _, size := range []int{1023, 1024, 10239, 10240} {
			message, err := server.preparePublication(model.PublishRequest{PayloadEncoding: encoding, PayloadSize: size}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if got := hopwavePayloadSize(message.wire); got != size {
				t.Fatalf("%s: %d application bytes, %d wire bytes, measured %d", encoding, size, len(message.wire), got)
			}
		}
	}
	for _, raw := range [][]byte{nil, []byte(`{"payload":"aGk="}`), []byte(`{"id":"x","publisher":"p","sentAt":1,"runId":"r","payload":"???"}`)} {
		if got := hopwavePayloadSize(raw); got != len(raw) {
			t.Fatalf("raw fallback: got %d, want %d", got, len(raw))
		}
	}
}

type hopwaveSendCount struct {
	messages   atomic.Int32
	publishing *publicationBridge
}

func (c *hopwaveSendCount) Trace(e *pb.TraceEvent) {
	c.messages.Add(int32(len(e.GetSendRPC().GetMeta().GetMessages())))
	if m := e.GetPublishMessage(); m != nil {
		c.publishing.record(m.GetTopic(), m.GetMessageID())
	}
}

func TestAdaptiveHopwaveScenarioReachesPeerForwarding(t *testing.T) {
	t.Run("default trough", func(t *testing.T) { testAdaptiveHopwaveFirstSend(t, "") })
	t.Run("explicit trough", func(t *testing.T) { testAdaptiveHopwaveFirstSend(t, "trough") })
	t.Run("crest", func(t *testing.T) { testAdaptiveHopwaveFirstSend(t, "crest") })
}

// The Trough payload policy suppresses the 1023-byte publication despite its
// larger wire envelope. A first-hop Crest forwards both publications.
func testAdaptiveHopwaveFirstSend(t *testing.T, start string) {
	const yaml = `version: 5
name: first-send
phases:
  - action: join
    group: workers
    count: 2
    node:
      gossipsub:
        hopwave: true
        floodPublish: true
        params:
          hopwaveInterval: 3%s
          hopwaveFactor: 1
          heartbeatInitialDelay: 1h
          hopwaveAdaptive:
            retentionWindow: 0s
            payloadRanges: [{minBytes: 0, maxBytes: 1024, factor: 0}]
`
	startLine := ""
	if start != "" {
		startLine = "\n          hopwaveStart: " + start
	}
	spec, err := scenario.Parse([]byte(fmt.Sprintf(yaml, startLine)))
	if err != nil {
		t.Fatal(err)
	}
	config := spec.Phases[0].Node
	server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "source", RunID: "run"}, NodeConfig: config}}
	sent := &hopwaveSendCount{publishing: &server.publishing}
	sourceTelemetry := newTelemetry(server.config.Node, "", "", nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	publisher, receiver := newConfigTestHost(t), newListeningKademliaHost(t)
	join := func(h host.Host, tracer pubsub.EventTracer, sink *telemetry) *pubsub.Topic {
		t.Helper()
		options, err := gossipSubOptions(config.GossipSub, tracer, sink)
		if err != nil {
			t.Fatal(err)
		}
		ps, err := pubsub.NewGossipSub(ctx, h, options...)
		if err != nil {
			t.Fatal(err)
		}
		topic, err := ps.Join("adaptive")
		if err != nil {
			t.Fatal(err)
		}
		return topic
	}
	source := join(publisher, sent, sourceTelemetry)
	target := join(receiver, discardEventTracer{}, nil)
	server.topics = map[string]*pubsub.Topic{"adaptive": source}
	sub, err := target.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Connect(ctx, corepeer.AddrInfo{ID: receiver.ID(), Addrs: receiver.Addrs()}); err != nil {
		t.Fatal(err)
	}
	for len(source.ListPeers()) == 0 {
		select {
		case <-ctx.Done():
			t.Fatal("subscriber did not join")
		case <-time.After(10 * time.Millisecond):
		}
	}
	sourceTelemetry.startMeasurement([]string{"adaptive"})
	small, err := server.preparePublication(model.PublishRequest{PayloadSize: 1023}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	boundary := make([]byte, 1024)
	small, err = server.publishMessage(ctx, "adaptive", small)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := server.publishMessage(ctx, "adaptive", publication{wire: boundary, encoding: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]byte{raw.pubsubID: boundary}
	wantHop := int32(1)
	if start == "crest" {
		want[small.pubsubID] = small.wire
		wantHop = 0
	}
	wantSends := len(want)
	for len(want) > 0 {
		got, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		id := hex.EncodeToString([]byte(got.ID))
		data, ok := want[id]
		if !ok || !bytes.Equal(got.Data, data) || got.HopCount == nil || got.GetHopCount() != wantHop {
			t.Fatalf("unexpected first-hop delivery: id %q, hop %d (present=%t, want %d), size %d", id, got.GetHopCount(), got.HopCount != nil, wantHop, len(got.Data))
		}
		delete(want, id)
	}
	// ListPeers synchronizes with the publisher event loop after the accepted
	// message went on the wire. A reordered remote delivery cannot mask a send.
	source.ListPeers()
	if count := sent.messages.Load(); int(count) != wantSends {
		t.Fatalf("sent %d messages, want %d", count, wantSends)
	}
	sourceTelemetry.checkpoint()
	if len(sourceTelemetry.events) != 2 {
		t.Fatalf("expected only measurement start and checkpoint, got %d events", len(sourceTelemetry.events))
	}
	if start := <-sourceTelemetry.events; start.Type != "measurement_start" {
		t.Fatalf("unexpected first event: %+v", start)
	}
	event := <-sourceTelemetry.events
	snapshot, ok := event.Fields["hopwaveAdaptive"].(hopwaveAdaptiveSnapshot)
	if event.Type != "measurement_checkpoint" || event.NodeID != "source" || !ok {
		t.Fatalf("missing adaptive checkpoint from source: %+v", event)
	}
	if start == "crest" {
		if snapshot.TroughDecisions != 0 || snapshot.Adjustments != 0 {
			t.Fatalf("Crest applied the adaptive Trough policy: %+v", snapshot)
		}
		return
	}
	if snapshot.TroughDecisions != 2 || snapshot.Adjustments != 1 || snapshot.PayloadAdjustments != 1 ||
		snapshot.RetentionAdjustments != 0 || snapshot.OmittedRecords != 0 || len(snapshot.Records) != 1 {
		t.Fatalf("unexpected adaptive counts: %+v", snapshot)
	}
	record := snapshot.Records[0]
	if record.Adjustment != 1 || record.At.IsZero() || record.PubsubMessageID != small.pubsubID || record.PubsubMessageID == raw.pubsubID ||
		record.Topic != "adaptive" || record.HopCount != 1 || record.PayloadBytes == nil || *record.PayloadBytes != 1023 ||
		!record.RangeMatched || record.ConfiguredFactor != 1 || record.BaseFactor != 0 || record.EffectiveFactor != 0 ||
		record.Retention != nil || record.EligiblePeers != 1 || record.SelectedPeers != 0 {
		t.Fatalf("unexpected record for the suppressed publication: %+v", record)
	}
}

func TestHopwaveOptionReachesPeerPubSub(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		config := model.NodeConfig{GossipSub: model.GossipSubConfig{Hopwave: &enabled}}.WithDefaults()
		options, err := gossipSubOptions(config.GossipSub, discardEventTracer{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		t.Cleanup(cancel)
		instance, err := pubsub.NewGossipSub(ctx, newConfigTestHost(t), options...)
		if err != nil {
			t.Fatal(err)
		}
		topic, err := instance.Join("kpl/default")
		if err != nil {
			t.Fatal(err)
		}
		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		if err := topic.Publish(ctx, []byte("local")); err != nil {
			t.Fatal(err)
		}
		msg, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			if msg.HopCount == nil || msg.GetHopCount() != 0 || msg.GetPropaType() != pb.PropagationType_EAGER_PUSH {
				t.Fatal("configured Peer did not seed Hopwave metadata")
			}
		} else if msg.HopCount != nil || msg.PropaType != nil {
			t.Fatal("disabled Peer emitted Hopwave metadata")
		}
		cancel()
	}
}

func TestHopwaveDeliveryAndDuplicatePreserveReportedMetadata(t *testing.T) {
	s := &Server{host: newConfigTestHost(t), config: model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run"}, NodeConfig: model.NodeConfig{}.WithDefaults()}}
	for _, encoding := range []string{"raw", "envelope"} {
		data := []byte("raw payload")
		if encoding == "envelope" {
			var err error
			data, err = json.Marshal(envelope{ID: "message", RunID: "run", Publisher: "remote", SentAt: 1})
			if err != nil {
				t.Fatal(err)
			}
		}
		topic, hops := "kpl/default", int32(3)
		msg := &pubsub.Message{Message: &pb.Message{Data: data, Topic: &topic, HopCount: &hops, PropaType: pb.PropagationType_LAZY_PULL.Enum()}, ID: "wire-id", ReceivedFrom: corepeer.ID("remote")}
		delivery, ok := s.deliveryEvent(msg, topic, time.Now())
		if !ok {
			t.Fatal("delivery rejected")
		}
		duplicate, ok := s.duplicateEvent(msg, time.Now())
		if !ok {
			t.Fatal("duplicate rejected")
		}
		for _, event := range []model.TraceEvent{delivery, duplicate} {
			if event.Fields["hopwaveHopCount"] != hops || event.Fields["hopwaveHopCountAvailable"] != true || event.Fields["hopwavePropagationType"] != "lazy-pull" || event.Fields["hopwaveSource"] != "wire" || event.Fields["hopwaveMetadataVersion"] != 1 {
				t.Fatalf("%s %s lost metadata: %+v", encoding, event.Type, event.Fields)
			}
			if event.Fields["hop"] != nil || event.Fields["linkEstimate"] != nil {
				t.Fatal("wire reports replaced inferred measurement fields")
			}
		}
		if msg.GetHopCount() != hops || msg.GetPropaType() != pb.PropagationType_LAZY_PULL {
			t.Fatal("telemetry mutated the received message")
		}
	}
	for _, msg := range []*pb.Message{{}, {PropaType: pb.PropagationType_EAGER_PUSH.Enum()}, {HopCount: func() *int32 { v := int32(-1); return &v }()}} {
		fields := map[string]any{}
		addHopwaveFields(fields, msg)
		if msg.HopCount == nil && msg.PropaType == nil {
			if !reflect.DeepEqual(fields, map[string]any{}) {
				t.Fatal("legacy message acquired fabricated metadata")
			}
		} else if fields["hopwaveHopCountAvailable"] != false || fields["hopwaveHopCount"] != nil {
			t.Fatal("missing or invalid hop count became zero")
		}
	}
}
