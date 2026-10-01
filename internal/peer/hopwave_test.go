package peer

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestHopWaveOptionReachesPeerPubSub(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		config := model.NodeConfig{GossipSub: model.GossipSubConfig{HopWave: &enabled}}.WithDefaults()
		options, err := gossipSubOptions(config.GossipSub, discardEventTracer{})
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
				t.Fatal("configured Peer did not seed HopWave metadata")
			}
		} else if msg.HopCount != nil || msg.PropaType != nil {
			t.Fatal("disabled Peer emitted HopWave metadata")
		}
		cancel()
	}
}

func TestHopWaveDeliveryAndDuplicatePreserveReportedMetadata(t *testing.T) {
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
			if event.Fields["hopWaveHopCount"] != hops || event.Fields["hopWaveHopCountAvailable"] != true || event.Fields["hopWavePropagationType"] != "lazy-pull" || event.Fields["hopWaveSource"] != "wire" || event.Fields["hopWaveMetadataVersion"] != 1 {
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
		addHopWaveFields(fields, msg)
		if msg.HopCount == nil && msg.PropaType == nil {
			if !reflect.DeepEqual(fields, map[string]any{}) {
				t.Fatal("legacy message acquired fabricated metadata")
			}
		} else if fields["hopWaveHopCountAvailable"] != false || fields["hopWaveHopCount"] != nil {
			t.Fatal("missing or invalid hop count became zero")
		}
	}
}
