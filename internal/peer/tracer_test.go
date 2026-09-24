package peer

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func TestGossipTracerAggregatesEveryControlTypeInMixedRPC(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	remote, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	topicA, topicB := "topic-a", "topic-b"
	timestamp := time.Now().Add(-time.Second).UnixNano()
	telemetry := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 10)}
	tracer := &gossipTracer{telemetry: telemetry, peerID: "local"}
	tracer.Trace(&pubsubpb.TraceEvent{
		Type: pubsubpb.TraceEvent_SEND_RPC.Enum(), Timestamp: &timestamp,
		SendRPC: &pubsubpb.TraceEvent_SendRPC{SendTo: []byte(remote), Meta: &pubsubpb.TraceEvent_RPCMeta{Control: &pubsubpb.TraceEvent_ControlMeta{
			Ihave: []*pubsubpb.TraceEvent_ControlIHaveMeta{
				{Topic: &topicB, MessageIDs: [][]byte{[]byte("one"), []byte("two")}},
				{Topic: &topicA, MessageIDs: [][]byte{[]byte("three")}},
			},
			Iwant:     []*pubsubpb.TraceEvent_ControlIWantMeta{{MessageIDs: [][]byte{[]byte("one"), []byte("three")}}},
			Idontwant: []*pubsubpb.TraceEvent_ControlIDontWantMeta{{MessageIDs: [][]byte{[]byte("two")}}},
			Graft:     []*pubsubpb.TraceEvent_ControlGraftMeta{{Topic: &topicA}, {Topic: &topicB}},
			Prune:     []*pubsubpb.TraceEvent_ControlPruneMeta{{Topic: &topicA, Peers: [][]byte{[]byte("px-1"), []byte("px-2")}}},
		}}},
	})

	events := make(map[string]model.TraceEvent)
	for range 5 {
		select {
		case event := <-telemetry.events:
			events[event.Type] = event
		default:
			t.Fatalf("only %d mixed control events were emitted", len(events))
		}
	}
	if len(events) != 5 {
		t.Fatalf("mixed RPC events = %v", events)
	}
	for eventType, event := range events {
		if event.RemotePeerID != remote.String() || event.Timestamp.UnixNano() != timestamp || event.RunID != "run" || event.EventID == "" {
			t.Fatalf("%s metadata=%+v", eventType, event)
		}
	}
	ihave := events["send_ihave"]
	if ihave.Topic != "" || ihave.Fields["controlEntries"] != 2 || ihave.Fields["messageIdCount"] != 3 || !reflect.DeepEqual(ihave.Fields["topics"], []string{topicA, topicB}) ||
		!reflect.DeepEqual(ihave.Fields["topicEntryCounts"], map[string]int{topicA: 1, topicB: 1}) || !reflect.DeepEqual(ihave.Fields["topicMessageIdCounts"], map[string]int{topicA: 1, topicB: 2}) {
		t.Fatalf("IHAVE aggregation=%+v", ihave)
	}
	if event := events["send_iwant"]; event.Fields["controlEntries"] != 1 || event.Fields["messageIdCount"] != 2 {
		t.Fatalf("IWANT aggregation=%+v", event)
	}
	if event := events["send_idontwant"]; event.Fields["controlEntries"] != 1 || event.Fields["messageIdCount"] != 1 {
		t.Fatalf("IDONTWANT aggregation=%+v", event)
	}
	if event := events["send_graft"]; event.Fields["controlEntries"] != 2 || event.Fields["messageIdCount"] != 0 {
		t.Fatalf("GRAFT aggregation=%+v", event)
	}
	if event := events["send_prune"]; event.Topic != "" || event.Fields["controlEntries"] != 1 || event.Fields["peerExchangeCount"] != 2 {
		t.Fatalf("PRUNE aggregation=%+v", event)
	}
}

func TestGossipTracerSeparatesReceivedAndDroppedControlRPCs(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	remote, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	topic := "topic"
	meta := &pubsubpb.TraceEvent_RPCMeta{Control: &pubsubpb.TraceEvent_ControlMeta{
		Ihave: []*pubsubpb.TraceEvent_ControlIHaveMeta{{Topic: &topic, MessageIDs: [][]byte{[]byte("one")}}},
	}}
	for _, test := range []struct {
		name, want string
		event      pubsubpb.TraceEvent
	}{
		{"receive", "recv_ihave", pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_RECV_RPC.Enum(), RecvRPC: &pubsubpb.TraceEvent_RecvRPC{ReceivedFrom: []byte(remote), Meta: meta}}},
		{"drop", "drop_ihave", pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_DROP_RPC.Enum(), DropRPC: &pubsubpb.TraceEvent_DropRPC{SendTo: []byte(remote), Meta: meta}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			telemetry := &telemetry{node: model.Node{RunID: "run"}, events: make(chan model.TraceEvent, 1)}
			(&gossipTracer{telemetry: telemetry}).Trace(&test.event)
			event := <-telemetry.events
			if event.Type != test.want || event.Topic != "" || event.Fields["rpcCount"] != 1 || event.Fields["direction"] != test.want[:len(test.want)-len("_ihave")] || !reflect.DeepEqual(event.Fields["topics"], []string{topic}) {
				t.Fatalf("event=%+v", event)
			}
		})
	}
	telemetry := &telemetry{events: make(chan model.TraceEvent, 1)}
	(&gossipTracer{telemetry: telemetry}).Trace(&pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_SEND_RPC.Enum(), SendRPC: &pubsubpb.TraceEvent_SendRPC{}})
	if len(telemetry.events) != 0 {
		t.Fatal("an RPC without control metadata produced telemetry")
	}
}

func TestGossipTracerPreservesPeerAndTopicLifecycle(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	remote, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	protocol, topic := "/meshsub/1.2.0", "topic-a"
	timestamp := time.Now().Add(-time.Second).UnixNano()
	for _, test := range []struct {
		event                         pubsubpb.TraceEvent
		wantType, wantPeer, wantTopic string
	}{
		{pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_ADD_PEER.Enum(), AddPeer: &pubsubpb.TraceEvent_AddPeer{PeerID: []byte(remote), Proto: &protocol}}, "add_peer", remote.String(), ""},
		{pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_REMOVE_PEER.Enum(), RemovePeer: &pubsubpb.TraceEvent_RemovePeer{PeerID: []byte(remote)}}, "remove_peer", remote.String(), ""},
		{pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_JOIN.Enum(), Join: &pubsubpb.TraceEvent_Join{Topic: &topic}}, "join", "", topic},
		{pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_LEAVE.Enum(), Leave: &pubsubpb.TraceEvent_Leave{Topic: &topic}}, "leave", "", topic},
	} {
		t.Run(test.wantType, func(t *testing.T) {
			telemetry := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 1)}
			tracer := &gossipTracer{telemetry: telemetry, peerID: "local"}
			test.event.Timestamp = &timestamp
			tracer.Trace(&test.event)
			select {
			case event := <-telemetry.events:
				if event.Type != test.wantType || event.RemotePeerID != test.wantPeer || event.Topic != test.wantTopic || event.Timestamp.UnixNano() != timestamp || event.RunID != "run" {
					t.Fatalf("lifecycle event=%+v", event)
				}
			default:
				t.Fatal("lifecycle event was dropped")
			}
		})
	}
}

func TestDuplicateTracerCorrelatesMessagesWithoutDoubleCounting(t *testing.T) {
	server, _ := publicationTestServer(t)
	remote := corepeer.ID("source-peer")
	now := time.Now().UTC()
	encoded, err := json.Marshal(envelope{ID: "application-message", RunID: "run", Publisher: "source-node", SentAt: now.Add(-25 * time.Millisecond).UnixNano(), Payload: []byte{42}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, encoding, wantID string
		data                   []byte
	}{
		{"envelope", "envelope", "application-message", encoded},
		{"raw", "raw", "pubsub-" + hex.EncodeToString([]byte("native-wire-id")), []byte{42}},
	} {
		t.Run(test.name, func(t *testing.T) {
			// Discard startup peer/topic traces before checking exactly one copy.
			for len(server.telemetry.events) > 0 {
				<-server.telemetry.events
			}
			message := publicationTestMessage(test.data, "native-wire-id", remote)
			message.From = []byte(remote)
			tracer := &messageTracer{server: server}
			tracer.DuplicateMessage(message)
			(&gossipTracer{telemetry: server.telemetry, peerID: server.host.ID().String()}).Trace(&pubsubpb.TraceEvent{
				Type:             pubsubpb.TraceEvent_DUPLICATE_MESSAGE.Enum(),
				DuplicateMessage: &pubsubpb.TraceEvent_DuplicateMessage{MessageID: []byte(message.ID), Topic: message.Topic, ReceivedFrom: []byte(remote)},
			})
			if len(server.telemetry.events) != 1 {
				t.Fatalf("one router duplicate produced %d events", len(server.telemetry.events))
			}
			event := <-server.telemetry.events
			if event.Type != "duplicate" || event.MessageID != test.wantID || event.Fields["payloadEncoding"] != test.encoding || event.Fields["localDelivery"] != false || event.Fields["pubsubMessageId"] != hex.EncodeToString([]byte(message.ID)) || event.EventID == "" {
				t.Fatalf("duplicate metadata=%+v", event)
			}
			if _, exists := event.Fields["latencyAvailable"]; exists || event.LatencyMS != 0 {
				t.Fatal("router duplicate fabricated an application delivery latency")
			}
			if test.encoding == "envelope" && event.Fields["publisher"] != "source-node" {
				t.Fatalf("duplicate lost envelope publisher: %+v", event)
			}
			if test.encoding == "raw" {
				if _, exists := event.Fields["publisher"]; exists {
					t.Fatal("raw duplicate fabricated an application publisher")
				}
			}
			// Two actual wire copies remain distinct events for source-ID dedup.
			tracer.DuplicateMessage(message)
			second := <-server.telemetry.events
			if event.EventID == second.EventID || event.MessageID != second.MessageID {
				t.Fatalf("duplicate copies have incorrect identities: %+v %+v", event, second)
			}
		})
	}
	tracer := &messageTracer{server: server}
	tracer.DuplicateMessage(nil)
	tracer.DuplicateMessage(&pubsub.Message{})
}

func TestApplicationDeliverySeparatesRemoteLatencyFromLocalCopies(t *testing.T) {
	server, _ := publicationTestServer(t)
	sentAt := time.Now().UTC()
	data, err := json.Marshal(envelope{ID: "message", RunID: "run", Publisher: "source-node", SentAt: sentAt.UnixNano(), Payload: []byte{42}})
	if err != nil {
		t.Fatal(err)
	}
	message := publicationTestMessage(data, "native-wire-id", corepeer.ID("relay-peer"))
	message.From = []byte(corepeer.ID("source-peer"))
	receivedAt := sentAt.Add(42*time.Millisecond + 500*time.Microsecond)
	event, ok := server.deliveryEvent(message, "topic-a", receivedAt)
	if !ok || event.LatencyMS != 42.5 || event.Timestamp != receivedAt || event.Fields["localDelivery"] != false || event.Fields["latencyAvailable"] != true || event.Fields["publisher"] != "source-node" {
		t.Fatalf("remote application delivery=%+v", event)
	}
	message.ReceivedFrom = server.host.ID()
	event, ok = server.deliveryEvent(message, "topic-a", receivedAt)
	if !ok || event.Fields["localDelivery"] != true {
		t.Fatalf("local subscription delivery classified as remote: %+v", event)
	}
	message.ReceivedFrom = corepeer.ID("relay-peer")
	message.From = []byte(server.host.ID())
	message.Data = []byte{42}
	event, ok = server.deliveryEvent(message, "topic-a", receivedAt)
	if !ok || event.Fields["localDelivery"] != true || event.Fields["latencyAvailable"] != false {
		t.Fatalf("raw publisher-return copy classified as remote: %+v", event)
	}
	message.From = []byte(corepeer.ID("source-peer"))
	message.Data = data
	event, ok = server.deliveryEvent(message, "topic-a", sentAt.Add(-time.Millisecond))
	if !ok || event.LatencyMS >= 0 || event.Fields["clockSkewDetected"] != true {
		t.Fatalf("negative one-way latency was hidden: %+v", event)
	}
}

func TestDetailedRPCMetadataLinksControlIDsDataAndSubscriptions(t *testing.T) {
	nodes := bootstrapTestNodes(t, 1)
	remote, err := corepeer.Decode(nodes[0].PeerID)
	if err != nil {
		t.Fatal(err)
	}
	topic, other, subscribe := "topic", "other", true
	stamp := time.Now().UnixNano()
	telemetry := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 10)}
	meta := &pubsubpb.TraceEvent_RPCMeta{Messages: []*pubsubpb.TraceEvent_MessageMeta{{MessageID: []byte{0, 255, 42}, Topic: &topic}}, Subscription: []*pubsubpb.TraceEvent_SubMeta{{Topic: &other, Subscribe: &subscribe}}, Control: &pubsubpb.TraceEvent_ControlMeta{Ihave: []*pubsubpb.TraceEvent_ControlIHaveMeta{{Topic: &topic, MessageIDs: [][]byte{{0, 255, 42}, []byte("second")}}}, Iwant: []*pubsubpb.TraceEvent_ControlIWantMeta{{MessageIDs: [][]byte{{0, 255, 42}}}}}}
	(&gossipTracer{telemetry: telemetry, peerID: "local"}).Trace(&pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_SEND_RPC.Enum(), Timestamp: &stamp, SendRPC: &pubsubpb.TraceEvent_SendRPC{SendTo: []byte(remote), Meta: meta}})
	if len(telemetry.events) != 3 {
		t.Fatalf("expected data and two control events, got %d", len(telemetry.events))
	}
	records := map[string]model.TraceEvent{}
	rpcID := ""
	eventIDs := map[string]bool{}
	for len(telemetry.events) > 0 {
		event := <-telemetry.events
		records[event.Type] = event
		id, _ := event.Fields["rpcObservationId"].(string)
		if id == "" {
			t.Fatal("missing RPC observation ID")
		}
		if rpcID != "" && id != rpcID {
			t.Fatal("mixed RPC records are not linked")
		}
		rpcID = id
		eventIDs[event.EventID] = true
		if event.Fields["sourceTimestamp"] != time.Unix(0, stamp).UTC().Format(time.RFC3339Nano) {
			t.Fatal("source time lost")
		}
	}
	if len(eventIDs) != 3 {
		t.Fatal("RPC linkage reused transport event identities")
	}
	ihave := records["send_ihave"]
	if !reflect.DeepEqual(ihave.Fields["topicMessageIds"], map[string][]string{topic: {"00ff2a", hex.EncodeToString([]byte("second"))}}) || ihave.Fields["messageIdCount"] != 2 || ihave.Fields["messageIdsComplete"] != true {
		t.Fatalf("IHAVE details=%+v", ihave)
	}
	iwant := records["send_iwant"]
	if !reflect.DeepEqual(iwant.Fields["messageIds"], []string{"00ff2a"}) {
		t.Fatal("IWANT binary ID not preserved")
	}
	data := records["rpc_metadata"]
	messages := data.Fields["messages"].([]map[string]any)
	if messages[0]["pubsubMessageId"] != "00ff2a" || data.Fields["rpcSubscriptionCount"] != 1 {
		t.Fatal("data/subscription metadata lost")
	}
	if meta.Messages[0].MessageID[1] != 255 {
		t.Fatal("logging mutated upstream metadata")
	}
}
func TestDetailedRPCMetadataBoundsIDsWithoutChangingCounters(t *testing.T) {
	topic := "topic"
	ids := make([][]byte, rpcDetailIDLimit+3)
	for i := range ids {
		ids[i] = []byte{1, 2, 3}
	}
	telemetry := &telemetry{events: make(chan model.TraceEvent, 2)}
	(&gossipTracer{telemetry: telemetry}).traceControlRPC(model.TraceEvent{}, "send", nil, &pubsubpb.TraceEvent_RPCMeta{Control: &pubsubpb.TraceEvent_ControlMeta{Ihave: []*pubsubpb.TraceEvent_ControlIHaveMeta{{Topic: &topic, MessageIDs: ids}}}})
	event := <-telemetry.events
	if event.Fields["messageIdCount"] != len(ids) || event.Fields["messageIdsComplete"] != false || event.Fields["omittedMessageIds"] != 3 {
		t.Fatal("truncation changed total or hid omitted IDs")
	}
	if len(event.Fields["topicMessageIds"].(map[string][]string)[topic]) != rpcDetailIDLimit {
		t.Fatal("metadata ID limit not applied")
	}
	encoded, err := json.Marshal(event)
	if err != nil || len(encoded) > 1<<20 {
		t.Fatal("metadata exceeds expected bounded size")
	}
}

func TestRejectedMessagesKeepReasonAndTimestampProvenance(t *testing.T) {
	topic, reason := "topic", "validation failed"
	for _, stamped := range []bool{false, true} {
		tm := &telemetry{node: model.Node{ID: "node"}, events: make(chan model.TraceEvent, 8)}
		tm.acceptClockEstimate(controllerClockEstimate{offset: time.Second, uncertainty: time.Millisecond}, time.Now())
		event := &pubsubpb.TraceEvent{Type: pubsubpb.TraceEvent_REJECT_MESSAGE.Enum(), RejectMessage: &pubsubpb.TraceEvent_RejectMessage{Topic: &topic, Reason: &reason, MessageID: []byte{0, 255}}}
		if stamped {
			stamp := time.Now().UnixNano()
			event.Timestamp = &stamp
		}
		(&gossipTracer{telemetry: tm}).Trace(event)
		got := <-tm.events
		if got.Type != "pubsub_reject" || got.Fields["pubsubMessageId"] != "00ff" || got.Fields["reason"] != reason || got.Fields["clockBasis"] != controllerClockBasis {
			t.Fatalf("rejection evidence lost: %+v", got)
		}
		if got.MessageID != "" {
			t.Fatal("wire ID was mislabeled as an application publication ID")
		}
		if stamped {
			if got.Fields["timestampSource"] != "libp2p-trace" || !got.Timestamp.Equal(time.Unix(0, event.GetTimestamp()).Add(time.Second)) {
				t.Fatal("original trace timestamp was not adjusted once")
			}
		} else if got.Fields["timestampSource"] != "peer-clock" || got.Fields["sourceTimestamp"] != nil {
			t.Fatal("fallback time was presented as an upstream timestamp")
		}
	}
}

func TestRPCIDByteBudgetRetainsShorterIDsAndReportsOmissions(t *testing.T) {
	large := make([]byte, rpcDetailByteLimit/2+1)
	details := controlIDDetails(map[string][][]byte{"topic": {large, {0, 255}}})
	if details["omittedMessageIds"] != 1 || details["messageIdsComplete"] != false || !reflect.DeepEqual(details["topicMessageIds"], map[string][]string{"topic": {"00ff"}}) {
		t.Fatalf("byte budget concealed or corrupted detail: %+v", details)
	}
}
