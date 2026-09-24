package peer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func publicationTestServer(t *testing.T) (*Server, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	node := model.Node{ID: "node", RunID: "run"}
	config := model.NodeConfig{GossipSub: model.GossipSubConfig{Topics: []string{"topic-b", "topic-a"}}}.WithDefaults()
	server := &Server{config: model.PeerProcessConfig{Node: node, NodeConfig: config}, host: newConfigTestHost(t), topics: make(map[string]*pubsub.Topic), telemetry: &telemetry{node: node, events: make(chan model.TraceEvent, 100)}}
	if err := server.startPubSub(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server, ctx
}

func TestRawPublicationHasExactDataLengthAndUnavailableLatency(t *testing.T) {
	server, ctx := publicationTestServer(t)
	for _, size := range []int{1, 32, 1024, 4096} {
		message, err := server.preparePublication(model.PublishRequest{PayloadSize: size, PayloadEncoding: "raw"}, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		if len(message.wire) != size || message.encoding != "raw" {
			t.Fatalf("requested=%d data=%d encoding=%s", size, len(message.wire), message.encoding)
		}
		message, err = server.publishMessage(ctx, "topic-a", message)
		if err != nil {
			t.Fatal(err)
		}
		wireID, err := hex.DecodeString(message.pubsubID)
		if err != nil {
			t.Fatal(err)
		}
		remote := corepeer.ID("previous-hop")
		event, ok := server.deliveryEvent(publicationTestMessage(message.wire, string(wireID), remote), "topic-a", time.Now().UTC())
		if !ok || event.MessageID != message.id || event.LatencyMS != -1 || event.Fields["latencyAvailable"] != false || event.RemotePeerID != remote.String() || event.Fields["localDelivery"] != false || event.Fields["pubsubMessageId"] != message.pubsubID {
			t.Fatalf("raw delivery metadata=%+v", event)
		}
		if _, exists := event.Fields["publisher"]; exists {
			t.Fatal("raw delivery fabricated publisher metadata")
		}
		if event.Fields["payloadBytes"] != size || event.Fields["wireBytes"] != size {
			t.Fatalf("incorrect byte counts: %+v", event.Fields)
		}
	}
}

func TestDefaultEnvelopeRetainsCorrelationAndLatency(t *testing.T) {
	server, _ := publicationTestServer(t)
	now := time.Now().UTC()
	message, err := server.preparePublication(model.PublishRequest{PayloadSize: 32}, now)
	if err != nil {
		t.Fatal(err)
	}
	if message.encoding != "envelope" || len(message.wire) <= 32 {
		t.Fatalf("default encoding changed: %+v", message)
	}
	event, ok := server.deliveryEvent(publicationTestMessage(message.wire, "envelope-wire-id", corepeer.ID("previous-hop")), "topic-a", now.Add(40*time.Millisecond))
	if !ok || event.MessageID != message.id || event.LatencyMS != 40 || event.Fields["publisher"] != "node" || event.Fields["payloadBytes"] != 32 {
		t.Fatalf("envelope metadata=%+v", event)
	}
	var envelope envelope
	if err := json.Unmarshal(message.wire, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.RunID = "other-run"
	foreign, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := server.deliveryEvent(publicationTestMessage(foreign, "foreign-wire-id", corepeer.ID("previous-hop")), "topic-a", now); ok {
		t.Fatal("foreign envelope passed run isolation")
	}
}

func TestEnvelopeCarriesControllerClockUncertainty(t *testing.T) {
	server, _ := publicationTestServer(t)
	server.telemetry.acceptClockEstimate(controllerClockEstimate{uncertainty: 2 * time.Millisecond}, time.Now())
	reading := server.telemetry.clockReading()
	message, err := server.preparePublicationWithClock(model.PublishRequest{PayloadSize: 32}, reading)
	if err != nil {
		t.Fatal(err)
	}
	receiverReading := reading
	receiverReading.timestamp = reading.timestamp.Add(-time.Millisecond)
	event, ok := server.deliveryEventWithClock(publicationTestMessage(message.wire, "clock-wire-id", corepeer.ID("previous-hop")), "topic-a", receiverReading)
	if !ok || event.LatencyMS != -1 || event.Fields["latencyClockSynchronized"] != true || event.Fields["latencyUncertaintyMs"] != float64(4) {
		t.Fatalf("synchronized envelope metadata=%+v", event)
	}
	var payload envelope
	if err := json.Unmarshal(message.wire, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ClockBasis != controllerClockBasis || payload.ClockUncertaintyNS != int64(2*time.Millisecond) {
		t.Fatalf("publisher clock provenance missing: %+v", payload)
	}
	server.telemetry.acceptClockEstimate(controllerClockEstimate{uncertainty: 2 * time.Millisecond}, time.Now().Add(-controllerClockMaxAge-time.Second))
	staleReceiver := server.telemetry.clockReading()
	staleReceiver.timestamp = reading.timestamp.Add(time.Millisecond)
	event, ok = server.deliveryEventWithClock(publicationTestMessage(message.wire, "stale-clock-wire-id", corepeer.ID("previous-hop")), "topic-a", staleReceiver)
	if !ok {
		t.Fatal("stale receiver discarded a valid envelope")
	}
	if _, exists := event.Fields["latencyClockSynchronized"]; exists {
		t.Fatalf("stale receiver asserted synchronized latency: %+v", event.Fields)
	}
}

func TestAllTopicsRawPublishUsesExactBytesAndDistinctMessages(t *testing.T) {
	server, parent := publicationTestServer(t)
	subs := make(map[string]*pubsub.Subscription)
	for topicName, topic := range server.topics {
		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sub.Cancel)
		subs[topicName] = sub
	}
	request := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{"topic":"*","payloadSize":1024,"payloadEncoding":"raw","targetNodes":99,"targetNodesByTopic":{"topic-a":4,"topic-b":7}}`))
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("publish status=%d body=%s", response.Code, response.Body.String())
	}
	var result struct {
		Messages []struct {
			Topic     string `json:"topic"`
			MessageID string `json:"messageId"`
			WireBytes int    `json:"wireBytes"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Messages) != 2 || result.Messages[0].Topic != "topic-a" || result.Messages[1].Topic != "topic-b" {
		t.Fatalf("all-topic response=%s", response.Body.String())
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	var previous []byte
	for _, expected := range result.Messages {
		message, err := subs[expected.Topic].Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(message.Data) != 1024 || expected.WireBytes != 1024 || "pubsub-"+hex.EncodeToString([]byte(message.ID)) != expected.MessageID {
			t.Fatalf("incorrect raw PubSub data for %s: bytes=%d result=%+v", expected.Topic, len(message.Data), expected)
		}
		if previous != nil && bytes.Equal(previous, message.Data) {
			t.Fatal("all-topic publication reused the same random payload")
		}
		previous = message.Data
	}
	counts := make(map[string]int)
	for len(server.telemetry.events) > 0 {
		event := <-server.telemetry.events
		if event.Type == "publish" {
			counts[event.Topic], _ = event.Fields["targetNodes"].(int)
		}
	}
	if !reflect.DeepEqual(counts, map[string]int{"topic-a": 4, "topic-b": 7}) {
		t.Fatalf("wildcard publication lost per-topic reachability targets: %v", counts)
	}
}

func publicationTestMessage(data []byte, id string, receivedFrom corepeer.ID) *pubsub.Message {
	topic := "topic-a"
	return &pubsub.Message{Message: &pubsubpb.Message{Data: data, Topic: &topic}, ID: id, ReceivedFrom: receivedFrom}
}

func TestConcurrentRawAndEnvelopePublicationKeepsNativeMessageIDs(t *testing.T) {
	server, parent := publicationTestServer(t)
	sub, err := server.topics["topic-a"].Subscribe(pubsub.WithBufferSize(128))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Cancel)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	const count = 32
	results := make(chan publication, count)
	errors := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			message := publication{wire: []byte{0x42}, encoding: "raw", sentAt: time.Now().UTC()}
			if i%2 != 0 {
				var err error
				message, err = server.preparePublication(model.PublishRequest{PayloadSize: 1}, message.sentAt)
				if err != nil {
					errors <- err
					return
				}
			}
			message, err := server.publishMessage(ctx, "topic-a", message)
			if err != nil {
				errors <- err
				return
			}
			results <- message
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	byWireID := make(map[string]publication, count)
	for i := 0; i < count; i++ {
		message := <-results
		if _, exists := byWireID[message.pubsubID]; exists {
			t.Fatal("distinct publications reused a native PubSub ID")
		}
		byWireID[message.pubsubID] = message
	}
	for i := 0; i < count; i++ {
		message, err := sub.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if message.ID != pubsub.DefaultMsgIdFn(message.Message) {
			t.Fatal("publication replaced PubSub's default message ID function")
		}
		wireID := hex.EncodeToString([]byte(message.ID))
		expected, exists := byWireID[wireID]
		if !exists || !bytes.Equal(expected.wire, message.Data) {
			t.Fatalf("concurrent publication got another message's ID: %q", wireID)
		}
		event, ok := server.deliveryEvent(message, "topic-a", time.Now().UTC())
		if !ok || event.MessageID != expected.id || event.Fields["localDelivery"] != true || event.Fields["pubsubMessageId"] != expected.pubsubID {
			t.Fatalf("local delivery correlation=%+v expected=%+v", event, expected)
		}
		if expected.encoding == "raw" && (len(message.Data) != 1 || expected.id != "pubsub-"+wireID) {
			t.Fatalf("raw wire contract changed: %+v", expected)
		}
		delete(byWireID, wireID)
	}
}

func TestPublishBridgeClearsFailedPublication(t *testing.T) {
	server, ctx := publicationTestServer(t)
	closed, err := server.pubsub.Join("closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	server.topics["closed"] = closed
	message := publication{wire: []byte{1}, encoding: "raw", sentAt: time.Now().UTC()}
	if _, err := server.publishMessage(ctx, "closed", message); err == nil {
		t.Fatal("closed topic accepted a publication")
	}
	if server.publishing.topic != "" || server.publishing.id != "" {
		t.Fatal("failed publication left a pending correlation")
	}
	if message, err := server.publishMessage(ctx, "topic-a", message); err != nil || !strings.HasPrefix(message.id, "pubsub-") {
		t.Fatalf("publication after failure=%+v error=%v", message, err)
	}
}

func TestHTTPPublishTimestampExcludesWaitingForPublicationGate(t *testing.T) {
	server, parent := publicationTestServer(t)
	sub, err := server.topics["topic-a"].Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.Cancel)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{"topic":"topic-a","payloadSize":32}`)).WithContext(ctx)
	response := httptest.NewRecorder()
	started, done := make(chan struct{}), make(chan struct{})
	if err := server.publishGate.acquire(parent); err != nil {
		t.Fatal(err)
	}
	go func() {
		close(started)
		server.handler().ServeHTTP(response, request)
		close(done)
	}()
	<-started
	// Hold the gate while the request reaches it. The assertion is against the
	// actual release timestamp, not a tolerance on total test execution time.
	select {
	case <-done:
		server.publishGate.release()
		t.Fatal("HTTP publication bypassed the publication gate")
	case <-time.After(25 * time.Millisecond):
	}
	releasedAt := time.Now().UTC()
	server.publishGate.release()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("HTTP publication did not finish after the gate was released")
	}
	if response.Code != http.StatusAccepted {
		t.Fatalf("publish status=%d body=%s", response.Code, response.Body.String())
	}
	message, err := sub.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var payload envelope
	if err := json.Unmarshal(message.Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SentAt < releasedAt.UnixNano() {
		t.Fatalf("wire timestamp included gate waiting: sent=%s released=%s", time.Unix(0, payload.SentAt), releasedAt)
	}
	for len(server.telemetry.events) > 0 {
		event := <-server.telemetry.events
		if event.Type == "publish" {
			if event.Timestamp.UnixNano() != payload.SentAt {
				t.Fatalf("publish event and wire timestamps differ: %+v %+v", event, payload)
			}
			return
		}
	}
	t.Fatal("missing publish event")
}

func TestPublishCohortPreservesUnknownAndExplicitEmpty(t *testing.T) {
	server, _ := publicationTestServer(t)
	for _, test := range []struct {
		name, body string
		known      bool
		cohort     []string
	}{
		{"legacy", `{"topic":"topic-a"}`, false, nil},
		{"null", `{"topic":"topic-a","targetNodeIds":null}`, false, nil},
		{"empty", `{"topic":"topic-a","targetNodeIds":[]}`, true, []string{}},
		{"explicit", `{"topic":"topic-a","targetNodeIds":["receiver"],"cohortCapturedAt":"2026-09-05T01:02:03.000000004Z"}`, true, []string{"receiver"}},
		{"topic-empty-override", `{"topic":"topic-a","targetNodeIds":["receiver"],"targetNodeIdsByTopic":{"topic-a":[]}}`, true, []string{}},
		{"topic-null-override", `{"topic":"topic-a","targetNodeIds":["receiver"],"targetNodeIdsByTopic":{"topic-a":null}}`, true, []string{}},
		{"topic-populated-override", `{"topic":"topic-a","targetNodeIds":["default"],"targetNodeIdsByTopic":{"topic-a":["topic-receiver"]}}`, true, []string{"topic-receiver"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(test.body)))
			if response.Code != http.StatusAccepted {
				t.Fatalf("publish status=%d body=%s", response.Code, response.Body.String())
			}
			var event model.TraceEvent
			for {
				select {
				case next := <-server.telemetry.events:
					if next.Type == "publish" {
						event = next
					}
				default:
					if event.Type == "" {
						t.Fatal("missing publish event")
					}
					goto collected
				}
			}
		collected:
			cohort, exists := event.Fields["targetNodeIds"]
			if exists != test.known || exists && !reflect.DeepEqual(cohort, test.cohort) {
				t.Fatalf("cohort=%#v exists=%v; want %#v known=%v", cohort, exists, test.cohort, test.known)
			}
			if event.EventID == "" || event.Fields["publisher"] != "node" || event.Fields["pubsubMessageId"] == "" {
				t.Fatalf("missing publication identity: %+v", event)
			}
			if test.name == "explicit" && event.Fields["cohortCapturedAt"] != "2026-09-05T01:02:03.000000004Z" {
				t.Fatalf("cohort capture timestamp changed: %+v", event.Fields)
			}
		})
	}
}

func TestPublishTopicsAndEncodingValidation(t *testing.T) {
	server, _ := publicationTestServer(t)
	for _, test := range []struct {
		requested string
		want      []string
	}{
		{"", []string{"topic-b"}},
		{"topic-a", []string{"topic-a"}},
		{"*", []string{"topic-a", "topic-b"}},
	} {
		topics, err := server.publishTopics(test.requested)
		if err != nil || !reflect.DeepEqual(topics, test.want) {
			t.Fatalf("publishTopics(%q)=%v, %v", test.requested, topics, err)
		}
	}
	for _, data := range []string{`{"topic":"missing","payloadSize":32}`, `{"topic":"*","payloadEncoding":"invalid","payloadSize":32}`} {
		response := httptest.NewRecorder()
		server.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(data)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid publish accepted: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestPublishMeasurementWindowValidationAndFields(t *testing.T) {
	server, _ := publicationTestServer(t)
	for _, window := range []string{"bad", "0s", "-1s", "1h1ns", "999999999999999999999h"} {
		body, _ := json.Marshal(model.PublishRequest{Topic: "*", DeliveryWindow: window})
		before := server.publishSeq.Load()
		response := httptest.NewRecorder()
		server.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body)))
		if response.Code != http.StatusBadRequest || server.publishSeq.Load() != before {
			t.Fatalf("invalid window %q published: status=%d", window, response.Code)
		}
	}
	for _, test := range []struct{ window, encoding, want string }{{"", "envelope", "10s"}, {"1500ms", "raw", "1.5s"}, {"1h", "envelope", "1h0m0s"}} {
		for len(server.telemetry.events) > 0 {
			<-server.telemetry.events
		}
		body, _ := json.Marshal(model.PublishRequest{Topic: "topic-a", PayloadSize: 32, PayloadEncoding: test.encoding, DeliveryWindow: test.window})
		response := httptest.NewRecorder()
		server.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(body)))
		if response.Code != http.StatusAccepted {
			t.Fatalf("window %q rejected: %d %s", test.window, response.Code, response.Body.String())
		}
		found := false
		for len(server.telemetry.events) > 0 {
			event := <-server.telemetry.events
			if event.Type != "publish" {
				continue
			}
			found = true
			if event.Fields["measurementDefinition"] != "session-window-v1" || event.Fields["deliveryWindow"] != test.want || event.Fields["payloadEncoding"] != test.encoding || event.SessionID == "" || event.Sequence == 0 {
				t.Fatalf("measurement metadata missing: %+v", event)
			}
		}
		if !found {
			t.Fatal("missing successful publication event")
		}
	}
}

func TestCanceledHTTPPublicationDoesNotWaitForGate(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "canceled"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			server, parent := publicationTestServer(t)
			ctx, cancel := context.WithCancel(parent)
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(parent, 25*time.Millisecond)
			}
			defer cancel()
			if err := server.publishGate.acquire(parent); err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{"topic":"topic-a","payloadSize":32}`)).WithContext(ctx)
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				server.handler().ServeHTTP(response, request)
				close(done)
			}()
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("canceled HTTP publication remained queued behind the gate")
			}
			server.publishGate.release()
			<-done
			if response.Code != http.StatusGatewayTimeout {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			for len(server.telemetry.events) > 0 {
				if event := <-server.telemetry.events; event.Type == "publish" {
					t.Fatal("canceled queued request emitted a publication")
				}
			}
		})
	}
}
