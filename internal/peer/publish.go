package peer

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// publicationGate serializes the wire-ID bridge while allowing canceled
// HTTP requests to leave the queue before the current publisher finishes.
// The zero value is ready to use, including in embedded/test Servers.
type publicationGate struct {
	once sync.Once
	slot chan struct{}
}

func (g *publicationGate) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.once.Do(func() { g.slot = make(chan struct{}, 1) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.slot <- struct{}{}:
		// Cancellation can become ready at the same time as the slot.
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	}
}

func (g *publicationGate) release() { <-g.slot }

type publication struct {
	wire     []byte
	id       string
	pubsubID string
	encoding string
	sentAt   time.Time
	clock    controllerClockReading
}

func (s *Server) publishTopics(requested string) ([]string, error) {
	if requested == "" {
		requested = s.config.NodeConfig.GossipSub.Topics[0]
	}
	if requested != "*" {
		if _, ok := s.topics[requested]; !ok {
			return nil, fmt.Errorf("unknown topic %q", requested)
		}
		return []string{requested}, nil
	}
	// The topic map contains only topics that this peer actually joined.
	topics := make([]string, 0, len(s.topics))
	for topic := range s.topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	if len(topics) == 0 {
		return nil, fmt.Errorf("peer has no joined topics")
	}
	return topics, nil
}

func (s *Server) preparePublication(request model.PublishRequest, now time.Time) (publication, error) {
	return s.preparePublicationWithClock(request, controllerClockReading{timestamp: now})
}

func (s *Server) preparePublicationWithClock(request model.PublishRequest, reading controllerClockReading) (publication, error) {
	encoding := request.PayloadEncoding
	if encoding == "" {
		encoding = "envelope"
	}
	if encoding != "envelope" && encoding != "raw" {
		return publication{}, fmt.Errorf("payloadEncoding must be envelope or raw")
	}
	payload := make([]byte, request.PayloadSize)
	if _, err := crand.Read(payload); err != nil {
		return publication{}, fmt.Errorf("generate payload: %w", err)
	}
	if encoding == "raw" {
		// No application header or base64 expansion: exactly payloadSize bytes
		// are passed to Topic.Publish. Libp2p still adds its own framing/signature.
		return publication{wire: payload, encoding: encoding, sentAt: reading.timestamp, clock: reading}, nil
	}
	sequence := s.publishSeq.Add(1)
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", s.config.Node.ID, reading.timestamp.UnixNano(), sequence)))
	message := envelope{ID: hex.EncodeToString(digest[:16]), RunID: s.config.Node.RunID, Publisher: s.config.Node.ID, SentAt: reading.timestamp.UnixNano(), Payload: payload}
	if reading.synchronized {
		message.ClockBasis = controllerClockBasis
		message.ClockUncertaintyNS = int64(reading.uncertainty)
	}
	wire, err := json.Marshal(message)
	if err != nil {
		return publication{}, fmt.Errorf("encode message: %w", err)
	}
	return publication{wire: wire, id: message.ID, encoding: encoding, sentAt: reading.timestamp, clock: reading}, nil
}

// publicationBridge observes the ID assigned by PubSub without replacing its
// message ID function or changing the bytes sent on the network. PublishMessage
// is called synchronously by Topic.Publish, before validation finishes.
type publicationBridge struct {
	mu    sync.Mutex
	topic string
	id    string
}

func (b *publicationBridge) begin(topic string) {
	b.mu.Lock()
	b.topic, b.id = topic, ""
	b.mu.Unlock()
}

func (b *publicationBridge) record(topic string, id []byte) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topic == topic && b.id == "" {
		b.id = hex.EncodeToString(id)
	}
}

func (b *publicationBridge) finish() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.id
	b.topic, b.id = "", ""
	return id
}

func (s *Server) publishMessage(ctx context.Context, topic string, message publication) (publication, error) {
	// All local publications share this gate: an envelope published concurrently
	// with raw data must not supply the pending raw publication's wire ID.
	if err := s.publishGate.acquire(ctx); err != nil {
		return publication{}, err
	}
	defer s.publishGate.release()
	return s.publishPreparedMessage(ctx, topic, message)
}

// publishPreparedMessage requires publishGate. The HTTP path prepares its payload
// under the same gate, so envelope timestamps exclude time waiting for another
// publication. Tests can also pass prepared wire bytes through publishMessage.
func (s *Server) publishPreparedMessage(ctx context.Context, topic string, message publication) (publication, error) {
	if err := ctx.Err(); err != nil {
		return publication{}, err
	}
	s.publishing.begin(topic)
	options, err := gossipPublishOptions(s.config.NodeConfig.GossipSub)
	if err != nil {
		s.publishing.finish()
		return publication{}, err
	}
	err = s.topics[topic].Publish(ctx, message.wire, options...)
	id := s.publishing.finish()
	if err != nil {
		return publication{}, err
	}
	if id == "" {
		return publication{}, fmt.Errorf("PubSub publication did not provide a message ID")
	}
	message.pubsubID = id
	if message.encoding == "raw" {
		message.id = "pubsub-" + id
	}
	return message, nil
}

func (s *Server) messageIdentity(message *pubsub.Message) (string, bool) {
	if message == nil || message.Message == nil {
		return "", false
	}
	id := message.ID
	if id == "" {
		// PubSub normally caches this before delivery. Use the same policy as
		// the router for uncached messages and keep the wire message untouched.
		id = gossipMessageID(s.config.NodeConfig.GossipSub, message.GetTopic())(message.Message)
	}
	local := message.ReceivedFrom == s.host.ID() || message.GetFrom() == s.host.ID()
	return hex.EncodeToString([]byte(id)), local
}

func (s *Server) deliveryEvent(message *pubsub.Message, topic string, now time.Time) (model.TraceEvent, bool) {
	return s.deliveryEventWithClock(message, topic, controllerClockReading{timestamp: now})
}

func (s *Server) deliveryEventWithClock(message *pubsub.Message, topic string, reading controllerClockReading) (model.TraceEvent, bool) {
	if message == nil || message.Message == nil {
		return model.TraceEvent{}, false
	}
	data := message.Data
	wireID, local := s.messageIdentity(message)
	event := model.TraceEvent{PeerID: s.host.ID().String(), Type: "deliver", Topic: topic, RemotePeerID: message.ReceivedFrom.String(), Timestamp: reading.timestamp}
	var payload envelope
	if json.Unmarshal(data, &payload) == nil && payload.ID != "" && payload.RunID != "" && payload.Publisher != "" && payload.SentAt > 0 {
		if payload.RunID != s.config.Node.RunID {
			return model.TraceEvent{}, false
		}
		event.MessageID = payload.ID
		event.LatencyMS = float64(reading.timestamp.UnixNano()-payload.SentAt) / float64(time.Millisecond)
		event.Fields = map[string]any{"publisher": payload.Publisher, "payloadBytes": len(payload.Payload), "wireBytes": len(data), "payloadEncoding": "envelope", "latencyAvailable": true, "localDelivery": local || payload.Publisher == s.config.Node.ID, "pubsubMessageId": wireID}
		if reading.synchronized && payload.ClockBasis == controllerClockBasis &&
			payload.ClockUncertaintyNS >= 0 && payload.ClockUncertaintyNS <= int64(5*time.Second) {
			event.Fields["latencyClockSynchronized"] = true
			event.Fields["latencyUncertaintyMs"] = float64(payload.ClockUncertaintyNS+int64(reading.uncertainty)) / float64(time.Millisecond)
		}
		if event.LatencyMS < 0 {
			event.Fields["clockSkewDetected"] = true
		}
		return event, true
	}
	// Raw bytes intentionally contain no publisher, run ID, or sent timestamp.
	// The configured PubSub ID correlates raw payloads; content hash policies
	// intentionally deduplicate identical bytes. Raw data carries no timestamp.
	if wireID == "" {
		return model.TraceEvent{}, false
	}
	event.MessageID = "pubsub-" + wireID
	event.LatencyMS = -1
	event.Fields = map[string]any{"payloadBytes": len(data), "wireBytes": len(data), "payloadEncoding": "raw", "latencyAvailable": false, "localDelivery": local, "pubsubMessageId": wireID}
	return event, true
}

func (s *Server) duplicateEvent(message *pubsub.Message, now time.Time) (model.TraceEvent, bool) {
	if message == nil || message.Message == nil {
		return model.TraceEvent{}, false
	}
	wireID, local := s.messageIdentity(message)
	event := model.TraceEvent{PeerID: s.host.ID().String(), Type: "duplicate", Topic: message.GetTopic(), RemotePeerID: message.ReceivedFrom.String(), Timestamp: now}
	event.Fields = map[string]any{"wireBytes": len(message.Data), "localDelivery": local, "pubsubMessageId": wireID}
	// Raw tracers run synchronously in PubSub. Read only correlation metadata;
	// decoding the base64 Payload here would allocate the full payload for every
	// duplicate, adding avoidable work to the propagation being measured.
	var header struct {
		ID        string `json:"id"`
		RunID     string `json:"runId"`
		Publisher string `json:"publisher"`
		SentAt    int64  `json:"sentAt"`
	}
	if json.Unmarshal(message.Data, &header) == nil && header.ID != "" && header.RunID != "" && header.Publisher != "" && header.SentAt > 0 {
		if header.RunID != s.config.Node.RunID {
			return model.TraceEvent{}, false
		}
		event.MessageID = header.ID
		event.Fields["publisher"] = header.Publisher
		event.Fields["payloadEncoding"] = "envelope"
		event.Fields["localDelivery"] = local || header.Publisher == s.config.Node.ID
		return event, true
	}
	if wireID == "" {
		return model.TraceEvent{}, false
	}
	event.MessageID = "pubsub-" + wireID
	event.Fields["payloadEncoding"] = "raw"
	return event, true
}
