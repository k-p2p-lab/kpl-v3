package peer

import (
	"encoding/hex"
	"maps"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type gossipTracer struct {
	telemetry  *telemetry
	peerID     string
	publishing *publicationBridge
}

func (t *gossipTracer) Trace(event *pubsubpb.TraceEvent) {
	if event == nil || event.Type == nil {
		return
	}
	trace := model.TraceEvent{PeerID: t.peerID, Timestamp: t.telemetry.now(), Fields: map[string]any{"timestampSource": "peer-clock"}}
	if event.GetTimestamp() > 0 {
		trace.Fields["timestampSource"] = "libp2p-trace"
		trace.Timestamp = t.telemetry.adjustTimestamp(time.Unix(0, event.GetTimestamp()).UTC())
		trace.Fields["sourceTimestamp"] = time.Unix(0, event.GetTimestamp()).UTC().Format(time.RFC3339Nano)
	}
	switch event.GetType() {
	case pubsubpb.TraceEvent_PUBLISH_MESSAGE:
		if event.PublishMessage != nil {
			t.publishing.record(event.PublishMessage.GetTopic(), event.PublishMessage.MessageID)
		}
		return
	case pubsubpb.TraceEvent_ADD_PEER:
		if event.AddPeer == nil {
			return
		}
		trace.Type = "add_peer"
		trace.RemotePeerID = peerID(event.AddPeer.PeerID)
		trace.Fields["protocol"] = event.AddPeer.GetProto()
	case pubsubpb.TraceEvent_REMOVE_PEER:
		if event.RemovePeer == nil {
			return
		}
		trace.Type = "remove_peer"
		trace.RemotePeerID = peerID(event.RemovePeer.PeerID)
	case pubsubpb.TraceEvent_JOIN:
		if event.Join == nil {
			return
		}
		trace.Type = "join"
		trace.Topic = event.Join.GetTopic()
	case pubsubpb.TraceEvent_LEAVE:
		if event.Leave == nil {
			return
		}
		trace.Type = "leave"
		trace.Topic = event.Leave.GetTopic()
	case pubsubpb.TraceEvent_REJECT_MESSAGE:
		if event.RejectMessage == nil {
			return
		}
		trace.Type = "pubsub_reject"
		trace.Topic = event.RejectMessage.GetTopic()
		trace.RemotePeerID = peerID(event.RejectMessage.GetReceivedFrom())
		trace.Fields["pubsubMessageId"] = hex.EncodeToString(event.RejectMessage.GetMessageID())
		trace.Fields["reason"] = event.RejectMessage.GetReason()
	case pubsubpb.TraceEvent_DUPLICATE_MESSAGE:
		// RawTracer has the original bytes needed to correlate the duplicate
		// with the application's publication. Counting both would double it.
		return
	case pubsubpb.TraceEvent_GRAFT:
		if event.Graft == nil {
			return
		}
		trace.Type = "graft"
		trace.Topic = event.Graft.GetTopic()
		trace.RemotePeerID = peerID(event.Graft.PeerID)
	case pubsubpb.TraceEvent_PRUNE:
		if event.Prune == nil {
			return
		}
		trace.Type = "prune"
		trace.Topic = event.Prune.GetTopic()
		trace.RemotePeerID = peerID(event.Prune.PeerID)
	case pubsubpb.TraceEvent_SEND_RPC:
		if event.SendRPC != nil {
			t.traceControlRPC(trace, "send", event.SendRPC.GetSendTo(), event.SendRPC.GetMeta())
		}
		return
	case pubsubpb.TraceEvent_RECV_RPC:
		if event.RecvRPC != nil {
			t.traceControlRPC(trace, "recv", event.RecvRPC.GetReceivedFrom(), event.RecvRPC.GetMeta())
		}
		return
	case pubsubpb.TraceEvent_DROP_RPC:
		if event.DropRPC != nil {
			t.traceControlRPC(trace, "drop", event.DropRPC.GetSendTo(), event.DropRPC.GetMeta())
		}
		return
	default:
		return
	}
	t.telemetry.emit(trace)
}

// traceControlRPC preserves v2's useful distinction between an RPC containing
// a control type and the number of logical message IDs carried by that type.
// Unlike the v2 parser, each type in a mixed RPC is emitted independently and
// IDONTWANT is included. One event represents one RPC/control-type pair.
func (t *gossipTracer) traceControlRPC(trace model.TraceEvent, direction string, remote []byte, meta *pubsubpb.TraceEvent_RPCMeta) {
	if meta == nil {
		return
	}
	trace.RemotePeerID = peerID(remote)
	t.prepareRPCTrace(&trace, direction, meta)
	control := meta.GetControl()
	if control == nil {
		return
	}

	ihaveEntries, ihaveIDs := 0, 0
	ihaveDetail := map[string][][]byte{}
	ihaveTopicEntries, ihaveTopicIDs := make(map[string]int), make(map[string]int)
	for _, entry := range control.GetIhave() {
		if entry == nil {
			continue
		}
		ihaveEntries++
		ihaveIDs += len(entry.GetMessageIDs())
		ihaveDetail[entry.GetTopic()] = append(ihaveDetail[entry.GetTopic()], entry.GetMessageIDs()...)
		addControlTopicCounts(ihaveTopicEntries, ihaveTopicIDs, entry.GetTopic(), len(entry.GetMessageIDs()))
	}
	t.emitControlRPC(trace, direction, "ihave", ihaveEntries, ihaveIDs, ihaveTopicEntries, ihaveTopicIDs, 0, controlIDDetails(ihaveDetail))

	iwantEntries, iwantIDs := 0, 0
	iwantDetail := [][]byte{}
	for _, entry := range control.GetIwant() {
		if entry == nil {
			continue
		}
		iwantEntries++
		iwantIDs += len(entry.GetMessageIDs())
		iwantDetail = append(iwantDetail, entry.GetMessageIDs()...)
	}
	t.emitControlRPC(trace, direction, "iwant", iwantEntries, iwantIDs, nil, nil, 0, controlIDDetails(map[string][][]byte{"": iwantDetail}))

	idontwantEntries, idontwantIDs := 0, 0
	idontwantDetail := [][]byte{}
	for _, entry := range control.GetIdontwant() {
		if entry == nil {
			continue
		}
		idontwantEntries++
		idontwantIDs += len(entry.GetMessageIDs())
		idontwantDetail = append(idontwantDetail, entry.GetMessageIDs()...)
	}
	t.emitControlRPC(trace, direction, "idontwant", idontwantEntries, idontwantIDs, nil, nil, 0, controlIDDetails(map[string][][]byte{"": idontwantDetail}))

	graftEntries := 0
	graftTopicEntries := make(map[string]int)
	for _, entry := range control.GetGraft() {
		if entry == nil {
			continue
		}
		graftEntries++
		addControlTopicCounts(graftTopicEntries, nil, entry.GetTopic(), 0)
	}
	t.emitControlRPC(trace, direction, "graft", graftEntries, 0, graftTopicEntries, nil, 0, nil)

	pruneEntries, peerExchangeEntries := 0, 0
	prunePeers := map[string][][]byte{}
	pruneTopicEntries := make(map[string]int)
	for _, entry := range control.GetPrune() {
		if entry == nil {
			continue
		}
		pruneEntries++
		peerExchangeEntries += len(entry.GetPeers())
		prunePeers[entry.GetTopic()] = append(prunePeers[entry.GetTopic()], entry.GetPeers()...)
		addControlTopicCounts(pruneTopicEntries, nil, entry.GetTopic(), 0)
	}
	t.emitControlRPC(trace, direction, "prune", pruneEntries, 0, pruneTopicEntries, nil, peerExchangeEntries, peerExchangeDetails(prunePeers))
}

func (t *gossipTracer) emitControlRPC(trace model.TraceEvent, direction, controlType string, entries, messageIDs int, topicEntries, topicMessageIDs map[string]int, peerExchangeEntries int, details map[string]any) {
	if entries == 0 {
		return
	}
	trace.Type = direction + "_" + controlType
	trace.Topic = ""
	base := maps.Clone(trace.Fields)
	trace.Fields = map[string]any{
		"direction":      direction,
		"controlType":    controlType,
		"rpcCount":       1,
		"controlEntries": entries,
		"messageIdCount": messageIDs,
	}
	for key, value := range base {
		trace.Fields[key] = value
	}
	for key, value := range details {
		trace.Fields[key] = value
	}
	if peerExchangeEntries > 0 {
		trace.Fields["peerExchangeCount"] = peerExchangeEntries
	}
	if len(topicEntries) > 0 {
		topics := make([]string, 0, len(topicEntries))
		for topic := range topicEntries {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		trace.Fields["topics"] = topics
		trace.Fields["topicEntryCounts"] = topicEntries
	}
	if len(topicMessageIDs) > 0 {
		trace.Fields["topicMessageIdCounts"] = topicMessageIDs
	}
	t.telemetry.emit(trace)
}

func addControlTopicCounts(entries, messageIDs map[string]int, topic string, idCount int) {
	if topic == "" {
		return
	}
	entries[topic]++
	if messageIDs != nil {
		messageIDs[topic] += idCount
	}
}

// messageTracer observes duplicate copies without changing validation, routing,
// or PubSub message IDs. Application deliveries are emitted by consume only
// after Subscription.Next succeeds, rather than before its queue accepts data.
type messageTracer struct {
	server *Server
}

var _ pubsub.RawTracer = (*messageTracer)(nil)

func (t *messageTracer) DuplicateMessage(message *pubsub.Message) {
	if message == nil || message.Message == nil {
		return
	}
	t.server.telemetry.emitObserved(func(reading controllerClockReading) (model.TraceEvent, bool) {
		return t.server.duplicateEvent(message, reading.timestamp)
	})
}

func (*messageTracer) AddPeer(peer.ID, protocol.ID)          {}
func (t *messageTracer) RemovePeer(id peer.ID)               { t.server.mesh.removePeer(id) }
func (t *messageTracer) Join(topic string)                   { t.server.mesh.join(topic) }
func (t *messageTracer) Leave(topic string)                  { t.server.mesh.leave(topic) }
func (t *messageTracer) Graft(id peer.ID, topic string)      { t.server.mesh.graft(id, topic) }
func (t *messageTracer) Prune(id peer.ID, topic string)      { t.server.mesh.prune(id, topic) }
func (*messageTracer) ValidateMessage(*pubsub.Message)       {}
func (*messageTracer) DeliverMessage(*pubsub.Message)        {}
func (*messageTracer) RejectMessage(*pubsub.Message, string) {}
func (*messageTracer) ThrottlePeer(peer.ID)                  {}
func (*messageTracer) RecvRPC(*pubsub.RPC)                   {}
func (*messageTracer) SendRPC(*pubsub.RPC, peer.ID)          {}
func (*messageTracer) DropRPC(*pubsub.RPC, peer.ID)          {}
func (*messageTracer) UndeliverableMessage(*pubsub.Message)  {}

func peerID(raw []byte) string {
	id, err := peer.IDFromBytes(raw)
	if err != nil {
		return ""
	}
	return id.String()
}
