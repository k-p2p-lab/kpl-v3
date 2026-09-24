package peer

import (
	"crypto/rand"
	"encoding/hex"
	"maps"
	"sort"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Detailed IDs are bounded independently of the existing complete counters.
// Oversized RPC metadata cannot block the byte-limited telemetry queue.
const rpcDetailIDLimit = 8192
const rpcDetailByteLimit = 512 << 10

type rpcDetailBudget struct{ count, bytes, omitted int }

func (b *rpcDetailBudget) id(raw []byte) (string, bool) {
	if b.count >= rpcDetailIDLimit || len(raw) > (rpcDetailByteLimit-b.bytes)/2 {
		b.omitted++
		return "", false
	}
	b.count++
	b.bytes += 2 * len(raw)
	return hex.EncodeToString(raw), true
}
func controlIDDetails(groups map[string][][]byte) map[string]any {
	budget := rpcDetailBudget{}
	ids := []string{}
	topics := map[string][]string{}
	// Sort topics so any truncation is deterministic.
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, topic := range keys {
		for _, raw := range groups[topic] {
			if id, ok := budget.id(raw); ok {
				if topic == "" {
					ids = append(ids, id)
				} else {
					topics[topic] = append(topics[topic], id)
				}
			}
		}
	}
	details := map[string]any{"messageIdsComplete": budget.omitted == 0, "omittedMessageIds": budget.omitted}
	if len(topics) > 0 {
		details["topicMessageIds"] = topics
	}
	if len(ids) > 0 || len(topics) == 0 {
		details["messageIds"] = ids
	}
	return details
}

func (t *gossipTracer) prepareRPCTrace(trace *model.TraceEvent, direction string, meta *pubsubpb.TraceEvent_RPCMeta) {
	trace.Fields = maps.Clone(trace.Fields)
	if trace.Fields == nil {
		trace.Fields = map[string]any{}
	}
	trace.Fields["rpcMetadataVersion"] = 1
	// Local observation identity shared by all records emitted for this callback;
	// it is not a network RPC ID and is not expected to match the remote endpoint.
	trace.Fields["rpcObservationId"] = rand.Text()
	trace.Fields["direction"] = direction
	trace.Fields["rpcMessageCount"] = len(meta.GetMessages())
	trace.Fields["rpcSubscriptionCount"] = len(meta.GetSubscription())
	if len(meta.GetMessages()) == 0 && len(meta.GetSubscription()) == 0 {
		return
	}
	record := *trace
	record.Type = "rpc_metadata"
	record.Fields = maps.Clone(trace.Fields)
	budget := rpcDetailBudget{}
	messages := []map[string]any{}
	for _, message := range meta.GetMessages() {
		if message == nil {
			continue
		}
		if id, ok := budget.id(message.GetMessageID()); ok {
			messages = append(messages, map[string]any{"topic": message.GetTopic(), "pubsubMessageId": id})
		}
	}
	subscriptions := []map[string]any{}
	for _, sub := range meta.GetSubscription() {
		if sub == nil {
			continue
		}
		if len(subscriptions) >= rpcDetailIDLimit {
			break
		}
		subscriptions = append(subscriptions, map[string]any{"topic": sub.GetTopic(), "subscribe": sub.GetSubscribe()})
	}
	record.Fields["messages"] = messages
	record.Fields["messageIdsComplete"] = budget.omitted == 0
	record.Fields["omittedMessageIds"] = budget.omitted
	record.Fields["subscriptions"] = subscriptions
	record.Fields["subscriptionsComplete"] = len(subscriptions) == len(meta.GetSubscription())
	record.Fields["omittedSubscriptions"] = len(meta.GetSubscription()) - len(subscriptions)
	t.telemetry.emit(record)
}

func peerExchangeDetails(groups map[string][][]byte) map[string]any {
	raw := controlIDDetails(groups)
	return map[string]any{"peerExchangeIdsByTopic": raw["topicMessageIds"], "peerExchangeIdsComplete": raw["messageIdsComplete"], "omittedPeerExchangeIds": raw["omittedMessageIds"]}
}
