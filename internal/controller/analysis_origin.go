package controller

import (
	"slices"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// This is a heuristic lookback, not a protocol timeout or a probability of
// correctness. Prefer per-message ID lists; legacy logs only contain counts.
const originEvidenceWindow = 5 * time.Second

type originMetadataEvent struct {
	at                          time.Time
	from, to, topic, kind, node string
	ids                         []string
	detailed, complete          bool
}
type originTopicEdge struct{ from, to, topic string }
type originTransition struct {
	at   time.Time
	mask uint8
}
type originMetadataIndex struct {
	mesh     map[originTopicEdge][]originTransition
	controls map[researchEdge][]originMetadataEvent
	resets   map[string]map[string][]time.Time
}

func (a *researchAccumulator) observeOriginMetadata(e model.TraceEvent) {
	if e.Timestamp.IsZero() {
		return
	}
	switch e.Type {
	case "graft", "prune", "remove_peer":
		if e.PeerID != "" && e.RemotePeerID != "" && (e.Topic != "" || e.Type == "remove_peer") {
			a.inference = append(a.inference, originMetadataEvent{at: e.Timestamp, from: e.PeerID, to: e.RemotePeerID, topic: e.Topic, kind: e.Type})
		}
	case "leave", "measurement_stop", "measurement_terminated":
		a.inference = append(a.inference, originMetadataEvent{at: e.Timestamp, from: e.PeerID, node: e.NodeID, topic: e.Topic, kind: e.Type})
	case "rpc_metadata":
		direction, _ := e.Fields["direction"].(string)
		if direction != "send" && direction != "recv" || e.PeerID == "" || e.RemotePeerID == "" {
			return
		}
		from, to := e.PeerID, e.RemotePeerID
		if direction == "recv" {
			from, to = to, from
		}
		byTopic := map[string][]string{}
		for _, record := range originRecords(e.Fields["messages"]) {
			topic, _ := record["topic"].(string)
			id, _ := record["pubsubMessageId"].(string)
			if topic != "" && id != "" {
				byTopic[topic] = append(byTopic[topic], id)
			}
		}
		for topic, ids := range byTopic {
			sort.Strings(ids)
			a.inference = append(a.inference, originMetadataEvent{at: e.Timestamp, from: from, to: to, topic: topic, kind: "data", ids: ids, detailed: true, complete: true})
		}
	case "send_ihave", "recv_ihave", "send_iwant", "recv_iwant":
		if e.PeerID == "" || e.RemotePeerID == "" || e.PeerID == e.RemotePeerID {
			return
		}
		count, ok := numericEventField(e.Fields, "messageIdCount")
		if !ok || count <= 0 {
			return
		}
		from, to := e.PeerID, e.RemotePeerID
		kind := "ihave"
		if e.Type == "recv_ihave" || e.Type == "send_iwant" {
			from, to = to, from
		}
		if e.Type == "send_iwant" || e.Type == "recv_iwant" {
			kind = "iwant"
		}
		topics, known := targetNodeIDs(e.Fields["topics"])
		if e.Topic != "" {
			topics = []string{e.Topic}
			known = true
		}
		if kind == "iwant" && (!known || len(topics) == 0) {
			topics = []string{""}
		}
		for _, topic := range topics {
			if kind == "ihave" && topic == "" {
				continue
			}
			ids, _ := targetNodeIDs(e.Fields["messageIds"])
			if kind == "ihave" {
				ids = originTopicIDs(e.Fields["topicMessageIds"], topic)
			}
			complete, detailed := e.Fields["messageIdsComplete"].(bool)
			ids = append([]string{}, ids...)
			sort.Strings(ids)
			a.inference = append(a.inference, originMetadataEvent{at: e.Timestamp, from: from, to: to, topic: topic, kind: kind, ids: ids, detailed: detailed, complete: complete})
		}
	}
}
func newOriginMetadataIndex(events []originMetadataEvent, peers map[string]string) *originMetadataIndex {
	out := &originMetadataIndex{mesh: map[originTopicEdge][]originTransition{}, controls: map[researchEdge][]originMetadataEvent{}, resets: map[string]map[string][]time.Time{}}
	// Stable total order makes equal-time conflicts explicit, independent of log order.
	sort.Slice(events, func(i, j int) bool {
		a, b := events[i], events[j]
		if !a.at.Equal(b.at) {
			return a.at.Before(b.at)
		}
		if a.from != b.from {
			return a.from < b.from
		}
		if a.to != b.to {
			return a.to < b.to
		}
		if a.topic != b.topic {
			return a.topic < b.topic
		}
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.detailed != b.detailed {
			return !a.detailed
		}
		if a.complete != b.complete {
			return !a.complete
		}
		if cmp := slices.Compare(a.ids, b.ids); cmp != 0 {
			return cmp < 0
		}
		return a.node < b.node
	})
	nodePeers := map[string][]string{}
	for peer, node := range peers {
		nodePeers[node] = append(nodePeers[node], peer)
	}
	for _, e := range events {
		switch e.kind {
		case "graft", "prune", "remove_peer":
			from, to := e.from, e.to
			if from > to {
				from, to = to, from
			}
			key := originTopicEdge{from, to, e.topic}
			mask := uint8(1)
			if e.kind == "prune" || e.kind == "remove_peer" {
				mask = 2
			}
			records := out.mesh[key]
			if len(records) > 0 && records[len(records)-1].at.Equal(e.at) {
				records[len(records)-1].mask |= mask
			} else {
				records = append(records, originTransition{e.at, mask})
			}
			out.mesh[key] = records
		case "ihave", "iwant", "data":
			out.controls[researchEdge{e.from, e.to}] = append(out.controls[researchEdge{e.from, e.to}], e)
		default:
			ids := nodePeers[e.node]
			if e.from != "" {
				ids = []string{e.from}
			}
			topic := e.topic
			if e.kind != "leave" {
				topic = ""
			}
			for _, id := range ids {
				if out.resets[id] == nil {
					out.resets[id] = map[string][]time.Time{}
				}
				out.resets[id][topic] = append(out.resets[id][topic], e.at)
			}
		}
	}
	return out
}
func (a *originMetadataIndex) lastReset(peer, topic string, at time.Time) time.Time {
	latest := time.Time{}
	for _, key := range []string{"", topic} {
		values := a.resets[peer][key]
		i := sort.Search(len(values), func(i int) bool { return values[i].After(at) })
		if i > 0 && values[i-1].After(latest) {
			latest = values[i-1]
		}
	}
	return latest
}
func (a *originMetadataIndex) estimate(from, to, topic, wireID string, published, received time.Time) (string, []string) {
	evidence := []string{}
	if from == "" || to == "" || from == to || received.IsZero() || !published.IsZero() && received.Before(published) {
		return "unknown", []string{"missing or invalid peer/time metadata"}
	}
	low, high := from, to
	if low > high {
		low, high = high, low
	}
	history := a.mesh[originTopicEdge{low, high, topic}]
	end := sort.Search(len(history), func(i int) bool { return history[i].at.After(received) })
	mesh, conflict := false, false
	reset := a.lastReset(from, topic, received)
	removed := a.mesh[originTopicEdge{low, high, ""}]
	ri := sort.Search(len(removed), func(i int) bool { return removed[i].at.After(received) })
	if ri > 0 && removed[ri-1].at.After(reset) {
		reset = removed[ri-1].at
		evidence = append(evidence, "peer connection removed; older mesh/control evidence discarded")
	}
	if other := a.lastReset(to, topic, received); other.After(reset) {
		reset = other
	}
	if end > 0 {
		last := history[end-1]
		if last.at.After(reset) {
			mesh = last.mask == 1
			conflict = last.mask == 3
			if mesh {
				evidence = append(evidence, "GRAFT active at first receipt")
			} else if conflict {
				evidence = append(evidence, "simultaneous GRAFT and PRUNE metadata")
			} else {
				evidence = append(evidence, "PRUNE observed before first receipt")
			}
		}
	}
	lower := received.Add(-originEvidenceWindow)
	if published.After(lower) {
		lower = published
	}
	if reset.After(lower) {
		lower = reset
	}
	controls := a.controls[researchEdge{from, to}]
	start := sort.Search(len(controls), func(i int) bool { return !controls[i].at.Before(lower) })
	advertised, requested := false, false
	matchedAdvertisement, matchedRequest, matchedPair, dataObserved, incomplete := false, false, false, false, false
	lastAdvertisement := time.Time{}
	for _, event := range controls[start:] {
		if !event.at.Before(received) {
			break
		}
		if event.topic != "" && event.topic != topic {
			continue
		}
		matched := false
		if event.detailed {
			index := sort.SearchStrings(event.ids, wireID)
			matched = wireID != "" && index < len(event.ids) && event.ids[index] == wireID
			if !matched {
				if !event.complete || wireID == "" {
					incomplete = true
				}
				continue
			}
		}
		if event.kind == "data" {
			dataObserved = true
			continue
		}
		if event.kind == "ihave" {
			advertised = true
			lastAdvertisement = event.at
			matchedAdvertisement = matched
		}
		if event.kind == "iwant" && !lastAdvertisement.IsZero() && event.at.After(lastAdvertisement) {
			requested = true
			matchedRequest = matchedRequest || matched
			matchedPair = matchedPair || matchedAdvertisement && matched
		}
	}
	if advertised {
		evidence = append(evidence, "recent IHAVE on the same peer pair and topic")
	}
	if requested {
		if matchedPair {
			evidence = append(evidence, "IHAVE and IWANT match the delivered pubsub message ID")
		} else if matchedRequest {
			evidence = append(evidence, "IWANT matches the delivered pubsub message ID; aggregate IHAVE evidence")
		} else {
			evidence = append(evidence, "IHAVE-to-IWANT time-window association only; individual ID evidence unavailable")
		}
	}
	if dataObserved {
		evidence = append(evidence, "matching message observed in data RPC metadata")
	}
	if incomplete {
		evidence = append(evidence, "truncated metadata or missing receipt wire ID")
	}

	if conflict || mesh && requested {
		return "unknown", append(evidence, "push and pull evidence conflict")
	}
	if requested {
		return "lazy", evidence
	}
	if mesh && !incomplete {
		return "eager", append(evidence, "no recent matching IHAVE-to-IWANT sequence observed")
	}
	if len(evidence) == 0 {
		evidence = append(evidence, "no usable mesh or pull-sequence evidence")
	}
	return "unknown", evidence
}

// Both live Go values and decoded JSON archives are accepted by the analyzer.
func originTopicIDs(value any, topic string) []string {
	switch v := value.(type) {
	case map[string][]string:
		return v[topic]
	case map[string]any:
		ids, _ := targetNodeIDs(v[topic])
		return ids
	}
	return nil
}
func originRecords(value any) []map[string]any {
	switch v := value.(type) {
	case []map[string]any:
		return v
	case []any:
		result := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if record, ok := item.(map[string]any); ok {
				result = append(result, record)
			}
		}
		return result
	}
	return nil
}
