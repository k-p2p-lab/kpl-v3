package controller

import (
	"context"
	"math"
	"sort"
	"time"
)

type researchMembershipLife struct {
	node, topic string
	start, end  time.Time
}

type researchMembershipSpan struct{ start, end time.Time }
type researchMembershipNode struct {
	node  string
	spans []researchMembershipSpan
}

// Lifetimes are immutable during message reconstruction. Grouping by topic and
// merging each node's half-open intervals avoids rescanning its full churn
// history for every publication without changing the cohort definition.
func researchMembershipIndex(lives []researchMembershipLife) map[string][]researchMembershipNode {
	byTopic := make(map[string]map[string][]researchMembershipSpan)
	for _, life := range lives {
		if byTopic[life.topic] == nil {
			byTopic[life.topic] = make(map[string][]researchMembershipSpan)
		}
		byTopic[life.topic][life.node] = append(byTopic[life.topic][life.node], researchMembershipSpan{life.start, life.end})
	}
	out := make(map[string][]researchMembershipNode, len(byTopic))
	for topic, nodes := range byTopic {
		for node, spans := range nodes {
			sort.Slice(spans, func(i, j int) bool { return spans[i].start.Before(spans[j].start) })
			merged := make([]researchMembershipSpan, 0, len(spans))
			for _, span := range spans {
				if len(merged) == 0 || !merged[len(merged)-1].end.IsZero() && span.start.After(merged[len(merged)-1].end) {
					merged = append(merged, span)
					continue
				}
				previous := &merged[len(merged)-1]
				if !previous.end.IsZero() && (span.end.IsZero() || span.end.After(previous.end)) {
					previous.end = span.end
				}
			}
			out[topic] = append(out[topic], researchMembershipNode{node, merged})
		}
		sort.Slice(out[topic], func(i, j int) bool { return out[topic][i].node < out[topic][j].node })
	}
	return out
}

func (node researchMembershipNode) receivedDuring(receipts []time.Time) bool {
	if len(receipts) == 0 {
		return false
	}
	first, last := receipts[0], receipts[len(receipts)-1]
	start := sort.Search(len(node.spans), func(i int) bool { return node.spans[i].end.IsZero() || node.spans[i].end.After(first) })
	for _, span := range node.spans[start:] {
		if span.start.After(last) {
			break
		}
		index := sort.Search(len(receipts), func(i int) bool { return !receipts[i].Before(span.start) })
		if index < len(receipts) && (span.end.IsZero() || receipts[index].Before(span.end)) {
			return true
		}
	}
	return false
}

type researchMessageProjection struct {
	item               researchMessage
	population         map[string]bool
	eligibleDuplicates []analysisPoint
	orphanReceipts     int
	published          bool
}

func (a *researchAccumulator) projectMessage(ctx context.Context, key messageMetricKey, message *messageMetric, members map[string][]researchMembershipNode, origins *originMetadataIndex, nodeCount *float64) (researchMessageProjection, error) {
	if err := ctx.Err(); err != nil {
		return researchMessageProjection{}, err
	}
	if !message.published {
		return researchMessageProjection{orphanReceipts: len(message.deliveries)}, nil
	}
	item := researchMessage{ID: key.id, Topic: key.topic, Publisher: message.publisher, At: message.publishedAt, Metrics: map[string]analysisStatistic{}, Population: []string{}, Nodes: []researchNode{}, DuplicateTimeline: []analysisPoint{}}
	nodeKeys := make([]string, 0, len(message.deliveries))
	for id := range message.deliveries {
		if id != message.publisher {
			nodeKeys = append(nodeKeys, id)
		}
	}
	sort.Strings(nodeKeys)
	byID := map[string]int{}
	latencies, earlyLatencies := []float64{}, []float64{}
	receiptTimes := []time.Time{}
	for _, id := range nodeKeys {
		if err := ctx.Err(); err != nil {
			return researchMessageProjection{}, err
		}
		delivery := message.deliveries[id]
		node := researchNode{ID: id, Source: "unknown", LinkEstimate: "unknown", Evidence: []string{}}
		seconds := math.NaN()
		if delivery.latencyAvailable && delivery.latencyMS >= 0 {
			seconds = delivery.latencyMS / 1000
		} else if detail := delivery.research; detail != nil {
			pub := a.publications[key]
			if !pub.at.IsZero() && !delivery.timestamp.IsZero() && (pub.synchronized && detail.synchronized || pub.agent != "" && pub.agent == delivery.agentID) {
				seconds = delivery.timestamp.Sub(pub.at).Seconds()
			}
		}
		if seconds >= 0 {
			node.Seconds = numberPointer(seconds)
			if node.Seconds != nil {
				latencies = append(latencies, *node.Seconds)
			}
		}
		if detail := delivery.research; detail != nil {
			node.Parent = a.peers[detail.from]
			node.LinkEstimate, node.Evidence = origins.estimate(detail.from, detail.peer, key.topic, detail.wireID, message.publishedAt, delivery.timestamp)

		}
		receiptTimes = append(receiptTimes, delivery.timestamp)
		byID[id] = len(item.Nodes)
		item.Nodes = append(item.Nodes, node)
	}
	visiting := map[string]bool{}
	resolved := map[string]bool{}
	var resolve func(string)
	resolve = func(id string) {
		if resolved[id] || visiting[id] {
			return
		}
		visiting[id] = true
		index, exists := byID[id]
		if !exists {
			return
		}
		node := &item.Nodes[index]
		parentSource := "unknown"
		var parentHop *int
		if node.Parent == message.publisher {
			zero := 0
			parentHop = &zero
			parentSource = "eager"
		} else if parentIndex, ok := byID[node.Parent]; ok && !visiting[node.Parent] {
			resolve(node.Parent)
			parentHop = item.Nodes[parentIndex].Hop
			parentSource = item.Nodes[parentIndex].Source
		}
		if parentHop != nil {
			hop := *parentHop + 1
			node.Hop = &hop
		}
		if parentHop != nil && (node.LinkEstimate == "lazy" || parentSource == "lazy") {
			node.Source = "lazy"
		} else if node.LinkEstimate == "eager" && parentSource == "eager" {
			node.Source = "eager"
		}
		visiting[id] = false
		resolved[id] = true
	}
	eager, lazy := 0, 0
	for _, id := range nodeKeys {
		if err := ctx.Err(); err != nil {
			return researchMessageProjection{}, err
		}
		resolve(id)
	}
	for _, node := range item.Nodes {
		switch node.Source {
		case "eager":
			eager++
			if node.Seconds != nil {
				earlyLatencies = append(earlyLatencies, *node.Seconds)
			}
		case "lazy":
			lazy++
		}
	}
	population := map[string]bool{}
	if len(receiptTimes) == 0 && !message.publishedAt.IsZero() {
		receiptTimes = append(receiptTimes, message.publishedAt)
	}
	sort.Slice(receiptTimes, func(i, j int) bool { return receiptTimes[i].Before(receiptTimes[j]) })
	membership := members[key.topic]
	membershipObserved := len(membership) > 0
	for _, member := range membership {
		if member.node != message.publisher && member.receivedDuring(receiptTimes) {
			population[member.node] = true
		}
	}
	item.PopulationBasis = "observed membership union"
	if !membershipObserved {
		item.PopulationBasis = "unavailable"
		if message.targets != nil {
			item.PopulationBasis = "dispatch targets"
			for id := range message.targets {
				if id != message.publisher {
					population[id] = true
				}
			}
		}
	}
	for id := range population {
		item.Population = append(item.Population, id)
	}
	sort.Strings(item.Population)

	// Count only receivers belonging to the explicitly reconstructed denominator.
	reached, eligibleEager := 0, 0
	for _, node := range item.Nodes {
		if population[node.ID] {
			reached++
			if node.Source == "eager" {
				eligibleEager++
			}
		}
	}
	denominator := float64(len(population))
	duplicates := float64(sumDuplicateCounts(message.duplicates))
	item.Metrics["frt"], item.Metrics["eager_frt"] = analysisStats(latencies), analysisStats(earlyLatencies)
	item.Metrics["drc"] = scalarStat(numberPointer(duplicates))
	item.Metrics["eager_count"], item.Metrics["lazy_count"] = scalarStat(numberPointer(float64(eager))), scalarStat(numberPointer(float64(lazy)))
	item.Metrics["unknown_count"] = scalarStat(numberPointer(float64(len(nodeKeys) - eager - lazy)))
	if denominator > 0 {
		item.Metrics["reachability"] = scalarStat(numberPointer(float64(reached) / denominator))
		item.Metrics["drc_per_target"] = scalarStat(numberPointer(duplicates / denominator))
		item.Metrics["eager_reachability"] = scalarStat(numberPointer(float64(eligibleEager) / denominator))
	}
	if nodes := nodeCount; nodes != nil && *nodes > 0 {
		item.Metrics["drc_per_node_count"] = scalarStat(numberPointer(duplicates / *nodes))
	}
	duplicateTimes := map[float64]float64{}
	eligibleDuplicates := []analysisPoint{}
	if !message.publishedAt.IsZero() {
		for at, count := range a.duplicateTimes[key] {
			seconds := time.Unix(0, at.at).Sub(message.publishedAt).Seconds()
			if seconds >= 0 {
				duplicateTimes[seconds] += float64(count)
				if population[at.node] {
					eligibleDuplicates = append(eligibleDuplicates, analysisPoint{X: seconds, Y: float64(count)})
				}
			}
		}
	}
	item.DuplicateTimeline = cumulativePoints(duplicateTimes, 1)

	return researchMessageProjection{item: item, population: population, eligibleDuplicates: eligibleDuplicates, published: true}, ctx.Err()
}
