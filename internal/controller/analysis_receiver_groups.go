package controller

import (
	"context"
	"math"
	"sort"
	"time"
)

// Receiver groups project already reconstructed paths: a cross-group parent
// remains part of the path and must not turn into an unresolved root.
// Empty Group means missing or conflicting identity evidence, never All Peers.
type researchReceiverGroup struct {
	Group              string                       `json:"group"`
	MessageCount       int                          `json:"messageCount"`
	EligiblePopulation int                          `json:"eligiblePopulation"`
	UnresolvedParents  int                          `json:"unresolvedParents"`
	UnknownOrigins     int                          `json:"unknownOrigins"`
	Summary            map[string]analysisStatistic `json:"summary"`
	Overview           *researchOverview            `json:"overview"`
	PropagationCDF     []analysisPoint              `json:"propagationCDF"`
	DuplicateCDF       []analysisPoint              `json:"duplicateCDF"`
	HopPDF             []analysisPoint              `json:"hopPDF"`
	HopCDF             []analysisPoint              `json:"hopCDF"`
	EagerCDF           []analysisPoint              `json:"eagerCDF"`
	LazyCDF            []analysisPoint              `json:"lazyCDF"`
	LatencyCDF         []analysisPoint              `json:"latencyCDF"`
	LatencyHistogram   []analysisPoint              `json:"latencyHistogram"`
}

func (a *researchAccumulator) observeNodeGroup(node, group string) {
	if node == "" || group == "" || a.groupConflicts[node] {
		return
	}
	if previous := a.nodeGroups[node]; previous != "" && previous != group {
		delete(a.nodeGroups, node)
		a.groupConflicts[node] = true
		return
	}
	a.nodeGroups[node] = group
}

func (a *researchAccumulator) observeGroups(observation analysisObservation) {
	for _, graph := range observation.Graphs {
		for i, node := range graph.Nodes {
			if i < len(graph.Groups) {
				a.observeNodeGroup(node, graph.Groups[i])
			}
		}
	}
}

type receiverGroupAccumulator struct {
	result                                         researchReceiverGroup
	messages                                       []researchMessage // Only timestamps and scalars; no duplicated node paths.
	arrivals, duplicates, times, hops, eager, lazy map[float64]float64
	latencies                                      []float64
	receipts, duplicateCount                       int
}

func newReceiverGroupAccumulator(group string) *receiverGroupAccumulator {
	return &receiverGroupAccumulator{
		result:   researchReceiverGroup{Group: group, Summary: map[string]analysisStatistic{}, Overview: &researchOverview{OriginCounts: map[string]int{"eager": 0, "lazy": 0, "unknown": 0}}},
		arrivals: map[float64]float64{}, duplicates: map[float64]float64{}, times: map[float64]float64{}, hops: map[float64]float64{}, eager: map[float64]float64{}, lazy: map[float64]float64{},
	}
}

type receiverMessageGroup struct {
	nodes      []researchNode
	population map[string]bool
	duplicates int
}

func (a *researchAccumulator) receiverGroups(ctx context.Context, metrics *runMetricAccumulator, messages []researchMessage, observations []analysisObservation, latencySamples []propagationSample, origin time.Time) ([]researchReceiverGroup, error) {
	groups := map[string]*receiverGroupAccumulator{}
	get := func(group string) *receiverGroupAccumulator {
		if groups[group] == nil {
			groups[group] = newReceiverGroupAccumulator(group)
		}
		return groups[group]
	}
	// DRC / node count retains the existing snapshot-mean definition, restricted
	// to this group's GossipSub participants. Missing snapshots remain N/A.
	nodeCounts := map[string][]float64{}
	for _, observation := range observations {
		for _, group := range observation.Groups {
			if group.Group == "" {
				continue
			}
			for _, layer := range group.Layers {
				if layer.Protocol == "gossipsub" {
					nodeCounts[group.Group] = append(nodeCounts[group.Group], float64(layer.Nodes))
				}
			}
		}
	}
	nodeStats := map[string]analysisStatistic{}
	for group, counts := range nodeCounts {
		nodeStats[group] = analysisStats(counts)
	}
	if origin.IsZero() {
		for _, message := range messages {
			if !message.At.IsZero() && (origin.IsZero() || message.At.Before(origin)) {
				origin = message.At
			}
		}
	}
	for _, message := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		partition := map[string]*receiverMessageGroup{}
		receiver := func(node string) *receiverMessageGroup {
			group := a.nodeGroups[node]
			if partition[group] == nil {
				partition[group] = &receiverMessageGroup{population: map[string]bool{}}
			}
			return partition[group]
		}
		for _, id := range message.Population {
			receiver(id).population[id] = true
		}
		for _, node := range message.Nodes {
			bucket := receiver(node.ID)
			bucket.nodes = append(bucket.nodes, node)
		}
		key := messageMetricKey{message.Topic, message.ID}
		for node, count := range metrics.messages[key].duplicates {
			if node != message.Publisher {
				receiver(node).duplicates += count
			}
		}
		for group, bucket := range partition {
			g := get(group)
			g.result.EligiblePopulation += len(bucket.population)
			g.duplicateCount += bucket.duplicates
			values, eagerValues := []float64{}, []float64{}
			reached, eagerReached, eager, lazy := 0, 0, 0, 0
			for _, node := range bucket.nodes {
				g.receipts++
				source := node.Source
				if source != "eager" && source != "lazy" {
					source = "unknown"
					g.result.UnknownOrigins++
				}
				g.result.Overview.OriginCounts[source]++
				if node.Hop != nil {
					g.hops[float64(*node.Hop)]++
				} else {
					g.result.UnresolvedParents++
				}
				if source == "eager" {
					eager++
				}
				if source == "lazy" {
					lazy++
				}
				if bucket.population[node.ID] {
					reached++
					if source == "eager" {
						eagerReached++
					}
				}
				if node.Seconds == nil {
					continue
				}
				seconds := *node.Seconds
				values = append(values, seconds)
				g.times[seconds]++
				if bucket.population[node.ID] {
					g.arrivals[seconds]++
				}
				if source == "eager" {
					eagerValues = append(eagerValues, seconds)
					g.eager[seconds]++
				}
				if source == "lazy" {
					g.lazy[seconds]++
				}
			}
			stats := map[string]analysisStatistic{
				"frt": analysisStats(values), "eager_frt": analysisStats(eagerValues),
				"drc":           scalarStat(numberPointer(float64(bucket.duplicates))),
				"eager_count":   scalarStat(numberPointer(float64(eager))),
				"lazy_count":    scalarStat(numberPointer(float64(lazy))),
				"unknown_count": scalarStat(numberPointer(float64(len(bucket.nodes) - eager - lazy))),
			}
			if denominator := float64(len(bucket.population)); denominator > 0 {
				stats["reachability"] = scalarStat(numberPointer(float64(reached) / denominator))
				stats["eager_reachability"] = scalarStat(numberPointer(float64(eagerReached) / denominator))
				stats["drc_per_target"] = scalarStat(numberPointer(float64(bucket.duplicates) / denominator))
			}
			if count := nodeStats[group].Average; count != nil && *count > 0 {
				stats["drc_per_node_count"] = scalarStat(numberPointer(float64(bucket.duplicates) / *count))
			}
			g.messages = append(g.messages, researchMessage{At: message.At, Metrics: stats})
		}
		// Duplicate timestamps carry the receiver identity; no attribution to the
		// publisher's group, and no second scan of all duplicates for every group.
		if !message.At.IsZero() {
			for at, count := range a.duplicateTimes[key] {
				group := a.nodeGroups[at.node]
				bucket := partition[group]
				seconds := time.Unix(0, at.at).Sub(message.At).Seconds()
				if bucket != nil && bucket.population[at.node] && seconds >= 0 {
					get(group).duplicates[seconds] += float64(count)
				}
			}
		}
	}
	low, high := math.Inf(1), math.Inf(-1)
	for _, sample := range latencySamples {
		low, high = math.Min(low, sample.seconds*1000), math.Max(high, sample.seconds*1000)
		group := get(a.nodeGroups[sample.nodeID])
		group.latencies = append(group.latencies, sample.seconds*1000)
	}
	names := make([]string, 0, len(groups))
	for group := range groups {
		names = append(names, group)
	}
	sort.Strings(names)
	out := make([]researchReceiverGroup, 0, len(names))
	for _, group := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		g := groups[group]
		r := &g.result
		r.MessageCount = len(g.messages)
		samples := map[string][]float64{}
		for _, message := range g.messages {
			for key, stat := range message.Metrics {
				if _, ok := samples[key]; !ok {
					samples[key] = nil
				}
				if stat.Average != nil {
					samples[key] = append(samples[key], *stat.Average)
				}
			}
		}
		for key, values := range samples {
			r.Summary[key] = analysisStats(values)
		}
		r.Summary["node_count"] = nodeStats[group]
		r.PropagationCDF = cumulativePoints(g.arrivals, float64(r.EligiblePopulation))
		r.DuplicateCDF = cumulativePoints(g.duplicates, float64(r.EligiblePopulation))
		r.EagerCDF = cumulativePoints(g.eager, receiverCount(g.eager))
		r.LazyCDF = cumulativePoints(g.lazy, receiverCount(g.lazy))
		hopTotal := receiverCount(g.hops)
		r.HopCDF = cumulativePoints(g.hops, hopTotal)
		r.HopPDF = []analysisPoint{}
		for _, point := range r.HopCDF {
			r.HopPDF = append(r.HopPDF, analysisPoint{X: point.X, Y: g.hops[point.X] / hopTotal})
		}
		r.LatencyCDF, _ = analysisDistribution(g.latencies)
		r.LatencyHistogram = receiverHistogram(g.latencies, low, high, len(latencySamples))
		r.Overview.MessageSeries = researchMessageSeries(g.messages, origin)
		r.Overview.ReceiversTime = cumulativePoints(g.times, float64(r.MessageCount))
		r.Overview.ReceiversHop = cumulativePoints(g.hops, float64(r.MessageCount))
		// A measured absence is zero; receipts missing timing/path evidence are N/A.
		if r.MessageCount > 0 && g.receipts == 0 {
			r.Overview.ReceiversTime = []analysisPoint{{X: 0, Y: 0}}
			r.Overview.ReceiversHop = []analysisPoint{{X: 0, Y: 0}}
			if r.EligiblePopulation > 0 {
				r.PropagationCDF = []analysisPoint{{X: 0, Y: 0}}
			}
		}
		if r.EligiblePopulation > 0 && g.duplicateCount == 0 {
			r.DuplicateCDF = []analysisPoint{{X: 0, Y: 0}}
		}
		out = append(out, *r)
	}
	return out, nil
}

func receiverCount(counts map[float64]float64) float64 {
	total := 0.
	for _, count := range counts {
		total += count
	}
	return total
}

// Use the exact same bins as the whole-run histogram, so side-by-side bars
// compare counts over identical intervals without interpolating measurements.
func receiverHistogram(values []float64, low, high float64, total int) []analysisPoint {
	if len(values) == 0 {
		return []analysisPoint{}
	}
	count := min(30, max(1, int(math.Ceil(math.Sqrt(float64(total))))))
	width := (high - low) / float64(count)
	if width == 0 {
		return []analysisPoint{{X: low, Y: float64(len(values))}}
	}
	out := make([]analysisPoint, count)
	for i := range out {
		out[i].X = low + (float64(i)+.5)*width
	}
	for _, value := range values {
		out[min(count-1, max(0, int((value-low)/width)))].Y++
	}
	return out
}
