package controller

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type analysisStatistic struct {
	Average   *float64 `json:"average"`
	Deviation *float64 `json:"deviation"`
	Median    *float64 `json:"median"`
	Count     int      `json:"count"`
}

type researchNode struct {
	ID           string   `json:"id"`
	Parent       string   `json:"parent,omitempty"`
	Seconds      *float64 `json:"seconds"`
	Hop          *int     `json:"hop"`
	Source       string   `json:"source"`
	LinkEstimate string   `json:"linkEstimate"`
	Evidence     []string `json:"evidence"`
}

type researchMessage struct {
	ID                string                       `json:"id"`
	Topic             string                       `json:"topic"`
	Publisher         string                       `json:"publisher"`
	At                time.Time                    `json:"at"`
	Metrics           map[string]analysisStatistic `json:"metrics"`
	Population        []string                     `json:"population"`
	PopulationBasis   string                       `json:"populationBasis"`
	Nodes             []researchNode               `json:"nodes"`
	DuplicateTimeline []analysisPoint              `json:"duplicateTimeline"`
}

type researchControlBin struct {
	At     time.Time          `json:"at"`
	Values map[string]float64 `json:"values"`
}

type researchAnalysis struct {
	ReceiverGroups      []researchReceiverGroup      `json:"receiverGroups,omitempty"`
	Overview            *researchOverview            `json:"overview,omitempty"`
	OriginMethod        string                       `json:"originMethod"`
	OriginWindowSeconds float64                      `json:"originWindowSeconds"`
	MessageCount        int                          `json:"messageCount"`
	Definition          string                       `json:"definition"`
	Summary             map[string]analysisStatistic `json:"summary"`
	Messages            []researchMessage            `json:"messages"`
	Controls            []researchControlBin         `json:"controls"`
	ControlBinSeconds   int64                        `json:"controlBinSeconds"`
	DegreeDistribution  []analysisPoint              `json:"degreeDistribution"`
	PropagationCDF      []analysisPoint              `json:"propagationCDF"`
	DuplicateCDF        []analysisPoint              `json:"duplicateCDF"`
	HopPDF              []analysisPoint              `json:"hopPDF"`
	HopCDF              []analysisPoint              `json:"hopCDF"`
	EagerCDF            []analysisPoint              `json:"eagerCDF"`
	LazyCDF             []analysisPoint              `json:"lazyCDF"`
	UnresolvedParents   int                          `json:"unresolvedParents"`
	UnknownOrigins      int                          `json:"unknownOrigins"`
	OrphanReceipts      int                          `json:"orphanReceipts"`
	PopulationBasis     string                       `json:"populationBasis"`
	DegreeFit           *degreeFit                   `json:"degreeFit,omitempty"`
	EligiblePopulation  int                          `json:"eligiblePopulation"`
}

type deliveryResearch struct {
	peer, from, wireID string
	synchronized       bool
}
type researchPublication struct {
	at           time.Time
	agent        string
	synchronized bool
}
type duplicateResearchKey struct {
	at   int64
	node string
}
type researchEdge struct{ from, to string }
type researchGraphEvent struct {
	at                              time.Time
	node, peer, remote, topic, kind string
}
type researchAccumulator struct {
	nodeGroups     map[string]string
	groupConflicts map[string]bool
	publications   map[messageMetricKey]researchPublication
	peers          map[string]string
	inference      []originMetadataEvent
	duplicateTimes map[messageMetricKey]map[duplicateResearchKey]int
	controls       map[int64]map[string]float64
	graphEvents    []researchGraphEvent
}

func newResearchAccumulator() *researchAccumulator {
	return &researchAccumulator{nodeGroups: map[string]string{}, groupConflicts: map[string]bool{}, publications: map[messageMetricKey]researchPublication{}, peers: map[string]string{}, duplicateTimes: map[messageMetricKey]map[duplicateResearchKey]int{}, controls: map[int64]map[string]float64{}}
}
func (a *researchAccumulator) addControl(at time.Time, key string, n float64) {
	if at.IsZero() {
		return
	}
	bin := analysisBucket(at.Unix(), 5)
	if a.controls[bin] == nil {
		a.controls[bin] = map[string]float64{}
	}
	a.controls[bin][key] += n
}
func (a *researchAccumulator) observe(e model.TraceEvent) {
	a.observeNodeGroup(e.NodeID, e.Group)
	if e.PeerID != "" && e.NodeID != "" {
		a.peers[e.PeerID] = e.NodeID
	}
	a.observeOriginMetadata(e)
	key := messageMetricKey{e.Topic, e.MessageID}
	if control, ok := gossipSubControlEvent(e.Type); ok {
		name := control.direction + "_" + control.controlType
		// Mesh transitions are counted separately from wire RPC occurrences.
		if control.controlType == "graft" || control.controlType == "prune" {
			name += "_rpc"
		}
		a.addControl(e.Timestamp, name, 1)
		if n, ok := numericEventField(e.Fields, "messageIdCount"); ok {
			a.addControl(e.Timestamp, "logical_"+name, n)
		}
		if n, ok := numericEventField(e.Fields, "controlEntries"); ok {
			a.addControl(e.Timestamp, "entries_"+name, n)
		}
	}
	switch e.Type {
	case "publish":
		basis, _ := e.Fields["clockBasis"].(string)
		if _, ok := a.publications[key]; !ok {
			a.publications[key] = researchPublication{e.Timestamp, e.AgentID, basis == "controller-offset-v1"}
		}
	case "measurement_start":
		if topics, ok := targetNodeIDs(e.Fields["subscribedTopics"]); ok && e.NodeID != "" && !e.Timestamp.IsZero() {
			for _, topic := range topics {
				a.graphEvents = append(a.graphEvents, researchGraphEvent{e.Timestamp, e.NodeID, e.PeerID, "", topic, "join"})
			}
		}
	case "duplicate":
		local, _ := e.Fields["localDelivery"].(bool)
		if local || e.MessageID == "" || e.Timestamp.IsZero() {
			return
		}
		if a.duplicateTimes[key] == nil {
			a.duplicateTimes[key] = map[duplicateResearchKey]int{}
		}
		a.duplicateTimes[key][duplicateResearchKey{e.Timestamp.UnixNano(), e.NodeID}]++
	case "graft", "prune", "join", "leave", "measurement_stop", "measurement_terminated":
		if e.NodeID != "" && !e.Timestamp.IsZero() {
			a.graphEvents = append(a.graphEvents, researchGraphEvent{e.Timestamp, e.NodeID, e.PeerID, e.RemotePeerID, e.Topic, e.Type})
		}
	}
}

func analysisStats(values []float64) analysisStatistic {
	finite := make([]float64, 0, len(values))
	scale := 0.
	for _, v := range values {
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			finite = append(finite, v)
			scale = math.Max(scale, math.Abs(v))
		}
	}
	out := analysisStatistic{Count: len(finite)}
	if len(finite) == 0 {
		return out
	}
	sort.Float64s(finite)
	mean := 0.
	if scale > 0 {
		for i, v := range finite {
			mean += (v/scale - mean) / float64(i+1)
		}
	}
	variance := 0.
	if scale > 0 {
		for _, v := range finite {
			d := v/scale - mean
			variance += d * d / float64(len(finite))
		}
	}
	out.Average = numberPointer(mean * scale)
	out.Deviation = numberPointer(math.Sqrt(variance) * scale)
	median := finite[len(finite)/2]
	if len(finite)%2 == 0 {
		median = finite[len(finite)/2-1]/2 + median/2
	}
	out.Median = numberPointer(median)
	return out
}
func scalarStat(value *float64) analysisStatistic {
	if value == nil {
		return analysisStatistic{}
	}
	return analysisStats([]float64{*value})
}
func cumulativePoints(counts map[float64]float64, denominator float64) []analysisPoint {
	out := []analysisPoint{}
	if denominator <= 0 {
		return out
	}
	xs := make([]float64, 0, len(counts))
	for x := range counts {
		if x >= 0 && !math.IsNaN(x) && !math.IsInf(x, 0) {
			xs = append(xs, x)
		}
	}
	sort.Float64s(xs)
	total := 0.
	stride := max(1, (len(xs)+359)/360)
	for i, x := range xs {
		total += counts[x]
		if i%stride == 0 || i == len(xs)-1 {
			out = append(out, analysisPoint{x, total / denominator})
		}
	}
	return out
}

func (a *researchAccumulator) finish(ctx context.Context, metrics *runMetricAccumulator, observations []analysisObservation) (researchAnalysis, error) {
	out := researchAnalysis{OriginMethod: "message-id-preferred-graft-ihave-iwant-estimate-v1", OriginWindowSeconds: originEvidenceWindow.Seconds(), Definition: "v2-corrected-observations-v1", PopulationBasis: "union of subscribed non-publisher nodes at first receipts; dispatch targets only when membership history is absent", Summary: map[string]analysisStatistic{}, Messages: []researchMessage{}, Controls: []researchControlBin{}, ControlBinSeconds: 5, DegreeDistribution: []analysisPoint{}, PropagationCDF: []analysisPoint{}, DuplicateCDF: []analysisPoint{}, HopPDF: []analysisPoint{}, HopCDF: []analysisPoint{}, EagerCDF: []analysisPoint{}, LazyCDF: []analysisPoint{}}
	sort.Slice(a.graphEvents, func(i, j int) bool {
		if a.graphEvents[i].at.Equal(a.graphEvents[j].at) {
			return a.graphEvents[i].kind < a.graphEvents[j].kind
		}
		return a.graphEvents[i].at.Before(a.graphEvents[j].at)
	})
	// Reciprocal GRAFTs establish one logical edge; PRUNE/LEAVE clears both
	// directions so an old pending GRAFT cannot create a phantom later edge.
	pending := map[string]map[researchEdge]bool{}
	confirmed := map[string]map[researchEdge]bool{}
	type life struct {
		node, topic string
		start, end  time.Time
	}
	lives := []life{}
	open := map[string]int{}
	for _, e := range a.graphEvents {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if e.kind == "join" {
			key := e.node + "\x00" + e.topic
			if _, ok := open[key]; !ok {
				open[key] = len(lives)
				lives = append(lives, life{node: e.node, topic: e.topic, start: e.at})
			}
		}
		if e.kind == "leave" || e.kind == "measurement_stop" || e.kind == "measurement_terminated" {
			for key, index := range open {
				if lives[index].node == e.node && (e.kind != "leave" || lives[index].topic == e.topic) {
					lives[index].end = e.at
					delete(open, key)
				}
			}
			for topic, edges := range pending {
				if e.kind == "leave" && topic != e.topic {
					continue
				}
				for edge := range edges {
					if a.peers[edge.from] == e.node || a.peers[edge.to] == e.node {
						delete(edges, edge)
						delete(confirmed[topic], edge)
						delete(confirmed[topic], researchEdge{edge.to, edge.from})
					}
				}
			}
		}
		if e.kind != "graft" && e.kind != "prune" {
			continue
		}
		a.addControl(e.at, "send_"+e.kind, 1)
		if pending[e.topic] == nil {
			pending[e.topic] = map[researchEdge]bool{}
			confirmed[e.topic] = map[researchEdge]bool{}
		}
		edge, reverse := researchEdge{e.peer, e.remote}, researchEdge{e.remote, e.peer}
		if edge.from == "" || edge.to == "" || edge.from == edge.to {
			continue
		}
		if e.kind == "graft" {
			pending[e.topic][edge] = true
			if pending[e.topic][reverse] && !confirmed[e.topic][edge] {
				confirmed[e.topic][edge], confirmed[e.topic][reverse] = true, true
				a.addControl(e.at, "logical_send_graft", 1)
			}
		} else {
			if confirmed[e.topic][edge] {
				a.addControl(e.at, "logical_send_prune", 1)
			}
			delete(pending[e.topic], edge)
			delete(pending[e.topic], reverse)
			delete(confirmed[e.topic], edge)
			delete(confirmed[e.topic], reverse)
		}
	}
	// Equally weighted saved snapshots are separate from receiver latency samples.
	graphSeries := map[string][]float64{}
	distribution := map[float64]float64{}
	degreeSamples := 0
	for _, observation := range observations {
		for _, group := range observation.Groups {
			if group.Group != "" {
				continue
			}
			for _, layer := range group.Layers {
				if layer.Protocol != "gossipsub" {
					continue
				}
				graphSeries["node_count"] = append(graphSeries["node_count"], float64(layer.Nodes))
				if layer.AverageDegree != nil {
					graphSeries["average_degree"] = append(graphSeries["average_degree"], *layer.AverageDegree)
				}
				if layer.Clustering != nil {
					graphSeries["clustering_coefficient"] = append(graphSeries["clustering_coefficient"], *layer.Clustering)
				}
				for key, value := range layer.Metrics {
					if value != nil && key != "node_count" && key != "average_degree" && key != "clustering_coefficient" {
						graphSeries[key] = append(graphSeries[key], *value)
					}
				}
				if layer.Nodes > 0 {
					degreeSamples++
					for _, point := range layer.Degrees {
						distribution[point.X] += point.Y / float64(layer.Nodes)
					}
				}
			}
		}
	}
	for key, values := range graphSeries {
		out.Summary[key] = analysisStats(values)
	}
	if degreeSamples > 0 {
		for degree, count := range distribution {
			out.DegreeDistribution = append(out.DegreeDistribution, analysisPoint{degree, count / float64(degreeSamples)})
		}
		sort.Slice(out.DegreeDistribution, func(i, j int) bool { return out.DegreeDistribution[i].X < out.DegreeDistribution[j].X })
	}
	origins := newOriginMetadataIndex(a.inference, a.peers)
	keys := make([]messageMetricKey, 0, len(metrics.messages))
	for key := range metrics.messages {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].topic == keys[j].topic {
			return keys[i].id < keys[j].id
		}
		return keys[i].topic < keys[j].topic
	})
	samples := map[string][]float64{}
	arrivalCounts, duplicateCounts, eagerCounts, lazyCounts, hopCounts := map[float64]float64{}, map[float64]float64{}, map[float64]float64{}, map[float64]float64{}, map[float64]float64{}
	totalPopulation, hopTotal, eagerTotal, lazyTotal := 0., 0., 0., 0.
	for ki, key := range keys {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if ki%100 == 0 {
			reportAnalysisProgress(ctx, fmt.Sprintf("propagation %d/%d", ki+1, len(keys)), 0)
		}
		message := metrics.messages[key]
		if !message.published {
			out.OrphanReceipts += len(message.deliveries)
			continue
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
			resolve(id)
		}
		for _, node := range item.Nodes {
			if node.Hop == nil {
				out.UnresolvedParents++
			} else {
				hopCounts[float64(*node.Hop)]++
				hopTotal++
			}
			switch node.Source {
			case "eager":
				eager++
				a.addControl(message.deliveries[node.ID].timestamp, "eager_count", 1)
				if node.Seconds != nil {
					earlyLatencies = append(earlyLatencies, *node.Seconds)
					eagerCounts[*node.Seconds]++
					eagerTotal++
				}
			case "lazy":
				lazy++
				a.addControl(message.deliveries[node.ID].timestamp, "lazy_count", 1)
				if node.Seconds != nil {
					lazyCounts[*node.Seconds]++
					lazyTotal++
				}
			default:
				out.UnknownOrigins++
			}
		}
		population := map[string]bool{}
		membershipObserved := false
		if len(receiptTimes) == 0 && !message.publishedAt.IsZero() {
			receiptTimes = append(receiptTimes, message.publishedAt)
		}
		sort.Slice(receiptTimes, func(i, j int) bool { return receiptTimes[i].Before(receiptTimes[j]) })
		for _, life := range lives {
			if life.topic != key.topic {
				continue
			}
			membershipObserved = true
			if life.node == message.publisher {
				continue
			}
			i := sort.Search(len(receiptTimes), func(i int) bool { return !receiptTimes[i].Before(life.start) })
			if i < len(receiptTimes) && (life.end.IsZero() || receiptTimes[i].Before(life.end)) {
				population[life.node] = true
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
				if node.Seconds != nil {
					arrivalCounts[*node.Seconds]++
				}
			}
		}
		denominator := float64(len(population))
		totalPopulation += denominator
		item.Metrics["frt"], item.Metrics["eager_frt"] = analysisStats(latencies), analysisStats(earlyLatencies)
		item.Metrics["drc"] = scalarStat(numberPointer(float64(sumDuplicateCounts(message.duplicates))))
		item.Metrics["eager_count"], item.Metrics["lazy_count"] = scalarStat(numberPointer(float64(eager))), scalarStat(numberPointer(float64(lazy)))
		item.Metrics["unknown_count"] = scalarStat(numberPointer(float64(len(nodeKeys) - eager - lazy)))
		if denominator > 0 {
			item.Metrics["reachability"] = scalarStat(numberPointer(float64(reached) / denominator))
			item.Metrics["drc_per_target"] = scalarStat(numberPointer(float64(sumDuplicateCounts(message.duplicates)) / denominator))
			item.Metrics["eager_reachability"] = scalarStat(numberPointer(float64(eligibleEager) / denominator))
		}
		if nodes := out.Summary["node_count"].Average; nodes != nil && *nodes > 0 {
			item.Metrics["drc_per_node_count"] = scalarStat(numberPointer(float64(sumDuplicateCounts(message.duplicates)) / *nodes))
		}
		duplicateTimes := map[float64]float64{}
		if !message.publishedAt.IsZero() {
			for at, count := range a.duplicateTimes[key] {
				seconds := time.Unix(0, at.at).Sub(message.publishedAt).Seconds()
				if seconds >= 0 {
					duplicateTimes[seconds] += float64(count)
					if population[at.node] {
						duplicateCounts[seconds] += float64(count)
					}
				}
			}
		}
		item.DuplicateTimeline = cumulativePoints(duplicateTimes, 1)
		for key, stat := range item.Metrics {
			if stat.Average != nil {
				samples[key] = append(samples[key], *stat.Average)
			}
		}
		out.Messages = append(out.Messages, item)
	}
	for key, values := range samples {
		out.Summary[key] = analysisStats(values)
	}
	out.EligiblePopulation = int(totalPopulation)
	out.PropagationCDF = cumulativePoints(arrivalCounts, totalPopulation)
	out.DuplicateCDF = cumulativePoints(duplicateCounts, totalPopulation)
	out.EagerCDF = cumulativePoints(eagerCounts, eagerTotal)
	out.LazyCDF = cumulativePoints(lazyCounts, lazyTotal)
	if hopTotal > 0 {
		for hop, count := range hopCounts {
			out.HopPDF = append(out.HopPDF, analysisPoint{hop, count / hopTotal})
		}
		sort.Slice(out.HopPDF, func(i, j int) bool { return out.HopPDF[i].X < out.HopPDF[j].X })
		out.HopCDF = cumulativePoints(hopCounts, hopTotal)
	}
	for len(a.controls) > 360 {
		out.ControlBinSeconds *= 2
		merged := map[int64]map[string]float64{}
		for at, values := range a.controls {
			key := analysisBucket(at, out.ControlBinSeconds)
			if merged[key] == nil {
				merged[key] = map[string]float64{}
			}
			for name, value := range values {
				merged[key][name] += value
			}
		}
		a.controls = merged
	}
	totals := map[string]float64{}
	for at, values := range a.controls {
		out.Controls = append(out.Controls, researchControlBin{time.Unix(at, 0).UTC(), values})
		for name, value := range values {
			totals[name] += value
		}
	}
	sort.Slice(out.Controls, func(i, j int) bool { return out.Controls[i].At.Before(out.Controls[j].At) })
	for key, value := range totals {
		if key != "eager_count" && key != "lazy_count" {
			out.Summary[key] = scalarStat(numberPointer(value))
		}
	}
	out.MessageCount = len(out.Messages)
	out.DegreeFit = fitDegreeDistribution(ctx, out.DegreeDistribution)
	return out, ctx.Err()
}
func sumDuplicateCounts(counts map[string]int) int {
	sum := 0
	for _, n := range counts {
		sum += n
	}
	return sum
}
