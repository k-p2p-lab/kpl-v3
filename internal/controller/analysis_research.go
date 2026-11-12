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
	return a.finishWithWorkers(ctx, metrics, observations, analysisParallelism())
}

func (a *researchAccumulator) finishWithWorkers(ctx context.Context, metrics *runMetricAccumulator, observations []analysisObservation, workers int) (researchAnalysis, error) {
	// Derived counts belong to this artifact. Retrying a canceled calculation
	// or rebuilding from the same immutable accumulator must not add them to
	// the raw control evidence a second time.
	controls := make(map[int64]map[string]float64, len(a.controls))
	for at, values := range a.controls {
		copy := make(map[string]float64, len(values))
		for name, value := range values {
			copy[name] = value
		}
		controls[at] = copy
	}
	addControl := func(at time.Time, key string, count float64) {
		if at.IsZero() {
			return
		}
		bin := analysisBucket(at.Unix(), 5)
		if controls[bin] == nil {
			controls[bin] = make(map[string]float64)
		}
		controls[bin][key] += count
	}
	out := researchAnalysis{OriginMethod: "message-id-preferred-graft-ihave-iwant-estimate-v2", OriginWindowSeconds: originEvidenceWindow.Seconds(), Definition: "v2-corrected-observations-v1", PopulationBasis: "union of subscribed non-publisher nodes at first receipts; dispatch targets only when membership history is absent", Summary: map[string]analysisStatistic{}, Messages: []researchMessage{}, Controls: []researchControlBin{}, ControlBinSeconds: 5, DegreeDistribution: []analysisPoint{}, PropagationCDF: []analysisPoint{}, DuplicateCDF: []analysisPoint{}, HopPDF: []analysisPoint{}, HopCDF: []analysisPoint{}, EagerCDF: []analysisPoint{}, LazyCDF: []analysisPoint{}}
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
	incident := map[string]map[originTopicEdge]struct{}{}
	addPending := func(topic string, edge researchEdge) {
		pending[topic][edge] = true
		key := originTopicEdge{from: edge.from, to: edge.to, topic: topic}
		for _, peer := range []string{edge.from, edge.to} {
			if node := a.peers[peer]; node != "" {
				if incident[node] == nil {
					incident[node] = make(map[originTopicEdge]struct{})
				}
				incident[node][key] = struct{}{}
			}
		}
	}
	removePending := func(topic string, edge researchEdge) {
		delete(pending[topic], edge)
		delete(confirmed[topic], edge)
		key := originTopicEdge{from: edge.from, to: edge.to, topic: topic}
		for _, peer := range []string{edge.from, edge.to} {
			if node := a.peers[peer]; node != "" {
				delete(incident[node], key)
				if len(incident[node]) == 0 {
					delete(incident, node)
				}
			}
		}
	}
	lives := []researchMembershipLife{}
	open := map[string]map[string]int{}
	for _, e := range a.graphEvents {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if e.kind == "join" {
			if open[e.node] == nil {
				open[e.node] = make(map[string]int)
			}
			if _, ok := open[e.node][e.topic]; !ok {
				open[e.node][e.topic] = len(lives)
				lives = append(lives, researchMembershipLife{node: e.node, topic: e.topic, start: e.at})
			}
		}
		if e.kind == "leave" || e.kind == "measurement_stop" || e.kind == "measurement_terminated" {
			for topic, index := range open[e.node] {
				if e.kind != "leave" || topic == e.topic {
					lives[index].end = e.at
					delete(open[e.node], topic)
				}
			}
			if len(open[e.node]) == 0 {
				delete(open, e.node)
			}
			for edge := range incident[e.node] {
				if e.kind == "leave" && edge.topic != e.topic {
					continue
				}
				removePending(edge.topic, researchEdge{edge.from, edge.to})
				removePending(edge.topic, researchEdge{edge.to, edge.from})
			}
		}
		if e.kind != "graft" && e.kind != "prune" {
			continue
		}
		addControl(e.at, "send_"+e.kind, 1)
		if pending[e.topic] == nil {
			pending[e.topic] = map[researchEdge]bool{}
			confirmed[e.topic] = map[researchEdge]bool{}
		}
		edge, reverse := researchEdge{e.peer, e.remote}, researchEdge{e.remote, e.peer}
		if edge.from == "" || edge.to == "" || edge.from == edge.to {
			continue
		}
		if e.kind == "graft" {
			addPending(e.topic, edge)
			if pending[e.topic][reverse] && !confirmed[e.topic][edge] {
				confirmed[e.topic][edge], confirmed[e.topic][reverse] = true, true
				addControl(e.at, "logical_send_graft", 1)
			}
		} else {
			if confirmed[e.topic][edge] {
				addControl(e.at, "logical_send_prune", 1)
			}
			removePending(e.topic, edge)
			removePending(e.topic, reverse)
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
	members := researchMembershipIndex(lives)
	workers = max(1, workers)
	// Keep temporary population/duplicate indexes bounded; completed output
	// messages already belong to the final artifact. Only the caller reduces
	// results and emits progress, in the original sorted-key order.
	batchSize := min(len(keys), max(1, min(128, workers*2)))
	projections := make([]researchMessageProjection, batchSize)
	if len(keys) > 0 {
		reportAnalysisProgress(ctx, fmt.Sprintf("propagation 1/%d", len(keys)), 0)
	}
	for start := 0; start < len(keys); start += batchSize {
		count := min(batchSize, len(keys)-start)
		if err := parallelAnalysis(ctx, count, workers, func(ctx context.Context, index int) error {
			key := keys[start+index]
			projection, err := a.projectMessage(ctx, key, metrics.messages[key], members, origins, out.Summary["node_count"].Average)
			projections[index] = projection
			return err
		}); err != nil {
			return out, err
		}
		for index := range count {
			ki, key := start+index, keys[start+index]
			if err := ctx.Err(); err != nil {
				return out, err
			}
			if ki > 0 && ki%100 == 0 {
				reportAnalysisProgress(ctx, fmt.Sprintf("propagation %d/%d", ki+1, len(keys)), 0)
			}
			projection := projections[index]
			projections[index] = researchMessageProjection{}
			if !projection.published {
				out.OrphanReceipts += projection.orphanReceipts
				continue
			}
			item := projection.item
			message := metrics.messages[key]
			for _, node := range item.Nodes {
				if node.Hop == nil {
					out.UnresolvedParents++
				} else {
					hopCounts[float64(*node.Hop)]++
					hopTotal++
				}
				switch node.Source {
				case "eager":
					addControl(message.deliveries[node.ID].timestamp, "eager_count", 1)
					if node.Seconds != nil {
						eagerCounts[*node.Seconds]++
						eagerTotal++
					}
				case "lazy":
					addControl(message.deliveries[node.ID].timestamp, "lazy_count", 1)
					if node.Seconds != nil {
						lazyCounts[*node.Seconds]++
						lazyTotal++
					}
				default:
					out.UnknownOrigins++
				}
				if projection.population[node.ID] && node.Seconds != nil {
					arrivalCounts[*node.Seconds]++
				}
			}
			for _, duplicate := range projection.eligibleDuplicates {
				duplicateCounts[duplicate.X] += duplicate.Y
			}
			totalPopulation += float64(len(projection.population))
			for key, stat := range item.Metrics {
				if stat.Average != nil {
					samples[key] = append(samples[key], *stat.Average)
				}
			}
			out.Messages = append(out.Messages, item)
		}
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
	for len(controls) > 360 {
		out.ControlBinSeconds *= 2
		merged := map[int64]map[string]float64{}
		for at, values := range controls {
			key := analysisBucket(at, out.ControlBinSeconds)
			if merged[key] == nil {
				merged[key] = map[string]float64{}
			}
			for name, value := range values {
				merged[key][name] += value
			}
		}
		controls = merged
	}
	totals := map[string]float64{}
	for at, values := range controls {
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
