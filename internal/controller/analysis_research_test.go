package controller

import (
	"context"
	"encoding/json"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"math"
	"reflect"
	"testing"
	"time"
)

func researchClose(t *testing.T, got *float64, want float64) {
	t.Helper()
	if got == nil || math.Abs(*got-want) > 1e-8 {
		t.Fatalf("got %v want %g", got, want)
	}
}
func TestResearchGraphGoldenPathAndDisconnected(t *testing.T) {
	raw := analysisGraph{Protocol: "gossipsub", Nodes: []string{"a", "b", "c", "d"}, Edges: [][2]int{{0, 1}, {1, 2}, {2, 3}}}
	result, err := calculateGraph(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]float64{"node_count": 4, "average_degree": 1.5, "average_degree_excluding_leaves": 3, "diameter": 3, "shortest_path_length": 5. / 3, "clustering_coefficient": 0, "betweenness_centrality": 1. / 3, "closeness_centrality": .625, "degree_centrality": .5, "page_rank": .25, "assortativity": -.5, "modularity": 1. / 6, "connected_pair_fraction": 1} {
		t.Run(key, func(t *testing.T) { researchClose(t, result.Values[key], want) })
	}
	norm := 0.
	for _, v := range result.PerNode["eigenvector_centrality"] {
		norm += v * v
	}
	researchClose(t, &norm, 1)
	raw.Nodes = append(raw.Nodes, "isolate")
	disconnected, err := calculateGraph(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	researchClose(t, disconnected.Values["connected_pair_fraction"], .6)
	researchClose(t, disconnected.Values["shortest_path_length"], 5./3)
	if disconnected.PerNode["closeness_centrality"][4] != 0 {
		t.Fatal("isolate has nonzero closeness")
	}
	for i := 0; i < 5; i++ {
		again, _ := calculateGraph(context.Background(), raw)
		if !reflect.DeepEqual(disconnected, again) {
			t.Fatal("graph result depends on map iteration")
		}
	}
	raw.Edges = nil
	empty, err := calculateGraph(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"assortativity", "modularity", "diameter", "eigenvector_centrality", "average_degree_excluding_leaves"} {
		if empty.Values[key] != nil {
			t.Fatalf("undefined %s is not null", key)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := calculateGraph(ctx, raw); err == nil {
		t.Fatal("canceled calculation continued")
	}
	raw.Edges = [][2]int{{0, 90}}
	if _, err := calculateGraph(context.Background(), raw); err == nil {
		t.Fatal("invalid edge accepted")
	}
}
func TestResearchMembershipUnionOrderingOriginsAndRawFRT(t *testing.T) {
	epoch := time.Unix(100, 0).UTC()
	event := func(kind, node, remote string, second int) model.TraceEvent {
		return model.TraceEvent{Type: kind, Timestamp: epoch.Add(time.Duration(second) * time.Second), Topic: "topic", MessageID: "m", NodeID: node, PeerID: "peer-" + node, RemotePeerID: "peer-" + remote, Fields: map[string]any{"clockBasis": "controller-offset-v1"}}
	}
	events := []model.TraceEvent{event("join", "p", "", 0), event("join", "a", "", 0), event("join", "b", "", 0), event("publish", "p", "", 1), event("deliver", "a", "p", 2), event("leave", "b", "", 3), event("join", "c", "", 3), event("deliver", "c", "a", 4), event("duplicate", "a", "c", 5), event("duplicate", "outside", "p", 5)}
	events[3].Fields["targetNodeIds"] = []string{"a"} // Research membership union must not become dispatch targets.
	events = append(events, event("graft", "p", "a", 0))
	ihave := event("send_ihave", "a", "c", 3)
	ihave.Fields["messageIdCount"] = 1
	iwant := event("send_iwant", "c", "a", 3)
	iwant.Timestamp = iwant.Timestamp.Add(100 * time.Millisecond)
	iwant.Fields["messageIdCount"] = 1
	events = append(events, ihave, iwant)
	run := func(reverse bool) researchAnalysis {
		a := newRunMetricAccumulator()
		a.research = newResearchAccumulator()
		for i := range events {
			index := i
			if reverse {
				index = len(events) - 1 - i
			}
			a.observe(events[index])
		}
		result, err := a.research.finish(context.Background(), a, nil)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	got := run(false)
	reverse := run(true)
	if !reflect.DeepEqual(got, reverse) {
		left, _ := json.Marshal(got)
		right, _ := json.Marshal(reverse)
		t.Fatalf("order dependent\n%s\n%s", left, right)
	}
	if len(got.Messages) != 1 {
		t.Fatal(got.Messages)
	}
	m := got.Messages[0]
	if !reflect.DeepEqual(m.Population, []string{"a", "b", "c"}) {
		t.Fatalf("intersection or targets used: %v", m.Population)
	}
	researchClose(t, m.Metrics["reachability"].Average, 2./3)
	researchClose(t, m.Metrics["frt"].Average, 2)
	researchClose(t, m.Metrics["frt"].Median, 2)
	researchClose(t, m.Metrics["eager_count"].Average, 1)
	researchClose(t, m.Metrics["lazy_count"].Average, 1)
	researchClose(t, m.Metrics["eager_reachability"].Average, 1./3)
	if m.Nodes[1].Hop == nil || *m.Nodes[1].Hop != 2 || m.Nodes[1].Source != "lazy" {
		t.Fatalf("path not reconstructed: %+v", m.Nodes)
	}
	if got.PropagationCDF[len(got.PropagationCDF)-1].Y != 2./3 || got.DuplicateCDF[len(got.DuplicateCDF)-1].Y != 1./3 {
		t.Fatal("cumulative numerator is outside denominator cohort")
	}
}
func TestResearchAmbiguousOriginsOrphanAndZeroDenominator(t *testing.T) {
	a := newRunMetricAccumulator()
	a.research = newResearchAccumulator()
	a.messages[messageMetricKey{"t", "m"}] = &messageMetric{published: true, publisher: "p", targets: map[string]struct{}{}, deliveries: map[string]deliveryMetric{"a": {research: &deliveryResearch{peer: "A", from: "P"}, latencyAvailable: true, latencyMS: 10}}, duplicates: map[string]int{}}
	a.messages[messageMetricKey{"t", "orphan"}] = &messageMetric{deliveries: map[string]deliveryMetric{"a": {latencyAvailable: true, latencyMS: 1}}}
	a.research.peers = map[string]string{"A": "a", "P": "p"}

	result, err := a.research.finish(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	researchClose(t, result.Messages[0].Metrics["eager_count"].Average, 0)
	researchClose(t, result.Messages[0].Metrics["lazy_count"].Average, 0)
	researchClose(t, result.Messages[0].Metrics["unknown_count"].Average, 1)
	if result.UnknownOrigins != 1 || result.OrphanReceipts != 1 {
		t.Fatalf("lost missing evidence: %+v", result)
	}
	for _, key := range []string{"reachability"} {
		if result.Messages[0].Metrics[key].Average != nil {
			t.Fatalf("fabricated %s", key)
		}
	}
	if len(result.PropagationCDF) != 0 || len(result.DuplicateCDF) != 0 {
		t.Fatal("zero denominator generated a ratio")
	}
}
func TestResearchPruneClearsPendingGraft(t *testing.T) {
	a := newRunMetricAccumulator()
	a.research = newResearchAccumulator()
	epoch := time.Unix(100, 0)
	for i, e := range []struct{ kind, from, to string }{{"graft", "a", "b"}, {"prune", "a", "b"}, {"graft", "b", "a"}, {"graft", "a", "b"}, {"prune", "b", "a"}} {
		a.observe(model.TraceEvent{Type: e.kind, NodeID: e.from, PeerID: e.from, RemotePeerID: e.to, Topic: "t", Timestamp: epoch.Add(time.Duration(i) * time.Second)})
	}
	result, err := a.research.finish(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	researchClose(t, result.Summary["logical_send_graft"].Average, 1)
	researchClose(t, result.Summary["logical_send_prune"].Average, 1)
}
func TestResearchDegreeFitAndStatistics(t *testing.T) {
	stats := analysisStats([]float64{1, 2, 3, 4, math.NaN(), math.Inf(1)})
	researchClose(t, stats.Average, 2.5)
	researchClose(t, stats.Median, 2.5)
	researchClose(t, stats.Deviation, math.Sqrt(1.25))
	if stats.Count != 4 {
		t.Fatal(stats.Count)
	}
	if analysisStats(nil).Average != nil {
		t.Fatal("empty average fabricated")
	}
	if fitDegreeDistribution(context.Background(), []analysisPoint{{0, 1}}).DF != nil {
		t.Fatal("degenerate fit fabricated")
	}
	points := []analysisPoint{}
	for i := 0; i < 21; i++ {
		x := float64(i)
		points = append(points, analysisPoint{x, math.Exp(-math.Pow((x-10)/3, 2) / 2)})
	}
	fit := fitDegreeDistribution(context.Background(), points)
	if fit.DF == nil || fit.Location == nil || fit.Scale == nil || len(fit.Density) == 0 {
		t.Fatalf("valid fit failed: %+v", fit)
	}
	if math.Abs(*fit.Location-10) > .02 || math.Abs(*fit.Scale-3) > .1 {
		t.Fatalf("fit not near symmetric reference: %+v", fit)
	}
}

func TestResearchCyclicPathsKeepLinkEvidenceButDoNotClassifyWholePath(t *testing.T) {
	a := newRunMetricAccumulator()
	a.research = newResearchAccumulator()
	epoch := time.Unix(100, 0)
	a.messages[messageMetricKey{"topic", "m"}] = &messageMetric{published: true, publisher: "p", publishedAt: epoch, targets: map[string]struct{}{"a": {}, "b": {}}, deliveries: map[string]deliveryMetric{
		"a": {timestamp: epoch.Add(3 * time.Second), research: &deliveryResearch{peer: "A", from: "B", wireID: "ff"}},
		"b": {timestamp: epoch.Add(3 * time.Second), research: &deliveryResearch{peer: "B", from: "A", wireID: "ff"}},
	}}
	a.research.peers = map[string]string{"P": "p", "A": "a", "B": "b"}
	a.research.inference = []originMetadataEvent{
		{at: epoch.Add(time.Second), from: "B", to: "A", topic: "topic", kind: "ihave", ids: []string{"ff"}, detailed: true, complete: true},
		{at: epoch.Add(2 * time.Second), from: "B", to: "A", kind: "iwant", ids: []string{"ff"}, detailed: true, complete: true},
	}
	result, err := a.research.finish(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.UnknownOrigins != 2 || result.UnresolvedParents != 2 {
		t.Fatalf("cycle classified as a complete path: %+v", result)
	}
	if result.Messages[0].Nodes[0].LinkEstimate != "lazy" {
		t.Fatal("useful edge evidence was discarded")
	}
}
