package controller

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func receiverGroupByName(t *testing.T, groups []researchReceiverGroup, name string) researchReceiverGroup {
	t.Helper()
	for _, group := range groups {
		if group.Group == name {
			return group
		}
	}
	t.Fatalf("missing receiver group %q", name)
	return researchReceiverGroup{}
}

func TestReceiverGroupsLegacySnapshotDeliveryCohortsAndCrossGroupPaths(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	epoch := time.Unix(100, 0).UTC()
	resultFixture(t, server, "run", "completed", epoch)
	event := func(kind, node, remote string, second int) model.TraceEvent {
		return model.TraceEvent{RunID: "run", Type: kind, NodeID: node, PeerID: "peer-" + node, RemotePeerID: "peer-" + remote, Timestamp: epoch.Add(time.Duration(second) * time.Second), Topic: "topic", MessageID: "m", Fields: map[string]any{"clockBasis": "controller-offset-v1"}}
	}
	events := []model.TraceEvent{event("publish", "p", "", 1)}
	events[0].Fields["targetNodeIds"] = []string{"a1", "a2", "b", "c", "u"}
	for _, node := range []string{"p", "a1", "a2", "b", "c", "u"} {
		events = append(events, event("join", node, "", 0))
	}
	events = append(events, event("graft", "p", "a1", 0), event("graft", "a1", "b", 0))
	for i, node := range []string{"a1", "b", "u"} {
		parent := "p"
		if node == "b" {
			parent = "a1"
		}
		delivery := event("deliver", node, parent, 2+i)
		delivery.EventID = "delivery-" + node
		delivery.Fields["payloadEncoding"], delivery.Fields["latencyAvailable"] = "envelope", true
		delivery.LatencyMS = float64(10 + 20*i)
		events = append(events, delivery, delivery) // Telemetry retry is not another receipt.
	}
	for _, id := range []string{"dup1", "dup2"} {
		duplicate := event("duplicate", "a1", "b", 5)
		duplicate.EventID = id
		events = append(events, duplicate, duplicate)
	}
	analysisWriteLines(t, server, "run", "events.jsonl", events)
	observations := make([]analysisObservation, 3001)
	for i := range observations {
		observations[i] = analysisObservation{RunID: "run", At: epoch.Add(time.Duration(i) * time.Second), Groups: []analysisGroup{
			{Group: "", Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: 6}}},
			{Group: "A", Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: 2}}},
			{Group: "B", Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: 1}}},
			{Group: "C", Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: 1}}},
		}}
	}
	// This identity-bearing snapshot is discarded by display downsampling. Its
	// metadata must still survive, without depending on currently running nodes.
	observations[1].Graphs = []analysisGraph{{Protocol: "transport", Nodes: []string{"p", "a1", "a2", "b", "c"}, Groups: []string{"publisher", "A", "A", "B", "C"}}}
	analysisWriteLines(t, server, "run", "observations.jsonl", observations)
	result := analysisRequest(t, New(server.config, nil), "run")
	if len(result.Research.ReceiverGroups) != 4 {
		t.Fatalf("groups: %+v", result.Research.ReceiverGroups)
	}
	a := receiverGroupByName(t, result.Research.ReceiverGroups, "A")
	b := receiverGroupByName(t, result.Research.ReceiverGroups, "B")
	c := receiverGroupByName(t, result.Research.ReceiverGroups, "C")
	u := receiverGroupByName(t, result.Research.ReceiverGroups, "")
	researchClose(t, a.Summary["reachability"].Average, .5)
	researchClose(t, b.Summary["reachability"].Average, 1)
	researchClose(t, c.Summary["reachability"].Average, 0)
	researchClose(t, u.Summary["reachability"].Average, 1)
	researchClose(t, a.Summary["frt"].Average, .01)
	researchClose(t, b.Summary["frt"].Average, .03)
	researchClose(t, a.Summary["drc"].Average, 2)
	researchClose(t, a.Summary["drc_per_node_count"].Average, 1)
	if a.EligiblePopulation != 2 || a.PropagationCDF[0].Y != .5 || a.DuplicateCDF[0].Y != 1 {
		t.Fatalf("incorrect denominators: %+v", a)
	}
	if len(b.HopPDF) != 1 || b.HopPDF[0].X != 2 || b.Overview.OriginCounts["eager"] != 1 {
		t.Fatalf("cross-group parent lost: %+v", b)
	}
	if len(c.PropagationCDF) != 1 || c.PropagationCDF[0].Y != 0 || len(c.LatencyCDF) != 0 || c.Summary["frt"].Average != nil {
		t.Fatalf("zero delivery or missing latency: %+v", c)
	}
	if a.Overview.MessageSeries["frt"][0].X != result.Research.Messages[0].At.Sub(result.Result.StartedAt).Seconds() {
		t.Fatal("group time origin is not run start")
	}
	for i, bin := range result.LatencyHistogram {
		sum := 0.
		for _, group := range result.Research.ReceiverGroups {
			if len(group.LatencyHistogram) == 0 {
				continue
			}
			if group.LatencyHistogram[i].X != bin.X {
				t.Fatal("histogram intervals differ")
			}
			sum += group.LatencyHistogram[i].Y
		}
		if sum != bin.Y {
			t.Fatal("group histograms lost or duplicated receipts")
		}
	}
	before, _ := json.Marshal(result.Research.ReceiverGroups)
	compactBatchAnalysis(&result)
	after, _ := json.Marshal(result.Research.ReceiverGroups)
	if string(before) != string(after) || len(result.Research.Messages) != 0 {
		t.Fatal("batch compaction lost receiver group data")
	}
}

func TestReceiverGroupsUnknownEvidenceZeroDenominatorAndCanceledWork(t *testing.T) {
	accumulator := newRunMetricAccumulator()
	accumulator.research = newResearchAccumulator()
	a := accumulator.research
	a.observeNodeGroup("receiver", "A")
	a.observeNodeGroup("receiver", "B")
	a.observeNodeGroup("receiver", "A")
	if a.nodeGroups["receiver"] != "" {
		t.Fatal("conflicting group guessed")
	}
	a.observeNodeGroup("empty", "C")
	a.observeNodeGroup("empty", "")
	if a.nodeGroups["empty"] != "C" {
		t.Fatal("missing metadata erased known identity")
	}
	key := messageMetricKey{"topic", "m"}
	accumulator.messages[key] = &messageMetric{published: true, publisher: "p", deliveries: map[string]deliveryMetric{"receiver": {}}, duplicates: map[string]int{}}
	research, err := a.finish(context.Background(), accumulator, nil)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := a.receiverGroups(context.Background(), accumulator, research.Messages, nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	unknown := receiverGroupByName(t, groups, "")
	if unknown.Summary["reachability"].Average != nil || unknown.Summary["frt"].Average != nil || len(unknown.PropagationCDF) != 0 || len(unknown.Overview.ReceiversTime) != 0 || len(unknown.HopCDF) != 0 {
		t.Fatalf("missing evidence fabricated: %+v", unknown)
	}
	if unknown.UnknownOrigins != 1 || unknown.UnresolvedParents != 1 {
		t.Fatal("unresolved coverage missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.receiverGroups(ctx, accumulator, research.Messages, nil, nil, time.Time{}); err == nil {
		t.Fatal("canceled work continued")
	}
	reverse := newResearchAccumulator()
	reverse.observeNodeGroup("receiver", "B")
	reverse.observeNodeGroup("receiver", "A")
	reverse.observeNodeGroup("receiver", "B")
	if !reflect.DeepEqual(a.groupConflicts, reverse.groupConflicts) {
		t.Fatal("identity resolution depends on order")
	}
}
