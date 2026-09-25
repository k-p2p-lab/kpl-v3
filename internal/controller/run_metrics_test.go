package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func cohortPublish(id, topic string, targets []string) model.TraceEvent {
	return model.TraceEvent{RunID: "run", EventID: "publish-" + id + topic, NodeID: "sender", Type: "publish", MessageID: id, Topic: topic, Timestamp: time.Unix(10, 0), Fields: map[string]any{"targetNodeIds": targets}}
}
func cohortDelivery(id, topic, node string, milliseconds float64) model.TraceEvent {
	return model.TraceEvent{RunID: "run", EventID: "deliver-" + id + topic + node, AgentID: "agent", NodeID: node, Type: "deliver", MessageID: id, Topic: topic, Timestamp: time.Unix(10, int64(milliseconds*1e6)), LatencyMS: milliseconds, Fields: map[string]any{"payloadEncoding": "envelope", "latencyAvailable": true}}
}
func cohortDuplicate(id, node, eventID string) model.TraceEvent {
	return model.TraceEvent{RunID: "run", EventID: eventID, NodeID: node, Type: "duplicate", MessageID: id, Topic: "topic", Timestamp: time.Unix(11, 0)}
}

func TestRunMetricsPreserveGossipSubControlBreakdownAcrossExportRebuild(t *testing.T) {
	events := []model.TraceEvent{
		{RunID: "run", NodeID: "one", EventID: "one", Type: "send_ihave", Fields: map[string]any{"controlEntries": 2, "messageIdCount": 8}},
		{RunID: "run", NodeID: "two", EventID: "two", Type: "send_ihave", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 3}},
		{RunID: "run", NodeID: "one", EventID: "three", Type: "recv_iwant", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 2}},
		{RunID: "run", NodeID: "one", EventID: "four", Type: "drop_prune", Fields: map[string]any{"controlEntries": 2, "messageIdCount": 0, "peerExchangeCount": 4}},
		{RunID: "run", AgentID: "agent-b", NodeID: "five", EventID: "five", Type: "send_ihave", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 5}},
	}
	accumulator := newRunMetricAccumulator()
	var log bytes.Buffer
	for _, event := range events {
		accumulator.observe(event)
		accumulator.observe(event)
		if err := json.NewEncoder(&log).Encode(event); err != nil {
			t.Fatal(err)
		}
	}
	live, _ := accumulator.summarize("run")
	rebuilt, err := summarizeRunEvents("run", &log)
	if err != nil {
		t.Fatal(err)
	}
	want := []model.GossipSubControlMetric{
		{Direction: "send", ControlType: "ihave", RPCs: 2, Entries: 3, MessageIDs: 11},
		{Direction: "recv", ControlType: "iwant", RPCs: 1, Entries: 1, MessageIDs: 2},
		{Direction: "drop", ControlType: "prune", RPCs: 1, Entries: 2, MessageIDs: 0, PeerExchangeRecords: 4},
		{AgentID: "agent-b", Direction: "send", ControlType: "ihave", RPCs: 1, Entries: 1, MessageIDs: 5},
	}
	if !reflect.DeepEqual(live.GossipSubControl, want) || !reflect.DeepEqual(rebuilt.GossipSubControl, want) {
		t.Fatalf("control metrics live=%+v rebuilt=%+v", live.GossipSubControl, rebuilt.GossipSubControl)
	}
}

func TestChurnMetricsFreezeCohortAndUseFirstRemoteDelivery(t *testing.T) {
	a := newRunMetricAccumulator()
	// Delivery/duplicate batches arrive before the publication batch.
	events := []model.TraceEvent{
		cohortDuplicate("m1", "b", "dup1"), cohortDuplicate("m1", "b", "dup2"),
		cohortDuplicate("m1", "departed", "dup3"), cohortDuplicate("m1", "late", "dup4"), cohortDuplicate("m1", "sender", "dup5"),
		cohortDelivery("m1", "topic", "b", 50), cohortDelivery("m1", "topic", "late", 1), cohortDelivery("m1", "topic", "sender", 0),
		cohortPublish("m1", "topic", []string{"sender", "b", "b", "departed"}),
		cohortPublish("m2", "topic", []string{"b"}), cohortDelivery("m2", "topic", "b", 20),
		cohortPublish("empty", "topic", []string{}),
	}
	for _, event := range events {
		a.observe(event)
		a.observe(event)
	}
	// An earlier valid first receipt received late replaces the latency sample.
	early := cohortDelivery("m1", "topic", "b", 10)
	early.EventID = "earlier-delivery"
	a.observe(early)
	result, _ := a.summarize("run")
	if result.Published != 3 || result.ExpectedDeliveries != 3 || result.EligibleDeliveries != 2 || math.Abs(result.Reachability-2.0/3) > 1e-12 {
		t.Fatalf("wrong churn denominator: %+v", result)
	}
	if result.LatencySamples != 2 || result.AverageLatencyMS != 15 || result.P95LatencyMS != 20 {
		t.Fatalf("wrong first-remote latency: %+v", result)
	}
	if result.Duplicates != 5 || result.EligibleDuplicates != 2 || result.DuplicateSamples != 2 || result.AverageDuplicates != 1 {
		t.Fatalf("wrong duplicate overhead: %+v", result)
	}
	// Node inventory changes cannot rewrite an earlier publication's cohort.
	nodes := []model.Node{{ID: "b", State: model.NodeStopped}, {ID: "departed", State: model.NodeStopped}, {ID: "late", State: model.NodeReady}}
	metrics := calculateMetrics(nodes, []model.Experiment{{ID: "run"}}, events)
	if metrics.ExpectedDeliveries != 3 || metrics.EligibleDeliveries != 2 {
		t.Fatalf("topology changed denominator: %+v", metrics)
	}
}

func TestMetricsUnknownEmptyRawAndTopicIsolation(t *testing.T) {
	a := newRunMetricAccumulator()
	a.observe(cohortPublish("legacy", "topic", nil))
	a.observe(cohortDelivery("legacy", "topic", "b", 99))
	a.observe(cohortPublish("empty", "topic", []string{}))
	result, _ := a.summarize("run")
	if result.DeliveryRatioAvailable || result.LatencySamples != 0 || result.DuplicateSamples != 0 || result.UnscopedPublications != 1 {
		t.Fatalf("fabricated legacy/empty metrics: %+v", result)
	}
	a.observe(cohortPublish("same-id", "one", []string{"b"}))
	a.observe(cohortPublish("same-id", "two", []string{"b"}))
	raw := cohortDelivery("same-id", "one", "b", -1)
	raw.Fields = map[string]any{"payloadEncoding": "raw", "latencyAvailable": false}
	a.observe(raw)
	a.observe(cohortDelivery("same-id", "two", "b", -5))
	result, _ = a.summarize("run")
	if result.Reachability != 1 || result.ExpectedDeliveries != 2 || result.LatencySamples != 0 || result.InvalidLatencySamples != 1 {
		t.Fatalf("raw/skew/topic separation incorrect: %+v", result)
	}
}

func TestWholeRunMetricsDedupPersistenceAndArchiveRebuild(t *testing.T) {
	s := newState(t.TempDir())
	s.experiments["run"] = model.Experiment{ID: "run", State: "running", StartedAt: time.Now()}
	s.experiments["queued"] = model.Experiment{ID: "queued", State: "queued"}
	targets := make([]string, 500)
	for i := range targets {
		targets[i] = fmt.Sprintf("receiver-%d", i)
	}
	events := []model.TraceEvent{cohortPublish("message", "topic", targets)}
	for _, receiver := range targets {
		events = append(events, cohortDelivery("message", "topic", receiver, 20))
	}
	events = append(events, cohortDuplicate("message", targets[0], "dup1"))
	batch := model.EventBatch{Events: events}
	if err := s.appendEvents(batch); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEvents(batch); err != nil {
		t.Fatal(err)
	}
	snapshot := s.snapshot()
	if len(snapshot.Events) != 300 || snapshot.Metrics.RunID != "run" || snapshot.Metrics.Published != 1 || snapshot.Metrics.EligibleDeliveries != 500 || snapshot.Metrics.LatencySamples != 500 {
		t.Fatalf("whole-run history lost: %+v", snapshot.Metrics)
	}
	data, err := os.ReadFile(filepath.Join(s.dataDir, currentRunsDirectory, "run", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(data, []byte{'\n'}) != len(events) {
		t.Fatal("batch retry duplicated persisted events")
	}
	rebuilt, err := summarizeRunEvents("run", bytes.NewReader(data))
	if err != nil || !reflect.DeepEqual(snapshot.Metrics, rebuilt) {
		t.Fatalf("archive rebuild differs: %+v %v", rebuilt, err)
	}
	metrics := gatherTestMetrics(t, s)
	requireMetricValue(t, metrics, "kpl_delivery_ratio", map[string]string{"run_id": "run"}, 1)
	requireMetricValue(t, metrics, "kpl_delivery_duplicates_per_reached_pair", map[string]string{"run_id": "run"}, 1.0/500)
	latency := metrics[metricTestKey("kpl_propagation_latency_seconds", map[string]string{"run_id": "run", "agent_id": "agent", "topic": "topic"})]
	if latency.count != 500 || math.Abs(latency.sum-10) > 1e-9 {
		t.Fatalf("Prometheus differs: %+v", latency)
	}
}

func TestFailedPersistenceDoesNotCountOrPartiallyWriteBatch(t *testing.T) {
	s := newState(t.TempDir())
	event := cohortPublish("message", "topic", []string{"b"})
	invalid := cohortDelivery("message", "topic", "b", math.NaN())
	if err := s.appendEvents(model.EventBatch{Events: []model.TraceEvent{event, invalid}}); err == nil {
		t.Fatal("accepted unencodable event")
	}
	if len(s.events) != 0 || len(s.runMetrics) != 0 {
		t.Fatal("failed persistence affected live metrics")
	}
	if err := s.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	result, _ := s.runMetrics["run"].summarize("run")
	if result.Published != 1 {
		t.Fatal(result)
	}
}

func TestCapturePublishCohortExcludesSenderStaleAgentsAndOtherTopics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.state.agents["online"] = model.Agent{State: model.AgentOnline, LastSeen: time.Now()}
	s.state.agents["stale"] = model.Agent{State: model.AgentOnline, LastSeen: time.Now().Add(-time.Minute)}
	for _, node := range []model.Node{
		{ID: "sender", AgentID: "online", RunID: "run", Type: "full", State: model.NodeReady},
		{ID: "receiver", AgentID: "online", RunID: "run", Type: "full", State: model.NodeReady},
		{ID: "stopped", AgentID: "online", RunID: "run", Type: "full", State: model.NodeStopped},
		{ID: "stale", AgentID: "stale", RunID: "run", Type: "full", State: model.NodeReady},
		{ID: "other-topic", AgentID: "online", RunID: "run", Type: "full", State: model.NodeReady, Metadata: map[string]string{"topics": "other"}},
	} {
		s.state.nodes[node.ID] = node
	}
	request := model.PublishRequest{RunID: "run", Topic: "kpl/default"}
	if !s.capturePublishCohort(&request, s.state.nodes["sender"], []string{"kpl/default"}) || !reflect.DeepEqual(request.TargetNodeIDs, []string{"receiver"}) || request.CohortCapturedAt.IsZero() {
		t.Fatalf("incorrect cohort: %+v", request)
	}
	request.Topic = "empty"
	s.capturePublishCohort(&request, s.state.nodes["sender"], []string{"empty"})
	data, _ := json.Marshal(request)
	var roundTrip model.PublishRequest
	if err := json.Unmarshal(data, &roundTrip); err != nil || roundTrip.TargetNodeIDs == nil || len(roundTrip.TargetNodeIDs) != 0 {
		t.Fatalf("empty cohort lost during RPC: %s, %v", data, err)
	}
}
