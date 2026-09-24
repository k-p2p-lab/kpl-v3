package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type observedMetric struct {
	value float64
	count uint64
	sum   float64
}

func metricTestKey(name string, labels map[string]string) string {
	parts := make([]string, 0, len(labels))
	for label, value := range labels {
		parts = append(parts, label+"="+strconv.Quote(value))
	}
	sort.Strings(parts)
	return name + "{" + strings.Join(parts, ",") + "}"
}

func gatherTestMetrics(t *testing.T, s *state) map[string]observedMetric {
	t.Helper()
	families, err := s.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]observedMetric)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			observed := observedMetric{}
			switch {
			case metric.Counter != nil:
				observed.value = metric.Counter.GetValue()
			case metric.Gauge != nil:
				observed.value = metric.Gauge.GetValue()
			case metric.Histogram != nil:
				observed.count = metric.Histogram.GetSampleCount()
				observed.sum = metric.Histogram.GetSampleSum()
			}
			result[metricTestKey(family.GetName(), labels)] = observed
		}
	}
	return result
}

func requireMetricValue(t *testing.T, metrics map[string]observedMetric, name string, labels map[string]string, want float64) {
	t.Helper()
	key := metricTestKey(name, labels)
	metric, ok := metrics[key]
	if !ok || metric.value != want {
		t.Fatalf("%s = %v (present %t), want %v", key, metric.value, ok, want)
	}
}

func TestMetricsAccumulateBeyondRecentEventHistory(t *testing.T) {
	s := newState(t.TempDir())
	for _, count := range []int{350, 51} {
		events := make([]model.TraceEvent, count)
		for i := range events {
			events[i] = model.TraceEvent{RunID: "run", NodeID: fmt.Sprintf("node-%d", i), MessageID: fmt.Sprintf("message-%d", i), Type: "publish", Topic: "topic", Fields: map[string]any{"wireBytes": 32, "payloadEncoding": "raw"}}
		}
		if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: events}); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.events) != recentEventLimit {
		t.Fatalf("recent history length = %d", len(s.events))
	}
	metrics := gatherTestMetrics(t, s)
	requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": "run", "agent_id": "agent", "event_type": "publish", "topic": "topic"}, 401)
	requireMetricValue(t, metrics, "kpl_message_bytes_total", map[string]string{"run_id": "run", "agent_id": "agent", "topic": "topic", "direction": "publish", "encoding": "raw"}, 401*32)
	series := 0
	for key := range metrics {
		if strings.HasPrefix(key, "kpl_events_total{") {
			series++
		}
		if strings.Contains(key, "node_id=") || strings.Contains(key, "message_id=") || strings.Contains(key, "error=") {
			t.Fatalf("high cardinality label in %s", key)
		}
	}
	if series != 1 {
		t.Fatalf("event series = %d, want aggregation across all node/message IDs", series)
	}
	for key := range gatherTestMetrics(t, newState(t.TempDir())) {
		if strings.HasPrefix(key, "kpl_events_total{") {
			t.Fatal("new Controller inherited another registry's counters")
		}
	}
}

func TestMetricsHeartbeatInitializesSharedZeroSeries(t *testing.T) {
	s := newState(t.TempDir())
	if _, err := s.registerAgent(model.Agent{ID: "agent", URL: "http://agent"}); err != nil {
		t.Fatal(err)
	}
	heartbeat := model.AgentHeartbeat{Agent: model.Agent{ID: "agent"}, Nodes: []model.Node{
		{ID: "node-1", RunID: "run", State: model.NodeReady, Metadata: map[string]string{"topicsJSON": `["topic","topic"]`}},
		{ID: "node-2", RunID: "run", State: model.NodeReady, Metadata: map[string]string{"topicsJSON": `["topic"]`}},
	}}
	if err := s.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	metrics := gatherTestMetrics(t, s)
	labels := map[string]string{"run_id": "run", "agent_id": "agent", "event_type": "publish", "topic": "topic"}
	requireMetricValue(t, metrics, "kpl_events_total", labels, 0)
	requireMetricValue(t, metrics, "kpl_operation_failures_total", map[string]string{"run_id": "run", "agent_id": "agent", "action": "leave"}, 0)
	requireMetricValue(t, metrics, "kpl_telemetry_dropped_events_total", map[string]string{"run_id": "run", "agent_id": "agent"}, 0)
	for _, metric := range []string{"kpl_gossipsub_control_rpcs_total", "kpl_gossipsub_control_entries_total", "kpl_gossipsub_control_message_ids_total", "kpl_gossipsub_control_peer_exchange_records_total"} {
		requireMetricValue(t, metrics, metric, map[string]string{"run_id": "run", "agent_id": "agent", "direction": "send", "control_type": "ihave"}, 0)
	}
	series := 0
	for key := range metrics {
		if strings.HasPrefix(key, "kpl_events_total{") {
			series++
		}
	}
	if series != 9 {
		t.Fatalf("initialized event series = %d, want 9 shared series", series)
	}
	if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: []model.TraceEvent{{RunID: "run", Type: "publish", Topic: "topic"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	requireMetricValue(t, gatherTestMetrics(t, s), "kpl_events_total", labels, 1)
}

func TestMetricsAggregateGossipSubControlRPCsAndLogicalIDs(t *testing.T) {
	s := newState(t.TempDir())
	events := []model.TraceEvent{
		{RunID: "run", NodeID: "one", Type: "send_ihave", Fields: map[string]any{"controlEntries": 2, "messageIdCount": 7}},
		{RunID: "run", NodeID: "two", Type: "send_ihave", Fields: map[string]any{"controlEntries": json.Number("1"), "messageIdCount": float64(3)}},
		{RunID: "run", NodeID: "three", Type: "recv_iwant", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 2}},
		{RunID: "run", NodeID: "four", Type: "drop_idontwant", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 4}},
		{RunID: "run", NodeID: "five", Type: "recv_prune", Fields: map[string]any{"controlEntries": 1, "peerExchangeCount": 3}},
	}
	if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: events}); err != nil {
		t.Fatal(err)
	}
	metrics := gatherTestMetrics(t, s)
	labels := map[string]string{"run_id": "run", "agent_id": "agent", "direction": "send", "control_type": "ihave"}
	requireMetricValue(t, metrics, "kpl_gossipsub_control_rpcs_total", labels, 2)
	requireMetricValue(t, metrics, "kpl_gossipsub_control_entries_total", labels, 3)
	requireMetricValue(t, metrics, "kpl_gossipsub_control_message_ids_total", labels, 10)
	requireMetricValue(t, metrics, "kpl_gossipsub_control_rpcs_total", map[string]string{"run_id": "run", "agent_id": "agent", "direction": "recv", "control_type": "iwant"}, 1)
	requireMetricValue(t, metrics, "kpl_gossipsub_control_message_ids_total", map[string]string{"run_id": "run", "agent_id": "agent", "direction": "drop", "control_type": "idontwant"}, 4)
	requireMetricValue(t, metrics, "kpl_gossipsub_control_peer_exchange_records_total", map[string]string{"run_id": "run", "agent_id": "agent", "direction": "recv", "control_type": "prune"}, 3)
	for key := range metrics {
		if strings.HasPrefix(key, "kpl_gossipsub_control_") && (strings.Contains(key, "node_id=") || strings.Contains(key, "remote_peer_id=") || strings.Contains(key, "message_id=") || strings.Contains(key, "topic=")) {
			t.Fatalf("high-cardinality control label in %s", key)
		}
	}
	for _, invalid := range []any{-1, 1.5, math.NaN(), math.Inf(1), "2"} {
		if _, ok := nonnegativeMetricCount(invalid); ok {
			t.Fatalf("accepted invalid control count %v", invalid)
		}
	}
}

func TestMetricsHeartbeatInitializesConnectionEventsWithoutResetting(t *testing.T) {
	s := newState(t.TempDir())
	if _, err := s.registerAgent(model.Agent{ID: "agent", URL: "http://agent"}); err != nil {
		t.Fatal(err)
	}
	heartbeat := model.AgentHeartbeat{Agent: model.Agent{ID: "agent"}, Nodes: []model.Node{
		{ID: "node-1", RunID: "run", State: model.NodeReady},
		{ID: "node-2", RunID: "run", State: model.NodeReady},
	}}
	if err := s.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	baseline := gatherTestMetrics(t, s)
	for _, eventType := range []string{"add_peer", "remove_peer"} {
		labels := map[string]string{"run_id": "run", "agent_id": "agent", "event_type": eventType, "topic": ""}
		requireMetricValue(t, baseline, "kpl_events_total", labels, 0)
		for _, count := range []int{1, 2} {
			events := make([]model.TraceEvent, count)
			for i := range events {
				events[i] = model.TraceEvent{RunID: "run", Type: eventType, NodeID: fmt.Sprintf("node-%d", i+1)}
			}
			if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: events}); err != nil {
				t.Fatal(err)
			}
			if err := s.heartbeat(heartbeat); err != nil {
				t.Fatal(err)
			}
		}
		requireMetricValue(t, gatherTestMetrics(t, s), "kpl_events_total", labels, 3)
	}
	// An event may precede the first heartbeat; initializing its already
	// existing series must also preserve the accumulated value.
	if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: []model.TraceEvent{{RunID: "early-run", Type: "remove_peer"}}}); err != nil {
		t.Fatal(err)
	}
	heartbeat.Nodes = []model.Node{{ID: "early-node", RunID: "early-run", State: model.NodeReady}}
	if err := s.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	requireMetricValue(t, gatherTestMetrics(t, s), "kpl_events_total", map[string]string{"run_id": "early-run", "agent_id": "agent", "event_type": "remove_peer", "topic": ""}, 1)
}

func TestMetricsBytesLatencyFailuresAndDrops(t *testing.T) {
	s := newState(t.TempDir())
	events := []model.TraceEvent{
		{Type: "deliver", LatencyMS: 25, Fields: map[string]any{"payloadEncoding": "envelope", "wireBytes": json.Number("100"), "latencyAvailable": true}},
		{Type: "deliver", LatencyMS: 0, Fields: map[string]any{"payloadEncoding": "envelope", "wireBytes": float64(100), "latencyAvailable": true}},
		{Type: "deliver", LatencyMS: 999, Fields: map[string]any{"payloadEncoding": "raw", "wireBytes": uint64(32), "latencyAvailable": true}},
		{Type: "deliver", LatencyMS: -1, Fields: map[string]any{"payloadEncoding": "envelope", "wireBytes": 100, "latencyAvailable": true}},
		{Type: "deliver", LatencyMS: 99, Fields: map[string]any{"payloadEncoding": "envelope", "latencyAvailable": false}},
		{Type: "deliver", LatencyMS: 99, Fields: map[string]any{"wireBytes": -1}},
		{Type: "phase-operation-failed", Fields: map[string]any{"action": "publish", "error": "arbitrary failure details"}},
		{Type: "telemetry_drop", Fields: map[string]any{"count": uint64(7)}},
		{Type: "telemetry_drop", Fields: map[string]any{"count": json.Number("3")}},
		{Type: "telemetry_drop", Fields: map[string]any{"count": -20}},
	}
	for i := range events {
		events[i].RunID, events[i].Topic = "run", "topic"
		events[i].NodeID, events[i].MessageID = fmt.Sprintf("receiver-%d", i), "message"
	}
	events = append(events, model.TraceEvent{RunID: "run", NodeID: "publisher", Type: "publish", Topic: "topic", MessageID: "message", Fields: map[string]any{"targetNodeIds": []string{"receiver-0", "receiver-1", "receiver-2", "receiver-3", "receiver-4", "receiver-5"}}})
	if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: events}); err != nil {
		t.Fatal(err)
	}
	metrics := gatherTestMetrics(t, s)
	latency := metrics[metricTestKey("kpl_propagation_latency_seconds", map[string]string{"run_id": "run", "agent_id": "agent", "topic": "topic"})]
	if latency.count != 2 || math.Abs(latency.sum-0.025) > 1e-10 {
		t.Fatalf("latency histogram = %+v, want 2 envelope samples summing to 0.025s", latency)
	}
	requireMetricValue(t, metrics, "kpl_message_bytes_total", map[string]string{"run_id": "run", "agent_id": "agent", "topic": "topic", "direction": "deliver", "encoding": "envelope"}, 300)
	requireMetricValue(t, metrics, "kpl_message_bytes_total", map[string]string{"run_id": "run", "agent_id": "agent", "topic": "topic", "direction": "deliver", "encoding": "raw"}, 32)
	requireMetricValue(t, metrics, "kpl_operation_failures_total", map[string]string{"run_id": "run", "agent_id": "agent", "action": "publish"}, 1)
	requireMetricValue(t, metrics, "kpl_telemetry_dropped_events_total", map[string]string{"run_id": "run", "agent_id": "agent"}, 10)
	for _, invalid := range []any{math.NaN(), math.Inf(1), math.Inf(-1), -1, "12", json.Number("invalid")} {
		if number, ok := nonnegativeMetricNumber(invalid); ok {
			t.Fatalf("accepted invalid metric number %v as %v", invalid, number)
		}
	}
}

func TestMetricsCurrentStateAggregationAndTransitions(t *testing.T) {
	s := newState(t.TempDir())
	seen := time.Now().UTC().Truncate(time.Second)
	s.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline, Capacity: 8, ActiveNodes: 2, LastSeen: seen}
	for _, id := range []string{"one", "two"} {
		s.nodes[id] = model.Node{ID: id, RunID: "run", AgentID: "agent", Group: "workers", Role: "worker", Type: "full", State: model.NodeReady}
	}
	s.experiments["run"] = model.Experiment{ID: "run", Name: "experiment", State: "running", Phase: 3, ActiveJobs: 2, CompletedJobs: 4, FailedJobs: 1, CanceledJobs: 3}
	metrics := gatherTestMetrics(t, s)
	agentLabels := map[string]string{"agent_id": "agent"}
	requireMetricValue(t, metrics, "kpl_agent_up", agentLabels, 1)
	requireMetricValue(t, metrics, "kpl_agent_capacity", agentLabels, 8)
	requireMetricValue(t, metrics, "kpl_agent_active_nodes", agentLabels, 2)
	requireMetricValue(t, metrics, "kpl_agent_last_seen_timestamp_seconds", agentLabels, float64(seen.Unix()))
	nodeLabels := map[string]string{"run_id": "run", "agent_id": "agent", "group": "workers", "role": "worker", "node_type": "full", "state": "ready"}
	requireMetricValue(t, metrics, "kpl_nodes", nodeLabels, 2)
	requireMetricValue(t, metrics, "kpl_experiment_info", map[string]string{"run_id": "run", "name": "experiment", "state": "running"}, 1)
	requireMetricValue(t, metrics, "kpl_experiment_phase", map[string]string{"run_id": "run"}, 3)
	for state, count := range map[string]float64{"active": 2, "completed": 4, "failed": 1, "canceled": 3} {
		requireMetricValue(t, metrics, "kpl_experiment_jobs", map[string]string{"run_id": "run", "state": state}, count)
	}
	s.mu.Lock()
	agent := s.agents["agent"]
	agent.State = model.AgentOffline
	s.agents["agent"] = agent
	for id, node := range s.nodes {
		node.State = model.NodeStopped
		s.nodes[id] = node
	}
	experiment := s.experiments["run"]
	experiment.State = "completed"
	s.experiments["run"] = experiment
	s.mu.Unlock()
	metrics = gatherTestMetrics(t, s)
	requireMetricValue(t, metrics, "kpl_agent_up", agentLabels, 0)
	if _, ok := metrics[metricTestKey("kpl_nodes", nodeLabels)]; ok {
		t.Fatal("stale ready-node series remained after transition")
	}
	nodeLabels["state"] = "stopped"
	requireMetricValue(t, metrics, "kpl_nodes", nodeLabels, 2)
	if _, ok := metrics[metricTestKey("kpl_experiment_info", map[string]string{"run_id": "run", "name": "experiment", "state": "running"})]; ok {
		t.Fatal("stale experiment state remained after transition")
	}
}

func TestMetricsEndpointEscapesLabelsAndIncludesRuntime(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), Token: "token"}, nil)
	topic := "topic\"\\\nUnicode-\u2603"
	event := model.TraceEvent{RunID: "run", AgentID: "agent", Type: "publish", Topic: topic}
	if err := server.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("scrape status = %d, content type = %q, body = %s", recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "topic="+strconv.Quote(topic)) || !strings.Contains(body, "go_goroutines ") || !strings.Contains(body, "process_start_time_seconds ") {
		t.Fatalf("missing escaped label or runtime collectors in scrape: %s", body)
	}
	requireMetricValue(t, gatherTestMetrics(t, server.state), "kpl_events_total", map[string]string{"run_id": "run", "agent_id": "agent", "event_type": "publish", "topic": topic}, 1)
}

func TestMetricsConcurrentScrapesAndIngestion(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := server.state.registerAgent(model.Agent{ID: "agent", URL: "http://agent", Capacity: 100}); err != nil {
		t.Fatal(err)
	}
	handler := server.apiTestHandler(context.Background())
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 20; i++ {
				if err := server.state.appendEvents(model.EventBatch{AgentID: "agent", Events: []model.TraceEvent{{RunID: "run", Type: "deliver", Topic: "topic"}}}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for i := 0; i < 20; i++ {
			if err := server.state.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "agent"}, Nodes: []model.Node{{ID: "node", RunID: "run", State: model.NodeReady}}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for scraper := 0; scraper < 3; scraper++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 10; i++ {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				if recorder.Code != http.StatusOK {
					t.Errorf("concurrent scrape failed: %d %s", recorder.Code, recorder.Body.String())
					return
				}
			}
		}()
	}
	workers.Wait()
	requireMetricValue(t, gatherTestMetrics(t, server.state), "kpl_events_total", map[string]string{"run_id": "run", "agent_id": "agent", "event_type": "deliver", "topic": "topic"}, 80)
}
