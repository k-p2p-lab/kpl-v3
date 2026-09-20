package controller

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestDashboardRESTSnapshotExposesAgentsNetworkAndJobs(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	server.state.mu.Lock()
	server.state.agents["agent-a"] = model.Agent{
		ID: "agent-a", URL: "http://agent-a", Capacity: 10, ActiveNodes: 1,
		State: model.AgentOnline, LastSeen: now,
	}
	server.state.nodes["node-a"] = model.Node{
		ID: "node-a", RunID: "run-a", AgentID: "agent-a", Group: "workers",
		Role: "worker", Type: "full", PeerID: "peer-a", State: model.NodeReady,
		PeerScores: map[string]float64{"peer-b": 1.5}, LastSeen: now,
	}
	server.state.experiments["run-a"] = model.Experiment{
		ID: "run-a", Name: "rest-view", State: "running", ActiveJobs: 1,
		CompletedJobs: 2, FailedJobs: 3, CanceledJobs: 4, StartedAt: now,
	}
	server.state.mu.Unlock()

	handler := server.apiTestHandler(context.Background())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/snapshot", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot model.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Agents) != 1 || len(snapshot.Nodes) != 1 || len(snapshot.Experiments) != 1 {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
	}
	if snapshot.Nodes[0].PeerScores["peer-b"] != 1.5 {
		t.Fatalf("peer scores missing from REST snapshot: %+v", snapshot.Nodes[0].PeerScores)
	}
	run := snapshot.Experiments[0]
	if run.ActiveJobs != 1 || run.CompletedJobs != 2 || run.FailedJobs != 3 || run.CanceledJobs != 4 {
		t.Fatalf("job counters missing from REST snapshot: %+v", run)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "K-P2PLab") {
		t.Fatalf("dashboard response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestUIConfigExposesMonitoringPorts(t *testing.T) {
	server := New(ServerConfig{
		DataDir:        t.TempDir(),
		PrometheusPort: 19090,
		GrafanaPort:    13000,
	}, nil)
	handler := server.apiTestHandler(context.Background())

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/ui-config", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ui config status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if cacheControl := recorder.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("ui config Cache-Control=%q, want no-store", cacheControl)
	}
	var config struct {
		PrometheusPort int `json:"prometheusPort"`
		GrafanaPort    int `json:"grafanaPort"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if config.PrometheusPort != 19090 || config.GrafanaPort != 13000 {
		t.Fatalf("ui config=%+v", config)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/ui-config", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST ui config status=%d, want 405", recorder.Code)
	}
}

func TestUIConfigUsesDefaultMonitoringPorts(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	recorder := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/ui-config", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ui config status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var config map[string]int
	if err := json.Unmarshal(recorder.Body.Bytes(), &config); err != nil {
		t.Fatal(err)
	}
	if config["prometheusPort"] != 9090 || config["grafanaPort"] != 3000 {
		t.Fatalf("default ui config=%+v", config)
	}
}

func TestPrometheusAgentTargetsExposeOnlyValidOnlineAgentMetricsEndpoints(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	agents := []model.Agent{
		{ID: "agent-b", Name: "Worker B", MetricsURL: "https://[2001:db8::7]/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-a", Name: "Worker A", MetricsURL: "http://10.20.0.8:18090/metrics", State: model.AgentOnline, LastSeen: now, Labels: map[string]string{"swarmNodeId": "node-a"}},
		{ID: "agent-stale", Name: "Stale", MetricsURL: "http://10.20.0.9:18090/metrics", State: model.AgentOnline, LastSeen: now.Add(-agentStaleAfter - time.Second)},
		{ID: "agent-offline", Name: "Offline", MetricsURL: "http://10.20.0.10:18090/metrics", State: model.AgentOffline, LastSeen: now},
		{ID: "agent-bad-scheme", MetricsURL: "file://10.20.0.11/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-credentials", MetricsURL: "http://user:secret@10.20.0.12:18090/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-query", MetricsURL: "http://10.20.0.13:18090/metrics?token=secret", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-fragment", MetricsURL: "http://10.20.0.14:18090/metrics#section", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-empty-fragment", MetricsURL: "http://10.20.0.14:18090/metrics#", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-wrong-path", MetricsURL: "http://10.20.0.15:18090/status", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-encoded-path", MetricsURL: "http://10.20.0.16:18090/%6detrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-port", MetricsURL: "http://10.20.0.17:65536/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-whitespace", MetricsURL: " http://10.20.0.18:18090/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-localhost", MetricsURL: "http://localhost:18090/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-loopback", MetricsURL: "http://127.0.0.1:18090/metrics", State: model.AgentOnline, LastSeen: now},
		{ID: "agent-unspecified", MetricsURL: "http://[::]:18090/metrics", State: model.AgentOnline, LastSeen: now},
	}
	server.state.mu.Lock()
	for _, agent := range agents {
		server.state.agents[agent.ID] = agent
	}
	server.state.mu.Unlock()

	handler := server.apiTestHandler(context.Background())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/prometheus/agent-targets", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("agent targets status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("agent targets Cache-Control=%q, want no-store", recorder.Header().Get("Cache-Control"))
	}
	var groups []prometheusTargetGroup
	if err := json.Unmarshal(recorder.Body.Bytes(), &groups); err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("agent targets=%+v, want two valid online Agents", groups)
	}
	if len(groups[0].Targets) != 1 || groups[0].Targets[0] != "10.20.0.8:18090" {
		t.Fatalf("first target=%+v, want sorted agent-a target", groups[0])
	}
	wantFirstLabels := map[string]string{"agent_id": "agent-a", "agent_name": "Worker A", "swarm_node_id": "node-a"}
	if !maps.Equal(groups[0].Labels, wantFirstLabels) {
		t.Fatalf("first labels=%v, want %v", groups[0].Labels, wantFirstLabels)
	}
	if len(groups[1].Targets) != 1 || groups[1].Targets[0] != "[2001:db8::7]:443" {
		t.Fatalf("second target=%+v, want IPv6 HTTPS target", groups[1])
	}
	wantSecondLabels := map[string]string{"agent_id": "agent-b", "agent_name": "Worker B", "__scheme__": "https"}
	if !maps.Equal(groups[1].Labels, wantSecondLabels) {
		t.Fatalf("second labels=%v, want %v", groups[1].Labels, wantSecondLabels)
	}

	for _, method := range []string{http.MethodHead, http.MethodPost} {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(method, "/api/v1/prometheus/agent-targets", nil))
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s agent targets status=%d, want 405", method, recorder.Code)
		}
	}
}

func TestBootstrapRegistryRequiresRunAndReturnsOnlyReadyBootNodesFromThatRun(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.mu.Lock()
	server.state.nodes["run-a-boot"] = model.Node{
		ID: "run-a-boot", RunID: "run-a", Role: "boot", State: model.NodeReady,
		PeerID: "peer-a", Addresses: []string{"/ip4/127.0.0.1/tcp/10001"},
	}
	server.state.nodes["run-b-boot"] = model.Node{
		ID: "run-b-boot", RunID: "run-b", Role: "boot", State: model.NodeReady,
		PeerID: "peer-b", Addresses: []string{"/ip4/127.0.0.1/tcp/10002"},
	}
	server.state.nodes["run-a-starting-boot"] = model.Node{
		ID: "run-a-starting-boot", RunID: "run-a", Role: "boot", State: model.NodeStarting,
		PeerID: "peer-starting", Addresses: []string{"/ip4/127.0.0.1/tcp/10003"},
	}
	server.state.nodes["run-a-worker"] = model.Node{
		ID: "run-a-worker", RunID: "run-a", Role: "worker", State: model.NodeReady,
		PeerID: "peer-worker", Addresses: []string{"/ip4/127.0.0.1/tcp/10004"},
	}
	server.state.mu.Unlock()

	handler := server.apiTestHandler(context.Background())
	for _, target := range []string{"/api/v1/bootstrap", "/api/v1/bootstrap?runId=%20%20"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status=%d body=%s, want 400", target, recorder.Code, recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/bootstrap?runId=run-a", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("bootstrap status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var nodes []struct {
		NodeID string `json:"nodeId"`
		PeerID string `json:"peerId"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "run-a-boot" || nodes[0].PeerID != "peer-a" {
		t.Fatalf("bootstrap nodes = %+v, want only ready run-a boot node", nodes)
	}
}

func TestTopicDiscoveryRegistryFiltersAndPrioritizesCandidates(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	server.state.mu.Lock()
	server.state.agents["online"] = model.Agent{ID: "online", State: model.AgentOnline, LastSeen: now}
	server.state.agents["stale"] = model.Agent{ID: "stale", State: model.AgentOnline, LastSeen: now.Add(-agentStaleAfter - time.Second)}
	server.state.agents["offline"] = model.Agent{ID: "offline", State: model.AgentOffline, LastSeen: now}
	addNode := func(id, runID, agentID, state, peerID, address, nodeType, mode, topics string, enabled string) {
		metadata := map[string]string{"topicMode": mode, "topicsJSON": topics}
		if enabled != "" {
			metadata["pubsubEnabled"] = enabled
		}
		addresses := []string{address}
		if address == "" {
			addresses = nil
		}
		server.state.nodes[id] = model.Node{
			ID: id, RunID: runID, AgentID: agentID, State: state, PeerID: peerID,
			Addresses: addresses, Type: nodeType, Metadata: metadata,
		}
	}
	addNode("requester", "run-a", "online", model.NodeReady, "peer-requester", "/ip4/127.0.0.1/tcp/10000", "full", "subscribe", `["topic-a"]`, "true")
	addNode("subscriber", "run-a", "online", model.NodeReady, "peer-subscriber", "/ip4/127.0.0.1/tcp/10001", "full", "subscribe", `["topic-a"]`, "true")
	addNode("publisher", "run-a", "online", model.NodeReady, "peer-publisher", "/ip4/127.0.0.1/tcp/10002", "publisher", "publish", `["topic-a"]`, "true")
	addNode("relay", "run-a", "online", model.NodeReady, "peer-relay", "/ip4/127.0.0.1/tcp/10003", "relay", "relay", `["topic-a"]`, "true")
	addNode("wrong-topic", "run-a", "online", model.NodeReady, "peer-wrong", "/ip4/127.0.0.1/tcp/10004", "full", "subscribe", `["topic-b"]`, "true")
	addNode("starting", "run-a", "online", model.NodeStarting, "peer-starting", "/ip4/127.0.0.1/tcp/10005", "full", "subscribe", `["topic-a"]`, "true")
	addNode("other-run", "run-b", "online", model.NodeReady, "peer-other-run", "/ip4/127.0.0.1/tcp/10006", "full", "subscribe", `["topic-a"]`, "true")
	addNode("stale", "run-a", "stale", model.NodeReady, "peer-stale", "/ip4/127.0.0.1/tcp/10007", "full", "subscribe", `["topic-a"]`, "true")
	addNode("offline", "run-a", "offline", model.NodeReady, "peer-offline", "/ip4/127.0.0.1/tcp/10008", "full", "subscribe", `["topic-a"]`, "true")
	addNode("disabled", "run-a", "online", model.NodeReady, "peer-disabled", "/ip4/127.0.0.1/tcp/10009", "full", "subscribe", `["topic-a"]`, "false")
	addNode("boot", "run-a", "online", model.NodeReady, "peer-boot", "/ip4/127.0.0.1/tcp/10010", "boot", "subscribe", `["topic-a"]`, "")
	addNode("no-address", "run-a", "online", model.NodeReady, "peer-no-address", "", "full", "subscribe", `["topic-a"]`, "true")
	server.state.mu.Unlock()

	handler := server.apiTestHandler(context.Background())
	for _, target := range []string{
		"/api/v1/discovery",
		"/api/v1/discovery?runId=run-a&topic=topic-a",
		"/api/v1/discovery?runId=run-a&requesterNodeId=requester",
		"/api/v1/discovery?topic=topic-a&requesterNodeId=requester",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("GET %s status=%d body=%s, want 400", target, recorder.Code, recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	target := "/api/v1/discovery?runId=run-a&topic=topic-a&requesterNodeId=requester"
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("discovery status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var nodes []struct {
		NodeID     string   `json:"nodeId"`
		PeerID     string   `json:"peerId"`
		Addresses  []string `json:"addresses"`
		Subscribed bool     `json:"subscribed"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Fatalf("discovery nodes=%+v, want subscriber, publisher, and relay", nodes)
	}
	if nodes[0].NodeID != "subscriber" || !nodes[0].Subscribed {
		t.Fatalf("first discovery node=%+v, want subscribed candidate first", nodes[0])
	}
	if nodes[1].NodeID != "publisher" || nodes[1].Subscribed || nodes[2].NodeID != "relay" || nodes[2].Subscribed {
		t.Fatalf("fallback discovery nodes=%+v, want deterministic non-subscriber order", nodes[1:])
	}
	if nodes[0].PeerID != "peer-subscriber" || len(nodes[0].Addresses) != 1 {
		t.Fatalf("discovery identity/address missing: %+v", nodes[0])
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, target, nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST discovery status=%d, want 405", recorder.Code)
	}
}

func TestAgentTerminationUsesControllerReceiptAsComparableUpperBound(t *testing.T) {
	source := time.Date(2035, 1, 2, 3, 4, 5, 6, time.UTC)
	received := time.Date(2026, 9, 5, 1, 2, 3, 4, time.UTC)
	batch := model.EventBatch{Events: []model.TraceEvent{
		{Type: "measurement_terminated", Timestamp: source},
		{Type: "measurement_stop", Timestamp: source},
	}}
	normalizeControllerEventTimes(&batch, received)
	if !batch.Events[0].Timestamp.Equal(received) || batch.Events[0].Fields["sourceTimestamp"] != source.Format(time.RFC3339Nano) ||
		batch.Events[0].Fields["timestampBasis"] != "controller-receipt-upper-bound-v1" {
		t.Fatalf("termination timestamp was not normalized with provenance: %+v", batch.Events[0])
	}
	if !batch.Events[1].Timestamp.Equal(source) || batch.Events[1].Fields != nil {
		t.Fatalf("Peer-owned lifecycle timestamp was rewritten: %+v", batch.Events[1])
	}
}
