package controller

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestPublishDefaultDelayMatchesV2ParallelSemantics(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	parallel := publishDelays(rng, scenario.Distribution{}, 2, true)
	if parallel[0] != time.Second || parallel[1] != time.Second {
		t.Fatalf("parallel publish default delays = %v, want one-second offsets", parallel)
	}
	sequential := publishDelays(rng, scenario.Distribution{}, 2, false)
	if sequential[0] != 0 || sequential[1] != 0 {
		t.Fatalf("sequential publish default delays = %v, want no delay", sequential)
	}
}

func TestHeartbeatDoesNotErasePendingReservation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	_, err := server.state.registerAgent(model.Agent{ID: "a", URL: "http://agent", Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := server.tryReserveAgent("node-1"); !ok {
		t.Fatal("first reservation failed")
	}
	if err := server.state.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "a", Capacity: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := server.tryReserveAgent("node-2"); ok {
		t.Fatal("heartbeat erased pending reservation")
	}
}

func TestAcquireAgentWaitsUntilObservedSlotIsReleased(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	_, err := server.state.registerAgent(model.Agent{ID: "a", URL: "http://agent", Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := server.tryReserveAgent("node-1"); !ok {
		t.Fatal("first reservation failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := server.acquireAgent(ctx, "node-2")
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("acquire returned before capacity was released: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := server.state.heartbeat(model.AgentHeartbeat{
		Agent: model.Agent{ID: "a", Capacity: 1},
		Nodes: []model.Node{{ID: "node-1", AgentID: "a", State: model.NodeStopped}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("waiting reservation did not acquire the released slot")
	}
}

func TestStoppingNodeWaitsForObservedCapacityRelease(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := server.state.registerAgent(model.Agent{ID: "a", URL: "http://agent", Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	if err := server.state.heartbeat(model.AgentHeartbeat{
		Agent: model.Agent{ID: "a", Capacity: 1},
		Nodes: []model.Node{{ID: "node-1", AgentID: "a", RunID: "run", State: model.NodeReady}},
	}); err != nil {
		t.Fatal(err)
	}
	server.markNodeStoppingAndReleaseCapacity("node-1")

	agent, ok := server.agent("a")
	if !ok || agent.ActiveNodes != 1 {
		t.Fatalf("capacity was released before Agent confirmation: %+v", agent)
	}
	if _, ok := server.tryReserveAgent("node-2"); ok {
		t.Fatal("cleanup-occupied capacity was reused before heartbeat")
	}
	if err := server.state.heartbeat(model.AgentHeartbeat{
		Agent: model.Agent{ID: "a", Capacity: 1},
		Nodes: []model.Node{{ID: "node-1", AgentID: "a", RunID: "run", State: model.NodeStopped}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := server.tryReserveAgent("node-2"); !ok {
		t.Fatal("released capacity was not immediately reusable")
	}
}

func TestStaleHeartbeatCannotResurrectStoppingNode(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if _, err := server.state.registerAgent(model.Agent{ID: "a", URL: "http://agent", Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	ready := model.AgentHeartbeat{
		Agent: model.Agent{ID: "a", Capacity: 1, ActiveNodes: 1},
		Nodes: []model.Node{{ID: "node-1", AgentID: "a", RunID: "run", State: model.NodeReady}},
	}
	if err := server.state.heartbeat(ready); err != nil {
		t.Fatal(err)
	}
	server.markNodeStoppingAndReleaseCapacity("node-1")
	if err := server.state.heartbeat(ready); err != nil {
		t.Fatal(err)
	}

	server.state.mu.RLock()
	node := server.state.nodes["node-1"]
	agent := server.state.agents["a"]
	server.state.mu.RUnlock()
	if node.State != model.NodeStopping || agent.ActiveNodes != 1 {
		t.Fatalf("stale heartbeat resurrected stopped capacity: node=%+v agent=%+v", node, agent)
	}
}

func TestPublishSelectionRespectsCapabilitiesAndTopics(t *testing.T) {
	publisher := model.Node{
		ID: "publisher", AgentID: "online", Type: "publisher", State: model.NodeReady,
		Metadata: map[string]string{"pubsubEnabled": "true", "allowPublish": "true", "topicMode": "publish", "topics": "topic-a"},
	}
	subscriber := model.Node{
		ID: "subscriber", AgentID: "online", Type: "subscriber", State: model.NodeReady,
		Metadata: map[string]string{"pubsubEnabled": "true", "allowPublish": "false", "topicMode": "subscribe", "topics": "topic-a"},
	}
	dhtOnly := model.Node{
		ID: "dht", AgentID: "online", Type: "dht-only", State: model.NodeReady,
		Metadata: map[string]string{"pubsubEnabled": "false", "allowPublish": "false", "topicMode": "subscribe", "topics": "topic-a"},
	}
	if !nodeCanPublishTopic(publisher, "topic-a") {
		t.Fatal("publisher was not eligible for its configured topic")
	}
	if nodeCanPublishTopic(publisher, "topic-b") || nodeCanPublishTopic(subscriber, "topic-a") || nodeCanPublishTopic(dhtOnly, "topic-a") {
		t.Fatal("non-publishable node or topic was accepted")
	}

	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["online"] = model.Agent{ID: "online", State: model.AgentOnline}
	publisher.RunID, subscriber.RunID, dhtOnly.RunID = "run", "run", "run"
	server.state.nodes[publisher.ID] = publisher
	server.state.nodes[subscriber.ID] = subscriber
	server.state.nodes[dhtOnly.ID] = dhtOnly
	if got := server.topicSubscriberCount("run", "topic-a"); got != 1 {
		t.Fatalf("subscriber count = %d, want 1", got)
	}
	if got := server.topicReachTargetCount("run", "topic-a", publisher.ID); got != 2 {
		t.Fatalf("reachability target = %d, want subscriber plus publisher (2)", got)
	}
}

func TestPublishTargetsExcludeOfflineAgentsAndPreserveCommaTopics(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["online"] = model.Agent{ID: "online", State: model.AgentOnline}
	server.state.agents["offline"] = model.Agent{ID: "offline", State: model.AgentOffline}
	topic := "orders,priority"
	metadata := func(mode string) map[string]string {
		return map[string]string{
			"pubsubEnabled": "true",
			"allowPublish":  "true",
			"topicMode":     mode,
			"topics":        "orders,priority",
			"topicsJSON":    `["orders,priority"]`,
		}
	}
	server.state.nodes["publisher"] = model.Node{ID: "publisher", RunID: "run", AgentID: "online", Type: "publisher", State: model.NodeReady, Metadata: metadata("publish")}
	server.state.nodes["subscriber"] = model.Node{ID: "subscriber", RunID: "run", AgentID: "online", Type: "subscriber", State: model.NodeReady, Metadata: metadata("subscribe")}
	server.state.nodes["offline-subscriber"] = model.Node{ID: "offline-subscriber", RunID: "run", AgentID: "offline", Type: "subscriber", State: model.NodeReady, Metadata: metadata("subscribe")}

	publisher := server.state.nodes["publisher"]
	if !nodeCanPublishTopic(publisher, topic) || nodeCanPublishTopic(publisher, "orders") {
		t.Fatal("lossless topic metadata did not preserve the comma-containing topic")
	}
	if got := server.topicSubscriberCount("run", topic); got != 1 {
		t.Fatalf("online subscriber count = %d, want 1", got)
	}
	if got := server.topicReachTargetCount("run", topic, "publisher"); got != 2 {
		t.Fatalf("reach target = %d, want online subscriber plus publisher", got)
	}
}

func TestRunPublishSkipsReadyNodesOnOfflineAgents(t *testing.T) {
	var published atomic.Int32
	onlineAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/publish") {
			http.NotFound(w, r)
			return
		}
		published.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(onlineAPI.Close)

	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["online"] = model.Agent{ID: "online", URL: onlineAPI.URL, State: model.AgentOnline}
	server.state.agents["offline"] = model.Agent{ID: "offline", URL: "http://127.0.0.1:1", State: model.AgentOffline}
	metadata := map[string]string{"pubsubEnabled": "true", "allowPublish": "true", "topicMode": "subscribe", "topicsJSON": `["topic"]`}
	server.state.nodes["online-node"] = model.Node{ID: "online-node", RunID: "run", Group: "workers", AgentID: "online", Type: "full", State: model.NodeReady, Metadata: metadata}
	server.state.nodes["offline-node"] = model.Node{ID: "offline-node", RunID: "run", Group: "workers", AgentID: "offline", Type: "full", State: model.NodeReady, Metadata: metadata}

	phase := scenario.Phase{Group: "workers", Count: 2, Topic: "topic", PayloadSize: 8}
	if err := server.runPublish(context.Background(), "run", phase, rand.New(rand.NewSource(1))); err != nil {
		t.Fatalf("publish failed despite an online eligible node: %v", err)
	}
	if got := published.Load(); got != 1 {
		t.Fatalf("publish calls = %d, want only the online node", got)
	}
}

func TestStopNodesAttemptsEveryDeleteBeforeReturningErrors(t *testing.T) {
	attempted := make(chan string, 2)
	agentAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempted <- r.URL.Path
		if strings.HasSuffix(r.URL.Path, "/node-a") {
			http.Error(w, "injected", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(agentAPI.Close)

	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["agent"] = model.Agent{ID: "agent", URL: agentAPI.URL, State: model.AgentOnline}
	for _, id := range []string{"node-a", "node-b"} {
		server.state.nodes[id] = model.Node{ID: id, RunID: "run", AgentID: "agent", State: model.NodeReady}
	}
	err := server.stopNodes(context.Background(), "run", "", "", 0, rand.New(rand.NewSource(1)), scenario.Distribution{}, false, 0)
	if err == nil || !strings.Contains(err.Error(), "node-a") {
		t.Fatalf("stopNodes error = %v, want node-a failure", err)
	}
	if got := len(attempted); got != 2 {
		t.Fatalf("delete attempts = %d, want 2", got)
	}
	server.state.mu.RLock()
	stateB := server.state.nodes["node-b"].State
	server.state.mu.RUnlock()
	if stateB != model.NodeStopping {
		t.Fatalf("successful node state = %q, want stopping", stateB)
	}
}
