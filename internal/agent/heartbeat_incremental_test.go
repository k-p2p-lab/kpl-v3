package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestPeriodicHeartbeatAcknowledgesAndChunksTerminalHistory(t *testing.T) {
	s := topologyStatusServer()
	s.config.ID, s.config.AdvertiseURL, s.config.Capacity = "agent", "http://agent", 10
	s.startedAt = time.Now().UTC()
	s.processes = make(map[string]*process)
	for i := range 1500 {
		id := fmt.Sprintf("retired-%04d", i)
		s.processes[id] = &process{exited: true, node: model.Node{
			ID: id, RunID: "history", State: model.NodeStopped,
			Metadata: map[string]string{"topics": strings.Repeat("x", 8192), "stoppedAt": s.startedAt.Format(time.RFC3339Nano)},
		}}
	}
	// Cleanup failures must remain in every report with diagnostic data.
	s.processes["failed"] = &process{exited: true, cleanupErr: errors.New("daemon unavailable"), node: model.Node{ID: "failed", RunID: "history", State: model.NodeFailed, Error: "daemon unavailable", Metadata: map[string]string{"diagnostic": "retained"}}}
	s.processes["live"] = &process{node: model.Node{ID: "live", RunID: "history", State: model.NodeReady}}
	controllerServer := authenticatedTestController(t.TempDir())
	handler := controllerServer.Handler(context.Background())
	var sent []model.AgentHeartbeat
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/agents/heartbeat" {
			data, err := io.ReadAll(r.Body)
			if err != nil || len(data) > heartbeatBodyLimit {
				t.Errorf("heartbeat body exceeded receiver limit: bytes=%d err=%v", len(data), err)
			}
			var h model.AgentHeartbeat
			if err := json.Unmarshal(data, &h); err != nil {
				t.Error(err)
			}
			sent = append(sent, h)
			r.Body = io.NopCloser(strings.NewReader(string(data)))
		}
		handler.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	s.config.Token = controllerTestToken
	s.client, s.config.ControllerURL = endpoint.Client(), endpoint.URL
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	full, err := json.Marshal(s.snapshot())
	if err != nil || len(full) <= heartbeatBodyLimit {
		t.Fatalf("test inventory should exceed one heartbeat request: bytes=%d err=%v", len(full), err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatalf("large initial backlog cannot synchronize: %v", err)
	}
	if len(sent) < 2 {
		t.Fatal("large inventory was not split")
	}
	count := 0
	for _, h := range sent {
		if !h.Partial || !h.Agent.LastSeen.Equal(sent[0].Agent.LastSeen) {
			t.Fatal("chunks lost their partial marker or shared capture time")
		}
		count += len(h.Nodes)
	}
	if count != 1502 {
		t.Fatalf("first synchronization omitted nodes: %d", count)
	}
	response, err := endpoint.Client().Do(controllerReadRequest(t, handler, endpoint.URL+"/api/v1/nodes"))
	if err != nil {
		t.Fatal(err)
	}
	var nodes []model.Node
	err = json.NewDecoder(response.Body).Decode(&nodes)
	response.Body.Close()
	if err != nil || len(nodes) != 1502 {
		t.Fatalf("Controller did not merge all chunks: count=%d err=%v", len(nodes), err)
	}
	sent = nil
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || len(sent[0].Nodes) != 2 || sent[0].Nodes[0].ID != "failed" || sent[0].Nodes[0].Metadata["diagnostic"] != "retained" || sent[0].Nodes[1].ID != "live" {
		t.Fatalf("steady heartbeat retained history or lost active/failed diagnostics: %+v", sent)
	}
	if status := s.snapshot(); status.Partial || len(status.Nodes) != 1502 || len(s.nodes()) != 1502 {
		t.Fatal("acknowledgment changed the complete status inventory")
	}
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "retired-0000", RunID: "new-run", Group: "workers"}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("acknowledgment allowed a terminal node ID to be reused: %v", err)
	}
	// The same Agent instance re-registers against a new Controller process.
	controllerServer = authenticatedTestController(t.TempDir())
	handler = controllerServer.Handler(context.Background())
	sent = nil
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	count = 0
	for _, h := range sent {
		count += len(h.Nodes)
	}
	if count != 1502 {
		t.Fatalf("Controller restart did not receive retained history: %d nodes", count)
	}
}

func TestPeriodicHeartbeatRetriesUnacknowledgedTerminalHistory(t *testing.T) {
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer endpoint.Close()
	s := &Server{config: Config{ID: "agent", ControllerURL: endpoint.URL}, client: endpoint.Client(), processes: map[string]*process{
		"stopped": {exited: true, node: model.Node{ID: "stopped", State: model.NodeStopped}},
	}}
	if err := s.heartbeat(context.Background()); err == nil || s.processes["stopped"].heartbeatAcknowledged {
		t.Fatal("failed request acknowledged terminal history")
	}
	if h := s.snapshotWithHistory(false); len(h.Nodes) != 1 {
		t.Fatal("failed report was removed from retry inventory")
	}
	if err := s.heartbeat(context.Background()); err != nil || !s.processes["stopped"].heartbeatAcknowledged {
		t.Fatalf("retry failed: %v", err)
	}
	if h := s.snapshotWithHistory(false); len(h.Nodes) != 0 {
		t.Fatal("successful retry did not retire periodic history")
	}
}

func TestHeartbeatChunkingPreservesEscapingAndRejectsOversizedNode(t *testing.T) {
	h := model.AgentHeartbeat{Agent: model.Agent{ID: "a"}, Nodes: []model.Node{
		{ID: "one", Metadata: map[string]string{"padding": strings.Repeat("<", 60)}},
		{ID: "two", Metadata: map[string]string{"padding": strings.Repeat(">", 60)}},
	}}
	count, calls := 0, 0
	if err := sendHeartbeatBatches(h, 800, func(data []byte, nodes []model.Node) error {
		calls++
		if len(data) > 800 {
			t.Fatalf("escaped JSON exceeds limit: %d", len(data))
		}
		var decoded model.AgentHeartbeat
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if !decoded.Partial || len(decoded.Nodes) != len(nodes) {
			t.Fatal("encoded heartbeat does not match acknowledged nodes")
		}
		count += len(decoded.Nodes)
		return nil
	}); err != nil || count != 2 || calls != 2 {
		t.Fatalf("byte splitting failed: count=%d calls=%d err=%v", count, calls, err)
	}
	err := sendHeartbeatBatches(h, 300, func([]byte, []model.Node) error {
		t.Fatal("oversized node was acknowledged")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), `node "one" exceeds`) {
		t.Fatalf("oversized node failure is not explicit: %v", err)
	}
}
