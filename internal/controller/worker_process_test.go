package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestControllerOSWorkerHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "controller-worker-helper" {
		return
	}
	kind := os.Args[len(os.Args)-1]
	if err := workerproc.Serve(context.Background(), kind, WorkerFactory); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

type controllerWorkerFixture struct {
	server        *Server
	mu            sync.Mutex
	nodes         map[string]model.Node
	terminal      map[string]model.Node
	created       map[string][]model.Node
	drained       map[string]bool
	commands      []*exec.Cmd
	starts        []string
	stops         []string
	workerPending bool
}

func newControllerWorkerFixture(t *testing.T) *controllerWorkerFixture {
	t.Helper()
	f := &controllerWorkerFixture{nodes: make(map[string]model.Node), terminal: make(map[string]model.Node), created: make(map[string][]model.Node), drained: make(map[string]bool)}
	started := time.Now().Add(-time.Minute).UTC()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes":
			var request model.CreateNodeRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				http.Error(w, "invalid create", 400)
				return
			}
			node := model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, Role: request.Role, Type: request.Type, Profile: request.Profile, State: model.NodeReady}
			f.mu.Lock()
			f.nodes[node.ID] = node
			f.created[node.RunID] = append(f.created[node.RunID], node)
			f.mu.Unlock()
			writeJSON(w, 201, node)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
			f.mu.Lock()
			nodes := make([]model.Node, 0, len(f.nodes))
			for _, node := range f.nodes {
				nodes = append(nodes, node)
			}
			active := len(f.nodes)
			for _, node := range f.terminal {
				nodes = append(nodes, node)
			}
			f.mu.Unlock()
			writeJSON(w, 200, model.AgentHeartbeat{Agent: model.Agent{ID: "agent", StartedAt: started, Capacity: 20, RunDrain: true, RunProcesses: true, ActiveNodes: active}, Nodes: nodes})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/nodes")
			f.mu.Lock()
			f.stops = append(f.stops, id)
			for key, node := range f.nodes {
				if node.RunID == id {
					node.State = model.NodeStopped
					node.Metadata = map[string]string{"cleanupComplete": "true"}
					f.terminal[key] = node
					delete(f.nodes, key)
				}
			}
			f.mu.Unlock()
			w.WriteHeader(202)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/drain")
			f.mu.Lock()
			pending := f.workerPending
			already := f.drained[id]
			nodes := append([]model.Node(nil), f.created[id]...)
			live := 0
			for _, node := range f.nodes {
				if node.RunID == id {
					live++
				}
			}
			f.mu.Unlock()
			if live != 0 {
				http.Error(w, "drain before Peer cleanup", 500)
				return
			}
			if pending {
				writeJSON(w, 503, model.RunDrainError{Code: model.RunWorkerPendingCode, WorkerPending: true, Error: "worker not reaped"})
				return
			}
			if !already && len(nodes) >= 2 {
				if err := f.server.state.appendEvents(model.EventBatch{AgentID: "agent", Events: workerFixtureEvents(id, nodes)}); err != nil {
					t.Error(err)
					http.Error(w, "journal failed", 500)
					return
				}
			}
			f.mu.Lock()
			f.drained[id] = true
			f.mu.Unlock()
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(endpoint.Close)
	f.server = New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.server.workerCommand = func(kind string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		f.mu.Lock()
		f.commands = append(f.commands, cmd)
		f.starts = append(f.starts, kind)
		f.mu.Unlock()
		return cmd
	}
	if _, err := f.server.state.registerAgent(model.Agent{ID: "agent", URL: endpoint.URL, StartedAt: started, Capacity: 20, RunDrain: true, RunProcesses: true}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.server.cancelMu.Lock()
		for _, cancel := range f.server.cancels {
			cancel()
		}
		f.server.cancelMu.Unlock()
		waitWorkerRuns(t, f.server)
	})
	return f
}

func workerFixtureEvents(id string, nodes []model.Node) []model.TraceEvent {
	base := time.Now().Add(-30 * time.Second).UTC()
	var events []model.TraceEvent
	for _, node := range nodes[:2] {
		events = append(events, model.TraceEvent{RunID: id, AgentID: "agent", NodeID: node.ID, SessionID: node.ID + "-session", Sequence: 1, Type: "measurement_start", Timestamp: base, Fields: map[string]any{"subscribedTopics": []string{"topic"}}})
	}
	events = append(events, model.TraceEvent{RunID: id, AgentID: "agent", NodeID: nodes[0].ID, SessionID: nodes[0].ID + "-session", Sequence: 2, Type: "publish", MessageID: "message", Topic: "topic", Timestamp: base.Add(time.Second), Fields: map[string]any{"measurementDefinition": sessionWindowDefinition, "deliveryWindow": "1s"}})
	events = append(events, model.TraceEvent{RunID: id, AgentID: "agent", NodeID: nodes[1].ID, SessionID: nodes[1].ID + "-session", Sequence: 2, Type: "deliver", MessageID: "message", Topic: "topic", Timestamp: base.Add(time.Second + time.Millisecond), LatencyMS: 1, Fields: map[string]any{"payloadEncoding": "envelope", "latencyAvailable": true}})
	for _, node := range nodes[:2] {
		events = append(events, model.TraceEvent{RunID: id, AgentID: "agent", NodeID: node.ID, SessionID: node.ID + "-session", Sequence: 3, Type: "measurement_stop", Timestamp: base.Add(5 * time.Second)})
	}
	return events
}

func waitWorkerRuns(t *testing.T, s *Server) {
	t.Helper()
	done := make(chan struct{})
	go func() { s.runs.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Controller worker runs did not terminate")
	}
}

func TestControllerWorkersReplaceEachRunAndKeepRawIndexOutOfMain(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: worker-isolation\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n  - {action: wait, duration: 20ms}\n")
	first, err := f.server.StartScenarioRepeated(t.Context(), raw, 2)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	f.server.state.mu.RLock()
	runs := make([]model.Experiment, 0)
	for _, run := range f.server.state.experiments {
		if run.ExecutionID == first.ExecutionID {
			runs = append(runs, run)
		}
	}
	heavy := len(f.server.state.runMetrics)
	reservations, uncertain, active := len(f.server.state.reservations), len(f.server.state.uncertainCreates), len(f.server.state.activeNodeIDs)
	f.server.state.mu.RUnlock()
	if len(runs) != 2 || heavy != 0 {
		t.Fatalf("runs=%d main retained heavy indexes=%d", len(runs), heavy)
	}
	if reservations != 0 || uncertain != 0 || active != 0 {
		t.Fatalf("runtime baseline not restored: reservations=%d uncertain=%d active=%d", reservations, uncertain, active)
	}
	for _, run := range runs {
		if run.State != "completed" || run.CleanupState != "complete" || run.DataState != "complete" || run.TelemetryPending {
			t.Fatalf("Run did not seal and clean: %+v", run)
		}
		summary, ok := f.server.state.workerMetricSummary(run.ID)
		if !ok || summary.Metrics.ExpectedDeliveries != 1 || summary.Metrics.EligibleDeliveries != 1 {
			t.Fatalf("final journal not aggregated in worker: %+v known=%v", summary, ok)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commands) != 2 || f.commands[0].Process.Pid == f.commands[1].Process.Pid {
		t.Fatalf("Run did not receive distinct processes: %v", f.starts)
	}
	for _, cmd := range f.commands {
		if cmd.ProcessState == nil {
			t.Fatal("FIFO advanced before worker Wait")
		}
	}
	if len(f.nodes) != 0 || len(f.drained) != 2 || len(f.stops) < 2 {
		t.Fatalf("Peers/drain state survived Run boundary: nodes=%d drained=%d stops=%d", len(f.nodes), len(f.drained), len(f.stops))
	}
}

func TestJournalOnlyModeDeduplicatesWithoutScientificIndex(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	for _, event := range []model.TraceEvent{{RunID: "run", NodeID: "peer", SessionID: "session", Sequence: 1, EventID: "seq", Type: "publish"}, {RunID: "run", NodeID: "peer", EventID: "legacy", Type: "publish"}} {
		for range 2 {
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(s.state.runMetrics) != 0 {
		t.Fatal("ingress allocated a scientific receipt index")
	}
	data, err := os.ReadFile(s.config.DataDir + "/current-run/run/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 2 {
		t.Fatalf("retries were persisted as distinct evidence: %s", data)
	}
}

func TestWorkerPendingIsCleanupEvidenceWithoutInventingPendingLogs(t *testing.T) {
	err := parseRunWorkerPending([]byte(`{"error":"not reaped","code":"run_worker_pending","workerPending":true,"remainingEvents":0}`))
	if !hasPendingRunWorker(err) || hasPendingRunTelemetry(err) {
		t.Fatalf("worker/log obligations conflated: %v", err)
	}
	encoded := encodeWorkerError(errors.Join(context.DeadlineExceeded, err))
	decoded := encoded.decode()
	if !hasPendingRunWorker(decoded) || hasPendingRunTelemetry(decoded) || !errors.Is(decoded, context.DeadlineExceeded) {
		t.Fatalf("IPC lost typed cleanup evidence: %v", decoded)
	}
}
