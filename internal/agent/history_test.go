package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func historyTestServer(t *testing.T) *Server {
	t.Helper()
	s, err := New(Config{ID: "agent", Capacity: 2, ControllerURL: "http://controller:8080", AdvertiseURL: "http://agent:8090", DockerImage: "kpl:test", DockerNetwork: "peers", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	t.Cleanup(func() {
		s.heartbeatMu.Lock()
		defer s.heartbeatMu.Unlock()
		if s.historyReplay != nil && s.historyReplay.directory != nil {
			s.historyReplay.directory.Close()
		}
		s.historyReplay = nil
	})
	return s
}
func historyTerminal(id, run string, at time.Time) *process {
	return &process{exited: true, node: model.Node{ID: id, RunID: run, State: model.NodeStopped, LastSeen: at, Metadata: map[string]string{"cleanupComplete": "true", "stoppedAt": at.Format(time.RFC3339Nano)}}}
}
func historyAdmission(s *Server, id, run string, generation uint64) error {
	err := s.lockAdmission(context.Background(), model.CreateNodeRequest{ID: id, RunID: run, Generation: generation})
	if err == nil {
		s.mu.Unlock()
	}
	return err
}
func pruneHistoryToRecent(t *testing.T, s *Server) {
	t.Helper()
	for len(s.processes) > peerHistoryRecent {
		before := len(s.processes)
		if err := s.reclaimHistory(); err != nil {
			t.Fatal(err)
		}
		if len(s.processes) >= before {
			t.Fatal("history pruning made no progress")
		}
	}
}

func TestChurnHistoryMemoryIsBoundedAndRetiredIDsRemainFenced(t *testing.T) {
	s := historyTestServer(t)
	var heapFirst, heapLast runtime.MemStats
	const rounds = 12
	for round := range rounds {
		for i := range 256 {
			id := fmt.Sprintf("peer-%02d-%03d", round, i)
			s.processes[id] = historyTerminal(id, fmt.Sprintf("run-%02d", round), time.Unix(int64(round*256+i+1), 0))
		}
		if err := s.heartbeat(context.Background()); err != nil {
			t.Fatal(err)
		}
		pruneHistoryToRecent(t, s)
		if len(s.processes) != peerHistoryRecent {
			t.Fatalf("history retained %d records after round %d", len(s.processes), round)
		}
		if round == 0 {
			runtime.GC()
			runtime.ReadMemStats(&heapFirst)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&heapLast)
	t.Logf("%d retired Peers: live history stays at %d records; heap after first/final round %.2f / %.2f MiB", rounds*256, peerHistoryRecent, float64(heapFirst.HeapAlloc)/(1<<20), float64(heapLast.HeapAlloc)/(1<<20))
	registry := newAgentMetricsRegistry(s)
	if got := localMetricGauge(t, registry, "kpl_local_history_records", map[string]string{"agent_id": "agent", "kind": "peers"}); got != peerHistoryRecent {
		t.Fatalf("history metric %v", got)
	}
	if s.historyPeersRetired != rounds*256-peerHistoryRecent {
		t.Fatal("retirement counter is incomplete")
	}
	for _, id := range []string{"peer-00-000", "peer-05-100"} {
		if err := historyAdmission(s, id, "new-run", 99); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("retired ID reused: %v", err)
		}
	}
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(s.snapshotWithHistory(false).Nodes) != peerHistoryRecent {
		t.Fatal("registration replay grew to all historical Peers")
	}
	restarted, err := New(s.config, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.processes) != 0 || len(restarted.runFences) != 0 {
		t.Fatal("restart loaded an unbounded history index")
	}
	if err := historyAdmission(restarted, "peer-00-000", "new-run", 99); err == nil {
		t.Fatal("restart lost retired identity")
	}
	// Stop-all closes runs already retired from memory, without loading them all.
	response := httptest.NewRecorder()
	s.handleNodes(response, httptest.NewRequest("DELETE", "/api/v1/nodes", nil))
	if response.Code != 204 {
		t.Fatalf("cleanup %d %s", response.Code, response.Body)
	}
	if err := historyAdmission(s, "late-peer", "run-00", 99); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("retired run escaped stop-all: %v", err)
	}
	if err := historyAdmission(s, "next-peer", "new-run", 1); err != nil {
		t.Fatalf("stop-all blocked a new run: %v", err)
	}
}

func TestRunFenceCacheEvictionPersistsMaximumAndSurvivesRestart(t *testing.T) {
	s := historyTestServer(t)
	for i := range 300 {
		s.stopRunGeneration(fmt.Sprintf("run-%03d", i), 9)
	}
	for len(s.runFences) > runFenceCacheLimit {
		if err := s.reclaimHistory(); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.runFences) != runFenceCacheLimit {
		t.Fatal("wrong fence cache size")
	}
	s.stopRunGeneration("run-000", 2)
	if err := historyAdmission(s, "old-peer", "run-000", 9); err == nil {
		t.Fatal("older stop lowered disk fence")
	}
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(s.config, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := historyAdmission(restarted, "old-peer", "run-000", 9); err == nil {
		t.Fatal("restart lost stored fence")
	}
	if err := historyAdmission(restarted, "new-peer", "run-000", 10); err != nil {
		t.Fatalf("newer generation rejected %v", err)
	}
	if err := os.WriteFile(s.history.path("fences", "corrupt"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := historyAdmission(s, "peer", "corrupt", 99); err == nil {
		t.Fatal("unreadable fence failed open")
	}
}

func TestHistoryPreservesUnacknowledgedAndFailedCleanupEvidence(t *testing.T) {
	s := historyTestServer(t)
	for i := range 200 {
		id := fmt.Sprintf("peer-%03d", i)
		s.processes[id] = historyTerminal(id, "run", time.Unix(int64(i+1), 0))
	}
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("controller unavailable") })}
	if err := s.heartbeat(context.Background()); err == nil {
		t.Fatal("expected failed report")
	}
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	if len(s.processes) != 200 {
		t.Fatal("unacknowledged history dropped")
	}
	for _, proc := range s.processes {
		proc.heartbeatAcknowledged = true
	}
	failed := historyTerminal("cleanup-failed", "run", time.Unix(0, 0))
	failed.node.State = model.NodeFailed
	failed.cleanupErr = errors.New("Docker unavailable")
	failed.heartbeatAcknowledged = true
	failed.containerID = "keep-for-retry"
	s.processes[failed.node.ID] = failed
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	if s.processes[failed.node.ID] != failed || s.capacityUsedLocked() != 1 {
		t.Fatal("cleanup failure was forgotten or freed capacity")
	}
	if retired, err := s.history.contains(failed.node.ID); err != nil || retired {
		t.Fatal("failed removal falsely archived")
	}
}

func TestFailedPeerReleasesTopologyAfterConfirmedContainerRemoval(t *testing.T) {
	s := historyTestServer(t)
	node := topologyStatusAt(time.Now())
	node.ID = "failed"
	node.RunID = "run"
	proc := &process{node: node, done: make(chan struct{})}
	s.processes[node.ID] = proc
	s.finishProcess(node.ID, proc, errors.New("peer exited 1"), nil)
	if proc.node.State != model.NodeFailed || proc.node.Error != "peer exited 1" || proc.node.Metadata["cleanupComplete"] != "true" {
		t.Fatalf("failed evidence lost: %+v", proc.node)
	}
	if len(proc.node.ConnectedPeers)+len(proc.node.PeerScores)+len(proc.node.MeshPeers)+len(proc.node.RoutingPeers) != 0 {
		t.Fatal("failed Peer retained topology")
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !proc.heartbeatAcknowledged || len(s.snapshotWithHistory(false).Nodes) != 0 {
		t.Fatal("cleaned failure stayed in every heartbeat")
	}
}

func TestHistoryStorageFailureKeepsRecordsAndBackpressuresAdmission(t *testing.T) {
	s := historyTestServer(t)
	for i := range peerHistoryLimit {
		id := fmt.Sprintf("peer-%05d", i)
		p := historyTerminal(id, "run", time.Unix(int64(i), 0))
		p.heartbeatAcknowledged = true
		s.processes[id] = p
	}
	blocker := filepath.Join(t.TempDir(), "not-directory")
	if err := os.WriteFile(blocker, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	actual := s.history.directory
	s.history.directory = blocker
	if err := s.reclaimHistory(); err == nil {
		t.Fatal("storage failure hidden")
	}
	if len(s.processes) != peerHistoryLimit {
		t.Fatal("failed persistence dropped evidence")
	}
	s.history.directory = actual
	if err := historyAdmission(s, "new", "run", 1); !errors.Is(err, errPeerHistoryFull) {
		t.Fatalf("unbounded terminal backlog admitted: %v", err)
	}
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	if err := historyAdmission(s, "new", "run", 1); err != nil {
		t.Fatalf("recovery did not reopen admission %v", err)
	}
}

func TestHistoryMaintenanceDoesNotRaceHeartbeatsOrRegistration(t *testing.T) {
	s := historyTestServer(t)
	for i := range 400 {
		id := fmt.Sprintf("peer-%03d", i)
		p := historyTerminal(id, "run", time.Now())
		p.heartbeatAcknowledged = true
		s.processes[id] = p
	}
	var wg sync.WaitGroup
	for worker := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				var err error
				switch worker {
				case 0:
					err = s.reclaimHistory()
				case 1:
					err = s.heartbeat(context.Background())
				case 2:
					err = s.register(context.Background())
					s.snapshot()
				}
				if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	pruneHistoryToRecent(t, s)
}

func TestRetiredHistoryReplaysInBoundedPagesAfterControllerRestart(t *testing.T) {
	s := historyTestServer(t)
	for i := range 400 {
		id := fmt.Sprintf("peer-%03d", i)
		proc := historyTerminal(id, "run", time.Unix(int64(i), 0))
		proc.heartbeatAcknowledged = true
		s.processes[id] = proc
	}
	pruneHistoryToRecent(t, s)
	seen := map[string]bool{}
	failHistory := true
	liveSends, largest := 0, 0
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v1/agents/heartbeat" {
			var heartbeat model.AgentHeartbeat
			if err := json.NewDecoder(r.Body).Decode(&heartbeat); err != nil {
				t.Error(err)
			}
			if len(heartbeat.Nodes) == 0 || len(heartbeat.Nodes) == peerHistoryRecent {
				liveSends++
			} else {
				largest = max(largest, len(heartbeat.Nodes))
				if failHistory {
					failHistory = false
					return nil, errors.New("history request failed")
				}
			}
			for _, node := range heartbeat.Nodes {
				seen[node.ID] = true
			}
		}
		return &http.Response{StatusCode: 204, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err == nil {
		t.Fatal("history failure not exercised")
	}
	if len(s.historyReplay.pending) != historyPruneBatch {
		t.Fatal("failed replay page was not retained")
	}
	for i := 0; i < 10 && s.historyReplay != nil; i++ {
		if err := s.heartbeat(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 400 || s.historyReplay != nil || largest > historyPruneBatch || liveSends < 4 {
		t.Fatalf("unbounded/incomplete replay: seen=%d page=%d live=%d", len(seen), largest, liveSends)
	}
	if len(s.processes) != peerHistoryRecent {
		t.Fatal("replay reloaded retired processes into live memory")
	}
}

func TestRetiredHistoryCannotHideUndeliveredTerminationBacklog(t *testing.T) {
	s := historyTestServer(t)
	s.terminations = make(map[string]model.TraceEvent)
	for i := range peerHistoryLimit {
		id := fmt.Sprintf("retired-%04d", i)
		s.terminations[id] = model.TraceEvent{NodeID: id, RunID: "run", EventID: id, Type: "measurement_terminated", Timestamp: time.Now()}
	}
	// Heartbeats may succeed and retire all process records while event delivery
	// remains blocked. That independent exit backlog must still bound admission.
	if err := historyAdmission(s, "next", "run", 1); !errors.Is(err, errPeerHistoryFull) {
		t.Fatalf("unbounded termination backlog admitted: %v", err)
	}
	registry := newAgentMetricsRegistry(s)
	if got := localMetricGauge(t, registry, "kpl_local_telemetry_queue_events", map[string]string{"agent_id": "agent"}); got != peerHistoryLimit {
		t.Fatalf("pending exits hidden from metrics: %v", got)
	}
	s.flushEvents(context.Background())
	if len(s.terminations) != 0 || len(s.events) != 0 {
		t.Fatal("termination evidence did not drain")
	}
	if err := historyAdmission(s, "next", "run", 1); err != nil {
		t.Fatalf("recovery blocked admission: %v", err)
	}
}

func TestHistoryRemovesOnlyAcknowledgedFinishedPeerConfig(t *testing.T) {
	s := historyTestServer(t)
	for _, id := range []string{"done", "unacknowledged", "cleanup-failed"} {
		proc := historyTerminal(id, "run", time.Now())
		proc.heartbeatAcknowledged = id != "unacknowledged"
		if id == "cleanup-failed" {
			proc.cleanupErr = errors.New("Docker unavailable")
		}
		dir := filepath.Join(t.TempDir(), id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		proc.configPath = filepath.Join(dir, "peer.json")
		if err := os.WriteFile(proc.configPath, []byte(`{"token":"private"}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("unrelated data"), 0600); err != nil {
			t.Fatal(err)
		}
		s.processes[id] = proc
	}
	doneDir := filepath.Dir(s.processes["done"].configPath)
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(doneDir, "peer.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finished config retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(doneDir, "keep.txt")); err != nil {
		t.Fatal("unrelated file removed")
	}
	for _, id := range []string{"unacknowledged", "cleanup-failed"} {
		if _, err := os.Stat(s.processes[id].configPath); err != nil {
			t.Fatalf("pending config removed for %s", id)
		}
	}
	if len(s.processes) != 3 {
		t.Fatal("config cleanup discarded recent lifecycle evidence")
	}
}

func TestTelemetryBacklogPausesNewPeerAdmissionUntilRecovery(t *testing.T) {
	s := historyTestServer(t)
	s.spool = &telemetrySpool{records: make([]telemetryRecord, telemetryBacklogEvents)}
	if err := historyAdmission(s, "new", "run", 1); !errors.Is(err, errTelemetryBacklogFull) {
		t.Fatalf("unbounded terminal backlog allowed: %v", err)
	}
	s.spool.records = nil
	s.spool.bytes = telemetryBacklogBytes
	if err := historyAdmission(s, "new", "run", 1); !errors.Is(err, errTelemetryBacklogFull) {
		t.Fatalf("byte backlog allowed: %v", err)
	}
	s.spool.bytes = 0
	if err := historyAdmission(s, "new", "run", 1); err != nil {
		t.Fatalf("recovered admission still blocked: %v", err)
	}
}
