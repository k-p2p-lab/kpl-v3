package controller

import (
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func runMetricValues(t *testing.T, s *Server, id string) map[string]observedMetric {
	t.Helper()
	values := make(map[string]observedMetric)
	for key, value := range gatherTestMetrics(t, s.state) {
		if strings.Contains(key, `run_id="`+id+`"`) {
			values[key] = value
		}
	}
	return values
}

func TestSealedMetricsReconstructWithoutMemoryCache(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: durable-summary\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n")
	run, err := f.server.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	before := runMetricValues(t, f.server, run.ID)
	if len(before) == 0 {
		t.Fatal("fixture did not expose sealed Run metrics")
	}
	path := filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID, workerSummaryFile)
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Lose only controlled test-process caches, as on a fresh Controller. The
	// source logs and fsynced summary remain byte-for-byte untouched.
	f.server.state.mu.Lock()
	f.server.state.workerSummaries = nil
	f.server.state.mu.Unlock()
	f.server.state.metrics.lifecycleMu.Lock()
	f.server.state.metrics.sealed = nil
	f.server.state.metrics.lifecycleMu.Unlock()
	after := runMetricValues(t, f.server, run.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("cold summary changed historical metric visibility: before=%v after=%v", before, after)
	}
	retained, err := os.ReadFile(path)
	if err != nil || string(retained) != string(persisted) {
		t.Fatalf("cache reconstruction modified durable evidence: %v", err)
	}
	runs, err := f.server.openResultRuns()
	if err != nil {
		t.Fatal(err)
	}
	defer runs.Close()
	result, err := f.server.readSavedResult(runs, run.ID)
	if err != nil || result.State != "completed" {
		t.Fatalf("Saved Results no longer reconstructs the completed Run: %+v %v", result, err)
	}
}

func TestRetiredFenceCacheEvictionRetainsAuthority(t *testing.T) {
	s := newControllerWorkerFixture(t).server
	dirPath := filepath.Join(s.config.DataDir, retiredRuntimeDirectory)
	if err := os.MkdirAll(dirPath, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.OpenRoot(dirPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	for i := 0; i < closedRunCacheEntries+4; i++ {
		id := "retired-" + string(rune('A'+i/26)) + string(rune('a'+i%26))
		if err := writeRetiredRuntimeFence(dir, id); err != nil {
			t.Fatal(err)
		}
		closed, err := s.state.lookupRetiredRun(id)
		if err != nil || !closed {
			t.Fatalf("persisted fence unavailable: %s %v", id, err)
		}
	}
	s.state.mu.RLock()
	cached := len(s.state.closedRuns)
	s.state.mu.RUnlock()
	if cached > closedRunCacheEntries {
		t.Fatalf("retired identity cache unbounded: %d", cached)
	}
	closed, err := s.state.lookupRetiredRun("retired-Aa")
	if err != nil || !closed {
		t.Fatalf("evicted fence lost durable authority: %v", err)
	}
}

func TestSealedSummarySurvivesRestartAndArchivePayloadRetirement(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: archived-summary\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n")
	run, err := f.server.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	before := runMetricValues(t, f.server, run.ID)
	archiveTestRun(t, f.server, run.ID)
	local := filepath.Join(f.server.config.DataDir, currentRunsDirectory, run.ID)
	if _, err := os.Stat(filepath.Join(local, "events.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("archive did not retire raw local tail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(local, workerSummaryFile)); err != nil {
		t.Fatalf("archive removed local durable projection: %v", err)
	}
	// The archive may be absent. Historical projection reads must be entirely
	// local and still expose the same gauges, histograms and event counters.
	archive := filepath.Join(f.server.config.DataDir, archivedRunsDirectory)
	if err := os.Rename(archive, archive+"-offline"); err != nil {
		t.Fatal(err)
	}
	restarted := New(ServerConfig{DataDir: f.server.config.DataDir, RunWorkers: true}, nil)
	f.server.state.mu.RLock()
	for id, experiment := range f.server.state.experiments {
		restarted.state.experiments[id] = experiment
	}
	f.server.state.mu.RUnlock()
	if err := restarted.loadWorkerSummaries(); err != nil {
		t.Fatal(err)
	}
	after := runMetricValues(t, restarted, run.ID)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("archive/restart changed historical projection: before=%v after=%v", before, after)
	}
	if len(restarted.state.workerSummaries) != 0 || len(restarted.state.metrics.sealed) != 0 {
		t.Fatal("restart loaded historical payloads into service RAM")
	}
}

func TestLateRetiredEventsUseQueuedCanonicalCounterProjection(t *testing.T) {
	f := newControllerWorkerFixture(t)
	raw := []byte("version: 4\nname: late-summary\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n")
	run, err := f.server.StartScenario(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	before := runMetricValues(t, f.server, run.ID)
	f.mu.Lock()
	node := f.created[run.ID][0]
	f.mu.Unlock()
	event := model.TraceEvent{RunID: run.ID, AgentID: "agent", NodeID: node.ID, SessionID: node.ID + "-session", Sequence: 4, Type: "late_event", Timestamp: time.Now().UTC()}
	batch := model.EventBatch{AgentID: "agent", Events: []model.TraceEvent{event, event}}
	if err := f.server.state.appendEvents(batch); err != nil {
		t.Fatal(err)
	}
	// Ingress acknowledges only the durable source; sealed measurement changes
	// are admitted by the common FIFO worker, never partial counter vectors.
	if after := runMetricValues(t, f.server, run.ID); !reflect.DeepEqual(before, after) {
		t.Fatal("late ingress exposed an uncommitted partial historical projection")
	}
	if err := f.server.computeWorkerSummary(t.Context(), run.ID); err != nil {
		t.Fatal(err)
	}
	after := runMetricValues(t, f.server, run.ID)
	var seen bool
	for key, value := range after {
		if strings.Contains(key, `event_type="late_event"`) {
			if value.value != 1 {
				t.Fatalf("duplicate canonical event counted %g times", value.value)
			}
			seen = true
		}
	}
	if !seen {
		t.Fatal("queued projection omitted the late event")
	}
	for key, value := range before {
		if after[key] != value {
			t.Fatalf("canonical correction changed prior series %s: %v -> %v", key, value, after[key])
		}
	}
	if len(f.server.state.workerSummaries) != 0 || len(f.server.state.metrics.sealed) != 0 {
		t.Fatal("late correction retained historical payload in service RAM")
	}
}

func TestWorkerRecoveryDrainACKClearsOnlyDeliveryDebtBeforeRetirement(t *testing.T) {
	f := newControllerWorkerFixture(t)
	const id = "recovered-delivery-debt"
	experiment, _ := resultFixture(t, f.server, id, "failed", time.Now().Add(-time.Minute))
	experiment.TelemetryPending = true
	experiment.DataState = "incomplete"
	experiment.IntegrityError = "original scientific evidence was incomplete"
	experiment.CleanupState = "failed"
	f.server.state.mu.Lock()
	f.server.state.experiments[id] = experiment
	f.server.state.mu.Unlock()
	if err := f.server.cleanupBeforeResume(t.Context(), []string{id}, scenario.Scenario{JobShutdownTimeout: "5s"}); err != nil {
		t.Fatal(err)
	}
	f.server.state.mu.RLock()
	recovered := f.server.state.experiments[id]
	f.server.state.mu.RUnlock()
	if recovered.TelemetryPending || recovered.DataState != experiment.DataState || recovered.IntegrityError != experiment.IntegrityError || recovered.State != "failed" {
		t.Fatalf("delivery ACK erased evidence or left a permanent barrier: %+v", recovered)
	}
	if _, ok := f.server.state.workerMetricSummary(id); !ok {
		t.Fatal("crash recovery omitted the durable scientific projection")
	}
	if err := f.server.verifyRetiredRuntime(id); err != nil {
		t.Fatal(err)
	}
}
