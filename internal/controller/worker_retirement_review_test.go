package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const retirementReviewScenario = "version: 4\nname: repeated-reset-review\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n  - {action: wait, duration: 10ms}\n"

func retirementReviewBaseline(s *Server, ids []string) error {
	for _, id := range ids {
		if err := s.verifyRetiredRuntime(id); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
	}
	s.state.mu.RLock()
	for _, id := range ids {
		if s.state.runTimings[id] != nil {
			s.state.mu.RUnlock()
			return fmt.Errorf("retired Run %s retained timing observations", id)
		}
		if _, exists := s.state.workerSummaries[id]; exists {
			s.state.mu.RUnlock()
			return fmt.Errorf("retired Run %s retained its disk-backed summary in RAM", id)
		}
	}
	counts := []int{len(s.state.runMetrics), len(s.state.journalIndices), len(s.state.nodes), len(s.state.nodeReportTimes), len(s.state.reservations), len(s.state.uncertainCreates), len(s.state.activeNodeIDs)}
	s.state.mu.RUnlock()
	for _, count := range counts {
		if count != 0 {
			return fmt.Errorf("persistent Controller retained runtime state: %v", counts)
		}
	}
	retired := make(map[string]bool, len(ids))
	for _, id := range ids {
		retired[id] = true
	}
	var initialized string
	s.state.metrics.initializedAgents.Range(func(key, _ any) bool {
		if retired[key.([2]string)[0]] {
			initialized = "Agent counter initialization"
			return false
		}
		return true
	})
	s.state.metrics.initializedTopics.Range(func(key, _ any) bool {
		if retired[key.([3]string)[0]] {
			initialized = "topic counter initialization"
			return false
		}
		return true
	})
	if initialized != "" {
		return fmt.Errorf("retired Run retained %s", initialized)
	}
	s.state.metrics.lifecycleMu.Lock()
	for _, id := range ids {
		if _, exists := s.state.metrics.sealed[id]; exists {
			s.state.metrics.lifecycleMu.Unlock()
			return fmt.Errorf("retired Run %s retained its disk-backed counters in RAM", id)
		}
	}
	s.state.metrics.lifecycleMu.Unlock()
	s.snapshotMu.Lock()
	cache := s.snapshotData != nil || s.dashboardFrame != nil || s.dashboardSummaryFrame != nil || s.snapshotFlight != nil || s.dashboardFlight != nil || s.dashboardSummaryFlight != nil || s.snapshotResetDone != nil
	s.snapshotMu.Unlock()
	if cache {
		return errors.New("persistent Controller retained a snapshot cache or producer")
	}
	s.attackTargets.mu.Lock()
	attack := len(s.attackTargets.raw) != 0 || s.attackTargets.runID != ""
	s.attackTargets.mu.Unlock()
	if attack {
		return errors.New("persistent Controller retained the previous attack-target registry")
	}
	s.workerMu.Lock()
	unreaped := len(s.unreapedWorkers)
	s.workerMu.Unlock()
	if unreaped != 0 {
		return errors.New("persistent Controller retained unreaped workers")
	}
	return filepath.WalkDir(s.config.DataDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".kpl-metadata-") || strings.HasPrefix(name, ".incoming-") || strings.HasPrefix(name, ".analysis-") && strings.HasSuffix(name, ".tmp") {
			return fmt.Errorf("Run reset retained a temporary file: %s", path)
		}
		return nil
	})
}

func TestRetirementReviewTwentyActualRunsResetBeforeEveryNextProcess(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	originalCommand := s.workerCommand
	var previous []string
	var checked int
	s.workerCommand = func(kind string) *exec.Cmd {
		if kind != "controller-run" {
			t.Errorf("unexpected work between experiment boundaries: %s", kind)
		}
		if err := retirementReviewBaseline(s, previous); err != nil {
			t.Error(err)
		}
		f.mu.Lock()
		if len(f.nodes) != 0 {
			t.Errorf("next child started with %d live Peers", len(f.nodes))
		}
		for _, command := range f.commands {
			if command.ProcessState == nil {
				t.Error("next child started before the previous OS Wait")
			}
		}
		f.mu.Unlock()
		s.state.mu.RLock()
		var current string
		for id, run := range s.state.experiments {
			if run.State == "running" {
				current = id
			}
		}
		s.state.mu.RUnlock()
		if current == "" {
			t.Error("next child has no current Run")
		}
		previous = append(previous, current)
		checked++
		// Populate the actual persistent-service cache owners for this Run.
		// Retiring it must remove these references before invoking us again.
		data := bytes.Repeat([]byte("x"), 1<<20)
		s.snapshotMu.Lock()
		s.snapshotData = data
		s.snapshotAt = time.Now()
		s.dashboardFrame = &dashboardFrame{data: data}
		s.dashboardSummaryFrame = &dashboardFrame{data: data}
		s.snapshotMu.Unlock()
		s.attackTargets.mu.Lock()
		s.attackTargets.runID, s.attackTargets.raw = current, data
		s.attackTargets.mu.Unlock()
		return originalCommand(kind)
	}
	for range 10 {
		if _, err := s.StartScenario(t.Context(), []byte(retirementReviewScenario)); err != nil {
			t.Fatal(err)
		}
		waitWorkerRuns(t, s)
	}
	if _, err := s.StartScenarioRepeated(t.Context(), []byte(retirementReviewScenario), 10); err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, s)
	if checked != 20 {
		t.Fatalf("verified only %d process boundaries", checked)
	}
	if err := retirementReviewBaseline(s, previous); err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	for _, id := range previous {
		run := s.state.experiments[id]
		if run.State != "completed" || run.CleanupState != "complete" || run.DataState != "complete" || run.TelemetryPending {
			t.Errorf("ordinary Run did not complete at its reset boundary: %+v", run)
		}
	}
	s.state.mu.RUnlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	identities := make(map[int]bool)
	for _, command := range f.commands {
		if command.ProcessState == nil || !command.ProcessState.Success() {
			t.Fatal("Run completed before collecting a successful OS worker")
		}
		if identities[command.Process.Pid] {
			t.Fatal("separate Runs reused an experiment process")
		}
		identities[command.Process.Pid] = true
	}
	if len(f.nodes) != 0 || len(f.drained) != 20 || len(identities) != 20 {
		t.Fatalf("incomplete repeated reset: live=%d drained=%d process identities=%d", len(f.nodes), len(f.drained), len(identities))
	}
}

type retirementReviewTransport struct {
	base http.RoundTripper
	hold atomic.Bool
}

func (transport *retirementReviewTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err != nil || request.Method != http.MethodGet || request.URL.Path != "/api/v1/status" || !transport.hold.Load() {
		return response, err
	}
	defer response.Body.Close()
	var heartbeat model.AgentHeartbeat
	if err := json.NewDecoder(response.Body).Decode(&heartbeat); err != nil {
		return nil, err
	}
	for i := range heartbeat.Nodes {
		delete(heartbeat.Nodes[i].Metadata, "cleanupComplete")
	}
	data, err := json.Marshal(heartbeat)
	if err != nil {
		return nil, err
	}
	response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(data)), int64(len(data))
	return response, nil
}

func retirementReviewWait(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func retirementReviewHealth(t *testing.T, s *Server) {
	t.Helper()
	response := httptest.NewRecorder()
	s.apiTestHandler(t.Context()).ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("experiment failure affected persistent service health: %d %s", response.Code, response.Body)
	}
}

func TestRetirementReviewDrain204CannotReplaceTerminalCleanupEvidence(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	transport := &retirementReviewTransport{base: http.DefaultTransport}
	transport.hold.Store(true)
	defer transport.hold.Store(false)
	s.client.Transport = transport
	first, err := s.StartScenario(t.Context(), []byte(retirementReviewScenario))
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.StartScenario(t.Context(), []byte(retirementReviewScenario))
	if err != nil {
		t.Fatal(err)
	}
	retirementReviewWait(t, "Agent never returned its terminal drain 204", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.drained[first.ID]
	})
	time.Sleep(250 * time.Millisecond)
	s.state.mu.RLock()
	state, nextState := s.state.experiments[first.ID].State, s.state.experiments[next.ID].State
	s.state.mu.RUnlock()
	if state == "completed" || nextState != "queued" {
		t.Fatalf("unconfirmed terminal inventory advanced the queue: first=%s next=%s", state, nextState)
	}
	f.mu.Lock()
	createdNext, processes := len(f.created[next.ID]), len(f.commands)
	f.mu.Unlock()
	if createdNext != 0 || processes != 1 {
		t.Fatalf("unconfirmed cleanup admitted later work: next Peers=%d processes=%d", createdNext, processes)
	}
	retirementReviewHealth(t, s)
	transport.hold.Store(false)
	waitWorkerRuns(t, s)
	if err := retirementReviewBaseline(s, []string{first.ID, next.ID}); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementReviewKilledChildKeepsFailureAndNextRunWaitsForCleanup(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	transport := &retirementReviewTransport{base: http.DefaultTransport}
	transport.hold.Store(true)
	defer transport.hold.Store(false)
	s.client.Transport = transport
	long := strings.Replace(retirementReviewScenario, "duration: 10ms", "duration: 10s", 1)
	first, err := s.StartScenario(t.Context(), []byte(long))
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.StartScenario(t.Context(), []byte(retirementReviewScenario))
	if err != nil {
		t.Fatal(err)
	}
	retirementReviewWait(t, "first Run did not create its Peers", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.created[first.ID]) == 2
	})
	f.mu.Lock()
	command := f.commands[0]
	f.mu.Unlock()
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	retirementReviewWait(t, "killed experiment worker was not recorded as failed", func() bool {
		s.state.mu.RLock()
		defer s.state.mu.RUnlock()
		return s.state.experiments[first.ID].State == "failed"
	})
	f.mu.Lock()
	createdNext := len(f.created[next.ID])
	f.mu.Unlock()
	if createdNext != 0 {
		t.Fatal("next Run started before killed worker cleanup was confirmed")
	}
	retirementReviewHealth(t, s)
	transport.hold.Store(false)
	waitWorkerRuns(t, s)
	s.state.mu.RLock()
	failed, completed := s.state.experiments[first.ID], s.state.experiments[next.ID]
	s.state.mu.RUnlock()
	if failed.State != "failed" || failed.DataState != "incomplete" || failed.CleanupState != "complete" || completed.State != "completed" || completed.CleanupState != "complete" {
		t.Fatalf("worker recovery erased failure or contaminated next Run: first=%+v next=%+v", failed, completed)
	}
	if err := retirementReviewBaseline(s, []string{first.ID, next.ID}); err != nil {
		t.Fatal(err)
	}
}

type retirementReviewBlockedJSON struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (blocked *retirementReviewBlockedJSON) MarshalJSON() ([]byte, error) {
	blocked.once.Do(func() { close(blocked.entered) })
	<-blocked.release
	return json.Marshal(strings.Repeat("x", 2<<20))
}

func TestRetirementReviewOldSnapshotProducerCannotRepublishLargeCache(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "snapshot-retirement"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
	blocked := &retirementReviewBlockedJSON{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(blocked.release) })
	s.state.mu.Lock()
	s.state.events = []model.TraceEvent{{RunID: id, Type: "publish", Fields: map[string]any{"oldSnapshot": blocked}}}
	s.state.mu.Unlock()
	readerDone := make(chan error, 1)
	go func() { _, _, err := s.streamSnapshotAtContext(t.Context(), nil); readerDone <- err }()
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot did not retain its old producer")
	}
	s.snapshotMu.Lock()
	oldEpoch := s.snapshotEpoch
	s.dashboardFrame = &dashboardFrame{data: bytes.Repeat([]byte("x"), 2<<20)}
	s.dashboardSummaryFrame = &dashboardFrame{data: bytes.Repeat([]byte("y"), 2<<20)}
	s.snapshotMu.Unlock()
	retired := make(chan error, 1)
	go func() { retired <- s.retireRunRuntime(t.Context(), id) }()
	retirementReviewWait(t, "retirement never entered its snapshot reset barrier", func() bool {
		s.snapshotMu.Lock()
		defer s.snapshotMu.Unlock()
		return s.snapshotResetDone != nil
	})
	select {
	case err := <-retired:
		t.Fatalf("retirement claimed completion with an old snapshot producer: %v", err)
	default:
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := s.streamSnapshotAtContext(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new snapshot reader bypassed the retirement barrier: %v", err)
	}
	retirementReviewHealth(t, s)
	releaseOnce.Do(func() { close(blocked.release) })
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-retired; err != nil {
		t.Fatal(err)
	}
	s.snapshotMu.Lock()
	newEpoch := s.snapshotEpoch
	s.snapshotMu.Unlock()
	if newEpoch <= oldEpoch {
		t.Fatal("Run retirement did not invalidate the old snapshot generation")
	}
	if err := retirementReviewBaseline(s, []string{id}); err != nil {
		t.Fatal(err)
	}
}

func TestRetirementReviewCanceledResetStillFencesLateSnapshotPublication(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "snapshot-canceled-retirement"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
	blocked := &retirementReviewBlockedJSON{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(blocked.release) })
	s.state.mu.Lock()
	s.state.events = []model.TraceEvent{{RunID: id, Type: "publish", Fields: map[string]any{"oldSnapshot": blocked}}}
	s.state.mu.Unlock()
	readerDone := make(chan error, 1)
	go func() { _, _, err := s.streamSnapshotAtContext(t.Context(), nil); readerDone <- err }()
	select {
	case <-blocked.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old snapshot producer never started")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := s.retireRunRuntime(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked old producer lost its reset obligation: %v", err)
	}
	releaseOnce.Do(func() { close(blocked.release) })
	if err := <-readerDone; err != nil {
		t.Fatal(err)
	}
	s.snapshotMu.Lock()
	republished := s.snapshotData != nil
	s.snapshotMu.Unlock()
	if republished {
		t.Fatal("old producer republished a large cache after the reset caller timed out")
	}
	if err := s.retireRunRuntime(t.Context(), id); err != nil {
		t.Fatalf("released producer prevented reset recovery: %v", err)
	}
	if err := retirementReviewBaseline(s, []string{id}); err != nil {
		t.Fatal(err)
	}
}
