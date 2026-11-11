package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type queueTestWork struct {
	slot     *workSlot
	entered  chan struct{}
	release  chan struct{}
	done     chan error
	finished chan struct{}
	once     sync.Once
}

func queueTestEnqueue(t *testing.T, s *Server, kind string) *queueTestWork {
	t.Helper()
	return queueTestEnqueueWork(t, s, kind, kind == "analysis" || kind == "batch-analysis")
}

func queueTestEnqueueWork(t *testing.T, s *Server, kind string, resultAnalysis bool) *queueTestWork {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	work := &queueTestWork{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1), finished: make(chan struct{})}
	s.cancelMu.Lock()
	work.slot = s.enqueueWorkSlotLocked(ctx, model.WorkQueueItem{Kind: kind, Name: kind}, cancel, resultAnalysis)
	s.cancelMu.Unlock()
	t.Cleanup(func() {
		cancel()
		work.once.Do(func() { close(work.release) })
		queueTestAwait(t, work.finished, "worker cleanup")
	})
	go func() {
		defer close(work.finished)
		defer s.finishWork(work.slot)
		err := s.waitWork(ctx, work.slot)
		if err == nil {
			close(work.entered)
			// Deliberately model cleanup that must finish even after cancellation.
			<-work.release
			err = ctx.Err()
		}
		work.done <- err
	}()
	return work
}

func queueTestAwait(t *testing.T, ready <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal(name + " did not enter its FIFO slot")
	}
}

func TestWorkQueueSerializesExperimentsAndBothAnalysisKinds(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	first := queueRun(t, s, queueLongScenario, 1)
	t.Cleanup(func() { _ = s.StopScenario(first.ID) })
	queueAwait(t, s, first.ID, func(run model.Experiment) bool { return run.Phase == 1 })
	analysis := queueTestEnqueue(t, s, "analysis")
	second := queueRun(t, s, queueLongScenario, 2)
	t.Cleanup(func() { _ = s.StopScenario(second.ID) })
	batch := queueTestEnqueue(t, s, "batch-analysis")
	if second.QueuePosition != 2 || second.Timing != nil {
		t.Fatalf("analysis predecessor was omitted from FIFO/ETA: %+v", second)
	}
	select {
	case <-analysis.entered:
		t.Fatal("analysis ran during an experiment")
	default:
	}
	if err := s.StopScenario(first.ID); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, analysis.entered, "analysis")
	if run := queueAwait(t, s, second.ID, func(run model.Experiment) bool { return run.QueuePosition == 1 }); run.State != "queued" {
		t.Fatalf("experiment ran during analysis: %+v", run)
	}
	analysis.once.Do(func() { close(analysis.release) })
	queueAwait(t, s, second.ID, func(run model.Experiment) bool { return run.State == "running" && run.Phase == 1 })
	projected, exists := s.state.experimentWithTiming(second.ID)
	if !exists || projected.Timing == nil {
		t.Fatalf("experiment ETA did not recover after preceding analysis: %+v", projected)
	}
	select {
	case <-batch.entered:
		t.Fatal("batch analysis interrupted a repetition sequence")
	default:
	}
	if err := s.StopScenario(second.ID); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, batch.entered, "batch analysis")
	batch.once.Do(func() { close(batch.release) })
	s.runs.Wait()
	s.workWorkers.Wait()
	if got := s.state.snapshot().WorkQueue; len(got) != 0 {
		t.Fatalf("finished FIFO retained live work: %+v", got)
	}
}

func TestWorkQueueCancellationRemovesWaitersAndRetainsRunningCleanup(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	head := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, head.entered, "head")
	s.updateWorkStatus(head.slot.id, "analyzing", 37.5)
	waiting := queueTestEnqueue(t, s, "batch-analysis")
	third := queueTestEnqueue(t, s, "analysis")
	if err := s.cancelWork(waiting.slot.id); err != nil {
		t.Fatal(err)
	}
	if position, ok := s.workPosition(third.slot.id); !ok || position != 1 {
		t.Fatalf("canceled waiter held its FIFO slot: %d %v", position, ok)
	}
	if err := <-waiting.done; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued work was not canceled: %v", err)
	}
	if err := s.cancelWork(waiting.slot.id); err != nil {
		t.Fatalf("duplicate cancel was unsafe: %v", err)
	}
	if err := s.cancelWork("unknown-work"); !errors.Is(err, errWorkNotFound) {
		t.Fatalf("unknown work did not return its sentinel: %v", err)
	}
	if err := s.cancelWork(head.slot.id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-third.entered:
		t.Fatal("cancel released a running slot before cleanup finished")
	default:
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 2 || queue[0].ID != head.slot.id || queue[0].Phase != "Stopping" || queue[0].Progress != 37.5 {
		t.Fatalf("running cancellation lost its place: %+v", queue)
	}
	head.once.Do(func() { close(head.release) })
	queueTestAwait(t, third.entered, "next after cleanup")
	third.once.Do(func() { close(third.release) })
	s.workWorkers.Wait()
}

func TestWorkQueueParallelAnalysisKeepsDistinctWorkersAndStopsAtExperiment(t *testing.T) {
	endpoints := []string{"http://analysis-worker-1:8093", "http://analysis-worker-2:8093"}
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisWorkerURLs: endpoints}, nil)
	first := queueTestEnqueue(t, s, "experiment")
	local := queueTestEnqueue(t, s, "analysis")
	remoteOne := queueTestEnqueue(t, s, "batch-analysis")
	remoteTwo := queueTestEnqueue(t, s, "analysis")
	internal := queueTestEnqueueWork(t, s, "analysis", false)
	next := queueTestEnqueue(t, s, "experiment")
	later := queueTestEnqueue(t, s, "batch-analysis")
	queueTestAwait(t, first.entered, "first experiment")
	first.once.Do(func() { close(first.release) })
	queueTestAwait(t, local.entered, "Controller analysis")
	queueTestAwait(t, remoteOne.entered, "first worker")
	queueTestAwait(t, remoteTwo.entered, "second worker")
	if local.slot.analysisWorkerURL != "" || remoteOne.slot.analysisWorkerURL != endpoints[0] || remoteTwo.slot.analysisWorkerURL != endpoints[1] {
		t.Fatal("analysis jobs did not receive distinct workers")
	}
	if err := s.cancelWork(remoteTwo.slot.id); err != nil {
		t.Fatal(err)
	}
	local.once.Do(func() { close(local.release) })
	remoteOne.once.Do(func() { close(remoteOne.release) })
	queueTestAwait(t, local.finished, "Controller analysis cleanup")
	queueTestAwait(t, remoteOne.finished, "first worker cleanup")
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 4 || queue[0].ID != remoteTwo.slot.id || queue[0].Phase != "Stopping" || queue[1].State != "queued" || queue[2].State != "queued" || queue[3].State != "queued" {
		t.Fatalf("experiment or a later analysis overtook active cleanup: %+v", queue)
	}
	remoteTwo.once.Do(func() { close(remoteTwo.release) })
	queueTestAwait(t, internal.entered, "internal analysis barrier")
	if internal.slot.analysisWorkerURL != "" || internal.slot.analysisLeader {
		t.Fatal("internal analysis was dispatched to the worker pool")
	}
	internal.once.Do(func() { close(internal.release) })
	queueTestAwait(t, next.entered, "next experiment")
	select {
	case <-later.entered:
		t.Fatal("analysis overtook the experiment barrier")
	default:
	}
	next.once.Do(func() { close(next.release) })
	queueTestAwait(t, later.entered, "analysis after the experiment")
}

func TestWorkQueueSnapshotDoesNotExposeMutableStartedTime(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	head := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, head.entered, "head")
	view := s.state.snapshot().WorkQueue
	started := *view[0].StartedAt
	*view[0].StartedAt = time.Time{}
	view[0].Name = "overwritten"
	current := s.state.snapshot().WorkQueue
	if current[0].Name != "analysis" || !current[0].StartedAt.Equal(started) {
		t.Fatalf("snapshot mutation changed queue state: %+v", current)
	}
	head.once.Do(func() { close(head.release) })
	s.workWorkers.Wait()
}

func TestAnalysisCancellationKeepsFailedPeerCleanupBarrier(t *testing.T) {
	var healthy atomic.Bool
	var once sync.Once
	fenced := make(chan struct{})
	var agent model.Agent
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			once.Do(func() { close(fenced) })
			if !healthy.Load() {
				w.WriteHeader(503)
				return
			}
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
			return
		}
		w.WriteHeader(204)
	}))
	t.Cleanup(api.Close)
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	run := model.Experiment{ID: "failed-work", State: "failed", StartedAt: time.Now(), CleanupState: "failed", DataState: "incomplete", IntegrityError: "original data failure"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	if err := s.rememberRunAgent(run.ID, agent); err != nil {
		t.Fatal(err)
	}
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(run.ID)
	s.cancelMu.Unlock()
	analysis := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, fenced, "failed cleanup")
	if err := s.cancelWork(analysis.slot.id); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, analysis.finished, "canceled analysis")
	select {
	case <-analysis.entered:
		t.Fatal("analysis ran despite failed Peer cleanup")
	default:
	}
	s.cancelMu.Lock()
	barriers := append([]string(nil), s.queueCleanup...)
	s.cancelMu.Unlock()
	if len(barriers) != 1 || barriers[0] != run.ID {
		t.Fatalf("cancel discarded an unresolved cleanup barrier: %v", barriers)
	}
	healthy.Store(true)
	next := queueRun(t, s, queueLongScenario, 1)
	t.Cleanup(func() { _ = s.StopScenario(next.ID); s.runs.Wait() })
	queueAwait(t, s, next.ID, func(run model.Experiment) bool { return run.Phase == 1 })
	s.state.mu.RLock()
	previous := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if previous.CleanupState != "complete" || previous.DataState != "incomplete" || previous.IntegrityError != "original data failure" {
		t.Fatalf("cleanup recovery erased incomplete evidence: %+v", previous)
	}
	_ = s.StopScenario(next.ID)
	s.runs.Wait()
	s.workWorkers.Wait()
}

func TestAnalysisWorkWaitsForRetainedPeerCleanupAndSealsEvidence(t *testing.T) {
	var agent model.Agent
	fencing, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			once.Do(func() { close(fencing); <-release })
			w.WriteHeader(204)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
		default:
			w.WriteHeader(204)
		}
	}))
	t.Cleanup(api.Close)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	run := model.Experiment{ID: "retained-work", State: "completed", StartedAt: time.Now(), CleanupState: "retained", DataState: "collecting"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	if err := s.rememberRunAgent(run.ID, agent); err != nil {
		t.Fatal(err)
	}
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(run.ID)
	s.cancelMu.Unlock()
	analysis := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, fencing, "retained Peer fence")
	select {
	case <-analysis.entered:
		t.Fatal("analysis overlapped a retained Peer")
	default:
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 1 || queue[0].Phase != "Waiting for previous Peer cleanup" {
		t.Fatalf("cleanup wait was not visible: %+v", queue)
	}
	close(release)
	queueTestAwait(t, analysis.entered, "analysis after retained cleanup")
	s.state.mu.RLock()
	sealed := s.state.experiments[run.ID]
	count := len(s.state.experiments)
	s.state.mu.RUnlock()
	if sealed.CleanupState != "complete" || sealed.DataState != "complete" || count != 1 {
		t.Fatalf("retained evidence was not sealed or synthetic Run was added: %+v count=%d", sealed, count)
	}
	analysis.once.Do(func() { close(analysis.release) })
	s.workWorkers.Wait()
}
