package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const summaryBoundaryScenario = "version: 4\nname: boundary-run\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n"

type summaryBoundaryTransport func(*http.Request) (*http.Response, error)

func (fn summaryBoundaryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type summaryBoundaryLogHandler struct{ handle func(slog.Record) error }

func (h summaryBoundaryLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h summaryBoundaryLogHandler) Handle(_ context.Context, record slog.Record) error {
	return h.handle(record)
}
func (h summaryBoundaryLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h summaryBoundaryLogHandler) WithGroup(string) slog.Handler      { return h }

func summaryBoundaryEvent(id string) model.TraceEvent {
	return model.TraceEvent{RunID: id, AgentID: "agent", NodeID: "late-origin", EventID: "boundary-late-" + id, Type: "boundary_late", Topic: "topic", Timestamp: time.Now().UTC()}
}
func summaryBoundaryLateCount(s *Server, id string) (float64, error) {
	summary, err := s.state.readWorkerSummary(id)
	if err != nil {
		return 0, err
	}
	for _, counter := range summary.Counters {
		if counter.Family == "events" && len(counter.Labels) == 4 && counter.Labels[2] == "boundary_late" {
			return counter.Value, nil
		}
	}
	return 0, nil
}
func summaryBoundaryQueue(t *testing.T, s *Server) []model.WorkQueueItem {
	t.Helper()
	s.state.mu.RLock()
	queue := cloneWorkQueue(s.state.workQueue)
	s.state.mu.RUnlock()
	for _, item := range queue {
		if item.Name == "Refresh run metrics" {
			t.Errorf("automatic correction created a visible queue item: %+v", item)
		}
	}
	return queue
}
func summaryBoundaryWait(t *testing.T, ready <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(8 * time.Second):
		t.Fatal(name + " did not finish")
	}
}
func summaryBoundaryAssertReleased(t *testing.T, s *Server) {
	t.Helper()
	done := make(chan struct{})
	go func() { s.workWorkers.Wait(); close(done) }()
	summaryBoundaryWait(t, done, "maintenance/FIFO ownership")
	s.cancelMu.Lock()
	active, slots, idleCancel := s.summaryRefreshActive, len(s.workSlots), s.summaryRefreshIdleCancel != nil
	s.cancelMu.Unlock()
	if active || slots != 0 || idleCancel {
		t.Fatalf("maintenance ownership survived completion: active=%t slots=%d idleCancel=%t", active, slots, idleCancel)
	}
	s.workerMu.Lock()
	unreaped := len(s.unreapedWorkers)
	s.workerMu.Unlock()
	if unreaped != 0 {
		t.Fatalf("maintenance retained unreaped Controller processes: %d", unreaped)
	}
}
func summaryBoundaryCompleteRun(t *testing.T, f *controllerWorkerFixture) model.Experiment {
	t.Helper()
	run, err := f.server.StartScenario(t.Context(), []byte(summaryBoundaryScenario))
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, f.server)
	f.server.state.mu.RLock()
	run = f.server.state.experiments[run.ID]
	f.server.state.mu.RUnlock()
	if run.State != "completed" {
		t.Fatalf("initial Run failed: %+v", run)
	}
	return run
}
func summaryBoundaryReadCreate(request *http.Request) (model.CreateNodeRequest, error) {
	var create model.CreateNodeRequest
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return create, err
	}
	request.Body = io.NopCloser(bytes.NewReader(raw))
	err = json.Unmarshal(raw, &create)
	return create, err
}

func TestRunMetricCorrectionFinishesBetweenEveryRepetition(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	var corrected atomic.Int32
	s.logger = slog.New(summaryBoundaryLogHandler{handle: func(record slog.Record) error {
		if record.Message != "experiment worker exited" {
			return nil
		}
		id := ""
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "run" {
				id = attr.Value.String()
			}
			return true
		})
		if id != "" {
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(id)}}); err != nil {
				t.Errorf("late final append: %v", err)
			}
		}
		return nil
	}})
	base := s.client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	s.client.Transport = summaryBoundaryTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == "/api/v1/nodes" {
			create, err := summaryBoundaryReadCreate(request)
			if err != nil {
				return nil, err
			}
			s.state.mu.RLock()
			current := s.state.experiments[create.RunID]
			previous := ""
			for id, run := range s.state.experiments {
				if run.BatchID == current.BatchID && run.Iteration == current.Iteration-1 {
					previous = id
					break
				}
			}
			s.state.mu.RUnlock()
			if previous != "" {
				count, err := summaryBoundaryLateCount(s, previous)
				if err != nil || count != 1 {
					t.Errorf("iteration %d created a Peer before iteration %d correction: count=%g err=%v", current.Iteration, current.Iteration-1, count, err)
				}
				corrected.Add(1)
			}
			queue := summaryBoundaryQueue(t, s)
			if len(queue) != 1 || queue[0].Kind != "experiment" {
				t.Errorf("repetition boundary split its original slot: %+v", queue)
			}
		}
		return base.RoundTrip(request)
	})
	first, err := s.StartScenarioRepeated(t.Context(), []byte(summaryBoundaryScenario), 3)
	if err != nil {
		t.Fatal(err)
	}
	waitWorkerRuns(t, s)
	for _, run := range retryMembers(t, s, first.BatchID) {
		if run.State != "completed" {
			t.Fatalf("repetition failed: %+v", run)
		}
		if count, err := summaryBoundaryLateCount(s, run.ID); err != nil || count != 1 {
			t.Fatalf("last boundary omitted correction: %s count=%g err=%v", run.ID, count, err)
		}
	}
	if corrected.Load() != 4 {
		t.Fatalf("expected two following iterations' first two Peer POSTs, got %d", corrected.Load())
	}
	summaryBoundaryAssertReleased(t, s)
}

func TestLateOldRunCorrectionDoesNotPreemptActiveRun(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	old := summaryBoundaryCompleteRun(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	base := s.client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	s.client.Transport = summaryBoundaryTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == "/api/v1/nodes" {
			create, err := summaryBoundaryReadCreate(request)
			if err != nil {
				return nil, err
			}
			if create.RunID != old.ID {
				once.Do(func() {
					close(entered)
					select {
					case <-release:
					case <-request.Context().Done():
					}
				})
			}
		}
		return base.RoundTrip(request)
	})
	current, err := s.StartScenario(t.Context(), []byte(summaryBoundaryScenario))
	if err != nil {
		t.Fatal(err)
	}
	summaryBoundaryWait(t, entered, "Run B first Peer POST")
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(old.ID)}}); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if s.tryRefreshIdleRunMetrics(t.Context(), old.ID) {
			t.Fatal("late A correction claimed CPU while B was active")
		}
	}
	if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 0 {
		t.Fatalf("A changed before B's boundary: count=%g err=%v", count, err)
	}
	queue := summaryBoundaryQueue(t, s)
	if len(queue) != 1 || queue[0].RunID != current.ID {
		t.Fatalf("late correction entered the user FIFO: %+v", queue)
	}
	releaseOnce.Do(func() { close(release) })
	waitWorkerRuns(t, s)
	if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 0 {
		t.Fatalf("B's end boundary recomputed unrelated A history: count=%g err=%v", count, err)
	}
	s.state.mu.RLock()
	dirty := s.state.summaryDirty[old.ID]
	s.state.mu.RUnlock()
	if !dirty || !s.tryRefreshIdleRunMetrics(t.Context(), old.ID) {
		t.Fatal("deferred A correction was lost or could not resume while idle")
	}
	if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 1 {
		t.Fatalf("idle correction lost A's late evidence: count=%g err=%v", count, err)
	}
	summaryBoundaryAssertReleased(t, s)
}

func TestRunMetricBoundaryUsesOnlyItsInitialDueSnapshot(t *testing.T) {
	f := newControllerWorkerFixture(t)
	s := f.server
	first := summaryBoundaryCompleteRun(t, f)
	second := summaryBoundaryCompleteRun(t, f)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(first.ID)}}); err != nil {
		t.Fatal(err)
	}
	head := queueTestEnqueue(t, s, "experiment")
	summaryBoundaryWait(t, head.entered, "original experiment slot")
	s.cancelMu.Lock()
	head.slot.batch = &repeatBatch{queuedIDs: []string{first.ID, second.ID}}
	s.cancelMu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	var gate atomic.Bool
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	original := s.workerCommand
	s.workerCommand = func(kind string) *exec.Cmd {
		if kind == "controller-summary" && gate.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return original(kind)
	}
	done := make(chan struct{})
	go func() {
		s.refreshRunMetricsBoundary(t.Context(), head.slot)
		close(done)
	}()
	summaryBoundaryWait(t, entered, "first due correction")
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(second.ID)}}); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	summaryBoundaryWait(t, done, "finite metric boundary")
	if count, err := summaryBoundaryLateCount(s, first.ID); err != nil || count != 1 {
		t.Fatalf("initial due Run was not corrected: %g %v", count, err)
	}
	if count, err := summaryBoundaryLateCount(s, second.ID); err != nil || count != 0 {
		t.Fatalf("newly dirty Run extended an existing boundary: %g %v", count, err)
	}
	if queue := summaryBoundaryQueue(t, s); len(queue) != 1 || queue[0].ID != head.slot.id {
		t.Fatalf("metric boundary replaced the experiment's original FIFO slot: %+v", queue)
	}
	s.refreshRunMetricsBoundary(t.Context(), head.slot)
	if count, err := summaryBoundaryLateCount(s, second.ID); err != nil || count != 1 {
		t.Fatalf("the next boundary lost deferred late evidence: %g %v", count, err)
	}
	head.once.Do(func() { close(head.release) })
	summaryBoundaryWait(t, head.finished, "original experiment cleanup")
	summaryBoundaryAssertReleased(t, s)
}

func TestIdleMetricMaintenanceYieldsToNewWorkWithoutReleasingBeforeCleanup(t *testing.T) {
	for _, mode := range []string{"idle-experiment-first", "idle-analysis-first"} {
		t.Run(mode, func(t *testing.T) {
			f := newControllerWorkerFixture(t)
			s := f.server
			old := summaryBoundaryCompleteRun(t, f)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(old.ID)}}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var gate atomic.Bool
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			original := s.workerCommand
			s.workerCommand = func(kind string) *exec.Cmd {
				if kind == "controller-summary" && gate.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return original(kind)
			}
			idleDone := make(chan bool, 1)
			go func() { idleDone <- s.tryRefreshIdleRunMetrics(t.Context(), old.ID) }()
			summaryBoundaryWait(t, entered, "idle correction claim")
			if queue := summaryBoundaryQueue(t, s); len(queue) != 0 {
				t.Fatalf("idle correction became a queue item: %+v", queue)
			}
			var run model.Experiment
			var job analysisJobStatus
			var err error
			if mode == "idle-analysis-first" {
				job, err = s.startAnalysisJob(t.Context(), old.ID, false)
				if err != nil {
					t.Fatal(err)
				}
			}
			run, err = s.StartScenario(t.Context(), []byte(summaryBoundaryScenario))
			if err != nil {
				t.Fatal(err)
			}
			if mode != "idle-analysis-first" {
				job, err = s.startAnalysisJob(t.Context(), old.ID, false)
				if err != nil {
					t.Fatal(err)
				}
			}
			if run.State != "queued" || !run.StartedAt.IsZero() {
				t.Fatalf("new Run clock started inside metric maintenance: %+v", run)
			}
			status, err := s.analysisJobStatus(old.ID)
			if err != nil || status.ID != job.ID || status.State != "queued" {
				t.Fatalf("analysis ran inside maintenance: %+v %v", status, err)
			}
			queue := summaryBoundaryQueue(t, s)
			if len(queue) != 2 {
				t.Fatalf("maintenance changed the visible FIFO: %+v", queue)
			}
			if mode == "idle-analysis-first" {
				if queue[0].Kind != "analysis" || queue[1].Kind != "experiment" {
					t.Fatalf("admission order changed: %+v", queue)
				}
			} else if queue[0].Kind != "experiment" || queue[1].Kind != "analysis" {
				t.Fatalf("admission order changed: %+v", queue)
			}
			s.cancelMu.Lock()
			active := s.summaryRefreshActive
			head := s.experimentQueue[0]
			opened := head.opened
			s.cancelMu.Unlock()
			if !active || opened {
				t.Fatalf("wrong idle maintenance gate: active=%t opened=%t", active, opened)
			}
			select {
			case <-head.ready:
				t.Fatal("idle correction released the new FIFO head early")
			default:
			}
			for _, item := range queue {
				if item.State != "queued" {
					t.Fatalf("idle admission displayed running work: %+v", queue)
				}
			}
			s.state.mu.RLock()
			current := s.state.experiments[run.ID]
			s.state.mu.RUnlock()
			if current.State != "queued" || !current.StartedAt.IsZero() {
				t.Fatal("Run changed state before its maintenance boundary finished")
			}
			f.mu.Lock()
			creates := len(f.created[run.ID])
			f.mu.Unlock()
			if creates != 0 {
				t.Fatal("new Peer escaped the maintenance gate")
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case claimed := <-idleDone:
				if !claimed {
					t.Fatal("idle claim was lost")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("idle correction did not release")
			}
			waitWorkerRuns(t, s)
			s.analysisWorkers.Wait()
			status, err = s.analysisJobStatus(old.ID)
			if err != nil || status.State != "completed" {
				t.Fatalf("analysis did not resume in FIFO order: %+v %v", status, err)
			}
			if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 0 {
				t.Fatalf("new work restarted the interrupted historical correction: %g %v", count, err)
			}
			s.state.mu.RLock()
			dirty := s.state.summaryDirty[old.ID]
			s.state.mu.RUnlock()
			if !dirty || !s.tryRefreshIdleRunMetrics(t.Context(), old.ID) {
				t.Fatal("interrupted correction was not retained for idle retry")
			}
			if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 1 {
				t.Fatalf("idle retry lost late evidence: %g %v", count, err)
			}
			summaryBoundaryAssertReleased(t, s)
		})
	}
}

func TestBoundarySummaryFailureHelper(t *testing.T) {
	if len(os.Args) > 1 && os.Args[len(os.Args)-1] == "boundary-summary-failure" {
		os.Exit(31)
	}
}

func TestCanceledOrFailedIdleCorrectionKeepsSavedProjectionAndReleasesOwnership(t *testing.T) {
	for _, mode := range []string{"canceled", "worker-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newControllerWorkerFixture(t)
			s := f.server
			old := summaryBoundaryCompleteRun(t, f)
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, old.ID, workerSummaryFile)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{summaryBoundaryEvent(old.ID)}}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			original := s.workerCommand
			s.workerCommand = func(kind string) *exec.Cmd {
				if kind == "controller-summary" {
					once.Do(func() { close(entered) })
					<-release
					if mode == "worker-failure" {
						cmd := exec.Command(os.Args[0], "-test.run=^TestBoundarySummaryFailureHelper$", "--", "boundary-summary-failure")
						cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
						return cmd
					}
				}
				return original(kind)
			}
			done := make(chan bool, 1)
			go func() { done <- s.tryRefreshIdleRunMetrics(ctx, old.ID) }()
			summaryBoundaryWait(t, entered, "failing correction start")
			if queue := summaryBoundaryQueue(t, s); len(queue) != 0 {
				t.Fatalf("correction failure entered the FIFO: %+v", queue)
			}
			if mode == "canceled" {
				cancel()
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case claimed := <-done:
				if !claimed {
					t.Fatal("maintenance was never claimed")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("failed correction retained ownership")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed correction replaced the saved scientific projection: %v", err)
			}
			if count, err := summaryBoundaryLateCount(s, old.ID); err != nil || count != 0 {
				t.Fatalf("failed correction exposed partial counters: %g %v", count, err)
			}
			summaryBoundaryAssertReleased(t, s)
			s.state.persistMu.Lock()
			pins, downloads := s.resultReadPins[old.ID], s.resultDownloads[old.ID]
			s.state.persistMu.Unlock()
			if pins != 0 || downloads != 0 {
				t.Fatalf("failed correction retained source ownership: pins=%d downloads=%d", pins, downloads)
			}
		})
	}
}
