package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func isolatedAnalysisServer(t *testing.T) (*Server, func() []*exec.Cmd) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var mu sync.Mutex
	var commands []*exec.Cmd
	s.workerCommand = func(kind string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		mu.Lock()
		commands = append(commands, cmd)
		mu.Unlock()
		return cmd
	}
	return s, func() []*exec.Cmd { mu.Lock(); defer mu.Unlock(); return append([]*exec.Cmd(nil), commands...) }
}

func TestExportMetricsWorkerKeepsPinnedBoundaryAndSharesOneCalculation(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	resultFixture(t, s, "export-run", "completed", time.Now().Add(-time.Minute).UTC())
	if err := s.state.appendEvents(model.EventBatch{AgentID: "agent", Events: workerFixtureEvents("export-run", []model.Node{{ID: "sender"}, {ID: "receiver"}})}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), "export-run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "export-run", NodeID: "sender", EventID: "late-publish", Type: "publish", Topic: "topic", MessageID: "excluded-later", Timestamp: time.Now().Add(-time.Minute).UTC()}}}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan model.Metrics, 4)
	for range 4 {
		wg.Go(func() {
			metrics, err := snapshot.metricsCache.get(t.Context())
			if err != nil {
				t.Error(err)
				return
			}
			results <- metrics
		})
	}
	wg.Wait()
	close(results)
	for metrics := range results {
		if metrics.Published != 1 || metrics.Delivered != 1 {
			t.Fatalf("export included bytes after its boundary: %+v", metrics)
		}
	}
	started := commands()
	if len(started) != 1 || started[0].ProcessState == nil {
		t.Fatal("concurrent exports did not share and collect one calculation process")
	}
	var zip bytes.Buffer
	if err := snapshot.writeZIP(t.Context(), &zip); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, zip.Bytes())
	var exported model.Metrics
	if err := json.Unmarshal(files["metrics.json"], &exported); err != nil || exported.Published != 1 {
		t.Fatalf("ZIP did not reuse its immutable snapshot metrics: %v %+v", err, exported)
	}
	if len(commands()) != 1 || len(s.state.runMetrics) != 0 {
		t.Fatal("ZIP rebuilt a raw index or launched another calculation")
	}
}

func TestBatchAnalysisWorkerReadsMoreArchivedRunsThanNASReaderSlots(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	for index, id := range []string{"first", "second", "third", "fourth"} {
		batchFixture(t, s, "archived-batch", id, "completed", index+1, 4, 1)
		archiveTestRun(t, s, id)
	}
	if _, err := s.startBatchAnalysis(t.Context(), "archived-batch", false); err != nil {
		t.Fatal(err)
	}
	status := awaitBatch(t, s, "archived-batch")
	s.analysisWorkers.Wait()
	if status.State != "completed" {
		t.Fatalf("archived members exhausted nested reader slots: %+v", status)
	}
	started := commands()
	if len(started) != 1 || started[0].ProcessState == nil || len(s.archiveReadSlots) != 0 {
		t.Fatal("batch retained its worker or NAS reader slots")
	}
}

func TestConcurrentArchivedExportsReleaseCaptureSlotsBeforeQueuedCalculation(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	var snapshots []*resultSnapshot
	for _, id := range []string{"download-one", "download-two"} {
		resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
		if err := s.state.appendEvents(model.EventBatch{Events: workerFixtureEvents(id, []model.Node{{ID: "sender"}, {ID: "receiver"}})}); err != nil {
			t.Fatal(err)
		}
		archiveTestRun(t, s, id)
		snapshot, err := s.captureResultFilesContext(t.Context(), id, true)
		if err != nil {
			t.Fatal(err)
		}
		defer snapshot.close()
		snapshots = append(snapshots, snapshot)
	}
	if len(s.archiveReadSlots) != cap(s.archiveReadSlots) {
		t.Fatal("fixture did not occupy both streaming read slots")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, snapshot := range snapshots {
		wg.Go(func() {
			var data bytes.Buffer
			if err := snapshot.writeZIP(ctx, &data); err != nil {
				t.Error(err)
				return
			}
			if len(decodeResultZIP(t, data.Bytes())["metrics.json"]) == 0 {
				t.Error("archive omitted calculated metrics")
			}
		})
	}
	wg.Wait()
	if len(commands()) != 2 || len(s.archiveReadSlots) != 0 {
		t.Fatal("queued downloads retained their streaming leases")
	}
}

func TestExportMetricsCacheTracksObservationTimeWithoutChangingSourcePrefix(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	const id = "maturing-export"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
	events := workerFixtureEvents(id, []model.Node{{ID: "sender"}, {ID: "receiver"}})
	if err := s.state.appendEvents(model.EventBatch{Events: events}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	snapshot.exportedAt = events[0].Timestamp.Add(1500 * time.Millisecond)
	first, err := snapshot.metricsCache.get(t.Context())
	if err != nil || first.PendingPublications != 1 {
		t.Fatalf("fixture did not retain an immature delivery window: %+v %v", first, err)
	}
	snapshot.exportedAt = events[0].Timestamp.Add(10 * time.Second)
	second, err := snapshot.metricsCache.get(t.Context())
	if err != nil || second.PendingPublications != 0 {
		t.Fatalf("export cache reused an earlier observation boundary: %+v %v", second, err)
	}
	if len(commands()) != 2 {
		t.Fatal("new observation boundary did not get a fresh calculation process")
	}
}

func TestLargeAnalysisOutputHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "analysis-output-helper" {
		return
	}
	kind := os.Args[len(os.Args)-1]
	err := workerproc.Serve(context.Background(), kind, func(_ context.Context, _ string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		return func(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
			full, err := writeAnalysisProcessJSON(ctx, peer, "full", map[string]any{"version": 1, "analysisId": "large", "padding": strings.Repeat("x", 20<<20)})
			if err != nil {
				return nil, err
			}
			compact, err := writeAnalysisProcessJSON(ctx, peer, "compact", map[string]any{"version": 1, "analysisId": "large"})
			return analysisProcessResult{Outputs: []analysisOutputInfo{full, compact}}, err
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestAnalysisArtifactLargerThanIPCFrameStreamsWithoutReturningToMain(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	resultFixture(t, s, "large-output", "completed", time.Now().Add(-time.Minute).UTC())
	snapshot, err := s.captureResultFilesContext(t.Context(), "large-output", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	root, err := s.analysisDirectory("large-output")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	s.workerCommand = func(kind string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestLargeAnalysisOutputHelper$", "--", "analysis-output-helper", kind)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		return cmd
	}
	artifacts, result, err := s.computeAnalysisProcess(t.Context(), analysisProcessInput{RunID: "large-output", AnalysisID: "large", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}, []*resultSnapshot{snapshot}, root, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.discard()
	if len(result.Outputs) != 2 || artifacts.sinks["full"].bytes <= 20<<20 {
		t.Fatal("large analysis was not streamed into the pinned output file")
	}
	info, err := root.Stat(artifacts.sinks["full"].staged.temp)
	if err != nil || info.Size() != artifacts.sinks["full"].bytes {
		t.Fatalf("large stage size: %v", err)
	}
}

func TestAnalysisWorkerPublishesEquivalentArtifactsAndReapsBeforeCompletion(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	resultFixture(t, s, "isolated-analysis", "completed", time.Now().Add(-time.Minute).UTC())
	if err := s.state.appendEvents(model.EventBatch{AgentID: "agent", Events: workerFixtureEvents("isolated-analysis", []model.Node{{ID: "sender"}, {ID: "receiver"}})}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startAnalysisJob(t.Context(), "isolated-analysis", false); err != nil {
		t.Fatal(err)
	}
	status := awaitAnalysisJob(t, s, "isolated-analysis")
	if status.State != "completed" {
		t.Fatalf("analysis did not complete: %+v", status)
	}
	s.analysisWorkers.Wait()
	started := commands()
	if len(started) != 1 || started[0].ProcessState == nil {
		t.Fatalf("completion preceded child collection: commands=%d", len(started))
	}
	response := resultRequest(s, "GET", status.ResultURL)
	if response.Code != 200 {
		t.Fatalf("artifact: %d %s", response.Code, response.Body)
	}
	var actual resultAnalysis
	if err := json.Unmarshal(response.Body.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), "isolated-analysis", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	// Fix exactly the worker's immutable observation boundary for comparison.
	snapshot.exportedAt = actual.AsOf
	snapshot.result.SourceHash = actual.Result.SourceHash
	expected, err := analyzeResult(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	expected.AnalysisID = actual.AnalysisID
	var want, got any
	wantBytes, _ := json.Marshal(expected)
	gotBytes, _ := json.Marshal(actual)
	_ = json.Unmarshal(wantBytes, &want)
	_ = json.Unmarshal(gotBytes, &got)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("isolated analysis differs from the existing calculation")
	}
	if len(s.state.runMetrics) != 0 {
		t.Fatal("analysis left a raw accumulator in the main service")
	}
	if _, err := s.startAnalysisJob(t.Context(), "isolated-analysis", true); err != nil {
		t.Fatal(err)
	}
	if next := awaitAnalysisJob(t, s, "isolated-analysis"); next.State != "completed" || next.ID == status.ID {
		t.Fatalf("forced replacement failed: %+v", next)
	}
	s.analysisWorkers.Wait()
	started = commands()
	if len(started) != 2 || started[1].ProcessState == nil || started[0].Process.Pid == started[1].Process.Pid {
		t.Fatal("forced analysis did not use and collect a fresh OS process")
	}
	if temps, _ := filepath.Glob(filepath.Join(s.config.DataDir, currentRunsDirectory, "isolated-analysis", ".analysis-*.tmp")); len(temps) != 0 {
		t.Fatalf("analysis left stage files: %v", temps)
	}
}

func TestBatchAnalysisWorkerPreservesMeansProvenanceAndRunsOutsideMain(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	batchFixture(t, s, "batch", "first", "completed", 1, 4, 1)
	batchFixture(t, s, "batch", "second", "completed", 2, 4, 3)
	batchFixture(t, s, "batch", "failed", "failed", 3, 4, 9)
	if _, err := s.startBatchAnalysis(t.Context(), "batch", false); err != nil {
		t.Fatal(err)
	}
	status := awaitBatch(t, s, "batch")
	if status.State != "completed" {
		t.Fatalf("batch: %+v", status)
	}
	s.analysisWorkers.Wait()
	response := resultRequest(s, "GET", status.ResultURL)
	if response.Code != 200 {
		t.Fatalf("artifact: %d %s", response.Code, response.Body)
	}
	var actual batchAnalysisResult
	if err := json.Unmarshal(response.Body.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	if len(actual.Runs) != 2 || len(actual.Excluded) != 1 || actual.MissingRuns != 1 || actual.Reliability.Failed != 1 || actual.SourceHash != status.SourceHash || actual.AnalysisID != status.ID {
		t.Fatalf("batch provenance changed: %+v", actual)
	}
	if actual.Runs[0].Metrics.Published+actual.Runs[1].Metrics.Published != 4 {
		t.Fatal("completed-run publication counts changed")
	}
	started := commands()
	if len(started) != 1 || started[0].ProcessState == nil || len(s.state.runMetrics) != 0 {
		t.Fatal("batch calculation was retained in the main service")
	}
}

type heldWorkerSource struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	reader  io.ReaderAt
}

func (r *heldWorkerSource) ReadAt(data []byte, offset int64) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.reader.ReadAt(data, offset)
}

func TestCanceledAnalysisKeepsCleanupBarrierUntilParentRPCIsQuiescent(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	resultFixture(t, s, "blocked-analysis", "completed", time.Now().Add(-time.Minute).UTC())
	if err := s.state.persistEvents("blocked-analysis", []model.TraceEvent{{RunID: "blocked-analysis", NodeID: "node", Type: "publish", MessageID: "message", Topic: "topic", Timestamp: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), "blocked-analysis", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	held := &heldWorkerSource{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(held.release) })
	for index := range snapshot.files {
		if snapshot.files[index].name == "events.jsonl" {
			held.reader = snapshot.files[index].reader()
			snapshot.files[index].readAt = held
		}
	}
	root, err := s.analysisDirectory("blocked-analysis")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := s.computeAnalysisProcess(ctx, analysisProcessInput{RunID: "blocked-analysis", AnalysisID: "attempt", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}, []*resultSnapshot{snapshot}, root, nil, nil)
		done <- err
	}()
	select {
	case <-held.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not enter the bounded parent source operation")
	}
	cancel()
	if response := resultRequest(s, "GET", "/api/v1/health"); response.Code != 200 {
		t.Fatalf("blocked analysis prevented main health: %d", response.Code)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("analysis cancellation: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("analysis cancellation did not return its failure")
	}
	s.cancelMu.Lock()
	blocked := len(s.queueCleanup) != 0
	s.cancelMu.Unlock()
	if !blocked {
		t.Fatal("analysis released its FIFO obligation while a source operation remained alive")
	}
	releaseOnce.Do(func() { close(held.release) })
	cleanupCtx, stopCleanup := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCleanup()
	if err := s.waitQueueCleanup(cleanupCtx, "", scenario.Scenario{JobShutdownTimeout: "1s"}); err != nil {
		t.Fatalf("analysis did not recover after source I/O returned: %v", err)
	}
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if len(s.queueCleanup) != 0 {
		t.Fatal("confirmed analysis cleanup did not restore the idle queue")
	}
}
