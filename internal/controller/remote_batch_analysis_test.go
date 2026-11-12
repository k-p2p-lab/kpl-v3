package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Run the transport and a real calculation child on a loopback worker. Holding
// the handshake lets the test observe each queue while the other is busy.
func gatedAnalysisWorker(t *testing.T, s *Server) (entered <-chan struct{}, release func(), reaped func() bool) {
	t.Helper()
	workerServer, commands := isolatedAnalysisServer(t)
	token := strings.Repeat("a", 32)
	worker := newRemoteAnalysisWorker(token, s.logger, workerServer.workerCommand)
	started := make(chan struct{})
	finished := make(chan struct{})
	allow := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(allow) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != remoteAnalysisPath {
			worker.ServeHTTP(w, r)
			return
		}
		close(started)
		defer close(finished)
		select {
		case <-allow:
			worker.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { release(); server.Close() })
	s.config.ControlNodeID = "controller-host"
	s.config.AnalysisAgentToken = token
	registerAnalysisPoolAgent(t, s, analysisPoolAgent("remote-agent", "remote-host", server.URL))
	return started, release, func() bool {
		queueTestAwait(t, finished, "remote child cleanup")
		processes := commands()
		return len(processes) == 1 && processes[0].ProcessState != nil
	}
}

func TestRemoteBatchAnalysisLeavesPendingSourceInExecutionQueue(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	s.config.ControlNodeID = "controller-host"
	s.config.AnalysisAgentToken = remoteTestToken
	registerAnalysisPoolAgent(t, s, analysisPoolAgent("remote-agent", "remote-host", "http://remote-agent:9091"))
	batchFixture(t, s, "batch", "first", "completed", 1, 2, 1)
	batchFixture(t, s, "batch", "second", "queued", 2, 2, 0)
	experiment := queueTestEnqueue(t, s, "experiment")
	queueTestAwait(t, experiment.entered, "experiment")
	if _, err := s.startBatchAnalysis(t.Context(), "batch", false); err != nil {
		t.Fatal(err)
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 2 || queue[1].Worker != "" || queue[1].State != "queued" || queue[1].Position != 1 {
		t.Fatalf("unready batch bypassed the execution queue: %+v", queue)
	}
	if err := s.cancelWork(queue[1].ID); err != nil {
		t.Fatal(err)
	}
	if status := awaitBatch(t, s, "batch"); status.State != "interrupted" {
		t.Fatalf("canceled unready batch: %+v", status)
	}
}

func readBatchArtifact(t *testing.T, s *Server, url string) batchAnalysisResult {
	t.Helper()
	response := resultRequest(s, http.MethodGet, url)
	var result batchAnalysisResult
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil {
		t.Fatalf("batch artifact: %d %s", response.Code, response.Body)
	}
	return result
}

func TestRemoteBatchAnalysisRunsInAnalysisGroupAndMatchesLocalWorker(t *testing.T) {
	s, localCommands := isolatedAnalysisServer(t)
	batchFixture(t, s, "batch", "local-run", "completed", 1, 4, 1)
	batchFixture(t, s, "batch", "archived-run", "completed", 2, 4, 3)
	batchFixture(t, s, "batch", "failed-run", "failed", 3, 4, 9)
	archiveTestRun(t, s, "archived-run")
	if _, err := s.startBatchAnalysis(t.Context(), "batch", false); err != nil {
		t.Fatal(err)
	}
	local := awaitBatch(t, s, "batch")
	s.workWorkers.Wait()
	if local.State != "completed" {
		t.Fatalf("local analysis: %+v", local)
	}
	want := readBatchArtifact(t, s, local.ResultURL)
	entered, release, reaped := gatedAnalysisWorker(t, s)
	first := queueTestEnqueue(t, s, "experiment")
	queueTestAwait(t, first.entered, "first experiment")
	second := queueTestEnqueue(t, s, "experiment")
	third := queueTestEnqueue(t, s, "experiment")
	localHold := queueTestEnqueue(t, s, "analysis")
	if _, err := s.startBatchAnalysis(t.Context(), "batch", true); err != nil {
		t.Fatal(err)
	}
	after := queueTestEnqueue(t, s, "experiment")
	for i, experiment := range []*queueTestWork{first, second, third} {
		queueTestAwait(t, experiment.entered, "experiment in order")
		queue := s.state.snapshot().WorkQueue
		if len(queue) != 6-i || queue[0].ID != experiment.slot.id || queue[len(queue)-2].State != "queued" {
			t.Fatalf("analysis began before preceding experiments ended: %+v", queue)
		}
		experiment.once.Do(func() { close(experiment.release) })
	}
	queueTestAwait(t, localHold.entered, "local analysis")
	queueTestAwait(t, entered, "remote analysis alongside local analysis")
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 3 || queue[0].Worker != "Controller" || queue[0].State != "running" ||
		queue[1].Worker != "Agent remote-agent" || queue[1].WorkerAgentID != "remote-agent" || queue[1].State != "running" || queue[2].State != "queued" {
		t.Fatalf("analysis group did not open together: %+v", queue)
	}
	localHold.once.Do(func() { close(localHold.release) })
	queueTestAwait(t, localHold.finished, "local analysis completion")
	select {
	case <-after.entered:
		t.Fatal("the next experiment overtook remote analysis")
	default:
	}
	release()
	remote := awaitBatch(t, s, "batch")
	if remote.State != "completed" || remote.ID == local.ID || remote.SourceHash != local.SourceHash {
		t.Fatalf("remote analysis: %+v", remote)
	}
	got := readBatchArtifact(t, s, remote.ResultURL)
	if got.AnalysisVersion != want.AnalysisVersion || got.Aggregation != want.Aggregation || got.MissingRuns != want.MissingRuns ||
		!reflect.DeepEqual(got.Excluded, want.Excluded) || !reflect.DeepEqual(got.Summary, want.Summary) || !reflect.DeepEqual(got.Reliability, want.Reliability) {
		t.Fatalf("remote aggregation differs from local calculation: %+v vs %+v", got.Summary, want.Summary)
	}
	if len(got.Runs) != len(want.Runs) {
		t.Fatalf("remote contributor count: %d", len(got.Runs))
	}
	for i := range got.Runs {
		if got.Runs[i].Result.ID != want.Runs[i].Result.ID || got.Runs[i].Result.SourceHash != want.Runs[i].Result.SourceHash ||
			!reflect.DeepEqual(got.Runs[i].Metrics, want.Runs[i].Metrics) || !reflect.DeepEqual(got.Runs[i].Research, want.Runs[i].Research) {
			t.Fatalf("remote contributor %d differs from local analysis", i)
		}
	}
	if len(localCommands()) != 1 || !reaped() {
		t.Fatal("expected one local child and a separately reclaimed remote child")
	}
	queueTestAwait(t, after.entered, "next experiment after both analyses")
	after.once.Do(func() { close(after.release) })
	s.workWorkers.Wait()
	if len(s.state.snapshot().WorkQueue) != 0 || len(s.archiveReadSlots) != 0 {
		t.Fatal("analysis retained its work slot or archive reader")
	}
}

func TestRemoteBatchAnalysisRejectsChangedInputsAndKeepsPreviousArtifact(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	batchFixture(t, s, "batch", "first", "completed", 1, 2, 1)
	batchFixture(t, s, "batch", "second", "completed", 2, 2, 2)
	if _, err := s.startBatchAnalysis(t.Context(), "batch", false); err != nil {
		t.Fatal(err)
	}
	previous := awaitBatch(t, s, "batch")
	s.workWorkers.Wait()
	if previous.State != "completed" {
		t.Fatalf("initial analysis: %+v", previous)
	}
	oldBytes := resultRequest(s, http.MethodGet, previous.ResultURL).Body.Bytes()
	entered, release, _ := gatedAnalysisWorker(t, s)
	localHold := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, localHold.entered, "local analysis")
	if _, err := s.startBatchAnalysis(t.Context(), "batch", true); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, entered, "captured batch sent to worker")
	file, err := os.OpenFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "first", "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := json.NewEncoder(file).Encode(model.TraceEvent{RunID: "first", EventID: "late-event", Type: "publish"})
	if closeErr := file.Close(); writeErr != nil || closeErr != nil {
		t.Fatalf("append late event: %v %v", writeErr, closeErr)
	}
	release()
	failed := awaitBatch(t, s, "batch")
	localHold.once.Do(func() { close(localHold.release) })
	s.workWorkers.Wait()
	if failed.State != "failed" || !strings.Contains(failed.Error, "saved batch members changed") || failed.SavedAnalysisID != previous.ID {
		t.Fatalf("changed input published or lost saved artifact: %+v", failed)
	}
	saved := resultRequest(s, http.MethodGet, previous.ResultURL+"&saved=1")
	if saved.Code != http.StatusOK || !bytes.Equal(saved.Body.Bytes(), oldBytes) {
		t.Fatalf("previous artifact was replaced: %d", saved.Code)
	}
}

func TestRemoteSingleAnalysisUsesItsAssignedWorkerWhileControllerComputes(t *testing.T) {
	s, localCommands := isolatedAnalysisServer(t)
	batchFixture(t, s, "batch", "run", "completed", 1, 2, 2)
	entered, release, reaped := gatedAnalysisWorker(t, s)
	local := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, local.entered, "local analysis")
	// A real local single analysis retains this permit throughout computation.
	s.analysisSlots <- struct{}{}
	defer func() { <-s.analysisSlots }()
	if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, entered, "remote single analysis")
	release()
	status := awaitAnalysisJob(t, s, "run")
	if status.State != "completed" || len(localCommands()) != 0 || !reaped() {
		t.Fatalf("single analysis did not use a remote child: %+v", status)
	}
	response := resultRequest(s, http.MethodGet, status.ResultURL)
	var result resultAnalysis
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Metrics.Published != 2 {
		t.Fatalf("single analysis result: %d %s", response.Code, response.Body)
	}
}
