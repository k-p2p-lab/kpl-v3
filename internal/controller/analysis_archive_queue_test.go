package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestQueuedAnalysisDoesNotPreventArchiveOrLoseItsSnapshot(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "queued-archive"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "retained")}}); err != nil {
		t.Fatal(err)
	}
	release := holdExecutionQueue(t, s)
	if _, err := s.startAnalysisJob(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	if status, _ := s.analysisJobStatus(id); status.State != "queued" || !status.SnapshotAt.IsZero() || len(s.analysisSlots) != 0 {
		t.Fatalf("queued analysis captured data before execution: %+v", status)
	}
	release()
	s.analysisWorkers.Wait()
	status, _ := s.analysisJobStatus(id)
	response := resultRequest(s, http.MethodGet, status.ResultURL)
	var analysis resultAnalysis
	if status.State != "completed" || response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &analysis) != nil || analysis.EventCount != 1 {
		t.Fatalf("archiving a queued source changed its analysis: %+v %d %s", status, response.Code, response.Body)
	}
}

func TestArchiveDuringAnalysisSourceResolutionKeepsPinnedTailAndPreview(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "loading-archive"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "first")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startAnalysisJob(context.Background(), id, false); err != nil {
		t.Fatal(err)
	}
	s.analysisWorkers.Wait()
	first, _ := s.analysisJobStatus(id)
	archiveTestRun(t, s, id)
	previewURL := first.ResultURL + "&saved=1"
	before := resultRequest(s, http.MethodGet, first.ResultURL)
	if before.Code != 200 {
		t.Fatal(before.Body)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "late")}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	s.archiveIOCheck = func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
	}
	t.Cleanup(func() { once.Do(func() { close(release) }); s.analysisWorkers.Wait() })
	if _, err := s.startAnalysisJob(context.Background(), id, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("analysis did not pause while resolving its archived prefix")
	}
	archiveTestRun(t, s, id)
	if preview := resultRequest(s, http.MethodGet, previewURL); preview.Code != 200 || preview.Body.String() != before.Body.String() {
		t.Fatalf("archive transition lost the completed preview: %d %s", preview.Code, preview.Body)
	}
	once.Do(func() { close(release) })
	s.analysisWorkers.Wait()
	status, _ := s.analysisJobStatus(id)
	response := resultRequest(s, http.MethodGet, status.ResultURL)
	var analysis resultAnalysis
	if status.State != "completed" || response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &analysis) != nil || analysis.EventCount != 2 {
		t.Fatalf("archive transition lost its pinned local tail: %+v %d %s", status, response.Code, response.Body)
	}
}

func TestRunningAnalysisStillDefersArchiveDuringArtifactStaging(t *testing.T) {
	f := newAnalysisStorageFixture(t, false)
	value, done := holdAnalysisSave(t, f.save)
	if err := f.server.archiveRun(context.Background(), "run", 0); !errors.Is(err, errResultBusy) {
		t.Fatalf("archive touched an active analysis artifact: %v", err)
	}
	if err := finishAnalysisSave(t, value, done); err != nil {
		t.Fatal(err)
	}
}
