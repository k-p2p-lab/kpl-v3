package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func holdExecutionQueue(t *testing.T, s *Server) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelMu.Lock()
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", Name: "Held experiment"}, cancel)
	s.cancelMu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { cancel(); s.finishWork(slot) }) }
	t.Cleanup(func() { release(); s.analysisWorkers.Wait() })
	return release
}

func TestAnalysisAndExperimentsFollowOneFIFO(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "first-source", "completed", time.Now())
	resultFixture(t, s, "second-source", "completed", time.Now())
	s.analysisSlots <- struct{}{}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { <-s.analysisSlots }) }
	t.Cleanup(func() { release(); s.analysisWorkers.Wait(); s.runs.Wait() })
	first, err := s.startAnalysisJob(context.Background(), "first-source", true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.startAnalysisJob(context.Background(), "second-source", false)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.StartScenario(context.Background(), []byte(queueShortScenario))
	if err != nil {
		t.Fatal(err)
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 3 || queue[0].Kind != "analysis" || queue[1].Kind != "analysis" || queue[2].Kind != "experiment" {
		t.Fatalf("work did not share FIFO admission: %+v", queue)
	}
	if status, _ := s.analysisJobStatus(second.RunID); status.State != "queued" {
		t.Fatalf("later analysis started concurrently: %+v", status)
	}
	s.state.mu.RLock()
	started := s.state.experiments[run.ID].StartedAt
	s.state.mu.RUnlock()
	if !started.IsZero() {
		t.Fatal("experiment started while analysis was blocked")
	}
	release()
	s.analysisWorkers.Wait()
	s.runs.Wait()
	a, _ := s.analysisJobStatus(first.RunID)
	b, _ := s.analysisJobStatus(second.RunID)
	s.state.mu.RLock()
	finishedRun := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if a.State != "completed" || b.State != "completed" || finishedRun.State != "completed" || b.StartedAt.Before(a.FinishedAt) || finishedRun.StartedAt.Before(b.FinishedAt) {
		t.Fatalf("work overlapped or failed: first=%+v second=%+v run=%+v", a, b, finishedRun)
	}
}

func TestQueuedRunAndBatchAnalysesCaptureCompletedSources(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			release := holdExecutionQueue(t, s)
			run, err := s.StartScenarioRepeated(context.Background(), []byte(queueShortScenario), 2)
			if err != nil {
				t.Fatal(err)
			}
			if batch {
				status, err := s.startBatchAnalysis(context.Background(), run.BatchID, true)
				if err != nil || status.State != "queued" {
					t.Fatalf("batch reservation failed: %+v %v", status, err)
				}
			} else {
				status, err := s.startAnalysisJob(context.Background(), run.ID, false)
				if err != nil || status.State != "queued" {
					t.Fatalf("single reservation failed: %+v %v", status, err)
				}
			}
			if len(s.analysisSlots) != 0 {
				t.Fatal("queued analysis captured sources before the experiment")
			}
			release()
			s.runs.Wait()
			s.analysisWorkers.Wait()
			if batch {
				status, err := s.batchAnalysisStatus(run.BatchID)
				if err != nil || status.State != "completed" || status.CompletedRuns != 2 || len(status.RunIDs) != 2 {
					t.Fatalf("batch did not reselect completed runs: %+v %v", status, err)
				}
			} else {
				status, err := s.analysisJobStatus(run.ID)
				if err != nil || status.State != "completed" {
					t.Fatalf("single did not capture completed run: %+v %v", status, err)
				}
			}
		})
	}
}

func TestLegacyAnalysisWaitCancellationKeepsSharedJob(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "legacy-source", "completed", time.Now())
	release := holdExecutionQueue(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleResultAnalysis(response, httptest.NewRequest(http.MethodGet, "/api/v1/experiments/legacy-source/analysis", nil).WithContext(ctx), "legacy-source")
	}()
	t.Cleanup(cancel)
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, err := s.analysisJobStatus("legacy-source")
		if err == nil && status.State == "queued" {
			shared, err := s.startAnalysisJob(context.Background(), "legacy-source", false)
			if err != nil || shared.ID != status.ID {
				t.Fatalf("legacy and POST did not share a job: %+v %+v %v", status, shared, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("legacy GET did not reserve a common analysis job")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("legacy HTTP wait ignored cancellation")
	}
	if status, _ := s.analysisJobStatus("legacy-source"); status.State != "queued" {
		t.Fatalf("HTTP cancellation canceled shared work: %+v", status)
	}
	release()
	s.analysisWorkers.Wait()
	if status, _ := s.analysisJobStatus("legacy-source"); status.State != "completed" {
		t.Fatalf("shared work did not complete after its caller left: %+v", status)
	}
}

func TestLegacyAnalysisCompletedCacheBypassesWaitingExperiment(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "cached", "completed", time.Now())
	if _, err := s.startAnalysisJob(context.Background(), "cached", false); err != nil {
		t.Fatal(err)
	}
	s.analysisWorkers.Wait()
	first, _ := s.analysisJobStatus("cached")
	holdExecutionQueue(t, s)
	response := httptest.NewRecorder()
	s.handleResultAnalysis(response, httptest.NewRequest(http.MethodGet, "/api/v1/experiments/cached/analysis", nil), "cached")
	var result resultAnalysis
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.AnalysisID != first.ID || response.Header().Get("Content-Disposition") != "" {
		t.Fatalf("legacy cached contract changed: %d %s", response.Code, response.Body)
	}
	if queue := s.state.snapshot().WorkQueue; len(queue) != 1 || queue[0].Kind != "experiment" {
		t.Fatalf("cached read occupied an execution slot: %+v", queue)
	}
}
