package controller

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAnalysisAdmissionDoesNotHoldControlLocksDuringArchivedRead(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "archived", "completed", time.Now().UTC())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "event")}}); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, "archived")

	entered, release, admitted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	s.archiveIOCheck = func() { enterOnce.Do(func() { close(entered); <-release }) }
	ctx, cancel := context.WithCancel(context.Background())
	var status analysisJobStatus
	var startErr error
	go func() {
		defer close(admitted)
		status, startErr = s.startAnalysisJob(ctx, "archived", false)
	}()
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case <-admitted:
		case <-time.After(2 * time.Second):
			t.Error("analysis admission did not finish after releasing the archive")
			return
		}
		done := make(chan struct{})
		go func() { s.analysisWorkers.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("analysis worker did not finish after cancellation")
		}
	})
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("analysis did not reach the archived source")
	}
	select {
	case <-admitted:
		if startErr != nil || status.ID == "" || status.State != "queued" {
			t.Fatalf("analysis admission: %+v, %v", status, startErr)
		}
	case <-time.After(time.Second):
		t.Fatal("analysis admission waited for NAS I/O while holding Controller locks")
	}
	if !s.cancelMu.TryLock() {
		t.Fatal("archived source read blocked experiment control")
	}
	s.cancelMu.Unlock()
	if !s.analysisJobMu.TryLock() {
		t.Fatal("archived source read blocked analysis status")
	}
	s.analysisJobMu.Unlock()
	if observed, err := s.analysisJobStatus("archived"); err != nil || observed.ID != status.ID {
		t.Fatalf("status unavailable while archive is blocked: %+v, %v", observed, err)
	}
}

func TestAnalysisAdmissionKeepsLocalValidationAndDefersUnavailableArchive(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		run, _ := resultFixture(t, s, "queued", "queued", time.Now().UTC())
		s.state.experiments[run.ID] = run
		release := holdExecutionQueue(t, s)
		status, err := s.startAnalysisJob(context.Background(), run.ID, false)
		if err != nil || status.State != "queued" {
			t.Fatalf("queued Run cannot reserve analysis: %+v %v", status, err)
		}
		release()
		failed := awaitAnalysisJob(t, s, run.ID)
		s.analysisWorkers.Wait()
		if failed.State != "failed" || failed.Error == "" {
			t.Fatalf("worker did not recheck an unowned queued source: %+v", failed)
		}
	})
	t.Run("invalid local metadata", func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		resultFixture(t, s, "invalid", "completed", time.Now().UTC())
		if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "invalid", "experiment.json"), []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := s.startAnalysisJob(context.Background(), "invalid", false); err == nil {
			t.Fatal("invalid local metadata was admitted")
		}
	})
	t.Run("unavailable archive", func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		resultFixture(t, s, "archived", "completed", time.Now().UTC())
		if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "event")}}); err != nil {
			t.Fatal(err)
		}
		archiveTestRun(t, s, "archived")
		if err := os.Rename(filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived"), filepath.Join(s.config.DataDir, archivedRunsDirectory, "offline")); err != nil {
			t.Fatal(err)
		}
		status, err := s.startAnalysisJob(context.Background(), "archived", false)
		if err != nil || status.State != "queued" || status.ID == "" {
			t.Fatalf("archived analysis must defer source reads to its worker: %+v, %v", status, err)
		}
		s.analysisWorkers.Wait()
		finished, err := s.analysisJobStatus("archived")
		if err != nil || finished.State != "failed" || finished.Error == "" {
			t.Fatalf("unavailable archive was not reported as a job failure: %+v, %v", finished, err)
		}
	})
}
