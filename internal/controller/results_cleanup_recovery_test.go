package controller

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestDeletedRetainedResultKeepsCleanupBarrierAcrossRestart(t *testing.T) {
	for _, cleanup := range []string{"retained", "failed"} {
		t.Run(cleanup, func(t *testing.T) {
			fencing, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			var agent model.Agent
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					once.Do(func() { close(fencing); <-release })
				}
				if r.Method == http.MethodGet {
					report := agent
					report.LastSeen = time.Now().UTC()
					_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: report})
					return
				}
				w.WriteHeader(204)
			}))
			t.Cleanup(api.Close)
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			original := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
			run := model.Experiment{ID: "deleted-retained", Name: "deleted retained source", State: "completed", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now(), CleanupState: cleanup, DataState: "collecting"}
			if cleanup == "failed" {
				run.State, run.DataState = "failed", "incomplete"
			}
			original.state.experiments[run.ID] = run
			if err := original.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			if err := original.rememberRunAgent(run.ID, agent); err != nil {
				t.Fatal(err)
			}
			delete(original.state.experiments, run.ID)
			if err := original.deleteSavedResult(run.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(original.config.DataDir, currentRunsDirectory, run.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source directory was not removed: %v", err)
			}
			restarted := New(original.config, nil)
			if len(restarted.state.agents) != 0 || len(restarted.queueCleanup) != 0 {
				t.Fatal("restart fixture already had live inventory or a cleanup barrier")
			}
			analysis := queueTestEnqueue(t, restarted, "analysis")
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				analysis.once.Do(func() { close(analysis.release) })
				restarted.workWorkers.Wait()
			})
			select {
			case <-fencing:
			case <-analysis.entered:
				t.Fatal("deleting retained metadata lost the cleanup barrier after restart")
			case <-time.After(3 * time.Second):
				t.Fatal("cleanup did not recover its persisted ownership before any heartbeat")
			}
			select {
			case <-analysis.entered:
				t.Fatal("analysis overlapped retained Peer cleanup")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			queueTestAwait(t, analysis.entered, "analysis after deleted retained cleanup")
			if _, err := os.Stat(filepath.Join(original.config.DataDir, "cleanup-pending", run.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("completed cleanup retained its pending marker: %v", err)
			}
			if len(restarted.state.experiments) != 0 {
				t.Fatal("cleanup resurrected deleted experiment metadata")
			}
		})
	}
}

func TestCompletedResultDeletionDoesNotCreateCleanupBarrier(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "clean-result", State: "completed", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now(), CleanupState: "complete", DataState: "complete"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	if err := s.rememberRunAgent(run.ID, model.Agent{ID: "old-owner", URL: "http://127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSavedResult(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "cleanup-pending", run.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("normal completed deletion created a cleanup obligation: %v", err)
	}
}

func TestFailedResultDeletionRetainsCleanupBarrier(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "partial-retained", State: "completed", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now(), CleanupState: "retained", DataState: "collecting"}
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID)
	if err := os.Chmod(directory, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	err := s.deleteSavedResult(run.ID)
	if err == nil {
		t.Skip("current identity can unlink read-only directory entries")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "cleanup-pending", run.ID)); err != nil {
		t.Fatalf("failed deletion lost its retained cleanup obligation: %v", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSavedResult(run.ID); err != nil {
		t.Fatalf("partial retained deletion could not be retried: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "cleanup-pending", run.ID)); err != nil {
		t.Fatalf("successful retry discarded outstanding Peer cleanup: %v", err)
	}
}
