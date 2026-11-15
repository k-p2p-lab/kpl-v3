package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestQueueCleanupCanceledObservationPreservesRetainedResult(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "retained-observation-cancel"
	inventory := make(chan struct{})
	var once sync.Once
	var owner model.Agent
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			once.Do(func() { s.state.persistMu.Lock(); close(inventory) })
			report := owner
			report.LastSeen = time.Now().UTC()
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: report})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	owner = model.Agent{ID: "retained-owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
	s.state.agents[owner.ID] = owner
	run := model.Experiment{ID: id, State: "completed", CleanupState: "retained", DataState: "collecting", StartedAt: time.Now().Add(-time.Minute)}
	s.state.experiments[id] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	if err := s.rememberRunAgent(id, owner); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json")
	manifest, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	s.queueCleanup = []string{id}
	if err := s.writePendingWorkCleanup(id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.waitQueueCleanup(ctx, "", scenario.Scenario{JobShutdownTimeout: "1s"}) }()
	select {
	case <-inventory:
	case err := <-done:
		t.Fatalf("cleanup exited before its terminal inventory: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not reach its terminal inventory")
	}
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	retirementReviewWait(t, "retained cleanup did not enter observation sealing", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("(*state).recordAnalysisObservation"))
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("retained observation cleanup lost its cancellation: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		s.state.persistMu.Unlock()
		locked = false
		lateErr := <-done
		s.state.mu.RLock()
		late := s.state.experiments[id]
		s.state.mu.RUnlock()
		t.Fatalf("retained cleanup ignored observation cancellation: error=%v state=%s data=%s integrity=%q", lateErr, late.State, late.DataState, late.IntegrityError)
	}
	s.state.mu.RLock()
	current := s.state.experiments[id]
	s.state.mu.RUnlock()
	if current.State != "completed" || current.DataState != "collecting" || current.CleanupState != "retained" || current.IntegrityError != "" {
		t.Fatalf("canceled pre-write sealing changed the retained scientific result: %+v", current)
	}
	currentManifest, err := os.ReadFile(manifestPath)
	if err != nil || !bytes.Equal(currentManifest, manifest) {
		t.Fatalf("canceled pre-write sealing changed durable metadata: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || !slices.Contains(s.queueCleanup, id) {
		t.Fatalf("canceled pre-write sealing discarded its retry obligation: %v %v", err, s.queueCleanup)
	}
	s.state.persistMu.Unlock()
	locked = false
	if err := s.waitQueueCleanup(t.Context(), "", scenario.Scenario{JobShutdownTimeout: "1s"}); err != nil {
		t.Fatalf("retry did not complete retained data sealing: %v", err)
	}
	s.state.mu.RLock()
	current = s.state.experiments[id]
	s.state.mu.RUnlock()
	if current.State != "completed" || current.DataState != "complete" || current.CleanupState != "complete" || current.IntegrityError != "" || len(s.queueCleanup) != 0 {
		t.Fatalf("successful retry changed scientific success or retained cleanup: %+v queue=%v", current, s.queueCleanup)
	}
}

// A real Agent inventory response completes Peer cleanup while an unrelated
// admission owns cancelMu. Cancellation must still retire the waiter without
// discarding the durable cleanup obligation that it could not commit.
func TestQueueCleanupCompletionHonorsCanceledAdmissionLock(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "cleanup-completion-cancel"
			inventory := make(chan struct{})
			var once sync.Once
			var owner model.Agent
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					once.Do(func() { s.cancelMu.Lock(); close(inventory) })
					report := owner
					report.LastSeen = time.Now().UTC()
					_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: report})
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer api.Close()
			owner = model.Agent{ID: "cleanup-owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now()}
			s.state.agents[owner.ID] = owner
			run := model.Experiment{ID: id, State: "failed", CleanupState: "retained", DataState: "incomplete", StartedAt: time.Now().Add(-time.Minute)}
			s.state.experiments[id] = run
			if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			if err := s.rememberRunAgent(id, owner); err != nil {
				t.Fatal(err)
			}
			s.queueCleanup = []string{id}
			if err := s.writePendingWorkCleanup(id); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 250*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.waitQueueCleanup(ctx, "", scenario.Scenario{JobShutdownTimeout: "1s"}) }()
			select {
			case <-inventory:
			case err := <-done:
				t.Fatalf("cleanup exited before the terminal inventory: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("cleanup did not reach the actual Agent inventory")
			}
			locked := true
			defer func() {
				if locked {
					s.cancelMu.Unlock()
				}
			}()
			retirementReviewWait(t, "cleanup did not commit terminal Peer evidence", func() bool {
				s.state.mu.RLock()
				defer s.state.mu.RUnlock()
				return s.state.experiments[id].CleanupState == "complete"
			})
			if mode == "cancel" {
				cancel()
			}
			<-ctx.Done()
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("cleanup completion lost its context error: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				s.cancelMu.Unlock()
				locked = false
				<-done
				t.Fatal("cleanup completion ignored cancellation behind the admission lock")
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil {
				t.Fatalf("canceled completion discarded its durable cleanup marker: %v", err)
			}
			if !slices.Contains(s.queueCleanup, id) {
				t.Fatal("canceled completion discarded its live cleanup barrier")
			}
			s.cancelMu.Unlock()
			locked = false
			if err := s.waitQueueCleanup(t.Context(), "", scenario.Scenario{JobShutdownTimeout: "1s"}); err != nil {
				t.Fatalf("retry did not finish the acknowledged cleanup: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); !errors.Is(err, os.ErrNotExist) || len(s.queueCleanup) != 0 {
				t.Fatalf("successful retry retained the cleanup obligation: marker=%v queue=%v", err, s.queueCleanup)
			}
		})
	}
}
