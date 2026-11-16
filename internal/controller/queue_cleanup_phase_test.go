package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// Cleanup progress must reach the current iteration without acquiring the
// persistence lock. That lock can belong to unrelated durable work while an
// Agent still owns the previous Run's network, and cancellation must reach the
// outstanding HTTP request without releasing the cleanup obligation.
func TestQueueCleanupPhaseDoesNotWaitForPersistence(t *testing.T) {
	for _, state := range []string{"queued", "running", "completed"} {
		t.Run(state, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			const previous = "previous-network-run"
			entered, left := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var otherRequests atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/runs/"+previous+"/nodes" {
					otherRequests.Add(1)
					w.WriteHeader(http.StatusConflict)
					return
				}
				once.Do(func() { close(entered) })
				<-r.Context().Done()
				close(left)
			}))
			defer endpoint.Close()
			owner := model.Agent{ID: "network-only-owner", URL: endpoint.URL, StartedAt: time.Now().UTC(), RunProcesses: true, RunDrain: true, StartupReconciled: true}
			s.state.agents[owner.ID] = owner
			if err := s.rememberRunNetworkAgents(t.Context(), previous, []model.Agent{owner}); err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 8)
			var startedAt time.Time
			if state != "queued" {
				startedAt = time.Now().UTC().Add(-time.Minute)
			}
			for i := range ids {
				ids[i] = fmt.Sprintf("cleanup-phase-run-%d", i+1)
				runState := "completed"
				if i >= 5 {
					runState = "queued"
				}
				run := model.Experiment{ID: ids[i], BatchID: "cleanup-phase-run-1", ExecutionID: "cleanup-phase-execution", State: runState, Iteration: i + 1, Repetitions: 8}
				if i == 5 {
					run.State = state
					run.StartedAt = startedAt
					if state != "queued" {
						run.PhaseName = "Existing scenario phase"
					}
				}
				s.state.experiments[run.ID] = run
				if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
					t.Fatal(err)
				}
			}
			manifestPath := filepath.Join(s.config.DataDir, currentRunsDirectory, ids[5], "experiment.json")
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			ownershipPath := filepath.Join(s.config.DataDir, ownershipDirectory, previous+".json")
			ownership, err := os.ReadFile(ownershipPath)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s.cancelMu.Lock()
			s.rememberQueueCleanupLocked(previous)
			slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", RunID: ids[0], BatchID: ids[0]}, cancel)
			slot.batch = &repeatBatch{queuedIDs: ids}
			s.cancelMu.Unlock()
			defer s.finishWork(slot)
			s.state.persistMu.Lock()
			locked := true
			done := make(chan error, 1)
			go func() { done <- s.waitQueueCleanup(ctx, ids[5], scenario.Scenario{JobShutdownTimeout: "5s"}) }()
			returned := false
			defer func() {
				cancel()
				if locked {
					s.state.persistMu.Unlock()
				}
				if !returned {
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Error("cleanup waiter did not finish during test teardown")
					}
				}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("cleanup phase publication waited for persistence before reaching the Agent")
			}
			s.state.mu.RLock()
			current := s.state.experiments[ids[5]]
			anchor := s.state.experiments[ids[0]]
			queue := cloneWorkQueue(s.state.workQueue)
			dirty := s.state.summaryDirty[ids[5]]
			s.state.mu.RUnlock()
			wantPhase := "Existing scenario phase"
			if state == "queued" {
				wantPhase = "Waiting for previous Peer cleanup"
			}
			if current.PhaseName != wantPhase || current.State != state || current.Iteration != 6 || current.Repetitions != 8 || !current.StartedAt.Equal(startedAt) {
				t.Fatalf("cleanup progress changed the wrong Run state: %+v", current)
			}
			if anchor.PhaseName != "" || len(queue) != 1 || queue[0].RunID != anchor.ID || dirty {
				t.Fatalf("cleanup progress changed the batch anchor or dirtied a metric summary: anchor=%+v queue=%+v dirty=%t", anchor, queue, dirty)
			}
			cancel()
			select {
			case err := <-done:
				returned = true
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cleanup lost cancellation while persistence remained occupied: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("cleanup ignored cancellation while persistence remained occupied")
			}
			select {
			case <-left:
			case <-time.After(time.Second):
				t.Fatal("canceled cleanup retained its Agent HTTP request")
			}
			currentManifest, err := os.ReadFile(manifestPath)
			if err != nil || !bytes.Equal(manifest, currentManifest) {
				t.Fatalf("transient progress changed the durable Run manifest: %v", err)
			}
			currentOwnership, err := os.ReadFile(ownershipPath)
			if err != nil || !bytes.Equal(ownership, currentOwnership) {
				t.Fatalf("canceled cleanup changed network ownership: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, previous)); err != nil || !slices.Contains(s.queueCleanup, previous) || otherRequests.Load() != 0 {
				t.Fatalf("canceled cleanup released its barrier or advanced past fencing: marker=%v queue=%v otherRequests=%d", err, s.queueCleanup, otherRequests.Load())
			}
			s.state.persistMu.Unlock()
			locked = false
		})
	}
}
