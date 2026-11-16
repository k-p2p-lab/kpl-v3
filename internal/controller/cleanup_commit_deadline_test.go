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

// A confirmed Agent response is not permission to wait past cancellation on
// unrelated persistence. Keep both delivery debt and durable cleanup ownership
// until their respective metadata transaction has actually started.
func TestCleanupCommitWaitHonorsCancellation(t *testing.T) {
	for _, stage := range []string{"delivery-ack", "terminal-proof", "cleanup-metadata"} {
		for _, mode := range []string{"cancel", "deadline"} {
			t.Run(stage+"/"+mode, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: stage != "cleanup-metadata"}, nil)
				const id = "cleanup-commit-deadline"
				locked := make(chan struct{})
				var once sync.Once
				var owner model.Agent
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if stage == "delivery-ack" && r.Method == http.MethodPost || stage != "delivery-ack" && r.Method == http.MethodGet {
						once.Do(func() { s.state.persistMu.Lock(); close(locked) })
					}
					if r.Method == http.MethodGet {
						report := owner
						report.LastSeen = time.Now().UTC()
						_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: report})
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer api.Close()
				owner = model.Agent{ID: "commit-owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true, StartupReconciled: true}
				s.state.agents[owner.ID] = owner
				run := model.Experiment{ID: id, State: "failed", CleanupState: "retained", TelemetryPending: true, DataState: "incomplete", Error: "original scientific failure", StartedAt: time.Now().Add(-time.Minute)}
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
				manifestPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json")
				manifest, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				want := context.Canceled
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 750*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- s.waitQueueCleanup(ctx, "", scenario.Scenario{JobShutdownTimeout: "5s"}) }()
				select {
				case <-locked:
				case err := <-done:
					t.Fatalf("cleanup returned before the Agent response: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("cleanup did not reach its Agent response")
				}
				held, returned := true, false
				defer func() {
					cancel()
					if held {
						s.state.persistMu.Unlock()
					}
					if !returned {
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Error("cleanup did not leave during teardown")
						}
					}
				}()
				retirementReviewWait(t, "cleanup did not reach the persistence wait after the Agent response", func() bool {
					stacks := make([]byte, 2<<20)
					n := runtime.Stack(stacks, true)
					for _, stack := range bytes.Split(stacks[:n], []byte("\n\n")) {
						if bytes.Contains(stack, []byte("(*Server).waitQueueCleanup")) && bytes.Contains(stack, []byte("semaphore.(*Weighted).Acquire")) {
							return true
						}
					}
					return false
				})
				if mode == "cancel" {
					cancel()
				}
				<-ctx.Done()
				select {
				case err := <-done:
					returned = true
					if !errors.Is(err, want) {
						t.Fatalf("cleanup lost its cancellation: %v", err)
					}
				case <-time.After(400 * time.Millisecond):
					t.Fatal("cleanup ignored cancellation behind a post-ACK persistence wait")
				}
				s.state.mu.RLock()
				current := s.state.experiments[id]
				s.state.mu.RUnlock()
				if current.State != run.State || current.Error != run.Error || current.CleanupState != "retained" || current.DataState != "incomplete" || stage != "terminal-proof" && !current.TelemetryPending {
					t.Fatalf("canceled metadata wait changed uncommitted state: %+v", current)
				}
				currentManifest, err := os.ReadFile(manifestPath)
				if stage != "terminal-proof" && (err != nil || !bytes.Equal(manifest, currentManifest)) {
					t.Fatalf("canceled metadata wait changed durable state: %v", err)
				}
				if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || !slices.Contains(s.queueCleanup, id) {
					t.Fatalf("canceled metadata wait released cleanup ownership: marker=%v queue=%v", err, s.queueCleanup)
				}
				s.state.persistMu.Unlock()
				held = false
			})
		}
	}
}

func TestQueueCleanupRetainsBarrierOnMetadataWriteFailure(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "cleanup-metadata-failure"
	manifestPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json")
	var owner model.Agent
	var once sync.Once
	var injected error
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			once.Do(func() {
				injected = os.Rename(manifestPath, manifestPath+".saved")
				if injected == nil {
					injected = os.Mkdir(manifestPath, 0700)
				}
			})
			report := owner
			report.LastSeen = time.Now().UTC()
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: report})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	owner = model.Agent{ID: "metadata-failure-owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
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
	err := s.waitQueueCleanup(t.Context(), "", scenario.Scenario{JobShutdownTimeout: "1s"})
	if injected != nil {
		t.Fatalf("could not inject metadata failure: %v", injected)
	}
	if err == nil {
		t.Fatal("cleanup reported success after its metadata write failed")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || !slices.Contains(s.queueCleanup, id) {
		t.Fatalf("failed metadata write released cleanup ownership: marker=%v queue=%v", err, s.queueCleanup)
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(manifestPath+".saved", manifestPath); err != nil {
		t.Fatal(err)
	}
	if err := s.waitQueueCleanup(t.Context(), "", scenario.Scenario{JobShutdownTimeout: "1s"}); err != nil {
		t.Fatalf("cleanup could not recover after metadata storage was repaired: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); !errors.Is(err, os.ErrNotExist) || len(s.queueCleanup) != 0 {
		t.Fatalf("successful retry retained a stale cleanup barrier: marker=%v queue=%v", err, s.queueCleanup)
	}
}

func TestQueueCleanupEntryHonorsCanceledAdmissionLock(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "cleanup-entry-deadline"
			s.queueCleanup = []string{id}
			if err := s.writePendingWorkCleanup(id); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			s.cancelMu.Lock()
			held, returned := true, false
			done := make(chan error, 1)
			go func() { done <- s.waitQueueCleanup(ctx, "", scenario.Scenario{}) }()
			defer func() {
				cancel()
				if held {
					s.cancelMu.Unlock()
				}
				if !returned {
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("cleanup entry did not leave during teardown")
					}
				}
			}()
			select {
			case err := <-done:
				returned = true
				t.Fatalf("cleanup bypassed the occupied admission lock: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if mode == "cancel" {
				cancel()
			}
			<-ctx.Done()
			select {
			case err := <-done:
				returned = true
				if !errors.Is(err, want) {
					t.Fatalf("cleanup entry lost cancellation: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("cleanup entry ignored cancellation while admission remained occupied")
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || !slices.Contains(s.queueCleanup, id) {
				t.Fatalf("canceled entry released its cleanup obligation: marker=%v queue=%v", err, s.queueCleanup)
			}
			s.cancelMu.Unlock()
			held = false
		})
	}
}
