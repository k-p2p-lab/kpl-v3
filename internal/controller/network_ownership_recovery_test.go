package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type networkOwnershipFixture struct {
	server                      *Server
	run                         model.Experiment
	agents                      []model.Agent
	fences, managerCalls, drain atomic.Int32
	failFence, failManager      bool
}

func newNetworkOwnershipFixture(t *testing.T, id string) *networkOwnershipFixture {
	t.Helper()
	f := &networkOwnershipFixture{server: New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)}
	f.run, _ = resultFixture(t, f.server, id, "failed", time.Now().Add(-time.Minute))
	f.run.StartedAt = time.Time{}
	f.run.Agents = nil
	f.run.CleanupState = "pending"
	f.server.state.experiments[id] = f.run
	if err := f.server.persistExperiment(f.run); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		agent := model.Agent{ID: fmt.Sprintf("network-agent-%d", i), Disabled: i == 1, State: model.AgentOnline, Capacity: 1, StartedAt: time.Now().UTC(), RunProcesses: true, RunDrain: true, StartupReconciled: true}
		endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.URL.Path == "/api/v1/network/fence":
				f.fences.Add(1)
				owners, err := f.server.readRunCleanupOwnership(id)
				if err != nil || len(owners) != 2 {
					t.Errorf("network RPC preceded durable ownership: owners=%v, err=%v", owners, err)
				}
				if _, err := os.Stat(filepath.Join(f.server.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil {
					t.Errorf("network RPC preceded durable cleanup marker: %v", err)
				}
				if f.failFence {
					http.Error(w, "fixture network fence failed after admission closed", http.StatusConflict)
					return
				}
			case r.URL.Path == "/api/v1/status":
				writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{}, Partial: true})
				return
			case strings.HasSuffix(r.URL.Path, "/drain"):
				f.drain.Add(1)
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(endpoint.Close)
		agent.URL = endpoint.URL
		f.agents = append(f.agents, agent)
		f.server.state.agents[agent.ID] = agent
	}
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.managerCalls.Add(1)
		if f.failManager {
			http.Error(w, "fixture Manager stopped after Agent fencing", http.StatusConflict)
			return
		}
		var request model.ExperimentNetworkRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, model.ExperimentNetwork{RunID: request.RunID, Epoch: request.Epoch, NetworkID: request.Epoch})
	}))
	t.Cleanup(manager.Close)
	f.server.config.NetworkManagerURL = manager.URL
	return f
}

func TestNetworkCleanupOwnershipSurvivesPreparationFailureAndDeletion(t *testing.T) {
	for _, failure := range []string{"Agent fence", "Manager", "delete after Manager failure"} {
		t.Run(failure, func(t *testing.T) {
			f := newNetworkOwnershipFixture(t, "pre-clock-network-run")
			f.failFence = failure == "Agent fence"
			f.failManager = !f.failFence
			if err := f.server.prepareExperimentNetwork(t.Context(), f.run); err == nil {
				t.Fatal("injected network preparation failure was ignored")
			}
			if run := f.server.state.experiments[f.run.ID]; !run.StartedAt.IsZero() || len(run.Agents) != 0 {
				t.Fatalf("network-only ownership changed the experiment clock or placement: %+v", run)
			}
			if f.failFence && f.managerCalls.Load() != 0 {
				t.Fatal("Manager was called after an unconfirmed Agent fence")
			}
			if strings.HasPrefix(failure, "delete") {
				if err := f.server.deleteSavedResult(f.run.ID); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(f.server.config, nil)
			for _, agent := range f.agents {
				restarted.state.agents[agent.ID] = agent
			}
			if err := restarted.recoverWorkCleanup(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(restarted.queueCleanup, f.run.ID) {
				t.Fatal("pre-clock network cleanup was lost during Controller restart")
			}
			if err := restarted.drainRunTelemetry(t.Context(), f.run.ID); err != nil {
				t.Fatalf("recovered network-only participants could not be drained: %v", err)
			}
			if f.drain.Load() != 2 {
				t.Fatalf("cleanup did not contact every recorded network participant: %d", f.drain.Load())
			}
			if strings.HasPrefix(failure, "delete") {
				if _, err := os.Stat(filepath.Join(f.server.config.DataDir, currentRunsDirectory, f.run.ID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("cleanup restored a deleted result: %v", err)
				}
			}
		})
	}
}

func TestNetworkCleanupOwnershipPreservesPlacementAndRecordedCapabilities(t *testing.T) {
	f := newNetworkOwnershipFixture(t, "network-owner-union")
	if err := f.server.prepareExperimentNetwork(t.Context(), f.run); err != nil {
		t.Fatal(err)
	}
	placed := f.agents[0]
	// A later weaker placement snapshot must not downgrade recorded cleanup
	// capability or overwrite the disabled network-only participant.
	placed.RunProcesses = false
	if err := f.server.rememberRunAgent(f.run.ID, placed); err != nil {
		t.Fatal(err)
	}
	owners, err := f.server.readRunCleanupOwnership(f.run.ID)
	if err != nil || len(owners) != 2 || !owners[0].RunProcesses || !owners[1].RunProcesses {
		t.Fatalf("placement discarded or weakened network cleanup ownership: %+v, %v", owners, err)
	}
	run := f.server.state.experiments[f.run.ID]
	if len(run.Agents) != 1 || run.Agents[0].ID != placed.ID || run.Agents[0].RunProcesses {
		t.Fatalf("network cleanup owners replaced the actual placement environment: %+v", run.Agents)
	}
	saved := persistedExperiment(t, f.server, f.run.ID)
	if len(saved.Agents) != 1 || saved.Agents[0].ID != placed.ID {
		t.Fatalf("saved placement environment included a network-only Agent: %+v", saved.Agents)
	}
}

func TestNetworkOwnershipFailureAndCancellationStartNoRPC(t *testing.T) {
	for _, boundary := range []string{"ownership write", "marker write", "admission lock", "persistence lock"} {
		t.Run(boundary, func(t *testing.T) {
			f := newNetworkOwnershipFixture(t, "network-persistence-failure")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var release func()
			switch boundary {
			case "ownership write", "marker write":
				directory := ownershipDirectory
				if boundary == "marker write" {
					directory = pendingWorkCleanupDirectory
				}
				if err := os.WriteFile(filepath.Join(f.server.config.DataDir, directory), []byte("not a directory"), 0600); err != nil {
					t.Fatal(err)
				}
			case "admission lock":
				f.server.cancelMu.Lock()
				release = f.server.cancelMu.Unlock
			case "persistence lock":
				f.server.state.persistMu.Lock()
				release = f.server.state.persistMu.Unlock
			}
			if release != nil {
				defer release()
			}
			done := make(chan error, 1)
			go func() { done <- f.server.prepareExperimentNetwork(ctx, f.run) }()
			if release != nil {
				select {
				case err := <-done:
					t.Fatalf("network preparation bypassed its held lock: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || release != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("network preparation lost its persistence/cancellation failure: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("network preparation ignored cancellation behind its metadata lock")
			}
			if f.fences.Load() != 0 || f.managerCalls.Load() != 0 {
				t.Fatal("network mutation started before durable cleanup ownership")
			}
			if release != nil {
				for _, path := range []string{filepath.Join(ownershipDirectory, f.run.ID+".json"), filepath.Join(pendingWorkCleanupDirectory, f.run.ID)} {
					if _, err := os.Stat(filepath.Join(f.server.config.DataDir, path)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("canceled metadata lock published %s: %v", path, err)
					}
				}
			}
		})
	}
}

func TestRetirementClearsNetworkCleanupMarkerAndRejectsLatePreparation(t *testing.T) {
	s, id, _ := summaryFreshnessFixture(t, time.Time{})
	agent := model.Agent{ID: "network-only-owner", URL: "http://network-owner:8090", StartedAt: time.Now().UTC(), RunProcesses: true, RunDrain: true}
	s.state.agents[agent.ID] = agent
	if err := s.rememberRunNetworkAgents(t.Context(), id, []model.Agent{agent}); err != nil {
		t.Fatal(err)
	}
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(id)
	s.cancelMu.Unlock()
	if err := s.retireRunRuntime(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) || slices.Contains(s.queueCleanup, id) {
		t.Fatalf("retirement retained its network cleanup barrier: %v, %v", err, s.queueCleanup)
	}
	if err := s.prepareExperimentNetwork(t.Context(), model.Experiment{ID: id}); err == nil || !strings.Contains(err.Error(), "already been retired") {
		t.Fatalf("late preparation replayed a retired Run: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late preparation recreated a retired cleanup marker: %v", err)
	}
}

func TestRetirementMarkerCleanupHonorsCancellationAndRetainsBarrier(t *testing.T) {
	s, id, _ := summaryFreshnessFixture(t, time.Time{})
	if err := s.rememberRunNetworkAgents(t.Context(), id, []model.Agent{{ID: "network-only-owner", URL: "http://owner:8090"}}); err != nil {
		t.Fatal(err)
	}
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(id)
	locked := true
	defer func() {
		if locked {
			s.cancelMu.Unlock()
		}
	}()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.retireRunRuntime(ctx, id) }()
	retirementReviewWait(t, "runtime retirement did not reach its final cleanup marker", func() bool {
		retired, err := s.isRuntimeRetired(id)
		return err == nil && retired
	})
	select {
	case err := <-done:
		t.Fatalf("retirement bypassed the held cleanup marker lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cleanup marker wait lost cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime retirement ignored cancellation behind the cleanup marker lock")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || !slices.Contains(s.queueCleanup, id) {
		t.Fatalf("canceled retirement discarded its cleanup barrier: %v, %v", err, s.queueCleanup)
	}
	s.cancelMu.Unlock()
	locked = false
	if err := s.retireRunRuntime(t.Context(), id); err != nil {
		t.Fatalf("retirement could not complete its retained marker on retry: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); !errors.Is(err, os.ErrNotExist) || slices.Contains(s.queueCleanup, id) {
		t.Fatalf("successful retry did not retire its cleanup barrier: %v, %v", err, s.queueCleanup)
	}
}
