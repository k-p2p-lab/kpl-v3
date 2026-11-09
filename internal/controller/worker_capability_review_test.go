package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// An older Agent can still implement the telemetry-only drain endpoint. Its
// 204 cannot acknowledge the stronger worker-exit boundary recorded by a new
// Run, including after an Agent replacement or Controller restart.
func TestRecordedRunWorkerCapabilityCannotDowngradeCleanup(t *testing.T) {
	for _, mode := range []string{"same-instance-registration", "replacement-same-url", "replacement-new-url", "missing-registration", "legacy-owner"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := isolatedAnalysisServer(t)
			var drains, stops atomic.Int32
			started := time.Now().Add(-time.Minute).UTC()
			current := model.Agent{ID: "owner", Hostname: "host-one", StartedAt: started, RunDrain: true, RunProcesses: false, StartupReconciled: true}
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodDelete:
					stops.Add(1)
					w.WriteHeader(http.StatusAccepted)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
					drains.Add(1)
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
					writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: current})
				default:
					http.NotFound(w, r)
				}
			}))
			defer endpoint.Close()
			current.URL = endpoint.URL
			old := current
			old.RunProcesses = mode != "legacy-owner"
			if strings.HasPrefix(mode, "replacement-") {
				old.StartedAt = started.Add(-time.Minute)
			}
			if mode == "replacement-new-url" {
				old.URL = "http://previous-agent.invalid"
			}
			run := model.Experiment{ID: "worker-capability-run", State: "failed", CleanupState: "failed", DataState: "incomplete", StartedAt: started}
			s.state.experiments[run.ID] = run
			if err := s.persistManifest(run, []byte("name: capability\nphases: [{action: wait, duration: 1ms}]\n")); err != nil {
				t.Fatal(err)
			}
			if err := s.rememberRunAgent(run.ID, old); err != nil {
				t.Fatal(err)
			}
			if mode != "missing-registration" {
				if mode == "same-instance-registration" {
					if _, err := s.state.registerAgent(old); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.state.registerAgent(current); err != nil {
					t.Fatal(err)
				}
			}
			// Physical removal must remain possible even when a capability has
			// disappeared. Only the final cleanup proof is held back.
			if _, err := s.cleanupParticipants(run.ID); err != nil {
				t.Fatalf("capability loss prevented container fencing: %v", err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			err := s.cleanupBeforeResume(ctx, []string{run.ID}, scenario.Scenario{JobShutdownTimeout: "1s"})
			if mode == "legacy-owner" {
				if err != nil || drains.Load() != 1 {
					t.Fatalf("legacy telemetry-only cleanup changed: drains=%d err=%v", drains.Load(), err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "Run worker") || !hasPendingRunWorker(err) || hasPendingRunTelemetry(err) {
				t.Fatalf("log-only Agent established process cleanup: drains=%d err=%v", drains.Load(), err)
			}
			if stops.Load() == 0 {
				t.Fatal("cleanup did not attempt physical Peer removal")
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, retiredRuntimeDirectory, run.ID+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unconfirmed cleanup produced retired runtime evidence: %v", err)
			}
			// Ownership evidence, including its required capability, must still
			// be present after the failed confirmation.
			data, err := os.ReadFile(filepath.Join(s.config.DataDir, ownershipDirectory, run.ID+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var owners []model.Agent
			if err := json.Unmarshal(data, &owners); err != nil || len(owners) != 1 || !owners[0].RunProcesses {
				t.Fatalf("recorded worker capability was lost: owners=%+v err=%v", owners, err)
			}
		})
	}
}

func TestRunWorkerDrainRequiresRawInstanceCapabilityAfterAcknowledgement(t *testing.T) {
	for _, mode := range []string{"valid", "cached-capability", "different-id", "different-instance", "missing-instance", "missing-drain", "status-error", "status-timeout", "legacy-owner"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			registered := model.Agent{ID: "owner", StartedAt: time.Now().Add(-time.Minute).UTC(), RunDrain: true, RunProcesses: true}
			raw := registered
			switch mode {
			case "cached-capability", "legacy-owner":
				raw.RunProcesses = false
			case "different-id":
				raw.ID = "other-owner"
			case "different-instance":
				raw.StartedAt = raw.StartedAt.Add(time.Minute)
			case "missing-instance":
				raw.StartedAt = time.Time{}
			case "missing-drain":
				raw.RunDrain = false
			}
			var drains, statuses atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain"):
					drains.Add(1)
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
					statuses.Add(1)
					if drains.Load() == 0 {
						t.Error("instance confirmation preceded the actual drain acknowledgement")
					}
					if mode == "status-error" {
						http.Error(w, "status unavailable", http.StatusServiceUnavailable)
						return
					}
					if mode == "status-timeout" {
						<-r.Context().Done()
						return
					}
					writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: raw})
				default:
					http.NotFound(w, r)
				}
			}))
			defer endpoint.Close()
			registered.URL = endpoint.URL
			if _, err := s.state.registerAgent(registered); err != nil {
				t.Fatal(err)
			}
			if mode == "cached-capability" {
				if err := s.state.heartbeat(model.AgentHeartbeat{Agent: raw, Partial: true}); err != nil {
					t.Fatal(err)
				}
				if current, _ := s.agent(registered.ID); !current.RunProcesses {
					t.Fatal("test did not exercise the capability-preserving heartbeat cache")
				}
			}
			if mode == "legacy-owner" {
				registered.RunProcesses = false
			}
			budget := time.Second
			if mode == "status-timeout" {
				budget = 100 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			err := s.drainAgentTelemetry(ctx, "run", registered)
			if mode == "valid" || mode == "legacy-owner" {
				if err != nil || drains.Load() != 1 {
					t.Fatalf("valid acknowledgement rejected: drains=%d err=%v", drains.Load(), err)
				}
				if mode == "legacy-owner" && statuses.Load() != 0 {
					t.Fatal("legacy cleanup unexpectedly acquired the new process proof requirement")
				}
				return
			}
			if err == nil || !hasPendingRunWorker(err) || hasPendingRunTelemetry(err) || drains.Load() != 1 || statuses.Load() != 1 {
				t.Fatalf("unconfirmed process reset accepted or misclassified: drains=%d statuses=%d err=%v", drains.Load(), statuses.Load(), err)
			}
			if mode == "status-timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("instance confirmation lost the caller deadline: %v", err)
			}
		})
	}
}

func TestRunWorkerReplacementRequiresInstanceOrReconciledHost(t *testing.T) {
	for _, mode := range []string{"same-instance-new-url", "reconciled-same-host", "reused-url-unreconciled", "reused-url-different-host", "reused-url-missing-host-id", "reused-url-older-instance"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			started := time.Now().Add(-time.Minute).UTC()
			old := model.Agent{ID: "owner", URL: "http://old-agent", Hostname: "shared-hostname", StartedAt: started, RunDrain: true, RunProcesses: true, Labels: map[string]string{"swarmNodeId": "node-one"}}
			current := old
			current.StartedAt = started.Add(time.Minute)
			current.StartupReconciled = true
			switch mode {
			case "same-instance-new-url":
				current.URL, current.StartedAt = "http://new-agent", started
			case "reused-url-unreconciled":
				current.StartupReconciled = false
			case "reused-url-different-host":
				current.Labels = map[string]string{"swarmNodeId": "node-two"}
			case "reused-url-missing-host-id":
				current.Labels = nil
			case "reused-url-older-instance":
				current.StartedAt = started.Add(-time.Minute)
			}
			run := model.Experiment{ID: "replacement-proof", State: "failed"}
			s.state.experiments[run.ID] = run
			if err := s.persistManifest(run, []byte("name: replacement\nphases: [{action: wait, duration: 1ms}]\n")); err != nil {
				t.Fatal(err)
			}
			if err := s.rememberRunAgent(run.ID, old); err != nil {
				t.Fatal(err)
			}
			if _, err := s.state.registerAgent(current); err != nil {
				t.Fatal(err)
			}
			owners, err := s.cleanupParticipantsChecked(run.ID, true)
			allowed := mode == "same-instance-new-url" || mode == "reconciled-same-host"
			if allowed {
				if err != nil || len(owners) != 1 || owners[0].URL != current.URL || !owners[0].StartedAt.Equal(current.StartedAt) {
					t.Fatalf("verified replacement was not usable: owners=%+v err=%v", owners, err)
				}
			} else if err == nil || !hasPendingRunWorker(err) || hasPendingRunTelemetry(err) {
				t.Fatalf("URL reuse established old-host cleanup without proof: owners=%+v err=%v", owners, err)
			}
		})
	}
}
