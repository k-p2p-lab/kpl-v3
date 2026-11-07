package controller

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestRunCleanupRefreshDoesNotDependOnUnrelatedAgent(t *testing.T) {
	for _, operation := range []string{"stop-all", "finalize", "resume"} {
		for _, ownerFails := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/healthy-owner", true: "/failed-owner"}[ownerFails], func(t *testing.T) {
				var unrelatedCalls, ownerReads atomic.Int32
				var owner model.Agent
				ownerAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodGet {
						ownerReads.Add(1)
						if ownerFails {
							w.WriteHeader(http.StatusServiceUnavailable)
							return
						}
						_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: owner})
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer ownerAPI.Close()
				unrelatedAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					unrelatedCalls.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
				defer unrelatedAPI.Close()
				s := New(ServerConfig{DataDir: t.TempDir()}, nil)
				owner = model.Agent{ID: "owner", URL: ownerAPI.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), Capacity: 1, RunDrain: true}
				unrelated := model.Agent{ID: "unrelated", URL: unrelatedAPI.URL, State: model.AgentOnline, StartedAt: owner.StartedAt, LastSeen: time.Now(), Capacity: 1, RunDrain: true}
				for _, agent := range []model.Agent{owner, unrelated} {
					if _, err := s.state.registerAgent(agent); err != nil {
						t.Fatal(err)
					}
				}
				run := model.Experiment{ID: "scoped-cleanup", State: "running", StartedAt: time.Now(), Repetitions: 2}
				s.state.experiments[run.ID] = run
				if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
					t.Fatal(err)
				}
				if err := s.rememberRunAgent(run.ID, owner); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var err error
				switch operation {
				case "stop-all":
					err = s.runPhase(ctx, run.ID, 1, scenario.Phase{Action: "stop-all"}, rand.New(rand.NewSource(1)), newPhaseJobs(), time.Second)
				case "resume":
					err = s.cleanupBeforeResume(ctx, []string{run.ID}, scenario.Scenario{JobShutdownTimeout: "1s"})
				case "finalize":
					spec, parseErr := scenario.Parse([]byte(appendScenario))
					if parseErr != nil {
						t.Fatal(parseErr)
					}
					s.runScenario(ctx, run, spec)
					s.state.mu.RLock()
					result := s.state.experiments[run.ID]
					s.state.mu.RUnlock()
					if ownerFails {
						if result.State != "failed" || result.CleanupState != "failed" || result.DataState != "incomplete" {
							t.Fatalf("owner failure was hidden: %+v", result)
						}
					} else if result.State != "completed" || result.CleanupState != "complete" || result.DataState != "complete" {
						t.Fatalf("unrelated Agent failed a completed Run: %+v", result)
					}
				}
				if operation != "finalize" {
					if ownerFails && (err == nil || !strings.Contains(err.Error(), "owner")) {
						t.Fatalf("owner refresh failure was hidden: %v", err)
					}
					if !ownerFails && err != nil {
						t.Fatalf("unrelated Agent blocked cleanup: %v", err)
					}
				}
				if ownerReads.Load() == 0 || unrelatedCalls.Load() != 0 {
					t.Fatalf("refresh scope changed: owner reads=%d unrelated calls=%d", ownerReads.Load(), unrelatedCalls.Load())
				}
			})
		}
	}
}
