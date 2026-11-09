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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPendingTelemetryRequiresPositiveTypedEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"none", nil, false},
		{"timeout", context.DeadlineExceeded, false},
		{"storage", errors.New("storage failed"), false},
		{"text-only", errors.New("run logs not fully delivered (4 remaining): deadline"), false},
		{"zero", &agentRunDrainPendingError{remaining: 0}, false},
		{"positive", &agentRunDrainPendingError{remaining: 4}, true},
		{"wrapped", fmt.Errorf("deadline: %w; last report: %w", context.DeadlineExceeded, &agentRunDrainPendingError{remaining: 4}), true},
		{"joined-zero-first", errors.Join(&agentRunDrainPendingError{remaining: 0}, &agentRunDrainPendingError{remaining: 4}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasPendingRunTelemetry(tc.err); got != tc.want {
				t.Fatalf("pending=%v want%v", got, tc.want)
			}
		})
	}
	legacy := model.Experiment{State: "failed", StartedAt: time.Now(), CleanupState: "complete", DataState: "incomplete", IntegrityError: "old unstructured incomplete result"}
	if needsRecoveredCleanup(legacy) {
		t.Fatal("generic historical incompleteness became a delivery barrier")
	}
	legacy.TelemetryPending = true
	if !needsRecoveredCleanup(legacy) {
		t.Fatal("known pending delivery ignored after container cleanup")
	}
}

func TestPendingTelemetrySurvivesLaterUnconfirmedFailures(t *testing.T) {
	for _, outcome := range []string{"server-error", "transport-error", "zero-report", "pending-timeout", "confirmed"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir()}, nil)
				attempts := 0
				transportErr := errors.New("connection reset")
				s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
					attempts++
					w := httptest.NewRecorder()
					if attempts == 1 {
						w.WriteHeader(503)
						w.Write([]byte(`{"error":"pending","code":"run_telemetry_pending","remainingEvents":7}`))
					} else {
						switch outcome {
						case "server-error":
							w.WriteHeader(500)
							w.Write([]byte(`{"error":"storage unavailable"}`))
						case "transport-error":
							return nil, transportErr
						case "zero-report":
							w.WriteHeader(503)
							w.Write([]byte(`{"error":"contradictory pending report","code":"run_telemetry_pending","remainingEvents":0}`))
						case "pending-timeout":
							w.WriteHeader(503)
							w.Write([]byte(`{"error":"still pending","code":"run_telemetry_pending","remainingEvents":7}`))
						case "confirmed":
							w.WriteHeader(204)
						}
					}
					return w.Result(), nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := s.drainAgentTelemetry(ctx, "pending", model.Agent{URL: "http://agent"})
				if outcome == "confirmed" {
					if err != nil || hasPendingRunTelemetry(err) {
						t.Fatalf("successful confirmation remained pending: %v", err)
					}
					return
				}
				if err == nil || !hasPendingRunTelemetry(err) {
					t.Fatalf("a later error erased positive pending evidence: %v", err)
				}
				if outcome == "transport-error" && !errors.Is(err, transportErr) {
					t.Fatal("transport cause was erased")
				}
				if outcome == "pending-timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("deadline cause was erased")
				}
			})
		})
	}
}

func TestPendingTelemetryBeyondDisplayedAgentFailures(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending-%v", pending), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agents := make(map[string]model.Agent)
			for index := range 6 {
				id := fmt.Sprintf("agent-%02d", index)
				agent := model.Agent{ID: id, URL: "http://" + id, State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
				agents[id] = agent
				if _, err := s.state.registerAgent(agent); err != nil {
					t.Fatal(err)
				}
			}
			transportErr := errors.New("last Agent connection reset")
			var calls atomic.Int32
			s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
				w := httptest.NewRecorder()
				if r.Method == http.MethodGet {
					json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agents[r.URL.Host]})
				} else if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain") {
					calls.Add(1)
					switch {
					case r.URL.Host == "agent-04" && pending:
						w.WriteHeader(503)
						w.Write([]byte(`{"error":"pending (7 remaining)","code":"run_telemetry_pending","remainingEvents":7}`))
					case r.URL.Host == "agent-05":
						return nil, transportErr
					default:
						w.WriteHeader(500)
						w.Write([]byte(`{"error":"Agent storage unavailable"}`))
					}
				} else {
					w.WriteHeader(204)
				}
				return w.Result(), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			err := s.drainRunTelemetry(ctx, "many-agents")
			cancel()
			if err == nil || calls.Load() != 6 || hasPendingRunTelemetry(err) != pending || !errors.Is(err, transportErr) {
				t.Fatalf("lost structured Agent failure: calls=%d pending=%v err=%v", calls.Load(), hasPendingRunTelemetry(err), err)
			}
			var report *agentRunDrainPendingError
			if errors.As(err, &report) != pending || pending && (report.remaining != 7 || !errors.Is(err, context.DeadlineExceeded)) {
				t.Fatalf("omitted Agent evidence was lost: %v", err)
			}
			if strings.Contains(err.Error(), "agent-04") || strings.Contains(err.Error(), "agent-05") || strings.Contains(err.Error(), "7 remaining") || !strings.Contains(err.Error(), "2 additional Agents") || strings.Count(err.Error(), "\n") != 4 {
				t.Fatalf("Agent failure display cap changed: %s", err)
			}
			run := queueRun(t, s, "name: capped-drain-failures\njobShutdownTimeout: 30ms\nphases: [{action: wait, duration: 1ms}]\n", 2)
			s.runs.Wait()
			s.state.mu.RLock()
			finished := s.state.experiments[run.ID]
			s.state.mu.RUnlock()
			if finished.State != "failed" || finished.CleanupState != "complete" || finished.TelemetryPending != pending {
				t.Fatalf("omitted pending evidence did not reach finalization: %+v", finished)
			}
			_, markerErr := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID))
			if pending && markerErr != nil || !pending && !errors.Is(markerErr, os.ErrNotExist) {
				t.Fatalf("incorrect durable telemetry barrier: pending=%v marker=%v", pending, markerErr)
			}
		})
	}
}

func TestRecordedTelemetryCapabilityCannotDowngradeConfirmation(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(fmt.Sprintf("recorded-%v", recorded), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			old := model.Agent{ID: "owner", URL: "http://agent", State: model.AgentOnline, RunDrain: recorded}
			run := model.Experiment{ID: "capability-run", State: "failed", CleanupState: "complete", DataState: "incomplete", TelemetryPending: recorded}
			s.state.experiments[run.ID] = run
			if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			if err := s.rememberRunAgent(run.ID, old); err != nil {
				t.Fatal(err)
			}
			current := old
			current.RunDrain = false
			if _, err := s.state.registerAgent(current); err != nil {
				t.Fatal(err)
			}
			if _, err := s.cleanupParticipants(run.ID); err != nil {
				t.Fatalf("capability loss prevented container fencing: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := s.drainRunTelemetry(ctx, run.ID)
			if recorded {
				if err == nil || !strings.Contains(err.Error(), "no longer supports") {
					t.Fatalf("capability downgrade claimed confirmation: %v", err)
				}
				if err := s.deleteSavedResult(run.ID); err != nil {
					t.Fatal(err)
				}
				restarted := New(s.config, nil)
				if _, err := restarted.state.registerAgent(current); err != nil {
					t.Fatal(err)
				}
				if err := restarted.drainRunTelemetry(ctx, run.ID); err == nil {
					t.Fatal("deletion/restart lost required capability evidence")
				}
				if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); err != nil {
					t.Fatal("capability downgrade lost the pending marker")
				}
			} else if err != nil {
				t.Fatalf("legacy unverified cleanup changed: %v", err)
			}
		})
	}
}

func TestRetryAndResumeDoNotCopyPreviousTelemetryObligation(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry-%v", retry), func(t *testing.T) {
			fixture := newResumeFixture(t)
			runs := fixture.failedBatch(t, 2)
			head := queueTestEnqueue(t, fixture.server, "analysis")
			queueTestAwait(t, head.entered, "prior FIFO task")
			fixture.server.updateExperiment(runs[0].ID, func(run *model.Experiment) { run.TelemetryPending = true })
			var first model.Experiment
			var err error
			if retry {
				first, err = fixture.server.RetryScenarioBatch(context.Background(), runs[0].BatchID)
			} else {
				first, err = fixture.server.ResumeScenarioBatch(context.Background(), runs[0].BatchID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if first.TelemetryPending {
				t.Fatal("new queued execution inherited an old delivery obligation")
			}
			if err := fixture.server.StopScenario(first.ID); err != nil {
				t.Fatal(err)
			}
			head.once.Do(func() { close(head.release) })
			fixture.server.runs.Wait()
			fixture.server.state.mu.RLock()
			old := fixture.server.state.experiments[runs[0].ID]
			fixture.server.state.mu.RUnlock()
			if !old.TelemetryPending {
				t.Fatal("canceling the new waiter erased old pending evidence")
			}
		})
	}
}

func TestFinalDrainFailureRegistersPendingTelemetryBarrier(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent := model.Agent{ID: "owner", URL: "http://agent", State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		if r.Method == http.MethodGet {
			json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
		} else if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain") {
			w.WriteHeader(503)
			w.Write([]byte(`{"error":"run logs not fully delivered (7 remaining): context deadline exceeded","code":"run_telemetry_pending","remainingEvents":7}`))
		} else {
			w.WriteHeader(204)
		}
		return w.Result(), nil
	})
	run := queueRun(t, s, "name: drain-timeout\njobShutdownTimeout: 30ms\nphases: [{action: wait, duration: 1ms}]\n", 2)
	s.runs.Wait()
	s.state.mu.RLock()
	finished := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if finished.State != "failed" || finished.CleanupState != "complete" || finished.DataState != "incomplete" || !finished.TelemetryPending {
		t.Fatalf("incorrect finalization: %+v", finished)
	}
	if !strings.Contains(finished.IntegrityError, "7 remaining") {
		t.Fatalf("lost Agent evidence: %s", finished.IntegrityError)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); err != nil {
		t.Fatalf("pending delivery was not durable: %v", err)
	}
	var saved []savedResult
	response := resultRequest(s, http.MethodGet, "/api/v1/results")
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &saved) != nil {
		t.Fatalf("results: %d %s", response.Code, response.Body)
	}
	found := false
	for _, result := range saved {
		if result.ID == run.ID {
			found = result.TelemetryPending
		}
	}
	if !found {
		t.Fatal("Saved Results omitted known pending telemetry")
	}
}

func TestPendingTelemetryBarrierSurvivesRestartDeletionAndCancellation(t *testing.T) {
	for _, mode := range []string{"same-controller", "restarted", "deleted-and-restarted", "canceled-waiter"} {
		t.Run(mode, func(t *testing.T) {
			var healthy atomic.Bool
			seen := make(chan struct{})
			var once sync.Once
			var agent model.Agent
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain") && !healthy.Load() {
					once.Do(func() { close(seen) })
					w.WriteHeader(503)
					w.Write([]byte(`{"error":"pending (7 remaining)","code":"run_telemetry_pending","remainingEvents":7}`))
					return
				}
				if r.Method == http.MethodGet {
					fresh := agent
					fresh.LastSeen = time.Now()
					json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: fresh})
					return
				}
				w.WriteHeader(204)
			}))
			defer api.Close()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
			if _, err := s.state.registerAgent(agent); err != nil {
				t.Fatal(err)
			}
			run := model.Experiment{ID: "pending-delivery", Name: "pending", State: "failed", StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now(), CleanupState: "complete", DataState: "incomplete", TelemetryPending: true, IntegrityError: "original final drain failure"}
			s.state.experiments[run.ID] = run
			if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			if err := s.rememberRunAgent(run.ID, agent); err != nil {
				t.Fatal(err)
			}
			deleted := mode == "deleted-and-restarted"
			if deleted {
				if err := s.deleteSavedResult(run.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "restarted" || deleted {
				s = New(s.config, nil)
				if _, err := s.state.registerAgent(agent); err != nil {
					t.Fatal(err)
				}
			}
			next := queueTestEnqueue(t, s, "analysis")
			queueTestAwait(t, seen, "Agent drain before next work")
			select {
			case <-next.entered:
				t.Fatal("work entered while previous Agent evidence remained pending")
			default:
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); err != nil {
				t.Fatalf("missing delivery marker: %v", err)
			}
			if mode == "canceled-waiter" {
				if err := s.cancelWork(next.slot.id); err != nil {
					t.Fatal(err)
				}
				queueTestAwait(t, next.finished, "canceled waiter")
				if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); err != nil {
					t.Fatalf("cancel lost marker: %v", err)
				}
				healthy.Store(true)
				next = queueTestEnqueue(t, s, "analysis")
			} else {
				healthy.Store(true)
			}
			queueTestAwait(t, next.entered, "work after confirmed delivery")
			next.once.Do(func() { close(next.release) })
			queueTestAwait(t, next.finished, "finished next work")
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, run.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("confirmed delivery retained marker: %v", err)
			}
			s.state.mu.RLock()
			current, exists := s.state.experiments[run.ID]
			s.state.mu.RUnlock()
			if deleted {
				if exists {
					t.Fatal("cleanup restored deleted history")
				}
				if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("cleanup recreated deleted result")
				}
			} else if !exists || current.TelemetryPending || current.DataState != "incomplete" || current.IntegrityError != run.IntegrityError {
				t.Fatalf("confirmed delivery rewrote data quality: %+v", current)
			}
		})
	}
}
