package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestRunDrainRetriesTypedAndLegacyPending(t *testing.T) {
	for _, response := range []string{
		`{"error":"run logs not fully delivered (175 remaining): context deadline exceeded","code":"run_telemetry_pending","remainingEvents":175}`,
		`{"error":"run logs not fully delivered (175 remaining): context deadline exceeded"}`,
	} {
		synctest.Test(t, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			attempts := 0
			s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
				attempts++
				wait, err := time.ParseDuration(r.URL.Query().Get("timeout"))
				if err != nil || wait <= 0 || wait > model.MaxRunDrainTimeout {
					t.Errorf("invalid Agent wait: %s", r.URL)
				}
				deadline, _ := r.Context().Deadline()
				if time.Until(deadline) < model.MaxRunDrainTimeout {
					t.Error("drain query fell back to ordinary control timeout")
				}
				w := httptest.NewRecorder()
				if attempts == 1 {
					w.WriteHeader(503)
					_, _ = w.Write([]byte(response))
				} else {
					w.WriteHeader(204)
				}
				return w.Result(), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := s.drainAgentTelemetry(ctx, "run", model.Agent{URL: "http://agent"}); err != nil || attempts != 2 {
				t.Fatalf("pending failed instead of retrying: attempts=%d, %v", attempts, err)
			}
		})
	}
}

func TestRunDrainDeadlinePreservesLastRemainingCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
			wait, err := time.ParseDuration(r.URL.Query().Get("timeout"))
			if err != nil || wait > 500*time.Millisecond {
				t.Errorf("attempt exceeds overall budget: %s", r.URL)
			}
			w := httptest.NewRecorder()
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"error":"run logs not fully delivered (175 remaining): context deadline exceeded"}`))
			return w.Result(), nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		err := s.drainAgentTelemetry(ctx, "run", model.Agent{URL: "http://agent"})
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "175 remaining") {
			t.Fatalf("pending considered complete or evidence lost: %v", err)
		}
	})
}

func TestRunDrainDoesNotRetryUnrelatedFailures(t *testing.T) {
	for _, response := range []string{
		`{"error":"disk failure"}`,
		`{"error":"run logs not fully delivered (0 remaining): bad"}`,
		`{"error":"bad","code":"run_telemetry_pending","remainingEvents":-1}`,
		`{"error":"bad","code":"run_telemetry_pending"}`,
		`{"error":"bad","code":"other","remainingEvents":5}`,
		`invalid`,
	} {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		attempts := 0
		s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
			attempts++
			w := httptest.NewRecorder()
			w.WriteHeader(503)
			_, _ = w.Write([]byte(response))
			return w.Result(), nil
		})
		if err := s.drainAgentTelemetry(context.Background(), "run", model.Agent{URL: "http://agent"}); err == nil || attempts != 1 {
			t.Fatalf("unrelated failure retried/hidden: %s attempts=%d, %v", response, attempts, err)
		}
	}
}

func TestRunDrainContactsAllOwnersWithBoundedConcurrency(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const count = 10
	for i := range count {
		id := fmt.Sprintf("agent-%02d", i)
		s.state.agents[id] = model.Agent{ID: id, URL: "http://" + id, RunDrain: true}
	}
	entered := make(chan struct{}, count)
	release := make(chan struct{})
	var active, peak, calls atomic.Int32
	s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
		n := active.Add(1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		defer active.Add(-1)
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		w := httptest.NewRecorder()
		w.WriteHeader(503) // non-pending failures must not hide other owners
		return w.Result(), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.drainRunTelemetry(ctx, "run") }()
	for range runDrainConcurrency {
		select {
		case <-entered:
		case <-ctx.Done():
			close(release)
			t.Fatal("Agent delivery was serialized")
		}
	}
	close(release)
	err := <-done
	if err == nil || calls.Load() != count || peak.Load() != runDrainConcurrency || !strings.Contains(err.Error(), "6 additional Agents") {
		t.Fatalf("collection lost owners or exceeded bound: calls=%d peak=%d err=%v", calls.Load(), peak.Load(), err)
	}
}

func TestResumeTelemetryReceivesFreshCleanupBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		s.state.agents["agent"] = model.Agent{ID: "agent", URL: "http://agent", RunDrain: true}
		var drained bool
		s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
			w := httptest.NewRecorder()
			switch r.Method {
			case http.MethodDelete:
				time.Sleep(2 * time.Second)
			case http.MethodPost:
				deadline, _ := r.Context().Deadline()
				if time.Until(deadline) < 3*time.Second {
					t.Error("Peer cleanup consumed telemetry deadline")
				}
				time.Sleep(2 * time.Second)
				drained = r.Context().Err() == nil
			case http.MethodGet:
				_, _ = w.Write([]byte(`{"agent":{"id":"agent"}}`))
				return w.Result(), nil
			}
			w.WriteHeader(204)
			return w.Result(), nil
		})
		err := s.cleanupBeforeResume(context.Background(), []string{"run"}, scenario.Scenario{JobShutdownTimeout: "3s"})
		if err != nil || !drained {
			t.Fatalf("fresh final-log budget missing: drained=%v err=%v", drained, err)
		}
	})
}

func TestRunDrainPendingControlsBatchCompletion(t *testing.T) {
	for _, recover := range []bool{true, false} {
		name, budget := "deadline", "50ms"
		if recover {
			name, budget = "recovered", "3s"
		}
		t.Run(name, func(t *testing.T) {
			var posts atomic.Int32
			var agent model.Agent
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
					return
				}
				if r.Method == http.MethodPost && (posts.Add(1) == 1 || !recover) {
					w.WriteHeader(503)
					_ = json.NewEncoder(w).Encode(model.RunDrainError{Error: "run logs not fully delivered (175 remaining): context deadline exceeded", Code: model.RunTelemetryPendingCode, RemainingEvents: 175})
					return
				}
				w.WriteHeader(204)
			}))
			defer api.Close()
			agent = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline, RunDrain: true, Capacity: 10, LastSeen: time.Now()}
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if _, err := s.state.registerAgent(agent); err != nil {
				t.Fatal(err)
			}
			raw := "jobShutdownTimeout: " + budget + "\n" + appendScenario
			if _, err := s.StartScenarioRepeated(context.Background(), []byte(raw), 2); err != nil {
				t.Fatal(err)
			}
			runs := waitRepetitions(t, s)
			if recover {
				for _, run := range runs {
					if run.State != "completed" || run.DataState != "complete" || run.CleanupState != "complete" {
						t.Fatalf("delivered logs still failed batch: %+v", runs)
					}
				}
			} else if runs[0].State != "failed" || runs[0].DataState != "incomplete" || runs[0].CleanupState != "complete" || runs[1].State != "canceled" || !strings.Contains(runs[0].Error, "175 remaining") {
				t.Fatalf("undelivered evidence hidden: %+v", runs)
			}
		})
	}
}

func TestRunFinalTelemetryDoesNotSharePeerCleanupDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		agent := model.Agent{ID: "agent", URL: "http://agent", RunDrain: true, State: model.AgentOnline, LastSeen: time.Now(), Capacity: 10}
		if _, err := s.state.registerAgent(agent); err != nil {
			t.Fatal(err)
		}
		s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
			w := httptest.NewRecorder()
			switch r.Method {
			case http.MethodDelete:
				if err := sleepContext(r.Context(), 2*time.Second); err != nil {
					return nil, err
				}
			case http.MethodPost:
				deadline, _ := r.Context().Deadline()
				if time.Until(deadline) < 3*time.Second {
					t.Error("Peer cleanup consumed final telemetry budget")
				}
				if err := sleepContext(r.Context(), 2*time.Second); err != nil {
					return nil, err
				}
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent})
				return w.Result(), nil
			}
			w.WriteHeader(204)
			return w.Result(), nil
		})
		if _, err := s.StartScenarioRepeated(context.Background(), []byte("jobShutdownTimeout: 3s\n"+appendScenario), 2); err != nil {
			t.Fatal(err)
		}
		s.runs.Wait()
		for _, run := range s.state.snapshot().Experiments {
			if run.State != "completed" || run.DataState != "complete" {
				t.Fatalf("slow cleanup prevented complete delivery: %+v", run)
			}
		}
	})
}
