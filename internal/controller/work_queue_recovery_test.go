package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAnalysisRecoversRetainedPeerBarrierAfterControllerRestart(t *testing.T) {
	for _, previousState := range []string{"completed", "running"} {
		t.Run(previousState, func(t *testing.T) {
			fencing, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var fences atomic.Int32
			var agent model.Agent
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					fences.Add(1)
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
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			original := New(ServerConfig{DataDir: t.TempDir()}, nil)
			agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
			run := model.Experiment{ID: "retained-restart", Name: "retained before restart", State: previousState, StartedAt: time.Now().Add(-time.Minute), CleanupState: "retained", DataState: "collecting", Repetitions: 1}
			if previousState == "running" {
				run.CleanupState = "pending"
			} else {
				run.FinishedAt = time.Now().UTC()
			}
			original.state.experiments[run.ID] = run
			if err := original.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			if err := original.rememberRunAgent(run.ID, agent); err != nil {
				t.Fatal(err)
			}
			evidence, _ := json.Marshal(model.TraceEvent{RunID: run.ID, EventID: "before-restart", Type: "peer-started", Timestamp: time.Now().UTC()})
			evidence = append(evidence, '\n')
			events := filepath.Join(original.config.DataDir, currentRunsDirectory, run.ID, "events.jsonl")
			if err := os.WriteFile(events, evidence, 0600); err != nil {
				t.Fatal(err)
			}
			restarted := New(original.config, nil)
			if _, err := restarted.state.registerAgent(agent); err != nil {
				t.Fatal(err)
			}
			if len(restarted.queueCleanup) != 0 || len(restarted.state.experiments) != 0 {
				t.Fatal("fixture did not simulate lost Controller memory")
			}
			analysis := queueTestEnqueue(t, restarted, "analysis")
			select {
			case <-fencing:
			case <-analysis.entered:
				t.Fatal("analysis skipped retained Peers after Controller-only restart")
			case <-time.After(3 * time.Second):
				t.Fatal("historical Peer cleanup did not begin")
			}
			select {
			case <-analysis.entered:
				t.Fatal("analysis overlapped historical Peer removal")
			default:
			}
			close(release)
			queueTestAwait(t, analysis.entered, "analysis after restart cleanup")
			sealed := persistedExperiment(t, restarted, run.ID)
			wantState := previousState
			if wantState == "running" {
				wantState = "interrupted"
			}
			if sealed.State != wantState || sealed.CleanupState != "complete" || sealed.DataState != "complete" || sealed.Name != run.Name {
				t.Fatalf("historical evidence was not sealed correctly: %+v", sealed)
			}
			retained, err := os.ReadFile(events)
			if err != nil || !bytes.Equal(retained, evidence) {
				t.Fatalf("restart cleanup changed source evidence: %v", err)
			}
			analysis.once.Do(func() { close(analysis.release) })
			restarted.workWorkers.Wait()
			next := queueTestEnqueue(t, restarted, "analysis")
			queueTestAwait(t, next.entered, "next analysis")
			if fences.Load() != 1 {
				t.Fatalf("sealed history was cleaned repeatedly: %d", fences.Load())
			}
			next.once.Do(func() { close(next.release) })
			restarted.workWorkers.Wait()
		})
	}
}

func TestAnalysisRecoversLivePeerWithoutHistoricalManifest(t *testing.T) {
	var removals atomic.Int32
	var agent model.Agent
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			removals.Add(1)
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
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	s.state.mu.Lock()
	s.state.setNodeLocked(model.Node{ID: "orphan", RunID: "deleted-history", AgentID: agent.ID, Generation: 1, State: model.NodeReady, LastSeen: time.Now()})
	s.state.mu.Unlock()
	analysis := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, analysis.entered, "analysis after observed orphan cleanup")
	if removals.Load() != 1 || len(s.state.experiments) != 0 {
		t.Fatalf("observed Peer cleanup was skipped or fabricated history: removals=%d runs=%d", removals.Load(), len(s.state.experiments))
	}
	analysis.once.Do(func() { close(analysis.release) })
	s.workWorkers.Wait()
}

func TestHeadRecoveryDoesNotFenceItsOwnNewSubmission(t *testing.T) {
	var deletes atomic.Int32
	var agent model.Agent
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
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
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline, LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	run := queueRun(t, s, queueLongScenario, 1)
	t.Cleanup(func() { _ = s.StopScenario(run.ID); s.runs.Wait() })
	queueAwait(t, s, run.ID, func(run model.Experiment) bool { return run.Phase == 1 })
	if deletes.Load() != 0 {
		t.Fatal("recovery fenced a newly scheduled Run before its first phase")
	}
	_ = s.StopScenario(run.ID)
	s.runs.Wait()
	s.workWorkers.Wait()
}

func TestHeadRecoveryFailurePreservesPreparationCause(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if err := os.MkdirAll(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, "invalid-directory-marker"), 0700); err != nil {
		t.Fatal(err)
	}
	run := queueRun(t, s, queueShortScenario, 1)
	waitRepetitions(t, s)
	finished := persistedExperiment(t, s, run.ID)
	if finished.State != "canceled" || finished.Phase != 0 || !strings.Contains(finished.Error, "Cannot prepare experiment queue: invalid pending cleanup marker") {
		t.Fatalf("queue preparation failure was reported as user cancellation: %+v", finished)
	}
	s.workWorkers.Wait()
}
