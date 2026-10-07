package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const queueShortScenario = "name: next\nphases: [{action: wait, duration: 10ms}]\n"
const queueLongScenario = "name: active\nphases: [{action: wait, duration: 1h}]\n"

func queueRun(t *testing.T, s *Server, raw string, count int) model.Experiment {
	t.Helper()
	run, err := s.StartScenarioRepeated(context.Background(), []byte(raw), count)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func queueAwait(t *testing.T, s *Server, id string, check func(model.Experiment) bool) model.Experiment {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.state.mu.RLock()
		run := s.state.experiments[id]
		s.state.mu.RUnlock()
		if check(run) {
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach the expected queue state", id)
	return model.Experiment{}
}

func TestExperimentQueueFIFOAndBatchFailureAdvances(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "failed"}[failed], func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					once.Do(func() { close(entered); <-release })
					w.WriteHeader(http.StatusAccepted)
					return
				}
				_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "agent", Capacity: 10}})
			}))
			t.Cleanup(api.Close)
			s := newLifecycleTestController(t)
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			if _, err := s.state.registerAgent(model.Agent{ID: "agent", URL: api.URL, Capacity: 10}); err != nil {
				t.Fatal(err)
			}
			raw := queueShortScenario
			if failed {
				raw = "name: fails\nphases: [{action: publish, group: absent, count: 1}]\n"
			}
			first := queueRun(t, s, raw, 2)
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("first cleanup never started")
			}
			second := queueRun(t, s, queueShortScenario, 2)
			third := queueRun(t, s, queueShortScenario, 1)
			for index, run := range []model.Experiment{second, third} {
				if run.State != "queued" || !run.StartedAt.IsZero() || run.QueuePosition != index+1 || run.Timing != nil {
					t.Fatalf("submission started before its turn: %+v", run)
				}
			}
			close(release)
			runs := waitRepetitions(t, s)
			byBatch := map[string][]model.Experiment{}
			for _, run := range runs {
				byBatch[run.BatchID] = append(byBatch[run.BatchID], run)
			}
			var last time.Time
			for _, id := range []string{first.BatchID, second.BatchID, third.BatchID} {
				members := byBatch[id]
				sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
				for _, run := range members {
					if failed && id == first.BatchID && run.Iteration == 2 {
						if run.State != "canceled" || !run.StartedAt.IsZero() {
							t.Fatalf("failed batch continued: %+v", run)
						}
						continue
					}
					want := "completed"
					if failed && id == first.BatchID {
						want = "failed"
					}
					if run.State != want || run.StartedAt.Before(last) || run.QueuePosition != 0 {
						t.Fatalf("FIFO/state mismatch: previous finish=%s run=%+v", last, run)
					}
					last = run.FinishedAt
				}
			}
			s.cancelMu.Lock()
			defer s.cancelMu.Unlock()
			if len(s.experimentQueue)+len(s.repeatBatches)+len(s.cancels) != 0 {
				t.Fatal("scheduler retained completed submissions")
			}
		})
	}
}

func TestExperimentQueueCancelWaitingSubmissionAndIdempotency(t *testing.T) {
	s := newLifecycleTestController(t)
	active := queueRun(t, s, queueLongScenario, 1)
	second, err := s.submitScenario(context.Background(), []byte(queueShortScenario), 2, "queue-idempotency")
	if err != nil {
		t.Fatal(err)
	}
	third := queueRun(t, s, queueShortScenario, 1)
	for _, run := range s.state.snapshot().Experiments {
		if run.QueuePosition > 0 && run.Timing != nil {
			t.Fatalf("waiting submission ignored earlier experiments in its estimate: %+v", run)
		}
	}
	replay, err := s.submitScenario(context.Background(), []byte(queueShortScenario), 2, "queue-idempotency")
	if err != nil || replay.ID != second.ID || replay.QueuePosition != 1 {
		t.Fatalf("duplicate admission: %+v %v", replay, err)
	}
	if len(s.state.snapshot().Experiments) != 4 {
		t.Fatal("idempotent replay queued extra work")
	}
	if err := s.stopScenarioRequest(second.ID, second.ExecutionID); err != nil {
		t.Fatal(err)
	}
	queueAwait(t, s, third.ID, func(run model.Experiment) bool { return run.QueuePosition == 1 })
	for _, run := range s.state.snapshot().Experiments {
		if run.BatchID == second.BatchID && (run.State != "canceled" || !run.StartedAt.IsZero()) {
			t.Fatalf("queued cancellation ran phases: %+v", run)
		}
		if run.ID == active.ID && run.State != "running" {
			t.Fatalf("queued cancellation stopped active run: %+v", run)
		}
		if run.ID == third.ID && (run.State != "queued" || !run.StartedAt.IsZero()) {
			t.Fatal("canceling middle submission released its successor early")
		}
	}
	if err := s.StopScenario(active.ID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, s)
	queueAwait(t, s, third.ID, func(run model.Experiment) bool { return run.State == "completed" })
}

func TestExperimentQueueCleanupBarrierSurvivesCanceledWaiter(t *testing.T) {
	var healthy atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if !healthy.Load() {
				http.Error(w, "Peer removal unavailable", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "agent", Capacity: 10}})
	}))
	t.Cleanup(api.Close)
	s := newLifecycleTestController(t)
	if _, err := s.state.registerAgent(model.Agent{ID: "agent", URL: api.URL, Capacity: 10}); err != nil {
		t.Fatal(err)
	}
	queueRun(t, s, "name: fails\nphases: [{action: publish, group: absent, count: 1}]\n", 1)
	waitRepetitions(t, s)
	second := queueRun(t, s, queueShortScenario, 1)
	third := queueRun(t, s, queueShortScenario, 1)
	blocked := func(run model.Experiment) bool { return strings.Contains(run.PhaseName, "Peer removal unavailable") }
	queueAwait(t, s, second.ID, blocked)
	if err := s.StopScenario(second.ID); err != nil {
		t.Fatal(err)
	}
	waiting := queueAwait(t, s, third.ID, blocked)
	queueAwait(t, s, second.ID, func(run model.Experiment) bool { return run.State == "canceled" && run.PhaseName == "" })
	if waiting.State != "queued" || !waiting.StartedAt.IsZero() {
		t.Fatalf("unconfirmed cleanup allowed work: %+v", waiting)
	}
	healthy.Store(true)
	waitRepetitions(t, s)
	queueAwait(t, s, third.ID, func(run model.Experiment) bool { return run.State == "completed" })
}

func TestExperimentQueueRemovesRetainedSingleRunPeers(t *testing.T) {
	s, agent := newLifecycleTestControllerWithAgent(t)
	first := queueRun(t, s, "name: retain\nphases: [{action: join, group: peers, count: 1}]\n", 1)
	waitRepetitions(t, s)
	queueAwait(t, s, first.ID, func(run model.Experiment) bool { return run.CleanupState == "retained" })
	if agent.nodeCount() != 1 {
		t.Fatal("fixture has no retained Peer")
	}
	next := queueRun(t, s, queueLongScenario, 1)
	queueAwait(t, s, next.ID, func(run model.Experiment) bool { return run.State == "running" })
	if agent.nodeCount() != 0 {
		t.Fatal("next experiment overlapped retained Peers")
	}
	queueAwait(t, s, first.ID, func(run model.Experiment) bool {
		return run.CleanupState == "complete" && run.DataState == "unverified"
	})
}

func TestExperimentQueueRetryAndAppendWaitTheirTurn(t *testing.T) {
	for _, appendRuns := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "append"}[appendRuns], func(t *testing.T) {
			s := newLifecycleTestController(t)
			status := "failed"
			if appendRuns {
				status = "completed"
			}
			lifecycleSaved(t, s, "old", status, 1, 1, nil)
			active := queueRun(t, s, queueLongScenario, 1)
			var pending model.Experiment
			var err error
			if appendRuns {
				pending, err = s.AppendScenarioBatch(context.Background(), "group", 1, 1)
			} else {
				pending, err = s.RetryScenarioBatch(context.Background(), "group")
			}
			if err != nil {
				t.Fatal(err)
			}
			if pending.State != "queued" || pending.QueuePosition != 1 || !pending.StartedAt.IsZero() {
				t.Fatalf("recovery bypassed FIFO: %+v", pending)
			}
			if err := s.StopScenario(active.ID); err != nil {
				t.Fatal(err)
			}
			queueAwait(t, s, pending.ID, func(run model.Experiment) bool { return run.State == "running" })
		})
	}
}

func TestExperimentQueueRestartKeepsReservationsForManualRecovery(t *testing.T) {
	s := newLifecycleTestController(t)
	queueRun(t, s, queueLongScenario, 1)
	queued := queueRun(t, s, queueShortScenario, 1)
	restarted := New(ServerConfig{DataDir: s.config.DataDir}, nil)
	r := resultRequest(restarted, http.MethodGet, "/api/v1/results")
	var results []savedResult
	if r.Code != http.StatusOK || json.Unmarshal(r.Body.Bytes(), &results) != nil {
		t.Fatal(r.Body.String())
	}
	for _, run := range results {
		if run.ID == queued.ID {
			if run.State != "interrupted" || !run.StartedAt.IsZero() {
				t.Fatalf("restart replayed pending phases: %+v", run)
			}
			return
		}
	}
	t.Fatal("accepted reservation was lost")
}
