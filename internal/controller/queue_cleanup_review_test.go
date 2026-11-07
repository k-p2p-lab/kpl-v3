package controller

import (
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

func TestQueueCleanupIncludesLateDeletionBarrier(t *testing.T) {
	for _, failLateCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "refused-and-canceled"}[failLateCleanup], func(t *testing.T) {
			testQueueCleanupLateDeletion(t, failLateCleanup)
		})
	}
}

func testQueueCleanupLateDeletion(t *testing.T, failLateCleanup bool) {
	t.Helper()
	const firstID, lateID = "retained-a", "late-b"
	var agent model.Agent
	firstEntered, firstRelease, lateFenced := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var firstOnce, releaseOnce, lateOnce sync.Once
	var reportedLate, stoppedLate atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if strings.Contains(r.URL.Path, "/runs/"+firstID+"/") {
				firstOnce.Do(func() { close(firstEntered); <-firstRelease })
			}
			if strings.Contains(r.URL.Path, "/runs/"+lateID+"/") {
				lateOnce.Do(func() { close(lateFenced) })
				if failLateCleanup {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				stoppedLate.Store(true)
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodGet {
			heartbeat := model.AgentHeartbeat{Agent: agent}
			heartbeat.Agent.LastSeen = time.Now().UTC()
			if reportedLate.Load() && !stoppedLate.Load() {
				heartbeat.Nodes = []model.Node{{ID: "late-peer", RunID: lateID, AgentID: agent.ID, State: model.NodeReady}}
			}
			_ = json.NewEncoder(w).Encode(heartbeat)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(api.Close)
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	agent = model.Agent{ID: "owner", URL: api.URL, State: model.AgentOnline, StartedAt: time.Now().Add(-time.Hour), LastSeen: time.Now(), RunDrain: true}
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
	for _, run := range []model.Experiment{
		{ID: firstID, State: "completed", CleanupState: "retained", DataState: "collecting", StartedAt: time.Now()},
		{ID: lateID, State: "completed", CleanupState: "complete", DataState: "complete", StartedAt: time.Now()},
	} {
		s.state.experiments[run.ID] = run
		if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
			t.Fatal(err)
		}
		if err := s.rememberRunAgent(run.ID, agent); err != nil {
			t.Fatal(err)
		}
	}
	work := queueTestEnqueue(t, s, "analysis")
	t.Cleanup(func() { releaseOnce.Do(func() { close(firstRelease) }) })
	queueTestAwait(t, firstEntered, "first cleanup")
	reportedLate.Store(true)
	current := agent
	current.LastSeen = time.Now().UTC()
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: current, Nodes: []model.Node{{ID: "late-peer", RunID: lateID, State: model.NodeReady}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSavedResult(lateID); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(firstRelease) })
	if failLateCleanup {
		queueTestAwait(t, lateFenced, "late refused cleanup")
		select {
		case <-work.entered:
			t.Fatal("analysis entered despite refusal of the newly known cleanup")
		default:
		}
		if err := s.cancelWork(work.slot.id); err != nil {
			t.Fatal(err)
		}
		queueTestAwait(t, work.finished, "canceled late cleanup waiter")
		s.cancelMu.Lock()
		pending := false
		for _, id := range s.queueCleanup {
			pending = pending || id == lateID
		}
		s.cancelMu.Unlock()
		if !pending {
			t.Fatal("canceling analysis discarded the unresolved late cleanup obligation")
		}
		if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, lateID)); err != nil {
			t.Fatalf("late cleanup lost its durable deletion-safe marker: %v", err)
		}
		return
	}
	queueTestAwait(t, work.entered, "analysis admission after cleanup")
	select {
	case <-lateFenced:
	default:
		t.Fatal("analysis entered while a known late cleanup barrier was still pending")
	}
}
