package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestSingleAgentActualWorkerSkipsLegacyFleet(t *testing.T) {
	f := newControllerWorkerFixture(t)
	var legacyCreates atomic.Int32
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes" {
			legacyCreates.Add(1)
			http.Error(w, "legacy Agent does not own Run workers", http.StatusBadRequest)
			return
		}
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "a-legacy", URL: "http://" + r.Host, Capacity: 20, RunDrain: true}})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer legacy.Close()
	if _, err := f.server.state.registerAgent(model.Agent{ID: "a-legacy", URL: legacy.URL, Capacity: 20, RunDrain: true}); err != nil {
		t.Fatal(err)
	}
	for _, seed := range []int64{1, 2, 3, 42} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			raw := []byte(fmt.Sprintf("version: 4\nname: single-agent-os\nseed: %d\nphases:\n  - {action: join, group: workers, type: full, count: 1, placement: single-agent}\n", seed))
			accepted, err := f.server.StartScenario(ctx, raw)
			if err != nil {
				t.Fatal(err)
			}
			waitWorkerRuns(t, f.server)
			f.server.state.mu.RLock()
			run := f.server.state.experiments[accepted.ID]
			f.server.state.mu.RUnlock()
			f.mu.Lock()
			created := append([]model.Node(nil), f.created[accepted.ID]...)
			f.mu.Unlock()
			if run.State != "completed" || len(created) != 1 || legacyCreates.Load() != 0 {
				t.Fatalf("actual worker pinned unsupported owner: state=%s error=%s created=%+v legacyCreates=%d", run.State, run.Error, created, legacyCreates.Load())
			}
		})
	}
	t.Run("capable-owner-enabled-after-start", func(t *testing.T) {
		setEnabledTest(t, f.server.state, "agent", false)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		raw := []byte("version: 4\nname: wait-for-capable-agent\nseed: 1\nphases:\n  - {action: join, group: workers, type: full, count: 1, placement: single-agent}\n")
		accepted, err := f.server.StartScenario(ctx, raw)
		if err != nil {
			t.Fatal(err)
		}
		queueAwait(t, f.server, accepted.ID, func(run model.Experiment) bool { return run.Phase == 1 })
		setEnabledTest(t, f.server.state, "agent", true)
		waitWorkerRuns(t, f.server)
		f.server.state.mu.RLock()
		run := f.server.state.experiments[accepted.ID]
		f.server.state.mu.RUnlock()
		f.mu.Lock()
		created := len(f.created[accepted.ID])
		f.mu.Unlock()
		if run.State != "completed" || created != 1 || legacyCreates.Load() != 0 {
			t.Fatalf("waiting worker did not admit the newly enabled capable owner: state=%s created=%d legacyCreates=%d", run.State, created, legacyCreates.Load())
		}
	})
}
