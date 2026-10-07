package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestEachRepetitionPreparesNetworkBeforeExperimentClock(t *testing.T) {
	var server *Server
	var agent model.Agent
	var fences, resets, activations atomic.Int32
	var timesMu sync.Mutex
	readyTimes := map[string]time.Time{}
	agentHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/network/fence":
			fences.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/network/activate":
			var network model.ExperimentNetwork
			if err := json.NewDecoder(r.Body).Decode(&network); err != nil {
				t.Error(err)
			}
			server.state.mu.RLock()
			run := server.state.experiments[network.RunID]
			server.state.mu.RUnlock()
			if !run.StartedAt.IsZero() || run.Phase != 0 {
				t.Errorf("experiment clock/phases advanced before activation: %+v", run)
			}
			activations.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/status":
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{}})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer agentHTTP.Close()
	agent = model.Agent{ID: "agent", URL: agentHTTP.URL, State: model.AgentOnline, Capacity: 10, RunDrain: true, StartupReconciled: true, StartedAt: time.Now().UTC(), LastSeen: time.Now().UTC()}
	managerHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.ExperimentNetworkRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		count := resets.Add(1)
		if fences.Load() != count {
			t.Error("network reset preceded cleanup confirmation")
		}
		timesMu.Lock()
		readyTimes[request.RunID] = time.Now().UTC()
		timesMu.Unlock()
		_ = json.NewEncoder(w).Encode(model.ExperimentNetwork{RunID: request.RunID, Epoch: request.Epoch, NetworkID: fmt.Sprintf("network-%d", count), NetworkName: "peers", GatewayURL: "http://gateway:18081", PeerGatewayURL: "http://peer-gateway:18081"})
	}))
	defer managerHTTP.Close()
	server = New(ServerConfig{DataDir: t.TempDir(), NetworkManagerURL: managerHTTP.URL}, nil)
	server.state.agents[agent.ID] = agent
	_, err := server.StartScenarioRepeated(t.Context(), []byte("version: 2\nname: network-reset\nphases:\n  - action: wait\n    duration: 1ms\n"), 2)
	if err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, server)
	if resets.Load() != 2 || activations.Load() != 2 {
		t.Fatalf("resets=%d activations=%d", resets.Load(), activations.Load())
	}
	for i, run := range runs {
		timesMu.Lock()
		ready := readyTimes[run.ID]
		timesMu.Unlock()
		if run.State != "completed" || run.PeerNetworkID != fmt.Sprintf("network-%d", i+1) || run.StartedAt.Before(ready) {
			t.Fatalf("run clock/network: %+v ready=%s", run, ready)
		}
	}
}

func TestDisabledAgentCleanupFailurePreventsNetworkReset(t *testing.T) {
	var resets atomic.Int32
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { resets.Add(1); w.WriteHeader(500) }))
	defer manager.Close()
	busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Peer removal unconfirmed", http.StatusConflict)
	}))
	defer busy.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), NetworkManagerURL: manager.URL}, nil)
	s.state.agents["disabled"] = model.Agent{ID: "disabled", URL: busy.URL, Disabled: true}
	err := s.prepareExperimentNetwork(t.Context(), model.Experiment{ID: "run"})
	if err == nil || resets.Load() != 0 {
		t.Fatalf("reset ignored disabled Agent cleanup: %v count=%d", err, resets.Load())
	}
}
