package controller

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Network activation touches every registered Agent, even when placement uses
// only a subset. Model the Agent's strict global NetworkBound reset proof over
// real HTTP so forgotten network participants cannot hide behind a clean owner.
func TestNetworkOnlyAgentIsDrainedBeforeUnrelatedRunRecovery(t *testing.T) {
	var manager *httptest.Server
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	for _, id := range []string{"network-owner-run", "failed-before-network-run", "next-network-run"} {
		run, _ := resultFixture(t, s, id, "failed", time.Now().Add(-time.Minute))
		s.state.experiments[id] = run
	}
	type networkAgent struct {
		mu    sync.Mutex
		owner string
		agent model.Agent
	}
	newNetworkAgent := func(id string, disabled bool) *networkAgent {
		state := &networkAgent{agent: model.Agent{ID: id, State: model.AgentOnline, Capacity: 1, Disabled: disabled, RunDrain: true, RunProcesses: true, StartupReconciled: true, StartedAt: time.Now().UTC(), LastSeen: time.Now().UTC()}}
		endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			state.mu.Lock()
			defer state.mu.Unlock()
			switch {
			case r.URL.Path == "/api/v1/network/fence":
				var request model.ExperimentNetworkRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				state.owner = request.RunID
				w.WriteHeader(http.StatusNoContent)
			case r.URL.Path == "/api/v1/network/activate":
				w.WriteHeader(http.StatusNoContent)
			case strings.HasSuffix(r.URL.Path, "/drain"):
				id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/drain")
				if state.owner != "" && state.owner != id {
					writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Code: model.RunWorkerPendingCode, WorkerPending: true, Error: "Run reset ownership or ingress is still pending"})
					return
				}
				state.owner = ""
				w.WriteHeader(http.StatusNoContent)
			case r.URL.Path == "/api/v1/status":
				writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: state.agent, Nodes: []model.Node{}, Partial: true})
			default:
				w.WriteHeader(http.StatusNoContent)
			}
		}))
		t.Cleanup(endpoint.Close)
		state.agent.URL = endpoint.URL
		s.state.agents[id] = state.agent
		return state
	}
	placed := newNetworkAgent("placed-agent", false)
	unplaced := newNetworkAgent("network-only-agent", true)
	manager = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.ExperimentNetworkRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, model.ExperimentNetwork{RunID: request.RunID, Epoch: request.Epoch, NetworkID: request.Epoch, NetworkName: "peers", GatewayURL: "http://gateway:18081", PeerGatewayURL: "http://peer-gateway:18081"})
	}))
	t.Cleanup(manager.Close)
	s.config.NetworkManagerURL = manager.URL
	if err := s.prepareExperimentNetwork(t.Context(), model.Experiment{ID: "network-owner-run"}); err != nil {
		t.Fatal(err)
	}
	// Peer creation records just the selected Agent. The disabled Agent also
	// acquired a network binding, and must remain in this Run's drain scope.
	if err := s.rememberRunAgent("network-owner-run", placed.agent); err != nil {
		t.Fatal(err)
	}
	if err := s.drainRunTelemetry(t.Context(), "network-owner-run"); err != nil {
		t.Fatalf("the previous Run did not drain its recorded participants: %v", err)
	}
	// This later Run fails before preparing a network or placing any Peers.
	// Its fallback cleanup must not inherit a retired Run's foreign binding.
	if err := s.drainRunTelemetry(t.Context(), "failed-before-network-run"); err != nil {
		t.Fatalf("a network-only Agent retained the prior Run and blocked unrelated recovery: %v", err)
	}
	unplaced.mu.Lock()
	owner := unplaced.owner
	unplaced.mu.Unlock()
	if owner != "" {
		t.Fatalf("retired network participant still owns Run %q", owner)
	}
	if err := s.prepareExperimentNetwork(t.Context(), model.Experiment{ID: "next-network-run"}); err != nil {
		t.Fatalf("a fresh Run could not prepare after complete cleanup: %v", err)
	}
}
