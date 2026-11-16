package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestSingleAgentJoinSelectsCapableOwnerBeforePinning(t *testing.T) {
	for _, seed := range []int64{0, 1, 2, 3} {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("seed-%d/reversed-%t", seed, reversed), func(t *testing.T) {
				s := newLifecycleTestController(t)
				s.config.RunWorkers = true
				var legacyCreates, capableCreates atomic.Int32
				endpoint := func(id string, calls *atomic.Int32) *httptest.Server {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						var request model.CreateNodeRequest
						if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						w.WriteHeader(http.StatusCreated)
						_ = json.NewEncoder(w).Encode(model.Node{ID: request.ID, RunID: request.RunID, AgentID: id, Generation: request.Generation, State: model.NodeStarting})
					}))
					t.Cleanup(server.Close)
					return server
				}
				legacy, capable := endpoint("a-legacy", &legacyCreates), endpoint("b-capable", &capableCreates)
				agents := []model.Agent{
					{ID: "a-legacy", URL: legacy.URL, Capacity: 2, State: model.AgentOnline, LastSeen: time.Now().UTC()},
					{ID: "b-capable", URL: capable.URL, Capacity: 2, State: model.AgentOnline, LastSeen: time.Now().UTC(), RunProcesses: true},
				}
				if reversed {
					agents[0], agents[1] = agents[1], agents[0]
				}
				for _, agent := range agents {
					s.state.agents[agent.ID] = agent
				}
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				defer cancel()
				phase := scenario.Phase{Action: "join", Group: "workers", NodeType: "full", Placement: "single-agent", Count: 1}
				err := s.runJoin(ctx, "single-agent-capability", 1, phase, rand.New(rand.NewSource(seed)))
				if err != nil || legacyCreates.Load() != 0 || capableCreates.Load() != 1 {
					t.Fatalf("single-agent join pinned an ineligible owner: err=%v legacy=%d capable=%d", err, legacyCreates.Load(), capableCreates.Load())
				}
			})
		}
	}
}

func TestSingleAgentSelectionSeedIgnoresIneligibleFleetOrder(t *testing.T) {
	var expected []string
	for _, order := range [][]string{{"b-capable", "d-capable"}, {"d-capable", "b-capable", "c-legacy", "a-legacy"}, {"a-legacy", "b-capable", "c-legacy", "d-capable"}} {
		s := newLifecycleTestController(t)
		s.config.RunWorkers = true
		for _, id := range order {
			s.state.agents[id] = model.Agent{ID: id, Capacity: 1, State: model.AgentOnline, LastSeen: time.Now().UTC(), RunProcesses: id == "b-capable" || id == "d-capable"}
		}
		rng := rand.New(rand.NewSource(42))
		var selected []string
		for range 8 {
			owner, err := s.selectBatchAgent(t.Context(), rng)
			if err != nil {
				t.Fatal(err)
			}
			selected = append(selected, owner)
		}
		if expected == nil {
			expected = selected
		} else if !slices.Equal(expected, selected) {
			t.Fatalf("ineligible fleet entries changed seeded placement: expected=%v actual=%v", expected, selected)
		}
	}
}

func TestSingleAgentSelectionKeepsCapabilityAndAvailabilityContracts(t *testing.T) {
	for _, mode := range []string{"no-capability", "legacy-controller", "disabled", "offline", "cooldown"} {
		t.Run(mode, func(t *testing.T) {
			s := newLifecycleTestController(t)
			s.config.RunWorkers = mode != "legacy-controller"
			agent := model.Agent{ID: "only", Capacity: 1, State: model.AgentOnline, LastSeen: time.Now().UTC(), RunProcesses: true}
			switch mode {
			case "no-capability", "legacy-controller":
				agent.RunProcesses = false
			case "disabled":
				agent.Disabled = true
			case "offline":
				agent.State = model.AgentOffline
			case "cooldown":
				s.state.agentAdmissionUntil[agent.ID] = time.Now().Add(time.Hour)
			}
			s.state.agents[agent.ID] = agent
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			selected, err := s.selectBatchAgent(ctx, rand.New(rand.NewSource(42)))
			if mode == "legacy-controller" {
				if err != nil || selected != agent.ID {
					t.Fatalf("legacy Controller no longer accepts its legacy Agent: owner=%q err=%v", selected, err)
				}
			} else if selected != "" || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("ineligible Agent was selected: owner=%q err=%v", selected, err)
			}
		})
	}
}
