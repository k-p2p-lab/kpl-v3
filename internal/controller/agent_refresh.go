package controller

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const agentRefreshTimeout = 15 * time.Second

type agentRefreshFailure struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Error string `json:"error"`
}

type agentRefreshResponse struct {
	Agents    []model.Agent         `json:"agents"`
	Requested int                   `json:"requested"`
	Refreshed int                   `json:"refreshed"`
	Failures  []agentRefreshFailure `json:"failures"`
}

func (s *Server) handleAgentRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !s.agentRefreshMu.TryLock() {
		writeError(w, http.StatusConflict, "Agent refresh is already in progress")
		return
	}
	defer s.agentRefreshMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), agentRefreshTimeout)
	defer cancel()
	writeJSON(w, http.StatusOK, s.refreshRegisteredAgents(ctx))
}

// Check offline Agents too, without restarting tasks or interrupting Peers.
// Retain unreachable records; normal heartbeat expiry determines offline state.
func (s *Server) refreshRegisteredAgents(ctx context.Context) agentRefreshResponse {
	agents := s.state.agentInventory()
	failures := make([]error, len(agents))
	jobs := make(chan int, len(agents))
	for i := range agents {
		jobs <- i
	}
	close(jobs)
	var workers sync.WaitGroup
	for range min(4, len(agents)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				agent := agents[i]
				var heartbeat model.AgentHeartbeat
				err := ctx.Err()
				if err == nil {
					err = s.callAgent(ctx, agent.URL, http.MethodGet, "/api/v1/status", nil, &heartbeat)
				}
				if err == nil && heartbeat.Agent.ID != agent.ID {
					err = fmt.Errorf("Agent identity mismatch: received %q", heartbeat.Agent.ID)
				}
				if err == nil {
					err = s.state.heartbeat(heartbeat)
				}
				failures[i] = err
			}
		}()
	}
	workers.Wait()
	s.state.markStaleAgents(agentStaleAfter)
	result := agentRefreshResponse{Requested: len(agents), Failures: []agentRefreshFailure{}}
	for i, err := range failures {
		if err != nil {
			result.Failures = append(result.Failures, agentRefreshFailure{ID: agents[i].ID, Name: agents[i].Name, Error: err.Error()})
		} else {
			result.Refreshed++
		}
	}
	result.Agents = s.state.agentInventory()
	if result.Agents == nil {
		result.Agents = []model.Agent{}
	}
	return result
}
