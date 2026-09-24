package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// The run workers have stopped before this method starts. An Agent acknowledges
// only after removing remaining containers and forwarding its entire event queue.
func (s *Server) cleanupAgents() error {
	s.state.mu.RLock()
	agents := make([]model.Agent, 0, len(s.state.agents))
	for _, agent := range s.state.agents {
		// Already removed/offline Agents with no occupied peers need no new RPC.
		if agentIsOnline(agent, time.Now()) || agent.ActiveNodes > 0 {
			agents = append(agents, agent)
		}
	}
	s.state.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 190*time.Second)
	defer cancel()
	failures := make(chan error, len(agents))
	var group sync.WaitGroup
	for _, agent := range agents {
		group.Add(1)
		go func(agent model.Agent) {
			defer group.Done()
			if err := s.callAgent(ctx, agent.URL, http.MethodDelete, "/api/v1/nodes", nil, nil); err != nil {
				failures <- fmt.Errorf("Agent %s shutdown cleanup/drain: %w", agent.ID, err)
			}
		}(agent)
	}
	group.Wait()
	close(failures)
	var result []error
	for err := range failures {
		result = append(result, err)
	}
	return errors.Join(result...)
}
