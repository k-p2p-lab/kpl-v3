package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type uncertainNodeCreateError struct{ error }

func (e *uncertainNodeCreateError) Unwrap() error { return e.error }

func retryableCreateTransport(err error) bool {
	var networkError net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.As(err, &networkError) && networkError.Timeout()
}

func (s *Server) createOnAgent(ctx context.Context, agent model.Agent, request model.CreateNodeRequest) (model.Node, error) {
	attempts := 1
	if agent.CreateReplay && !agent.StartedAt.IsZero() {
		request.AgentStartedAt = agent.StartedAt
		attempts = 3
	}
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return model.Node{}, &uncertainNodeCreateError{errors.Join(last, err)}
			}
			return model.Node{}, err
		}
		if attempt > 1 {
			s.state.mu.RLock()
			current, exists := s.state.agents[agent.ID]
			s.state.mu.RUnlock()
			if !exists || !current.StartedAt.Equal(agent.StartedAt) || current.URL != agent.URL {
				return model.Node{}, &uncertainNodeCreateError{errors.Join(last, errors.New("Agent instance changed during creation recovery"))}
			}
		}
		var node model.Node
		err := s.callAgent(ctx, agent.URL, http.MethodPost, "/api/v1/nodes", request, &node)
		if err == nil && attempts > 1 && (node.ID != request.ID || node.RunID != request.RunID || node.Generation != request.Generation || node.AgentID != agent.ID || node.Group != request.Group) {
			err = &uncertainNodeCreateError{errors.New("Peer creation acknowledgement identity mismatch")}
		}
		if err == nil {
			if attempt > 1 {
				s.logger.Warn("Recovered Peer creation acknowledgement", "runId", request.RunID, "nodeId", request.ID, "agentId", agent.ID, "attempt", attempt)
			}
			return node, nil
		}
		var uncertain *uncertainNodeCreateError
		if !errors.As(err, &uncertain) || !retryableCreateTransport(err) || attempt == attempts || ctx.Err() != nil {
			if last != nil {
				return model.Node{}, &uncertainNodeCreateError{errors.Join(last, err)}
			}
			return model.Node{}, err
		}
		last = err
		s.logger.Warn("Peer creation response interrupted; replaying on the same Agent", "runId", request.RunID, "nodeId", request.ID, "agentId", agent.ID, "attempt", attempt, "error", err)
		timer := time.NewTimer(time.Duration(attempt) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return model.Node{}, &uncertainNodeCreateError{errors.Join(last, ctx.Err())}
		case <-timer.C:
		}
	}
	return model.Node{}, fmt.Errorf("Peer creation attempts exhausted: %w", last)
}

type uncertainCreate struct {
	runID      string
	generation uint64
	agentID    string
}

// An interrupted POST may still create a Peer. Keep its slot until the Agent
// reports that Peer or acknowledges the run fence and container cleanup.
func (s *Server) retainUncertainCreate(request model.CreateNodeRequest, agentID string) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.reservations[request.ID] != agentID {
		return // An intervening heartbeat already accounted for the Peer.
	}
	if s.state.uncertainCreates == nil {
		s.state.uncertainCreates = make(map[string]uncertainCreate)
	}
	s.state.uncertainCreates[request.ID] = uncertainCreate{request.RunID, request.Generation, agentID}
}

func (s *Server) releaseUncertainCreates(runID string, generation uint64, cleaned map[string]struct{}) {
	s.state.mu.Lock()
	changed := false
	for id, pending := range s.state.uncertainCreates {
		if pending.runID != runID || pending.generation > generation {
			continue
		}
		if _, ok := cleaned[pending.agentID]; !ok {
			continue
		}
		if s.state.reservations[id] == pending.agentID {
			delete(s.state.reservations, id)
			agent := s.state.agents[pending.agentID]
			agent.ActiveNodes = max(0, agent.ActiveNodes-1)
			s.state.agents[pending.agentID] = agent
		}
		delete(s.state.uncertainCreates, id)
		changed = true
	}
	s.state.mu.Unlock()
	if changed {
		s.state.notify()
	}
}
