package agent

import (
	"context"

	"github.com/k-p2p-lab/kpl-v3/internal/contextlock"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Cursor publication is protected by heartbeatMu. The single sender may read
// its captured generation outside that lock; registration only replaces it.
// One bounded decoded page and a copied key survive a send, never a transaction.
type historyReplay struct {
	last    []byte
	pending []model.Node
	eof     bool
}

func (s *Server) resetHistoryReplay() {
	s.historyReplay = nil
	if s.history != nil {
		s.historyReplay = &historyReplay{}
	}
}

func (s *Server) replayRetiredHistory(ctx context.Context, agent model.Agent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Backfill is independent of current inventory. A slow page must not queue
	// additional senders or keep registration/terminal ACKs behind its I/O.
	if !s.historyReplaySendMu.TryLock() {
		return nil
	}
	defer s.historyReplaySendMu.Unlock()
	if err := contextlock.Lock(ctx, &s.heartbeatMu); err != nil {
		return err
	}
	replay := s.historyReplay
	s.heartbeatMu.Unlock()
	if replay == nil {
		return nil
	}
	// Only this sender reads the captured cursor. Registration replaces its
	// pointer under heartbeatMu and never mutates this old generation.
	next := *replay
	if len(next.pending) == 0 && !next.eof {
		nodes, last, eof, err := s.history.replayPage(ctx, next.last)
		if err != nil {
			return err
		}
		next.pending, next.last, next.eof = nodes, last, eof
		if err := contextlock.Lock(ctx, &s.heartbeatMu); err != nil {
			return err
		}
		if s.historyReplay != replay {
			s.heartbeatMu.Unlock()
			return nil
		}
		// Retain the exact page before sending, including on timeout/failure.
		*replay = next
		s.heartbeatMu.Unlock()
	}
	if len(next.pending) > 0 {
		h := model.AgentHeartbeat{Agent: agent, Nodes: next.pending, Partial: true}
		// A delayed backfill reply must not overwrite the capacity revision
		// applied by a newer serialized current report or registration.
		replayCtx := context.WithValue(ctx, heartbeatLeaseContextKey{}, true)
		if err := sendHeartbeatBatches(h, heartbeatBodyLimit, func(data []byte, _ []model.Node) error {
			return s.postData(replayCtx, "/api/v1/agents/heartbeat", data, nil)
		}); err != nil {
			return err
		}
	}
	if err := contextlock.Lock(ctx, &s.heartbeatMu); err != nil {
		return err
	}
	defer s.heartbeatMu.Unlock()
	if s.historyReplay == replay {
		replay.pending = nil
		if replay.eof {
			s.historyReplay = nil
		}
	}
	return nil
}
