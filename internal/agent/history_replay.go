package agent

import (
	"context"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Protected by heartbeatMu. Only one bounded decoded page and a copied key are
// retained. Neither an open transaction nor a complete index survives the send.
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
	replay := s.historyReplay
	if replay == nil {
		return nil
	}
	if len(replay.pending) == 0 && !replay.eof {
		nodes, last, eof, err := s.history.replayPage(ctx, replay.last)
		if err != nil {
			return err
		}
		replay.pending, replay.last, replay.eof = nodes, last, eof
	}
	if len(replay.pending) > 0 {
		h := model.AgentHeartbeat{Agent: agent, Nodes: replay.pending, Partial: true}
		if err := sendHeartbeatBatches(h, heartbeatBodyLimit, func(data []byte, _ []model.Node) error { return s.postData(ctx, "/api/v1/agents/heartbeat", data, nil) }); err != nil {
			return err
		}
		replay.pending = nil
	}
	if replay.eof {
		s.historyReplay = nil
	}
	return nil
}
