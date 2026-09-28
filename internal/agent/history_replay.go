package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Protected by heartbeatMu. Replay one bounded page after the live heartbeat,
// never materializing all historical records when the Controller restarts.
type historyReplay struct {
	directory *os.File
	next      string
	pending   []model.Node
	eof       bool
}

func (s *Server) resetHistoryReplay() {
	if s.historyReplay != nil && s.historyReplay.directory != nil {
		_ = s.historyReplay.directory.Close()
	}
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
	if replay.directory == nil && !replay.eof {
		directory, err := os.Open(filepath.Join(s.history.directory, "nodes"))
		if err != nil {
			return err
		}
		replay.directory = directory
	}
	if len(replay.pending) == 0 && !replay.eof {
		bytes := 0
		for len(replay.pending) < historyPruneBatch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if replay.next == "" {
				entries, err := replay.directory.ReadDir(1)
				if err == io.EOF {
					replay.eof = true
					replay.directory.Close()
					replay.directory = nil
					break
				}
				if err != nil {
					return err
				}
				entry := entries[0]
				if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
					continue
				}
				replay.next = entry.Name()
			}
			path := filepath.Join(s.history.directory, "nodes", replay.next)
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() > heartbeatBodyLimit {
				return errors.New("invalid retired Peer record")
			}
			// A single large record may use the whole heartbeat budget. Ordinary
			// replay pages stop at 1 MiB of encoded records and 128 Peers.
			if len(replay.pending) > 0 && int64(bytes)+info.Size() > 1<<20 {
				break
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var node model.Node
			if err := json.Unmarshal(data, &node); err != nil {
				return fmt.Errorf("read retired Peer: %w", err)
			}
			if node.ID == "" || (node.State != model.NodeStopped && node.State != model.NodeFailed) {
				return errors.New("invalid retired Peer identity or state")
			}
			bytes += len(data)
			replay.pending = append(replay.pending, node)
			replay.next = ""
		}
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
