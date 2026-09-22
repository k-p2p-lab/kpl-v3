package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/k-p2p-lab/v3/internal/model"
)

const heartbeatBodyLimit = 10 << 20

// Registration invalidates acknowledgments: a restarted Controller may have
// forgotten the full inventory. Serializing it with sends prevents an earlier
// response from acknowledging history after this reset.
func (s *Server) register(ctx context.Context) error {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	if err := s.postJSON(ctx, "/api/v1/agents/register", s.snapshotAgent(), nil); err != nil {
		return err
	}
	s.mu.Lock()
	for _, proc := range s.processes {
		proc.heartbeatAcknowledged = false
	}
	s.mu.Unlock()
	return nil
}

func (s *Server) heartbeat(ctx context.Context) error {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	h := s.snapshotWithHistory(false)
	h.Partial = true
	return sendHeartbeatBatches(h, heartbeatBodyLimit, func(data []byte, nodes []model.Node) error {
		if err := s.postData(ctx, "/api/v1/agents/heartbeat", data, nil); err != nil {
			return err
		}
		s.mu.Lock()
		for _, node := range nodes {
			if proc := s.processes[node.ID]; node.State == model.NodeStopped && proc != nil && processSuccessfullyStopped(proc) {
				proc.heartbeatAcknowledged = true
			}
		}
		s.mu.Unlock()
		return nil
	})
}

func processSuccessfullyStopped(proc *process) bool {
	return proc.exited && proc.cleanupErr == nil && proc.node.State == model.NodeStopped
}

// Encode each node once, splitting by actual JSON bytes rather than node count.
// All batches retain the capture timestamp; partial reports do not infer exits
// for omitted nodes. An empty inventory still refreshes the Agent lease.
func sendHeartbeatBatches(h model.AgentHeartbeat, limit int, send func([]byte, []model.Node) error) error {
	header, err := json.Marshal(struct {
		Agent   model.Agent `json:"agent"`
		Partial bool        `json:"partial"`
	}{h.Agent, true})
	if err != nil {
		return fmt.Errorf("encode heartbeat Agent: %w", err)
	}
	prefix := append(header[:len(header)-1], []byte(",\"nodes\":[")...)
	if len(prefix)+2 > limit {
		return fmt.Errorf("heartbeat Agent metadata exceeds %d-byte JSON body limit", limit)
	}
	var data bytes.Buffer
	data.Write(prefix)
	start := 0
	flush := func(end int) error {
		data.WriteString("]}")
		if err := send(data.Bytes(), h.Nodes[start:end]); err != nil {
			return err
		}
		data.Reset()
		data.Write(prefix)
		start = end
		return nil
	}
	for i, node := range h.Nodes {
		encoded, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("encode heartbeat node %q: %w", node.ID, err)
		}
		if len(prefix)+len(encoded)+2 > limit {
			return fmt.Errorf("heartbeat node %q exceeds %d-byte JSON body limit", node.ID, limit)
		}
		separator := 0
		if i > start {
			separator = 1
		}
		if data.Len()+separator+len(encoded)+2 > limit {
			if err := flush(i); err != nil {
				return err
			}
		}
		if i > start {
			data.WriteByte(',')
		}
		data.Write(encoded)
	}
	return flush(len(h.Nodes))
}
