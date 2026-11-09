package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const heartbeatBodyLimit = 10 << 20

type heartbeatLeaseContextKey struct{}

// Registration invalidates acknowledgments: a restarted Controller may have
// forgotten the full inventory. Serializing it with sends prevents an earlier
// response from acknowledging history after this reset.
func (s *Server) register(ctx context.Context) error {
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	return s.registerLocked(ctx)
}

// Caller holds heartbeatMu, including explicit discovery recovery requests.
func (s *Server) registerLocked(ctx context.Context) error {
	if err := s.postJSON(ctx, "/api/v1/agents/register", s.snapshotAgent(), nil); err != nil {
		return err
	}
	s.mu.Lock()
	for _, proc := range s.processes {
		proc.heartbeatAcknowledged = false
	}
	s.mu.Unlock()
	s.resetHistoryReplay()
	return nil
}

func (s *Server) heartbeat(ctx context.Context) error {
	finishView := s.acquireRunView()
	defer finishView()
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	stopLease := func() {}
	if s.config.RunWorkerFactory != nil {
		// Lease renewal does not wait behind a large experiment snapshot. The
		// following full partial reports still carry every observed Node.
		if err := s.postJSON(ctx, "/api/v1/agents/heartbeat", model.AgentHeartbeat{Agent: s.snapshotAgent(), Partial: true}, nil); err != nil {
			return err
		}
		leaseCtx, cancelLease := context.WithCancel(context.WithValue(ctx, heartbeatLeaseContextKey{}, true))
		leaseDone := make(chan struct{})
		go func() {
			defer close(leaseDone)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-leaseCtx.Done():
					return
				case <-ticker.C:
					_ = s.postJSON(leaseCtx, "/api/v1/agents/heartbeat", model.AgentHeartbeat{Agent: s.snapshotAgent(), Partial: true}, nil)
				}
			}
		}()
		// Only serialized full reports apply capacity revisions; late lease
		// responses must not overwrite the override acknowledged by them.
		stopLease = func() { cancelLease(); <-leaseDone }
		defer stopLease()
	}
	h, err := s.snapshotWithHistoryContext(ctx, false)
	if err != nil {
		return err
	}
	// A later lease must not make the complete observation look stale to the
	// Controller. Stop/join it and refresh compact states and the capture clock.
	stopLease()
	if s.config.RunWorkerFactory != nil {
		s.mu.RLock()
		for i, node := range h.Nodes {
			if proc := s.processes[node.ID]; proc != nil {
				if proc.exited || proc.node.State == model.NodeStopping {
					h.Nodes[i] = heartbeatNodeStatus(proc)
				} else {
					current := cloneNodeStatus(proc.node)
					current.Addresses, current.ConnectedPeers, current.RoutingPeers = node.Addresses, node.ConnectedPeers, node.RoutingPeers
					current.MeshPeers, current.TopicPeers, current.PeerScores, current.ScoreSample = node.MeshPeers, node.TopicPeers, node.PeerScores, node.ScoreSample
					current.OverlayObservedAt = node.OverlayObservedAt
					h.Nodes[i] = current
				}
			}
		}
		h.Agent = s.agentStatusLocked(h.Agent.Hostname)
		s.mu.RUnlock()
	}
	h.Partial = true
	err = sendHeartbeatBatches(h, heartbeatBodyLimit, func(data []byte, nodes []model.Node) error {
		if err := s.postData(ctx, "/api/v1/agents/heartbeat", data, nil); err != nil {
			return err
		}
		s.mu.Lock()
		for _, node := range nodes {
			if proc := s.processes[node.ID]; proc != nil && node.State == proc.node.State && node.LastSeen.Equal(proc.node.LastSeen) && processCleanupComplete(proc) {
				proc.heartbeatAcknowledged = true
			}
		}
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	return s.replayRetiredHistory(ctx, h.Agent)
}

func processCleanupComplete(proc *process) bool {
	return proc.exited && proc.cleanupErr == nil && (proc.node.State == model.NodeStopped || proc.node.State == model.NodeFailed)
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
