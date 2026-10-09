package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

// An explicit instance enables replay. Legacy callers retain duplicate-ID
// rejection. A retry must never create a replacement after an Agent restart.
func (s *Server) createRequestHash(request model.CreateNodeRequest) (string, error) {
	if request.AgentStartedAt.IsZero() {
		return "", nil
	}
	if !request.AgentStartedAt.Equal(s.startedAt) {
		return "", errors.New("Agent instance changed; refusing Peer creation replay")
	}
	data, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// Existing process/history storage owns receipts, so churn does not add an
// unbounded replay cache. Never restart a terminal Peer to recover a response.
func (s *Server) replayCreate(ctx context.Context, id, hash string) (model.Node, bool, error) {
	s.mu.RLock()
	proc := s.processes[id]
	var done <-chan struct{}
	if proc != nil {
		if proc.node.Metadata["createRequestHash"] != hash {
			s.mu.RUnlock()
			return model.Node{}, true, fmt.Errorf("node %q already exists with a different create request", id)
		}
		done = proc.admissionDone
	}
	s.mu.RUnlock()
	var node model.Node
	if proc != nil {
		if done != nil {
			select {
			case <-ctx.Done():
				return model.Node{}, true, ctx.Err()
			case <-done:
			}
		}
		s.mu.RLock()
		node = heartbeatNodeStatus(proc)
		s.mu.RUnlock()
	} else {
		var found bool
		var err error
		node, found, err = s.history.createReceipt(id)
		if !found || err != nil {
			return model.Node{}, found, err
		}
	}
	if node.Metadata["createRequestHash"] != hash {
		return model.Node{}, true, fmt.Errorf("node %q already exists with a different create request", id)
	}
	if node.Metadata["createAdmission"] != "accepted" {
		return model.Node{}, true, fmt.Errorf("node %q admission did not complete: %s", id, node.Error)
	}
	return node, true, nil
}

func (h *agentHistory) createReceipt(id string) (model.Node, bool, error) {
	var node model.Node
	if h == nil {
		return node, false, nil
	}
	db, err := h.store()
	if err != nil {
		return node, false, err
	}
	hash := sha256.Sum256([]byte(id))
	found := false
	err = db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(historyNodesBucket).Get(hash[:])
		if data == nil {
			return nil
		}
		found = true
		var err error
		node, err = validateHistoryNode(data)
		return err
	})
	if err == nil && found && node.ID != id {
		err = errors.New("retired Peer identity mismatch")
	}
	return node, found, err
}
