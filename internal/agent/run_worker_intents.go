package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type peerCreationIntent struct {
	Version        int       `json:"version"`
	AgentID        string    `json:"agentId"`
	RunID          string    `json:"runId"`
	NodeID         string    `json:"nodeId"`
	Generation     uint64    `json:"generation"`
	Epoch          string    `json:"epoch"`
	AttemptName    string    `json:"attemptName"`
	ContainerID    string    `json:"containerId,omitempty"`
	StartedAt      time.Time `json:"startedAt"`
	AdmissionUntil time.Time `json:"admissionUntil"`
	TerminationID  string    `json:"terminationId"`
	TerminatedAt   time.Time `json:"terminatedAt,omitempty"`
}

func (s *Server) beginPeerIntent(proc *process, node model.Node) (model.Node, error) {
	name, err := peerContainerName(node)
	if err != nil {
		return node, err
	}
	directory := filepath.Join(s.config.DataDir, "runtime-peer-intents")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return node, err
	}
	if err := syncTelemetryDirectory(s.config.DataDir); err != nil {
		return node, err
	}
	hash := sha256.Sum256([]byte(node.ID))
	path := filepath.Join(directory, hex.EncodeToString(hash[:])+".json")
	s.mu.RLock()
	epoch := proc.workerEpoch
	s.mu.RUnlock()
	intent := peerCreationIntent{Version: 1, AgentID: s.config.ID, RunID: node.RunID, NodeID: node.ID, Generation: node.Generation, Epoch: epoch, AttemptName: name, StartedAt: node.StartedAt, AdmissionUntil: time.Now().Add(dockerAdmissionTimeout), TerminationID: rand.Text()}
	data, err := json.Marshal(intent)
	if err == nil {
		err = writeTelemetryFile(path, data)
	}
	if err != nil {
		return node, fmt.Errorf("persist Peer creation intent before Docker admission: %w", err)
	}
	s.mu.Lock()
	proc.intentPath, proc.containerID = path, name
	s.mu.Unlock()
	node.Metadata = maps.Clone(node.Metadata)
	node.Metadata["supervisorAttemptName"] = name
	node.Metadata["supervisorAdmissionUntil"] = intent.AdmissionUntil.UTC().Format(time.RFC3339Nano)
	return node, nil
}

func (s *Server) commitPeerIntent(proc *process, id string) error {
	s.mu.RLock()
	path := proc.intentPath
	s.mu.RUnlock()
	if path == "" {
		return errors.New("Peer creation intent is missing")
	}
	intent, err := s.readPeerIntent(path)
	if err != nil {
		return err
	}
	intent.ContainerID = id
	data, err := json.Marshal(intent)
	if err == nil {
		err = writeTelemetryFile(path, data)
	}
	return err
}

func (s *Server) clearPeerIntent(proc *process) error {
	s.mu.RLock()
	path := proc.intentPath
	s.mu.RUnlock()
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove confirmed Peer creation intent: %w", err)
	}
	if err := syncTelemetryDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	s.mu.Lock()
	if proc.intentPath == path {
		proc.intentPath = ""
	}
	s.mu.Unlock()
	return nil
}

func (s *Server) completePeerIntent(proc *process, event model.TraceEvent, state, message string) error {
	s.mu.RLock()
	path := proc.intentPath
	node := compactRunWorkerNode(cloneNodeStatus(proc.node))
	s.mu.RUnlock()
	if path == "" {
		return s.queueTermination(event)
	}
	intent, err := s.readPeerIntent(path)
	if err != nil {
		return err
	}
	if intent.TerminationID == "" {
		intent.TerminationID = rand.Text()
	}
	if intent.TerminatedAt.IsZero() {
		intent.TerminatedAt = event.Timestamp
	}
	data, err := json.Marshal(intent)
	if err == nil {
		err = writeTelemetryFile(path, data)
	}
	if err != nil {
		return err
	}
	node.State, node.Error, node.LastSeen = state, message, intent.TerminatedAt
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}
	node.Metadata["cleanupComplete"] = "true"
	node.Metadata["stoppedAt"] = intent.TerminatedAt.Format(time.RFC3339Nano)
	s.history.mu.Lock()
	err = s.history.saveNodes([]model.Node{node})
	s.history.mu.Unlock()
	if err != nil {
		return err
	}
	event.EventID, event.Timestamp = intent.TerminationID, intent.TerminatedAt
	if err := s.queueTermination(event); err != nil {
		return err
	}
	return s.clearPeerIntent(proc)
}

// A supervisor restart owns old intents independently of the lost Run process.
// Await an interrupted create's bounded admission window before reconciling its
// unique name, and retain evidence until Docker cleanup and local writes succeed.
func (s *Server) recoverPeerIntents(ctx context.Context) error {
	directory := filepath.Join(s.config.DataDir, "runtime-peer-intents")
	dir, err := os.Open(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			intent, err := s.readPeerIntent(path)
			if err != nil {
				return err
			}
			if intent.ContainerID == "" {
				if remaining := time.Until(intent.AdmissionUntil); remaining > 0 {
					if remaining > dockerAdmissionTimeout+time.Second {
						return errors.New("saved Peer admission deadline is invalid")
					}
					timer := time.NewTimer(remaining)
					select {
					case <-ctx.Done():
						timer.Stop()
						return ctx.Err()
					case <-timer.C:
					}
				}
			}
			target := intent.ContainerID
			if target == "" {
				target = intent.AttemptName
			}
			if err := s.docker.remove(ctx, target); err != nil {
				return err
			}
			s.history.mu.Lock()
			err = s.history.saveFence(intent.RunID, ^uint64(0))
			if err == nil {
				err = s.history.saveNodes([]model.Node{{ID: intent.NodeID, AgentID: intent.AgentID, RunID: intent.RunID, Generation: intent.Generation, State: model.NodeFailed, StartedAt: intent.StartedAt, LastSeen: time.Now().UTC(), Error: "Agent supervisor restarted before Peer finalization", Metadata: map[string]string{"runtime": "docker", "cleanupComplete": "true"}}})
			}
			s.history.mu.Unlock()
			if err != nil {
				return err
			}
			if intent.TerminationID == "" {
				intent.TerminationID = rand.Text()
			}
			if intent.TerminatedAt.IsZero() {
				intent.TerminatedAt = time.Now().UTC()
			}
			data, err := json.Marshal(intent)
			if err == nil {
				err = writeTelemetryFile(path, data)
			}
			if err != nil {
				return err
			}
			if err := s.queueTermination(model.TraceEvent{EventID: intent.TerminationID, AgentID: intent.AgentID, RunID: intent.RunID, NodeID: intent.NodeID, Type: "measurement_terminated", Timestamp: intent.TerminatedAt}); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			if err := syncTelemetryDirectory(directory); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *Server) readPeerIntent(path string) (peerCreationIntent, error) {
	var intent peerCreationIntent
	info, err := os.Lstat(path)
	if err != nil {
		return intent, err
	}
	if !info.Mode().IsRegular() || info.Size() > 64<<10 {
		return intent, errors.New("unsafe saved Peer creation intent; retaining evidence")
	}
	file, err := os.Open(path)
	if err != nil {
		return intent, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10))
	if err := decoder.Decode(&intent); err != nil {
		return intent, err
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || intent.Version != 1 || intent.AgentID != s.config.ID || intent.NodeID == "" || intent.RunID == "" || !dockerPeerName.MatchString(intent.AttemptName) || intent.ContainerID != "" && !dockerContainerID.MatchString(intent.ContainerID) {
		return intent, errors.New("invalid saved Peer creation intent; retaining evidence")
	}
	return intent, nil
}
