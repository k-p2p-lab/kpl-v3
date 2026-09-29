package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

const peerHistoryRecent = 64
const peerHistoryLimit = 4096
const runFenceCacheLimit = 128
const historyPruneBatch = 128

var errTelemetryBacklogFull = errors.New("Agent telemetry backlog is full; retry Peer admission after delivery recovers")

var errPeerHistoryFull = errors.New("Agent terminal evidence is awaiting delivery or local storage; retry after recovery")

// Compact terminal identities live in one local database; run fences remain
// directly addressable files. No full record index is loaded into the Go heap.
// Maintenance never holds Server.mu while accessing disk.
type agentHistory struct {
	directory string
	mu        sync.Mutex
	storeMu   sync.Mutex
	db        *bolt.DB
	closed    bool
}

func openAgentHistory(dataDir string) (*agentHistory, error) {
	h := &agentHistory{directory: filepath.Join(dataDir, "peer-history")}
	for _, kind := range []string{"runs", "fences"} {
		if err := os.MkdirAll(filepath.Join(h.directory, kind), 0700); err != nil {
			return nil, err
		}
	}
	return h, nil
}
func (h *agentHistory) path(kind, id string) string {
	hash := sha256.Sum256([]byte(id))
	return filepath.Join(h.directory, kind, hex.EncodeToString(hash[:])+".json")
}
func (h *agentHistory) contains(id string) (bool, error) {
	if h == nil {
		return false, nil
	}
	return h.containsStored(id)
}
func (h *agentHistory) fence(id string) (uint64, bool, error) {
	if h == nil {
		return 0, false, nil
	}
	f, err := os.Open(h.path("fences", id))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	var generation uint64
	decoder := json.NewDecoder(io.LimitReader(f, 128))
	if err := decoder.Decode(&generation); err != nil {
		return 0, false, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return 0, false, errors.New("invalid stored run fence")
	}
	return generation, true, nil
}

// Caller holds history.mu. An older stop must never lower a persisted fence.
func (h *agentHistory) saveFence(id string, generation uint64) error {
	previous, exists, err := h.fence(id)
	if err != nil {
		return err
	}
	if exists && previous >= generation {
		return nil
	}
	data, _ := json.Marshal(generation)
	return writeTelemetryFile(h.path("fences", id), data)
}

// Returns with mu held on success. A fence may be written/evicted while the
// admission checks disk, so repeat if the fence revision changed. This keeps a
// late create fenced without doing storage I/O under the heartbeat/state lock.
func (s *Server) lockAdmission(ctx context.Context, request model.CreateNodeRequest) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.RLock()
		revision := s.historyRevision
		s.mu.RUnlock()
		used, err := s.history.contains(request.ID)
		if err != nil {
			return fmt.Errorf("check retired Peer identity: %w", err)
		}
		if used {
			return fmt.Errorf("node %q already exists in retired history", request.ID)
		}
		stored, exists, err := s.history.fence(request.RunID)
		if err != nil {
			return fmt.Errorf("check stored run fence: %w", err)
		}
		s.mu.Lock()
		if revision != s.historyRevision {
			s.mu.Unlock()
			continue
		}
		if s.shuttingDown {
			s.mu.Unlock()
			return errors.New("agent is shutting down")
		}
		if s.fencingAll {
			s.mu.Unlock()
			return errors.New("agent is stopping Peers")
		}
		current, currentExists := s.runFences[request.RunID]
		if currentExists && (!exists || current > stored) {
			stored, exists = current, true
		}
		if exists && request.Generation <= stored {
			s.mu.Unlock()
			return fmt.Errorf("run %q generation %d is fenced at generation %d", request.RunID, request.Generation, stored)
		}
		if _, exists := s.processes[request.ID]; exists {
			s.mu.Unlock()
			return fmt.Errorf("node %q already exists", request.ID)
		}
		occupied := s.capacityUsedLocked()
		if occupied >= s.capacityLocked() {
			s.mu.Unlock()
			return errCapacityReached
		}
		s.eventsMu.Lock()
		pendingTerminations := len(s.terminations)
		telemetryFull := s.spool != nil && (s.pendingEventsLocked() >= telemetryBacklogEvents || s.pendingBytesLocked() >= telemetryBacklogBytes)
		s.eventsMu.Unlock()
		if telemetryFull {
			s.mu.Unlock()
			return errTelemetryBacklogFull
		}
		if s.history != nil && (len(s.processes)-occupied >= peerHistoryLimit || pendingTerminations >= peerHistoryLimit) {
			s.mu.Unlock()
			return errPeerHistoryFull
		}
		return nil
	}
}

func (s *Server) reclaimHistory() error {
	if s.history == nil {
		return nil
	}
	s.history.mu.Lock()
	defer s.history.mu.Unlock()
	s.mu.RLock()
	candidates := []*process{}
	for _, proc := range s.processes {
		if proc.heartbeatAcknowledged && processCleanupComplete(proc) {
			candidates = append(candidates, proc)
		}
	}
	before := len(s.processes)
	fences := make(map[string]uint64, len(s.runFences))
	for id, generation := range s.runFences {
		fences[id] = generation
	}
	s.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].node.LastSeen.Before(candidates[j].node.LastSeen) })
	// Config was copied into the container at creation. Once removal and the
	// final heartbeat are acknowledged, it is no longer needed. Delete only our
	// generated file; never recursively remove user files or lifecycle evidence.
	cleaned := 0
	for _, proc := range candidates {
		s.mu.RLock()
		path := proc.configPath
		s.mu.RUnlock()
		if path == "" {
			continue
		}
		if cleaned >= historyPruneBatch {
			break
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		_ = os.Remove(filepath.Dir(path)) // Only succeeds for an empty directory.
		s.mu.Lock()
		if proc.configPath == path {
			proc.configPath = ""
		}
		s.mu.Unlock()
		cleaned++
	}
	count := min(historyPruneBatch, max(0, len(candidates)-peerHistoryRecent))
	// Commit the whole bounded batch before dropping any in-memory evidence.
	// A registration racing this write can still invalidate acknowledgements.
	nodes := make([]model.Node, 0, count)
	s.mu.RLock()
	for _, proc := range candidates[:count] {
		nodes = append(nodes, heartbeatNodeStatus(proc))
	}
	s.mu.RUnlock()
	if err := s.history.saveNodes(nodes); err != nil {
		return err
	}
	s.mu.Lock()
	for i, proc := range candidates[:count] {
		if s.processes[nodes[i].ID] == proc && proc.heartbeatAcknowledged && processCleanupComplete(proc) {
			delete(s.processes, nodes[i].ID)
			s.historyRevision++
			s.historyPeersRetired++
		}
	}
	s.mu.Unlock()
	ids := make([]string, 0, len(fences))
	for id := range fences {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids[:min(historyPruneBatch, max(0, len(ids)-runFenceCacheLimit))] {
		generation := fences[id]
		if err := s.history.saveFence(id, generation); err != nil {
			return err
		}
		s.mu.Lock()
		if current, exists := s.runFences[id]; exists && current == generation {
			delete(s.runFences, id)
			s.historyRevision++
			s.historyFencesRetired++
		}
		s.mu.Unlock()
	}
	// Go maps retain their buckets after delete. Rebuild after a large backlog
	// shrinks so recovery releases the high-water allocation as well as values.
	s.mu.Lock()
	s.historyMapPeak = max(s.historyMapPeak, before)
	if len(s.processes)*2 < s.historyMapPeak {
		compact := make(map[string]*process, len(s.processes))
		for id, proc := range s.processes {
			compact[id] = proc
		}
		s.processes = compact
		s.historyMapPeak = len(compact)
	}
	if len(fences) > runFenceCacheLimit && len(s.runFences) <= runFenceCacheLimit {
		compact := make(map[string]uint64, len(s.runFences))
		for id, generation := range s.runFences {
			compact[id] = generation
		}
		s.runFences = compact
	}
	s.mu.Unlock()
	return s.history.cleanLegacyNodes()
}
func (s *Server) historyLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := s.reclaimHistory(); err != nil {
			s.mu.Lock()
			s.historyErrors++
			s.mu.Unlock()
			s.logger.Warn("Peer history cleanup failed; retaining records", "error", err)
		}
	}
}

// Stop-all must also fence runs whose acknowledged Peers have left memory.
// Stream run IDs from disk; do not recreate an unbounded in-memory fence map.
func (s *Server) fenceRetiredRuns(ctx context.Context) error {
	if s.history == nil {
		return nil
	}
	s.history.mu.Lock()
	defer s.history.mu.Unlock()
	dir, err := os.Open(filepath.Join(s.history.directory, "runs"))
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			file, err := os.Open(filepath.Join(dir.Name(), entry.Name()))
			if err != nil {
				return err
			}
			var id string
			err = json.NewDecoder(io.LimitReader(file, 16<<10)).Decode(&id)
			file.Close()
			if err != nil {
				return err
			}
			if err := s.history.saveFence(id, ^uint64(0)); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}
