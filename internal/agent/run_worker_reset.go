package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var errRunResetPending = errors.New("previous Run reset is not complete")

type runResetProof struct {
	RunID               string `json:"runId"`
	ActivePeers         int    `json:"activePeers"`
	Reservations        int    `json:"reservations"`
	CleanupPending      int    `json:"cleanupPending"`
	CreationIntents     int    `json:"creationIntents"`
	TelemetryEvents     int    `json:"telemetryEvents"`
	TelemetryIngress    int32  `json:"telemetryIngress"`
	VolatilePeerFields  int    `json:"volatilePeerFields"`
	HistoryRecords      int    `json:"historyRecords"`
	FenceCacheRecords   int    `json:"fenceCacheRecords"`
	TemporaryConfigs    int    `json:"temporaryConfigs"`
	PeerWaitConnections int64  `json:"peerWaitConnections"`
	WorkerReaped        bool   `json:"workerReaped"`
	SnapshotReaders     int    `json:"snapshotReaders"`
	NetworkBound        bool   `json:"networkBound"`
}

func (s *Server) runWorkerAdmissionReady(id string) error {
	s.runWorkersMu.Lock()
	if s.runWorkers[id] != nil {
		s.runWorkersMu.Unlock()
		return nil
	}
	other := len(s.runWorkers) != 0
	s.runWorkersMu.Unlock()
	if other {
		return errRunResetPending
	}
	s.eventsMu.Lock()
	dirty := false
	if s.spool != nil {
		for _, record := range s.spool.records {
			if record.runID != id {
				dirty = true
				break
			}
		}
	}
	for _, event := range s.events {
		if event.RunID != id {
			dirty = true
		}
	}
	for _, event := range s.terminations {
		if event.RunID != id {
			dirty = true
		}
	}
	for run, count := range s.inFlightRuns {
		if run != id && count > 0 {
			dirty = true
		}
	}
	s.eventsMu.Unlock()
	if dirty {
		return errRunResetPending
	}
	intents, err := s.countPeerIntents("")
	if err != nil {
		return fmt.Errorf("%w: %v", errRunResetPending, err)
	}
	if intents != 0 {
		return errRunResetPending
	}
	return nil
}

func (s *Server) countPeerIntents(id string) (int, error) {
	dir, err := os.Open(filepath.Join(s.config.DataDir, "runtime-peer-intents"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer dir.Close()
	count := 0
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			if filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			intent, err := s.readPeerIntent(filepath.Join(dir.Name(), entry.Name()))
			if err != nil {
				return count, err
			}
			if id == "" || intent.RunID == id {
				count++
			}
		}
		if errors.Is(readErr, io.EOF) {
			return count, nil
		}
		if readErr != nil {
			return count, readErr
		}
	}
}

func (s *Server) remainingRunEventsLocked(id string) int {
	remaining := s.inFlightRuns[id]
	for _, event := range s.events {
		if event.RunID == id {
			remaining++
		}
	}
	if s.spool != nil {
		remaining = 0
		for _, record := range s.spool.records {
			if record.runID == id {
				remaining++
			}
		}
	}
	for _, event := range s.terminations {
		if event.RunID == id {
			remaining++
		}
	}
	return remaining
}

func (s *Server) waitRunWorkerIngress(ctx context.Context) error {
	for s.telemetryDecoders.Load() != 0 || s.telemetryWaiters.Load() != 0 {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &runWorkerPendingError{ctx.Err()}
		case <-timer.C:
		}
	}
	return nil
}

func (s *Server) prepareRunReset(ctx context.Context, id string) error {
	if err := s.waitRunWorkerIngress(ctx); err != nil {
		return err
	}
	for {
		s.runViewsMu.Lock()
		readers := s.runViews[id]
		s.runViewsMu.Unlock()
		if readers == 0 {
			break
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &runWorkerPendingError{fmt.Errorf("wait for experiment state readers: %w", ctx.Err())}
		case <-timer.C:
		}
	}
	if err := s.heartbeat(ctx); err != nil {
		return &runWorkerPendingError{fmt.Errorf("acknowledge terminal Peer inventory: %w", err)}
	}
	for {
		if err := ctx.Err(); err != nil {
			return &runWorkerPendingError{err}
		}
		if err := s.reclaimHistory(); err != nil {
			return &runWorkerPendingError{err}
		}
		s.mu.RLock()
		records := len(s.processes)
		fences := len(s.runFences)
		configs := 0
		for _, proc := range s.processes {
			if proc.configPath != "" {
				configs++
			}
		}
		s.mu.RUnlock()
		if records <= peerHistoryRecent && configs == 0 && fences <= runFenceCacheLimit {
			break
		}
		// Every current process is terminal at this boundary. A crossed heartbeat
		// can invalidate acknowledgements, so renew them instead of discarding rows.
		if err := s.heartbeat(ctx); err != nil {
			return &runWorkerPendingError{err}
		}
	}
	return nil
}

func (s *Server) proveRunReset(id string) (runResetProof, error) {
	return s.inspectRunReset(id, true)
}

func (s *Server) inspectRunReset(id string, requireWorkerExit bool) (runResetProof, error) {
	proof := runResetProof{RunID: id, TelemetryIngress: s.telemetryDecoders.Load() + s.telemetryWaiters.Load(), WorkerReaped: true}
	s.runViewsMu.Lock()
	proof.SnapshotReaders = s.runViews[id]
	s.runViewsMu.Unlock()
	s.runWorkersMu.Lock()
	if w := s.runWorkers[id]; w != nil {
		select {
		case <-w.ready:
			if w.process != nil {
				select {
				case <-w.process.Done():
				default:
					proof.WorkerReaped = false
				}
			}
		default:
			proof.WorkerReaped = false
		}
	}
	s.runWorkersMu.Unlock()
	if s.docker != nil {
		proof.PeerWaitConnections = s.docker.activeWaits.Load()
	}
	s.mu.RLock()
	proof.HistoryRecords = len(s.processes)
	proof.FenceCacheRecords = len(s.runFences)
	for _, proc := range s.processes {
		if proc.configPath != "" {
			proof.TemporaryConfigs++
		}
		if proc.node.RunID != id {
			continue
		}
		if !proc.exited {
			proof.ActivePeers++
		}
		if !proc.exited || proc.cleanupErr != nil {
			proof.Reservations++
		}
		if proc.cleanupErr != nil {
			proof.CleanupPending++
		}
		proof.VolatilePeerFields += len(proc.node.Addresses) + len(proc.node.ConnectedPeers) + len(proc.node.RoutingPeers) + len(proc.node.MeshPeers) + len(proc.node.TopicPeers) + len(proc.node.PeerScores)
		if proc.node.ScoreSample != nil {
			proof.VolatilePeerFields++
		}
	}
	proof.NetworkBound = s.experimentNetwork != nil || s.networkRequest.RunID != ""
	s.mu.RUnlock()
	s.eventsMu.Lock()
	proof.TelemetryEvents = s.remainingRunEventsLocked(id)
	s.eventsMu.Unlock()
	var err error
	proof.CreationIntents, err = s.countPeerIntents(id)
	if err != nil {
		return proof, err
	}
	if proof.TelemetryEvents != 0 {
		return proof, &runLogsPendingError{remaining: proof.TelemetryEvents, cause: errors.New("late Run telemetry arrived before reset completion")}
	}
	if proof.ActivePeers != 0 || proof.Reservations != 0 || proof.CleanupPending != 0 || proof.CreationIntents != 0 || proof.TelemetryIngress != 0 || proof.VolatilePeerFields != 0 || proof.HistoryRecords > peerHistoryRecent || proof.FenceCacheRecords > runFenceCacheLimit || proof.TemporaryConfigs != 0 || proof.PeerWaitConnections != 0 || proof.SnapshotReaders != 0 || requireWorkerExit && (!proof.WorkerReaped || proof.NetworkBound) {
		return proof, &runWorkerPendingError{errors.New("Run reset ownership or ingress is still pending")}
	}
	return proof, nil
}

// Hold the lease until the HTTP response or heartbeat has consumed its temporary
// full snapshots, not merely until the IPC response has been decoded.
func (s *Server) acquireRunView() func() {
	return s.acquireNodeRunView("")
}

func (s *Server) acquireNodeRunView(nodeID string) func() {
	if s.config.RunWorkerFactory == nil {
		return func() {}
	}
	ids := make(map[string]struct{})
	s.mu.RLock()
	for _, proc := range s.processes {
		if nodeID != "" && proc.node.ID != nodeID {
			continue
		}
		if !proc.exited && (proc.node.State == model.NodeStarting || proc.node.State == model.NodeReady) {
			ids[proc.node.RunID] = struct{}{}
		}
	}
	s.runViewsMu.Lock()
	if s.runViews == nil {
		s.runViews = make(map[string]int)
	}
	for id := range ids {
		s.runViews[id]++
	}
	s.runViewsMu.Unlock()
	s.mu.RUnlock()
	return func() {
		s.runViewsMu.Lock()
		defer s.runViewsMu.Unlock()
		for id := range ids {
			if s.runViews[id] <= 1 {
				delete(s.runViews, id)
			} else {
				s.runViews[id]--
			}
		}
	}
}

func (s *Server) clearRunNetworkState(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.experimentNetwork != nil && s.experimentNetwork.RunID == id {
		s.experimentNetwork = nil
	}
	if s.networkRequest.RunID == id {
		s.networkRequest = model.ExperimentNetworkRequest{}
	}
}
