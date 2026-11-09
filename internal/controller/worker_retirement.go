package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const retiredRuntimeDirectory = "runtime-retired"

type workerNodeCount struct {
	AgentID, Group, Role, Type, State string
	Count                             int
}
type retiredRunRuntime struct {
	RunID string       `json:"runId"`
	At    time.Time    `json:"at"`
	Nodes []model.Node `json:"nodes"`
}

// This boundary is entered only after an authoritative Agent fence/drain ACK
// and a quiescent OS worker. Persistent records are never used as a substitute
// for those acknowledgements by the live supervisor.
func (s *Server) retireRunRuntime(ctx context.Context, id string, final ...*model.Experiment) error {
	if err := s.waitControllerWorkers(ctx, []string{id}); err != nil {
		return err
	}
	if err := s.cleanupRunWorkerStages(ctx, id); err != nil {
		return err
	}
	s.state.persistMu.Lock()
	deleted, err := s.state.resultWriteFencedLocked(id)
	if err != nil {
		s.state.persistMu.Unlock()
		return err
	}
	if _, err := os.Lstat(filepath.Join(s.config.DataDir, currentRunsDirectory, id, journalPendingFile)); !deleted && err == nil {
		s.state.persistMu.Unlock()
		return errors.New("Run still has an uncommitted telemetry append")
	} else if !deleted && !errors.Is(err, os.ErrNotExist) {
		s.state.persistMu.Unlock()
		return err
	}
	s.state.mu.RLock()
	run := s.state.experiments[id]
	var terminal []model.Node
	for _, node := range s.state.nodes {
		if node.RunID == id {
			if nodeHasLiveState(node) || node.Metadata["cleanupComplete"] != "true" {
				s.state.mu.RUnlock()
				s.state.persistMu.Unlock()
				return fmt.Errorf("Run %s still has unconfirmed Peer %s", id, node.ID)
			}
			terminal = append(terminal, compactRetiredNode(node))
		}
	}
	for nodeID := range s.state.reservations {
		if strings.HasPrefix(nodeID, id+"-") {
			s.state.mu.RUnlock()
			s.state.persistMu.Unlock()
			return fmt.Errorf("Run %s still has a reservation", id)
		}
	}
	for _, pending := range s.state.uncertainCreates {
		if pending.runID == id {
			s.state.mu.RUnlock()
			s.state.persistMu.Unlock()
			return fmt.Errorf("Run %s still has uncertain admission", id)
		}
	}
	s.state.mu.RUnlock()
	var summary runWorkerSummary
	hasSummary := false
	if !deleted {
		summary, hasSummary = s.state.workerMetricSummary(id)
	}
	if len(final) > 0 && !hasSummary {
		s.state.persistMu.Unlock()
		return errors.New("worker omitted its durable metric summary")
	}
	if run.TelemetryPending {
		s.state.persistMu.Unlock()
		return errors.New("Run still has undelivered telemetry")
	}
	counters, err := s.state.metrics.sealRunCounters(id)
	if err != nil {
		s.state.persistMu.Unlock()
		return err
	}
	if hasSummary {
		summary.Retired = true
		summary.Counters = counters
		summary.Nodes = countRetiredNodes(terminal)
		root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
		if err != nil {
			s.state.persistMu.Unlock()
			return err
		}
		err = writeAnalysisJSON(root, workerSummaryFile, summary)
		root.Close()
		if err != nil {
			s.state.persistMu.Unlock()
			return err
		}
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		s.state.persistMu.Unlock()
		return err
	}
	if err = data.Mkdir(retiredRuntimeDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		data.Close()
		s.state.persistMu.Unlock()
		return err
	}
	directory, err := openResultDirectory(data, retiredRuntimeDirectory)
	if err == nil {
		if info, e := directory.Lstat(id + ".json"); errors.Is(e, os.ErrNotExist) {
			err = writeAnalysisJSON(directory, id+".json", retiredRunRuntime{RunID: id, At: time.Now().UTC(), Nodes: terminal})
		} else if e != nil {
			err = e
		} else if !info.Mode().IsRegular() {
			err = errors.New("retired runtime evidence is not a regular file")
		}
		if err == nil {
			err = writeRetiredRuntimeFence(directory, id)
		}
		if err == nil {
			err = errors.Join(syncRunDirectory(directory), syncRunDirectory(data))
		}
		directory.Close()
	}
	data.Close()
	if err != nil {
		s.state.persistMu.Unlock()
		return err
	}
	s.state.mu.Lock()
	for _, node := range s.state.nodes {
		if node.RunID == id && (nodeHasLiveState(node) || node.Metadata["cleanupComplete"] != "true") {
			s.state.mu.Unlock()
			s.state.persistMu.Unlock()
			return errors.New("Peer state changed during runtime retirement")
		}
	}
	s.state.cacheRetiredRunLocked(id)
	if deleted {
		delete(s.state.workerSummaries, id)
	}
	if hasSummary {
		s.state.workerSummaries[id] = summary
	}
	for nodeID, node := range s.state.nodes {
		if node.RunID == id {
			s.state.deleteNodeLocked(nodeID)
		}
	}
	delete(s.state.runMetrics, id)
	delete(s.state.runTimings, id)
	for i, event := range s.state.events {
		if event.RunID == id {
			s.state.events[i] = compactRetiredEvent(event)
		}
	}
	s.state.mu.Unlock()
	s.state.releaseRetiredSummary(id)
	s.state.persistMu.Unlock()
	s.attackTargets.mu.Lock()
	if s.attackTargets.runID == id {
		s.attackTargets.raw = nil
		s.attackTargets.runID = ""
		s.attackTargets.generation = 0
		s.attackTargets.observedAt = time.Time{}
	}
	s.attackTargets.mu.Unlock()
	if err := s.resetRuntimeSnapshotsAndCommit(ctx, func() error {
		if err := s.verifyRetiredRuntime(id); err != nil {
			return err
		}
		if len(final) > 0 && final[0] != nil {
			s.updateExperiment(id, func(current *model.Experiment) {
				mergeWorkerExperiment(current, *final[0])
				current.FinishedAt = time.Now().UTC()
			})
		}
		return nil
	}); err != nil {
		return err
	}
	if err := s.verifyRetiredRuntime(id); err != nil {
		return err
	}
	s.state.notify()
	s.logger.Info("experiment runtime reset verified", "run", id, "activeNodes", 0, "reservations", 0, "uncertainCreates", 0, "rawAccumulators", 0)
	return nil
}

func (s *Server) waitRunTerminalEvidence(ctx context.Context, id string) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("terminal Peer cleanup evidence is still unavailable for %s: %w", id, err)
		}
		if err := s.refreshAgentState(ctx, id); err != nil {
			return err
		}
		s.state.mu.RLock()
		pending := false
		for _, node := range s.state.nodes {
			if node.RunID == id && (nodeHasLiveState(node) || node.Metadata["cleanupComplete"] != "true") {
				pending = true
				break
			}
		}
		for nodeID := range s.state.reservations {
			if strings.HasPrefix(nodeID, id+"-") {
				pending = true
				break
			}
		}
		for _, uncertain := range s.state.uncertainCreates {
			if uncertain.runID == id {
				pending = true
				break
			}
		}
		s.state.mu.RUnlock()
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("terminal Peer cleanup evidence is still unavailable for %s: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

func compactRetiredNode(node model.Node) model.Node {
	node.Addresses = nil
	node.ConnectedPeers = nil
	node.RoutingPeers = nil
	node.MeshPeers = nil
	node.TopicPeers = nil
	node.PeerScores = nil
	node.ScoreSample = nil
	node.Metadata = map[string]string{"cleanupComplete": "true"}
	node.OverlayObservedAt = time.Time{}
	return node
}
func compactRetiredEvent(event model.TraceEvent) model.TraceEvent {
	event.ScoreSample = nil
	event.Bandwidth = nil
	fields := make(map[string]any)
	for _, name := range []string{"latencyAvailable", "direction", "controlType", "controlEntries", "messageIdCount", "peerExchangeCount", "action", "count"} {
		value, ok := event.Fields[name]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if len(typed) > 96 {
				continue
			}
		case bool, int, int64, uint64, float64:
		default:
			continue
		}
		fields[name] = value
	}
	event.Fields = fields
	if len(event.MessageID) > 256 {
		event.MessageID = ""
	}
	return event
}
func countRetiredNodes(nodes []model.Node) []workerNodeCount {
	counts := make(map[workerNodeCount]int)
	for _, node := range nodes {
		counts[workerNodeCount{AgentID: node.AgentID, Group: node.Group, Role: node.Role, Type: node.Type, State: node.State}]++
	}
	rows := make([]workerNodeCount, 0, len(counts))
	for row, count := range counts {
		row.Count = count
		rows = append(rows, row)
	}
	return rows
}
func (s *Server) verifyRetiredRuntime(id string) error {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	if s.state.runMetrics[id] != nil {
		return errors.New("retired Run retained a raw accumulator")
	}
	for _, node := range s.state.nodes {
		if node.RunID == id {
			return errors.New("retired Run retained runtime Node records")
		}
	}
	for nodeID := range s.state.reservations {
		if strings.HasPrefix(nodeID, id+"-") {
			return errors.New("retired Run retained a reservation")
		}
	}
	for _, pending := range s.state.uncertainCreates {
		if pending.runID == id {
			return errors.New("retired Run retained uncertain admission")
		}
	}
	return nil
}

func (s *Server) loadRetiredRuntimeFences() error {
	info, err := os.Lstat(filepath.Join(s.config.DataDir, retiredRuntimeDirectory))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid retired runtime directory")
	}
	return nil
}

func (s *Server) resetRuntimeSnapshots(ctx context.Context) error {
	return s.resetRuntimeSnapshotsAndCommit(ctx, nil)
}
func (s *Server) resetRuntimeSnapshotsAndCommit(ctx context.Context, commit func() error) error {
	s.snapshotMu.Lock()
	if s.snapshotResetDone != nil {
		done := s.snapshotResetDone
		s.snapshotMu.Unlock()
		if err := waitSnapshotFlight(ctx, done, nil); err != nil {
			return err
		}
		return s.resetRuntimeSnapshotsAndCommit(ctx, commit)
	}
	s.snapshotEpoch++
	done := make(chan struct{})
	s.snapshotResetDone = done
	var flights []<-chan struct{}
	if s.snapshotFlight != nil {
		flights = append(flights, s.snapshotFlight.done)
	}
	if s.dashboardFlight != nil {
		flights = append(flights, s.dashboardFlight.done)
	}
	if s.dashboardSummaryFlight != nil {
		flights = append(flights, s.dashboardSummaryFlight.done)
	}
	s.snapshotData = nil
	s.dashboardFrame = nil
	s.dashboardSummaryFrame = nil
	s.snapshotAt = time.Time{}
	s.snapshotMu.Unlock()
	defer func() {
		s.snapshotMu.Lock()
		s.snapshotData = nil
		s.dashboardFrame = nil
		s.dashboardSummaryFrame = nil
		s.snapshotAt = time.Time{}
		close(done)
		s.snapshotResetDone = nil
		s.snapshotMu.Unlock()
	}()
	for _, flight := range flights {
		if err := waitSnapshotFlight(ctx, flight, nil); err != nil {
			return fmt.Errorf("wait for previous runtime snapshot: %w", err)
		}
	}
	if commit != nil {
		return commit()
	}
	return nil
}
