package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type workerSummaryStart struct {
	RunID         string                   `json:"runId"`
	Snapshot      workerSnapshotDescriptor `json:"snapshot"`
	EventRevision string                   `json:"eventRevision"`
}

func summaryWorkerFactory(ctx context.Context, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var start workerSummaryStart
	if err := json.Unmarshal(raw, &start); err != nil {
		return nil, err
	}
	if start.RunID != peer.RunID() || start.Snapshot.Result.ID != start.RunID {
		return nil, errors.New("summary worker identity mismatch")
	}
	if start.EventRevision == "" {
		return nil, errors.New("summary worker event input identity is missing")
	}
	return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
		if method != "execute" {
			return nil, errors.New("unknown summary worker operation")
		}
		snapshot := workerSnapshotFromDescriptor(callCtx, peer, 0, start.Snapshot)
		accumulator := newLiveRunMetricAccumulator()
		counters := newControllerMetrics(&state{})
		var cursor int64
		for _, file := range snapshot.files {
			if file.name != "events.jsonl" {
				continue
			}
			cursor = file.size
			if err := scanAnalysisFile(callCtx, file, func(data []byte) error {
				var event model.TraceEvent
				if err := json.Unmarshal(data, &event); err != nil {
					return err
				}
				if event.RunID == start.RunID {
					accumulator.observe(event)
					counters.observeEvent(event)
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
		summary := makeRunWorkerSummary(accumulator, start.RunID, time.Now().UTC(), cursor)
		summary.SourceRevision = start.Snapshot.Result.SourceRevision
		summary.EventRevision = start.EventRevision
		var err error
		summary.Counters, err = counters.sealRunCounters(start.RunID)
		if err != nil {
			return nil, err
		}
		summary.CountersComplete = true
		return summary, nil
	}, nil
}

func makeRunWorkerSummary(accumulator *runMetricAccumulator, runID string, now time.Time, cursor int64) runWorkerSummary {
	metrics, histograms := accumulator.livePrometheusSummary(runID, now)
	summary := runWorkerSummary{RunID: runID, Metrics: metrics, At: now, Cursor: cursor}
	for key, histogram := range histograms {
		row := workerHistogram{AgentID: key.agentID, Topic: key.topic, Count: histogram.count, Sum: histogram.sum}
		for _, bound := range propagationBounds {
			row.Buckets = append(row.Buckets, histogram.buckets[bound])
		}
		summary.Histograms = append(summary.Histograms, row)
	}
	accumulator.dataMu.RLock()
	for _, publication := range accumulator.window.publications {
		if publication.deadline.After(now) && (summary.NextRefreshAt.IsZero() || publication.deadline.Before(summary.NextRefreshAt)) {
			summary.NextRefreshAt = publication.deadline
		}
	}
	accumulator.dataMu.RUnlock()
	return summary
}

func (s *Server) rememberUnreapedWorker(id string, process *workerproc.Process) {
	s.workerMu.Lock()
	if s.unreapedWorkers == nil {
		s.unreapedWorkers = make(map[string][]*workerproc.Process)
	}
	s.unreapedWorkers[id] = append(s.unreapedWorkers[id], process)
	s.workerMu.Unlock()
	s.cancelMu.Lock()
	s.rememberQueueCleanupLocked(id)
	err := s.persistQueueCleanupLocked()
	s.cancelMu.Unlock()
	if err != nil {
		s.logger.Error("persist unreaped Controller worker barrier", "run", id, "error", err)
	}
}

func (s *Server) waitControllerWorkers(ctx context.Context, ids []string) error {
	for _, id := range ids {
		s.workerMu.Lock()
		workers := append([]*workerproc.Process(nil), s.unreapedWorkers[id]...)
		s.workerMu.Unlock()
		for _, process := range workers {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-process.Done():
			}
		}
		s.workerMu.Lock()
		delete(s.unreapedWorkers, id)
		s.workerMu.Unlock()
	}
	return nil
}

// Metric maintenance belongs to Run finalization. Idle corrections use the same
// admission exclusion without creating a user-visible FIFO item. Scrapes only
// read immutable summaries and never reconstruct a raw index.
func (s *Server) runWorkerSummaryLoop(ctx context.Context) {
	if err := s.loadRetiredRuntimeFences(); err != nil {
		s.logger.Error("load retired runtime fences", "error", err)
		return
	}
	if err := s.loadWorkerSummaries(); err != nil {
		s.logger.Warn("load sealed runtime summaries", "error", err)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	retryAfter := make(map[string]time.Time)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			ids := s.state.workerSummaryDue(now)
			pending := make(map[string]bool, len(ids))
			for _, id := range ids {
				pending[id] = true
			}
			for id, deadline := range retryAfter {
				if !pending[id] || !now.Before(deadline) {
					delete(retryAfter, id)
				}
			}
			sort.Strings(ids)
			for _, id := range ids {
				if now.Before(retryAfter[id]) {
					continue
				}
				if s.tryRefreshIdleRunMetrics(ctx, id) {
					retryAfter[id] = now.Add(30 * time.Second)
					break
				}
			}
		}
	}
}

// A boundary owns the experiment's existing slot. An idle correction instead
// holds admission until its calculation and OS/RPC reclamation have finished.
func (s *Server) beginRunMetricMaintenance(slot *workSlot) bool {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown || s.summaryRefreshActive {
		return false
	}
	if slot == nil {
		if len(s.experimentQueue) != 0 {
			return false
		}
		s.workWorkers.Add(1)
	} else if slot.finished || !slot.opened || len(s.experimentQueue) == 0 || s.experimentQueue[0] != slot {
		return false
	}
	s.summaryRefreshActive = true
	s.state.mu.Lock()
	s.state.runMetricMaintenance = true
	s.state.mu.Unlock()
	return true
}

func (s *Server) endRunMetricMaintenance(slot *workSlot) {
	s.cancelMu.Lock()
	s.summaryRefreshActive = false
	s.state.mu.Lock()
	s.state.runMetricMaintenance = false
	s.state.mu.Unlock()
	s.updateQueuePositionsLocked()
	s.cancelMu.Unlock()
	if slot == nil {
		s.workWorkers.Done()
	}
}

func (s *Server) runMetricRefreshEligible(id string) bool {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.state.mu.RLock()
	run := s.state.experiments[id]
	s.state.mu.RUnlock()
	return run.State != "running" && run.State != "queued" && s.cancels[id] == nil
}

func (s *Server) refreshRunMetricSummary(ctx context.Context, id string) error {
	s.state.persistMu.Lock()
	rebased, err := s.state.rebaseRetiredWorkerSummaryLocked(id, time.Now().UTC())
	s.state.persistMu.Unlock()
	if err != nil || rebased {
		return err
	}
	return s.computeWorkerSummary(ctx, id)
}

func (s *Server) tryRefreshIdleRunMetrics(ctx context.Context, id string) bool {
	if ctx.Err() != nil || !s.beginRunMetricMaintenance(nil) {
		return false
	}
	defer s.endRunMetricMaintenance(nil)
	if !s.runMetricRefreshEligible(id) {
		return false
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	err := s.recoverWorkCleanup(refreshCtx)
	if err == nil {
		err = s.waitQueueCleanup(refreshCtx, id, scenario.Scenario{JobShutdownTimeout: "3m"})
	}
	if err == nil {
		err = s.refreshRunMetricSummary(refreshCtx, id)
	}
	if err != nil && ctx.Err() == nil {
		s.logger.Warn("refresh idle Run metrics", "run", id, "error", err)
	}
	return true
}

// Take a finite snapshot: a late append during calculation is retained for the
// next boundary instead of keeping this Run alive indefinitely. Future receipt
// deadlines never delay the next experiment.
func (s *Server) refreshRunMetricsBoundary(ctx context.Context, slot *workSlot) {
	if !s.config.RunWorkers || slot == nil || ctx.Err() != nil || !s.beginRunMetricMaintenance(slot) {
		return
	}
	defer s.endRunMetricMaintenance(slot)
	refreshCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	ids := s.state.workerSummaryDue(time.Now().UTC())
	sort.Strings(ids)
	defer s.updateWorkStatus(slot.id, "", 0)
	for index, id := range ids {
		if refreshCtx.Err() != nil {
			return
		}
		if !s.runMetricRefreshEligible(id) {
			continue
		}
		s.updateWorkStatus(slot.id, "Refreshing run metrics", 100*float64(index)/float64(len(ids)))
		err := s.waitQueueCleanup(refreshCtx, id, scenario.Scenario{JobShutdownTimeout: "3m"})
		if err == nil {
			err = s.refreshRunMetricSummary(refreshCtx, id)
		}
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Warn("refresh Run metrics at experiment boundary", "run", id, "error", err)
			}
			// A possibly unreaped calculation owns a cleanup barrier. Do not
			// start another calculation until the normal recovery path reaps it.
			s.cancelMu.Lock()
			pending := len(s.queueCleanup) != 0
			s.cancelMu.Unlock()
			if pending {
				return
			}
		}
	}
}

// The caller already owns the shared work slot (including cleanup recovery).
func (s *Server) computeWorkerSummary(ctx context.Context, id string) error {
	snapshot, err := s.captureResultFilesContext(ctx, id, false)
	if err != nil {
		return err
	}
	defer snapshot.close()
	if snapshot.active {
		return errResultBusy
	}
	process, err := workerproc.Start(ctx, "controller-summary", workerSummaryStart{RunID: id, Snapshot: describeWorkerSnapshot(snapshot), EventRevision: eventRevisionOf(snapshot.files)}, s.workerSnapshotSource([]*resultSnapshot{snapshot}), workerproc.Options{RunID: id, Command: s.workerCommand})
	if err != nil {
		if process != nil {
			s.rememberUnreapedWorker(id, process)
		}
		return err
	}
	var summary runWorkerSummary
	executeErr := process.Call(ctx, "execute", nil, &summary)
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	closeErr := process.CloseContext(closeCtx)
	cancel()
	select {
	case <-process.Done():
	default:
		s.rememberUnreapedWorker(id, process)
	}
	if err := errors.Join(executeErr, closeErr); err != nil {
		return err
	}
	current, err := s.runSourceRevision(id)
	if err != nil {
		return err
	}
	if current != snapshot.result.SourceRevision {
		return fmt.Errorf("summary source changed during refresh")
	}
	return s.publishWorkerSummary(summary)
}
