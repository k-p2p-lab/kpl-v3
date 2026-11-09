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
	RunID    string                   `json:"runId"`
	Snapshot workerSnapshotDescriptor `json:"snapshot"`
}

func summaryWorkerFactory(ctx context.Context, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
	var start workerSummaryStart
	if err := json.Unmarshal(raw, &start); err != nil {
		return nil, err
	}
	if start.RunID != peer.RunID() || start.Snapshot.Result.ID != start.RunID {
		return nil, errors.New("summary worker identity mismatch")
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

// Refreshing sealed metrics is ordinary queued computation. Scrapes only read
// immutable summaries and never spawn a process or reconstruct a raw index.
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
				s.cancelMu.Lock()
				if s.shuttingDown || s.summaryRefreshActive {
					s.cancelMu.Unlock()
					break
				}
				s.state.mu.RLock()
				run := s.state.experiments[id]
				s.state.mu.RUnlock()
				if run.State == "running" || run.State == "queued" || s.cancels[id] != nil {
					s.cancelMu.Unlock()
					continue
				}
				jobCtx, cancel := context.WithCancel(ctx)
				slot := s.enqueueWorkLocked(jobCtx, model.WorkQueueItem{Kind: "analysis", Name: "Refresh run metrics", RunID: id}, cancel)
				s.summaryRefreshActive = true
				s.cancelMu.Unlock()
				retryAfter[id] = now.Add(30 * time.Second)
				go func() {
					defer cancel()
					defer s.finishWork(slot)
					defer func() { s.cancelMu.Lock(); s.summaryRefreshActive = false; s.cancelMu.Unlock() }()
					if err := s.refreshWorkerSummary(jobCtx, id, slot); err != nil && jobCtx.Err() == nil {
						s.logger.Warn("refresh sealed runtime summary", "run", id, "error", err)
					}
				}()
				break
			}
		}
	}
}

func (s *Server) refreshWorkerSummary(ctx context.Context, id string, slot *workSlot) error {
	if err := s.waitWork(ctx, slot); err != nil {
		return err
	}
	if err := s.waitQueueCleanup(ctx, id, scenario.Scenario{JobShutdownTimeout: "3m"}); err != nil {
		return err
	}
	return s.computeWorkerSummary(ctx, id)
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
	process, err := workerproc.Start(ctx, "controller-summary", workerSummaryStart{RunID: id, Snapshot: describeWorkerSnapshot(snapshot)}, s.workerSnapshotSource([]*resultSnapshot{snapshot}), workerproc.Options{RunID: id, Command: s.workerCommand})
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
