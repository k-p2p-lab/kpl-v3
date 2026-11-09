package controller

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const workerSummaryFile = ".kpl-runtime-summary.json"

// Kept as a baseline sentinel for the old in-memory ingress index. Production
// ingress never allocates it: identities are in the local disk index, and
// scientific receipt/session indexes live exclusively in worker processes.
type runJournalIndex struct {
	sequences map[measurementSessionKey]sequenceRanges
}

func (s *state) appendJournalEvents(runID string, events []model.TraceEvent) (resultErr error) {
	defer func() {
		if errors.Is(resultErr, errJournalIndexRequired) {
			if s.requestJournalRecovery != nil {
				s.requestJournalRecovery(runID)
			}
		} else if resultErr != nil {
			s.recordRunWriteError(runID, resultErr)
		}
	}()
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if runID != "" && !validResultID(runID) {
		return errors.New("invalid journal Run identity")
	}
	if deleted, err := s.resultDeletedLocked(runID); err != nil || deleted {
		return err
	}
	if runID == "" {
		s.publishJournalEvents(runID, events)
		return nil
	}
	accepted, err := s.appendIndexedJournalLocked(runID, events)
	if len(accepted) != 0 {
		s.publishJournalEvents(runID, accepted)
	}
	return err
}

func (s *state) publishJournalEvents(runID string, accepted []model.TraceEvent) {
	retired, _ := s.lookupRetiredRun(runID)
	s.mu.Lock()
	for _, event := range accepted {
		if event.Type != "peer_score" && event.Type != "topology_assignment" && event.Type != "topology_applied" {
			if retired {
				event = compactRetiredEvent(event)
			}
			s.events = append(s.events, event)
		}
	}
	if len(s.events) > recentEventLimit {
		s.events = append([]model.TraceEvent(nil), s.events[len(s.events)-recentEventLimit:]...)
	}
	if s.summaryDirty == nil {
		s.summaryDirty = make(map[string]bool)
	}
	s.summaryDirty[runID] = true
	s.mu.Unlock()
	// Closed Runs are corrected by the same FIFO summary worker. Recreating
	// incremental vectors here after memo release would expose partial totals.
	if !retired {
		for _, event := range accepted {
			s.metrics.observeEvent(event)
		}
	}
}

func (s *Server) publishWorkerSummary(summary runWorkerSummary) error {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if deleted, err := s.state.resultDeletedLocked(summary.RunID); err != nil || deleted {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, summary.RunID))
	if err != nil {
		return err
	}
	defer root.Close()
	currentRevision, err := sourceRevisionAt(root, summary.RunID)
	if err != nil {
		return err
	}
	if summary.SourceRevision != "" && summary.SourceRevision != currentRevision {
		return errors.New("worker summary source changed before publication")
	}
	currentSize := int64(0)
	if info, e := root.Stat("events.jsonl"); e == nil {
		currentSize = info.Size()
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	complete := summary.SourceRevision != "" || summary.Cursor == currentSize
	summary.SourceRevision = currentRevision
	s.state.mu.RLock()
	old, cached := s.state.workerSummaries[summary.RunID]
	s.state.mu.RUnlock()
	if !cached {
		old, err = s.state.readWorkerSummary(summary.RunID)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if old.Retired {
		summary.Retired = true
		summary.Nodes = old.Nodes
		if summary.CountersComplete {
			summary.Counters = mergeWorkerCounterProjection(old.Counters, summary.Counters)
		} else {
			summary.Counters = old.Counters
		}
	}
	if err := writeAnalysisJSON(root, workerSummaryFile, summary); err != nil {
		return err
	}
	s.state.mu.Lock()
	if s.state.workerSummaries == nil {
		s.state.workerSummaries = make(map[string]runWorkerSummary)
	}
	s.state.workerSummaries[summary.RunID] = summary
	if s.state.summaryDue == nil {
		s.state.summaryDue = make(map[string]time.Time)
	}
	if summary.NextRefreshAt.IsZero() {
		delete(s.state.summaryDue, summary.RunID)
	} else {
		s.state.summaryDue[summary.RunID] = summary.NextRefreshAt
	}
	if complete {
		delete(s.state.summaryDirty, summary.RunID)
	} else {
		if s.state.summaryDirty == nil {
			s.state.summaryDirty = make(map[string]bool)
		}
		s.state.summaryDirty[summary.RunID] = true
	}
	s.state.mu.Unlock()
	if summary.Retired {
		s.state.releaseRetiredSummary(summary.RunID)
	}
	s.state.notify()
	return nil
}

func (s *state) workerMetricSummary(id string) (runWorkerSummary, bool) {
	s.mu.RLock()
	summary, ok := s.workerSummaries[id]
	s.mu.RUnlock()
	if ok {
		return summary, true
	}
	if !s.journalOnly {
		return summary, false
	}
	summary, err := s.readWorkerSummary(id)
	return summary, err == nil
}

func (s *Server) loadWorkerSummaries() error {
	// Retired payloads stay on disk. Only refresh obligations need to be queued;
	// no list of all historical summaries is retained in the service heap.
	return s.state.walkWorkerSummaries(nil, func(summary runWorkerSummary) error {
		current, err := s.runSourceRevision(summary.RunID)
		s.state.mu.Lock()
		defer s.state.mu.Unlock()
		if err != nil || current != summary.SourceRevision {
			if s.state.summaryDirty == nil {
				s.state.summaryDirty = make(map[string]bool)
			}
			s.state.summaryDirty[summary.RunID] = true
		}
		if !summary.NextRefreshAt.IsZero() {
			if s.state.summaryDue == nil {
				s.state.summaryDue = make(map[string]time.Time)
			}
			s.state.summaryDue[summary.RunID] = summary.NextRefreshAt
		}
		return nil
	})
}

func (summary runWorkerSummary) histograms() map[propagationSeriesKey]*propagationHistogram {
	result := make(map[propagationSeriesKey]*propagationHistogram, len(summary.Histograms))
	for _, row := range summary.Histograms {
		histogram := &propagationHistogram{count: row.Count, sum: row.Sum, buckets: make(map[float64]uint64)}
		for i, bound := range propagationBounds {
			if i < len(row.Buckets) {
				histogram.buckets[bound] = row.Buckets[i]
			}
		}
		result[propagationSeriesKey{row.AgentID, row.Topic}] = histogram
	}
	return result
}

func (s *state) workerSummarySnapshot() map[string]runWorkerSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]runWorkerSummary, len(s.workerSummaries))
	for id, summary := range s.workerSummaries {
		result[id] = summary
	}
	return result
}

func (s *state) workerSummaryDue(now time.Time) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var ids []string
	known := make(map[string]bool)
	for id := range s.summaryDirty {
		if id != "" {
			ids = append(ids, id)
			known[id] = true
		}
	}
	for id, deadline := range s.summaryDue {
		if !known[id] && !now.Before(deadline) {
			ids = append(ids, id)
		}
	}
	return ids
}
