package controller

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

var errWorkNotFound = errors.New("queued work not found or already finished")

// cancelMu protects the slot and FIFO. Progress lives separately in
// state.workQueue so analysisJobMu callers never acquire cancelMu in reverse.
type workSlot struct {
	id                string
	item              model.WorkQueueItem
	ctx               context.Context
	cancel            context.CancelFunc
	ready             chan struct{}
	opened            bool
	finished          bool
	batch             *repeatBatch
	resultAnalysis    bool
	analysisLeader    bool
	analysisWorkerURL string
	analysisAgent     *model.Agent
}

// Admission holds cancelMu and has checked shuttingDown before registering the
// worker. Every successful call must be paired with finishWork after cleanup.
func (s *Server) enqueueWorkLocked(ctx context.Context, item model.WorkQueueItem, cancel context.CancelFunc) *workSlot {
	return s.enqueueWorkSlotLocked(ctx, item, cancel, false)
}

func (s *Server) enqueueResultAnalysisLocked(ctx context.Context, item model.WorkQueueItem, cancel context.CancelFunc) *workSlot {
	return s.enqueueWorkSlotLocked(ctx, item, cancel, true)
}

func (s *Server) enqueueWorkSlotLocked(ctx context.Context, item model.WorkQueueItem, cancel context.CancelFunc, resultAnalysis bool) *workSlot {
	item.ID, item.CreatedAt = "work-"+rand.Text(), time.Now().UTC()
	item.State, item.Phase, item.Progress = "queued", "", 0
	item.StartedAt = nil
	slot := &workSlot{id: item.ID, item: item, ctx: ctx, cancel: cancel, ready: make(chan struct{}), resultAnalysis: resultAnalysis}
	if s.workSlots == nil {
		s.workSlots = make(map[string]*workSlot)
	}
	s.workSlots[slot.id] = slot
	s.workWorkers.Add(1)
	s.experimentQueue = append(s.experimentQueue, slot)
	if s.summaryRefreshIdleCancel != nil {
		// Cancellation only requests a yield. The maintenance owner retains
		// admission until its child and RPC handlers have been fully reaped.
		s.summaryRefreshIdleCancel()
	}
	s.updateQueuePositionsLocked()
	return slot
}

func cloneWorkQueue(queue []model.WorkQueueItem) []model.WorkQueueItem {
	copy := make([]model.WorkQueueItem, len(queue))
	for i, item := range queue {
		copy[i] = item
		if item.StartedAt != nil {
			started := *item.StartedAt
			copy[i].StartedAt = &started
		}
	}
	return copy
}

// Caller holds cancelMu. Analysis gaps in Experiment.QueuePosition intentionally
// suppress absolute experiment ETA until the unknown-duration analysis finishes.
func (s *Server) updateQueuePositionsLocked() {
	ready := s.readyWorkSlotsLocked()
	for _, slot := range ready {
		slot.opened = true
	}
	s.state.mu.Lock()
	previous := make(map[string]model.WorkQueueItem, len(s.state.workQueue))
	for _, item := range s.state.workQueue {
		previous[item.ID] = item
	}
	queue := make([]model.WorkQueueItem, 0, len(s.experimentQueue))
	for position, slot := range s.experimentQueue {
		waitingForMetrics := position == 0 && !slot.opened && s.summaryRefreshActive
		slot.item.Position = position
		slot.item.State = "queued"
		if slot.opened {
			slot.item.State = "running"
			if slot.item.StartedAt == nil {
				started := time.Now().UTC()
				slot.item.StartedAt = &started
			}
		}
		item := slot.item
		if old, exists := previous[item.ID]; exists {
			item.Phase, item.Progress = old.Phase, old.Progress
		}
		if waitingForMetrics {
			item.Phase, item.Progress = "Finishing previous Run metrics", 0
		} else if item.Phase == "Finishing previous Run metrics" {
			item.Phase, item.Progress = "", 0
		}
		queue = append(queue, item)
		if slot.batch != nil {
			for _, id := range slot.batch.queuedIDs {
				run, exists := s.state.experiments[id]
				if exists {
					run.QueuePosition = position
					s.state.experiments[id] = run
				}
			}
		}
	}
	s.state.workQueue = queue
	s.state.mu.Unlock()
	for _, slot := range ready {
		close(slot.ready)
	}
	s.state.notify()
}

// The Controller runs one analysis child locally. Each eligible Agent host
// adds one calculation for the contiguous analysis prefix, after Peer cleanup.
func (s *Server) readyWorkSlotsLocked() []*workSlot {
	queue := s.experimentQueue
	if len(queue) == 0 {
		s.analysisGroupReady = false
		s.analysisGroupWorkers = nil
		return nil
	}
	head := queue[0]
	parallel := head.resultAnalysis
	if !parallel {
		s.analysisGroupReady = false
		s.analysisGroupWorkers = nil
	}
	if s.summaryRefreshActive {
		return nil
	}
	if parallel && s.analysisGroupReady && len(s.queueCleanup) != 0 {
		// A calculation may have left a new cleanup obligation. Let open
		// workers finish before another leader recovers it and opens the group.
		if head.opened {
			return nil
		}
		s.analysisGroupReady = false
	}
	if !parallel || !s.analysisGroupReady {
		if head.opened {
			return nil
		}
		if parallel {
			head.analysisLeader = true
			head.item.Worker = "Controller"
			head.item.WorkerAgentID = ""
		}
		return []*workSlot{head}
	}
	end := 0
	used := make(map[string]bool, 1+len(s.analysisGroupWorkers))
	for ; end < len(queue) && queue[end].resultAnalysis; end++ {
		if queue[end].opened {
			used[queue[end].analysisWorkerURL] = true
		}
	}
	var ready []*workSlot
	for _, slot := range queue[:end] {
		if slot.opened {
			continue
		}
		if !used[""] {
			used[""] = true
			slot.item.Worker = "Controller"
			slot.item.WorkerAgentID = ""
			ready = append(ready, slot)
			continue
		}
		for _, worker := range s.analysisGroupWorkers {
			if used[worker.url] {
				continue
			}
			used[worker.url] = true
			slot.analysisWorkerURL = worker.url
			slot.analysisAgent = worker.agent
			slot.item.Worker = worker.label
			slot.item.WorkerAgentID = ""
			if worker.agent != nil {
				slot.item.WorkerAgentID = worker.agent.ID
			}
			ready = append(ready, slot)
			break
		}
		if slot.analysisWorkerURL == "" {
			break
		}
	}
	return ready
}

func (s *Server) removeWorkLocked(slot *workSlot) {
	index := slices.Index(s.experimentQueue, slot)
	if index < 0 {
		return
	}
	s.experimentQueue = slices.Delete(s.experimentQueue, index, index+1)
	if slot.batch != nil {
		s.state.mu.Lock()
		for _, id := range slot.batch.queuedIDs {
			if run, exists := s.state.experiments[id]; exists {
				run.QueuePosition = 0
				s.state.experiments[id] = run
			}
		}
		s.state.mu.Unlock()
	}
	s.updateQueuePositionsLocked()
}

func (s *Server) finishWorkLocked(slot *workSlot) {
	if slot == nil || slot.finished {
		return
	}
	slot.finished = true
	s.removeWorkLocked(slot)
	delete(s.workSlots, slot.id)
	s.workWorkers.Done()
}

// Run after all readers, computation and final status publication are complete.
// It must not be called while holding analysisJobMu or state.persistMu.
func (s *Server) finishWork(slot *workSlot) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.finishWorkLocked(slot)
}

func (s *Server) waitWork(ctx context.Context, slot *workSlot) (waitErr error) {
	select {
	case <-slot.ready:
	case <-ctx.Done():
		s.cancelMu.Lock()
		// A canceled waiter must not retain its place, but an already active
		// worker owns its slot until its caller's deferred finishWork.
		if !slot.opened && slices.Index(s.experimentQueue, slot) > 0 {
			s.removeWorkLocked(slot)
		}
		s.cancelMu.Unlock()
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if slot.resultAnalysis && !slot.analysisLeader {
		return nil
	}
	started := time.Now()
	stage := "analysis worker cleanup"
	logPreparation := func(message string, fields ...any) {
		identity := []any{"work", slot.id, "kind", slot.item.Kind, "batch", slot.item.BatchID, "run", slot.item.RunID}
		s.logger.Info(message, append(identity, fields...)...)
	}
	logPreparation("work preparation started", "stage", stage)
	defer func() {
		outcome := "ready"
		if waitErr != nil {
			outcome = "failed"
			if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
				outcome = "canceled"
			}
		}
		logPreparation("work preparation finished", "stage", stage, "elapsed", time.Since(started), "outcome", outcome, "error", waitErr)
	}()
	s.updateWorkStatus(slot.id, "Checking analysis worker cleanup", 0)
	if err := s.waitAnalysisWorkerCleanup(ctx, slot); err != nil {
		return err
	}
	stage = "previous Run cleanup recovery"
	logPreparation("work preparation advanced", "stage", stage, "elapsed", time.Since(started))
	s.updateWorkStatus(slot.id, "Recovering previous Run cleanup", 0)
	if err := s.recoverWorkCleanup(ctx); err != nil {
		return err
	}
	if slot.item.Kind == "experiment" {
		s.updateWorkStatus(slot.id, "", 0)
		return ctx.Err()
	}
	// Retained Peers and failed removal barriers must be resolved before CPU,
	// disk or network intensive analysis starts. No synthetic Run is created.
	stage = "previous Run Peer cleanup"
	for {
		if err := s.waitQueueCleanup(ctx, "", scenario.Scenario{}, func(phase string) {
			s.updateWorkStatus(slot.id, phase, 0)
		}); err != nil {
			return err
		}
		if !slot.analysisLeader {
			return nil
		}
		s.cancelMu.Lock()
		if err := ctx.Err(); err != nil {
			s.cancelMu.Unlock()
			return err
		}
		if len(s.queueCleanup) == 0 {
			s.analysisGroupWorkers = s.analysisWorkerTargetsLocked()
			s.analysisGroupReady = true
			s.updateQueuePositionsLocked()
			s.cancelMu.Unlock()
			return nil
		}
		s.cancelMu.Unlock()
	}
}

// Safe under analysisJobMu: this never takes cancelMu or does storage I/O.
func (s *Server) updateWorkStatus(id, phase string, progress float64) {
	if math.IsNaN(progress) || math.IsInf(progress, 0) {
		progress = 0
	}
	s.state.mu.Lock()
	changed := false
	for i := range s.state.workQueue {
		item := &s.state.workQueue[i]
		if item.ID == id && item.Phase != "Stopping" {
			item.Phase = phase
			if phase != "Stopping" {
				item.Progress = min(100, max(0, progress))
			}
			// Preparation owns the queue slot before the first Run starts.
			// Keep its live experiment view informative without storage I/O
			// or overwriting the active scenario's own phase and clock.
			if item.Kind == "experiment" {
				if run, exists := s.state.experiments[item.RunID]; exists && run.State == "queued" {
					run.PhaseName = phase
					s.state.experiments[item.RunID] = run
				}
			}
			changed = true
			break
		}
	}
	s.state.mu.Unlock()
	if changed {
		s.state.notify()
	}
}

func (s *Server) workPosition(id string) (int, bool) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	for _, item := range s.state.workQueue {
		if item.ID == id {
			return item.Position, true
		}
	}
	return 0, false
}

func (s *Server) cancelWork(id string) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if slices.Contains(s.recentWorkCancels, id) {
		return nil
	}
	slot := s.workSlots[id]
	if slot == nil {
		return errWorkNotFound
	}
	if slot.batch != nil {
		if !s.cancelRepeatLocked(slot.batch.queuedIDs[0]) {
			return errExecutionChanged
		}
	} else if slot.cancel != nil {
		slot.cancel()
	}
	s.recentWorkCancels = append(s.recentWorkCancels, id)
	if len(s.recentWorkCancels) > 128 {
		s.recentWorkCancels = slices.Clone(s.recentWorkCancels[len(s.recentWorkCancels)-128:])
	}
	if !slot.opened && slices.Index(s.experimentQueue, slot) > 0 {
		s.removeWorkLocked(slot)
	} else {
		s.updateWorkStatus(id, "Stopping", slot.item.Progress)
	}
	return nil
}

// Shutdown holds cancelMu and closes admission before waiting for workWorkers.
func (s *Server) cancelWorkQueueLocked() {
	for _, slot := range s.workSlots {
		if slot.cancel != nil {
			slot.cancel()
		}
	}
}
