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

// cancelMu protects the slot and canonical FIFO. Progress lives separately in
// state.workQueue so analysisJobMu callers never acquire cancelMu in reverse.
type workSlot struct {
	id       string
	item     model.WorkQueueItem
	ctx      context.Context
	cancel   context.CancelFunc
	ready    chan struct{}
	opened   bool
	finished bool
	batch    *repeatBatch
}

// Admission holds cancelMu and has checked shuttingDown before registering the
// worker. Every successful call must be paired with finishWork after cleanup.
func (s *Server) enqueueWorkLocked(ctx context.Context, item model.WorkQueueItem, cancel context.CancelFunc) *workSlot {
	item.ID, item.CreatedAt = "work-"+rand.Text(), time.Now().UTC()
	item.State, item.Phase, item.Progress = "queued", "", 0
	item.StartedAt = nil
	slot := &workSlot{id: item.ID, item: item, ctx: ctx, cancel: cancel, ready: make(chan struct{})}
	if s.workSlots == nil {
		s.workSlots = make(map[string]*workSlot)
	}
	s.workSlots[slot.id] = slot
	s.workWorkers.Add(1)
	s.experimentQueue = append(s.experimentQueue, slot)
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
	s.state.mu.Lock()
	previous := make(map[string]model.WorkQueueItem, len(s.state.workQueue))
	for _, item := range s.state.workQueue {
		previous[item.ID] = item
	}
	queue := make([]model.WorkQueueItem, 0, len(s.experimentQueue))
	for position, slot := range s.experimentQueue {
		slot.item.Position = position
		slot.item.State = "queued"
		if position == 0 {
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
	if len(s.experimentQueue) > 0 && !s.experimentQueue[0].opened {
		head := s.experimentQueue[0]
		head.opened = true
		close(head.ready)
	}
	s.state.notify()
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

func (s *Server) waitWork(ctx context.Context, slot *workSlot) error {
	select {
	case <-slot.ready:
	case <-ctx.Done():
		s.cancelMu.Lock()
		// A canceled waiter must not retain its place, but an already active
		// worker owns its slot until its caller's deferred finishWork.
		if slices.Index(s.experimentQueue, slot) > 0 {
			s.removeWorkLocked(slot)
		}
		s.cancelMu.Unlock()
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.recoverWorkCleanup(ctx); err != nil {
		return err
	}
	if slot.item.Kind == "experiment" {
		return nil
	}
	// Retained Peers and failed removal barriers must be resolved before CPU,
	// disk or network intensive analysis starts. No synthetic Run is created.
	return s.waitQueueCleanup(ctx, "", scenario.Scenario{}, func(phase string) {
		s.updateWorkStatus(slot.id, phase, 0)
	})
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
	if slices.Index(s.experimentQueue, slot) > 0 {
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
