package controller

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// Admission holds cancelMu. Each sequence owns one FIFO slot, including all
// repetitions and finalization. A canceled waiter removes only its own slot.
// Existing batch manifests/receipts retain accepted work across browser loss;
// after Controller restart unfinished records follow manual recovery as before.
func (s *Server) enqueueExperimentsLocked(ctx context.Context, batch *repeatBatch, runs []model.Experiment, spec scenario.Scenario) {
	batch.ready = make(chan struct{})
	for _, run := range runs {
		batch.queuedIDs = append(batch.queuedIDs, run.ID)
	}
	s.experimentQueue = append(s.experimentQueue, batch)
	s.updateQueuePositionsLocked()
	if len(s.experimentQueue) == 1 {
		close(batch.ready)
	}
	s.runs.Add(1)
	go s.runRepeatedScenarios(ctx, batch, runs, spec)
}

func (s *Server) updateQueuePositionsLocked() {
	s.state.mu.Lock()
	for position, batch := range s.experimentQueue {
		for _, id := range batch.queuedIDs {
			run := s.state.experiments[id]
			run.QueuePosition = position
			s.state.experiments[id] = run
		}
	}
	s.state.mu.Unlock()
	s.state.notify()
}

// Called after the scheduler retires its cancellation handles and timing state.
func (s *Server) finishQueuedExperimentsLocked(batch *repeatBatch) {
	index := slices.Index(s.experimentQueue, batch)
	if index < 0 {
		return
	}
	s.experimentQueue = slices.Delete(s.experimentQueue, index, index+1)
	s.state.mu.Lock()
	for _, id := range batch.queuedIDs {
		run := s.state.experiments[id]
		run.QueuePosition = 0
		s.state.experiments[id] = run
	}
	s.state.mu.Unlock()
	s.updateQueuePositionsLocked()
	if index == 0 && len(s.experimentQueue) > 0 {
		close(s.experimentQueue[0].ready)
	}
}

func (s *Server) rememberQueueCleanupLocked(id string) {
	if id != "" && !slices.Contains(s.queueCleanup, id) {
		s.queueCleanup = append(s.queueCleanup, id)
	}
}

// Only the head of the queue enters this method. Failed cleanup stays a barrier
// even if that head is canceled. Ownership records survive result deletion, so
// removing a failed result cannot lose the Agent addresses needed for fencing.
func (s *Server) waitQueueCleanup(ctx context.Context, waitingID string, spec scenario.Scenario) error {
	s.cancelMu.Lock()
	ids := slices.Clone(s.queueCleanup)
	s.cancelMu.Unlock()
	if len(ids) == 0 {
		return ctx.Err()
	}
	s.updateExperiment(waitingID, func(run *model.Experiment) {
		run.PhaseName = "Waiting for previous Peer cleanup"
	})
	previousError := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.cleanupBeforeResume(ctx, ids, spec)
		if err == nil {
			for _, id := range ids {
				s.state.mu.RLock()
				collecting := s.state.experiments[id].DataState == "collecting"
				s.state.mu.RUnlock()
				var dataErr error
				if collecting {
					// A retained network may have produced evidence after its
					// scenario ended. Seal that evidence before releasing it.
					dataErr = s.state.recordAnalysisObservation(id, time.Now().UTC())
					dataErr = errors.Join(dataErr, s.flushRunSources(id))
					if dataErr != nil {
						s.state.recordRunWriteError(id, dataErr)
					}
				}
				s.updateExperiment(id, func(run *model.Experiment) {
					run.CleanupState, run.CleanupError = "complete", ""
					// Preserve already incomplete evidence from a failed run.
					if run.DataState == "collecting" {
						run.DataState = "complete"
						for _, agent := range run.Agents {
							if !agent.RunDrain {
								run.DataState = "unverified"
							}
						}
						if dataErr != nil || run.IntegrityError != "" {
							run.DataState = "incomplete"
						}
					}
				})
			}
			s.cancelMu.Lock()
			s.queueCleanup = slices.DeleteFunc(s.queueCleanup, func(id string) bool { return slices.Contains(ids, id) })
			s.cancelMu.Unlock()
			s.updateExperiment(waitingID, func(run *model.Experiment) { run.PhaseName = "" })
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err.Error() != previousError {
			previousError = err.Error()
			s.logger.Warn("experiment queue waiting for Peer cleanup", "run", waitingID, "error", err)
			s.updateExperiment(waitingID, func(run *model.Experiment) {
				run.PhaseName = "Waiting for previous Peer cleanup: " + err.Error()
			})
		}
		if err := sleepContext(ctx, 3*time.Second); err != nil {
			return err
		}
	}
}
