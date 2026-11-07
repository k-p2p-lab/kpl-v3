package controller

import (
	"context"
	"errors"
	"fmt"
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
	for _, run := range runs {
		batch.queuedIDs = append(batch.queuedIDs, run.ID)
	}
	batch.slot = s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", Name: runs[0].Name, RunID: runs[0].ID, BatchID: runs[0].BatchID}, batch.cancel)
	batch.slot.batch = batch
	s.updateQueuePositionsLocked()
	s.runs.Add(1)
	go s.runRepeatedScenarios(ctx, batch, runs, spec)
}

// Called after the scheduler retires its cancellation handles and timing state.
func (s *Server) finishQueuedExperimentsLocked(batch *repeatBatch) {
	s.finishWorkLocked(batch.slot)
}

func (s *Server) rememberQueueCleanupLocked(id string) {
	if id != "" && !slices.Contains(s.queueCleanup, id) {
		s.queueCleanup = append(s.queueCleanup, id)
	}
}

// Only the head of the queue enters this method. Failed cleanup stays a barrier
// even if that head is canceled. Ownership records survive result deletion, so
// removing a failed result cannot lose the Agent addresses needed for fencing.
func (s *Server) waitQueueCleanup(ctx context.Context, waitingID string, spec scenario.Scenario, progress ...func(string)) error {
	s.cancelMu.Lock()
	ids := slices.Clone(s.queueCleanup)
	s.cancelMu.Unlock()
	if len(ids) == 0 {
		return ctx.Err()
	}
	phase := func(value string) {
		if len(progress) > 0 && progress[0] != nil {
			progress[0](value)
		} else if waitingID != "" {
			s.updateExperiment(waitingID, func(run *model.Experiment) { run.PhaseName = value })
		}
	}
	phase("Waiting for previous Peer cleanup")
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
			if err := s.completeQueueCleanup(ids); err != nil {
				return fmt.Errorf("clear confirmed Peer cleanup: %w", err)
			}
			// A deletion or late inventory recovery can add a durable cleanup
			// obligation while the Agent round-trip above is in progress. Drain
			// that newly known work before this head claims experiment isolation.
			s.cancelMu.Lock()
			ids = slices.Clone(s.queueCleanup)
			s.cancelMu.Unlock()
			if len(ids) == 0 {
				phase("")
				return ctx.Err()
			}
			previousError = ""
			phase("Waiting for previous Peer cleanup")
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err.Error() != previousError {
			previousError = err.Error()
			s.logger.Warn("experiment queue waiting for Peer cleanup", "run", waitingID, "error", err)
			phase("Waiting for previous Peer cleanup: " + err.Error())
		}
		if err := sleepContext(ctx, 3*time.Second); err != nil {
			return err
		}
	}
}

// Deletion retains pending cleanup under cancelMu. Retire both durable and live
// barriers in that same critical section so deletion cannot recreate a marker
// after it has been cleared but before the live obligation is removed.
func (s *Server) completeQueueCleanup(ids []string) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if err := s.clearPendingWorkCleanup(ids); err != nil {
		return err
	}
	s.queueCleanup = slices.DeleteFunc(s.queueCleanup, func(id string) bool { return slices.Contains(ids, id) })
	return nil
}
