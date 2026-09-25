package controller

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const maxScenarioRepetitions = 100

// Every member remains associated with its batch until the scheduler exits, so
// stopping a queued member or a just-completed iteration cancels the remainder.
// Server.cancelMu protects the map containing these batches.
type repeatBatch struct {
	cancel      context.CancelFunc
	repetitions int
	members     []string
	cleanupRuns []string
}

func (s *Server) cancelRepeatLocked(runID string) bool {
	batch, exists := s.repeatBatches[runID]
	if exists && batch.repetitions <= 1 {
		// A single run retires its cancellation handle before finalization.
		// Its batch bookkeeping must not reopen that already-closed stop gate.
		if _, cancellable := s.cancels[runID]; !cancellable {
			return false
		}
	}
	if exists {
		batch.cancel()
	}
	return exists
}

func (s *Server) StartScenarioRepeated(parent context.Context, raw []byte, repetitions int) (model.Experiment, error) {
	if repetitions < 1 || repetitions > maxScenarioRepetitions {
		return model.Experiment{}, fmt.Errorf("repetitions must be an integer between 1 and %d", maxScenarioRepetitions)
	}
	if err := s.checkRunStorage(); err != nil {
		return model.Experiment{}, err
	}
	spec, err := scenario.Parse(raw)
	if err != nil {
		return model.Experiment{}, err
	}
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown {
		return model.Experiment{}, errors.New("controller is shutting down")
	}
	if err := parent.Err(); err != nil {
		return model.Experiment{}, err
	}
	experiments := make([]model.Experiment, repetitions)
	for index := range experiments {
		var nonce [16]byte
		if _, err := cryptorand.Read(nonce[:]); err != nil {
			return model.Experiment{}, fmt.Errorf("generate experiment id: %w", err)
		}
		now := time.Now().UTC()
		experiments[index] = model.Experiment{
			ID:   fmt.Sprintf("run-%s-%x", now.Format("20060102T150405Z"), nonce),
			Name: spec.Name, State: "queued", Seed: spec.Seed, TotalPhases: len(spec.Phases),
			ScenarioYAML: string(raw), Iteration: index + 1, Repetitions: repetitions,
		}
		if spec.Seed == 0 {
			experiments[index].Seed = now.UnixNano() + int64(index)
		}
	}
	for index := range experiments {
		experiments[index].BatchID = experiments[0].ID
	}
	experiments[0].State = "running"
	experiments[0].StartedAt = time.Now().UTC()
	// Reserve every manifest before running any phase. A partial disk failure
	// rolls back only the new, exclusively created directories owned by this call.
	if err := s.reserveRepeatedResults(experiments, raw); err != nil {
		return model.Experiment{}, err
	}
	plan := newTimingPlan(spec)
	s.state.mu.Lock()
	for _, experiment := range experiments {
		s.state.runTimings[experiment.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, len(spec.Phases))}
	}
	s.state.estimateRunFinishesLocked(experiments, time.Now().UTC())
	s.state.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	batch := &repeatBatch{cancel: cancel, repetitions: repetitions}
	if s.repeatBatches == nil {
		s.repeatBatches = make(map[string]*repeatBatch)
	}
	for _, experiment := range experiments {
		s.repeatBatches[experiment.ID] = batch
		s.cancels[experiment.ID] = cancel
	}
	s.runs.Add(1)
	go s.runRepeatedScenarios(ctx, batch, experiments, spec)
	return experiments[0], nil
}

// Caller holds cancelMu; the global order is cancelMu -> persistMu -> state.mu.
func (s *Server) reserveRepeatedResults(experiments []model.Experiment, raw []byte) (resultErr error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	return s.reserveRepeatedResultsLocked(experiments, raw)
}

// Caller holds cancelMu and persistMu. Used by retry admission as well.
func (s *Server) reserveRepeatedResultsLocked(experiments []model.Experiment, raw []byte) (resultErr error) {
	if err := os.MkdirAll(s.config.DataDir, 0o755); err != nil {
		return err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	if err := data.Mkdir(currentRunsDirectory, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	runs, err := openResultDirectory(data, currentRunsDirectory)
	if err != nil {
		return err
	}
	defer runs.Close()
	var owned []string
	defer func() {
		if resultErr == nil {
			return
		}
		for _, id := range owned {
			if err := removeResultDirectory(runs, id); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("remove incomplete reserved result %s: %w", id, err))
			}
		}
	}()
	for _, experiment := range experiments {
		if err := runs.Mkdir(experiment.ID, 0o755); err != nil {
			return fmt.Errorf("reserve result %s: %w", experiment.ID, err)
		}
		owned = append(owned, experiment.ID)
		if err := s.persistManifest(experiment, raw); err != nil {
			return err
		}
	}
	s.state.mu.Lock()
	for _, experiment := range experiments {
		s.state.experiments[experiment.ID] = experiment
	}
	s.state.mu.Unlock()
	s.state.notify()
	return nil
}

func (s *Server) runRepeatedScenarios(ctx context.Context, batch *repeatBatch, experiments []model.Experiment, spec scenario.Scenario) {
	defer s.runs.Done()
	defer batch.cancel()
	defer func() {
		s.cancelMu.Lock()
		members := batch.members
		if len(members) == 0 {
			for _, experiment := range experiments {
				members = append(members, experiment.ID)
			}
		}
		for _, id := range members {
			delete(s.repeatBatches, id)
			delete(s.cancels, id)
		}
		// A continuation reuses the unstarted IDs. Retire their old timing
		// state before releasing admission to a new scheduler.
		s.state.mu.Lock()
		for _, experiment := range experiments {
			delete(s.state.runTimings, experiment.ID)
		}
		s.state.mu.Unlock()
		s.cancelMu.Unlock()
	}()
	if len(batch.cleanupRuns) > 0 {
		if err := s.cleanupBeforeResume(ctx, batch.cleanupRuns, spec); err != nil {
			s.cancelQueuedIterations(experiments, "Cannot continue batch: "+err.Error())
			return
		}
	}
	for index, experiment := range experiments {
		if err := ctx.Err(); err != nil {
			s.cancelQueuedIterations(experiments[index:], "Repetition batch was canceled before this iteration started")
			return
		}
		if experiment.State == "queued" {
			if err := s.waitRunStorage(ctx, experiment.ID); err != nil {
				s.cancelQueuedIterations(experiments[index:], "Cannot start next run: "+err.Error())
				return
			}
		}
		if experiment.State == "queued" {
			s.updateExperiment(experiment.ID, func(current *model.Experiment) {
				current.State = "running"
				current.PhaseName = ""
				current.StartedAt = time.Now().UTC()
			})
			s.state.mu.RLock()
			experiment = s.state.experiments[experiment.ID]
			s.state.mu.RUnlock()
		}
		s.runScenario(ctx, experiment, spec)
		s.state.persistMu.Lock()
		s.state.mu.RLock()
		finished := s.state.experiments[experiment.ID]
		s.state.mu.RUnlock()
		// A repeated run cannot advance on an unpersisted final result. Existing
		// single-run persistence logging remains unchanged for compatibility.
		if batch.repetitions > 1 {
			if err := s.persistExperiment(finished); err != nil {
				finished.State = "failed"
				finished.Error = fmt.Sprintf("persist final repetition result: %v", err)
				s.state.mu.Lock()
				s.state.experiments[finished.ID] = finished
				s.state.mu.Unlock()
				s.logger.Error("persist repeated experiment", "run", finished.ID, "error", err)
			}
		}
		s.state.persistMu.Unlock()
		if finished.State != "completed" || finished.Error != "" {
			batch.cancel()
			s.cancelQueuedIterations(experiments[index+1:], fmt.Sprintf("Not started because iteration %d ended as %s", experiment.Iteration, finished.State))
			return
		}
	}
}

func (s *Server) cancelQueuedIterations(experiments []model.Experiment, reason string) {
	for _, experiment := range experiments {
		s.updateExperiment(experiment.ID, func(current *model.Experiment) {
			current.State = "canceled"
			current.FinishedAt = time.Now().UTC()
			current.Error = reason
		})
	}
}
