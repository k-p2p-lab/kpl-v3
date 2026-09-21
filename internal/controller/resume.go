package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
	"github.com/k-p2p-lab/v3/internal/scenario"
)

var errBatchNotResumable = errors.New("batch has no remaining unstarted runs after a failure")

func (s *Server) handleBatchResume(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		experiment, err := s.ResumeScenarioBatch(ctx, r.PathValue("batchID"))
		if err != nil {
			switch {
			case errors.Is(err, errResultNotFound):
				http.NotFound(w, r)
			case errors.Is(err, errBatchNotResumable), errors.Is(err, errResultBusy):
				writeError(w, http.StatusConflict, err.Error())
			default:
				s.logger.Error("resume experiment batch", "error", err)
				writeError(w, http.StatusInternalServerError, "cannot continue this batch: "+err.Error())
			}
			return
		}
		writeJSON(w, http.StatusAccepted, experiment)
	}
}

// ResumeScenarioBatch requeues only never-started members. Existing run IDs,
// iteration numbers, seeds and attempted results remain part of the same batch.
// The scheduler retries prior Peer cleanup before starting any remaining run.
func (s *Server) ResumeScenarioBatch(parent context.Context, id string) (model.Experiment, error) {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.shuttingDown {
		return model.Experiment{}, errors.New("controller is shutting down")
	}
	if err := parent.Err(); err != nil {
		return model.Experiment{}, err
	}
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	members, err := s.batchMembers(parent, id)
	if err != nil {
		return model.Experiment{}, err
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()

	hasFailure, lastAttempt, expected := false, 0, 0
	seen := make(map[int]bool)
	batch := &repeatBatch{}
	for _, member := range members {
		if s.resultDeletionBusyLocked(member.ID) {
			return model.Experiment{}, errResultBusy
		}
		if job := s.analysisJobs[member.ID]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
			return model.Experiment{}, fmt.Errorf("%w: run analysis is still active", errResultBusy)
		}
		if expected == 0 {
			expected = member.Repetitions
		}
		if expected < 2 || expected > maxScenarioRepetitions || member.Repetitions != expected || member.Iteration < 1 || member.Iteration > expected || seen[member.Iteration] || member.State == "unreadable" {
			return model.Experiment{}, fmt.Errorf("%w: inconsistent batch metadata", errBatchNotResumable)
		}
		seen[member.Iteration] = true
		batch.members = append(batch.members, member.ID)
		if !member.StartedAt.IsZero() || member.State == "failed" || member.State == "completed" {
			lastAttempt = max(lastAttempt, member.Iteration)
			if member.State != "completed" {
				batch.cleanupRuns = append(batch.cleanupRuns, member.ID)
			}
		}
		hasFailure = hasFailure || member.State == "failed" || member.State == "interrupted" && !member.StartedAt.IsZero()
	}
	if job := s.batchAnalysisJobs[id]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
		return model.Experiment{}, fmt.Errorf("%w: batch analysis is still active", errResultBusy)
	}
	if !hasFailure {
		return model.Experiment{}, errBatchNotResumable
	}

	runs, err := s.openResultRuns()
	if err != nil {
		return model.Experiment{}, err
	}
	defer runs.Close()
	var originals, pending []model.Experiment
	var roots []*os.Root
	defer func() {
		for _, root := range roots {
			_ = root.Close()
		}
	}()
	var raw []byte
	for _, member := range members {
		if member.Iteration <= lastAttempt || !member.StartedAt.IsZero() || (member.State != "canceled" && member.State != "interrupted") {
			continue
		}
		root, err := openResultDirectory(runs, member.ID)
		if err != nil {
			return model.Experiment{}, err
		}
		roots = append(roots, root)
		metadata, err := readResumeFile(root, "experiment.json", resultMetadataLimit)
		if err != nil {
			return model.Experiment{}, err
		}
		var original model.Experiment
		if err := json.Unmarshal(metadata, &original); err != nil {
			return model.Experiment{}, err
		}
		if original.ID != member.ID || original.BatchID != id || !original.StartedAt.IsZero() {
			return model.Experiment{}, errBatchNotResumable
		}
		for _, name := range []string{"events.jsonl", "observations.jsonl"} {
			file, err := openResultFile(root, name)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return model.Experiment{}, err
			}
			_ = file.file.Close()
			if file.size != 0 {
				return model.Experiment{}, fmt.Errorf("%w: run %s already has observations", errBatchNotResumable, member.ID)
			}
		}
		yaml, err := readResumeFile(root, "scenario.yaml", scenarioYAMLLimit)
		if err != nil {
			return model.Experiment{}, err
		}
		if raw == nil {
			raw = yaml
		} else if !bytes.Equal(raw, yaml) {
			return model.Experiment{}, fmt.Errorf("%w: remaining scenarios differ", errBatchNotResumable)
		}
		next := original
		next.State, next.Error, next.PhaseName = "queued", "", ""
		next.Phase, next.ActiveJobs, next.CompletedJobs, next.FailedJobs, next.CanceledJobs = 0, 0, 0, 0, 0
		next.FinishedAt, next.Timing = time.Time{}, nil
		next.ScenarioYAML = string(yaml)
		originals, pending = append(originals, original), append(pending, next)
	}
	if len(pending) == 0 {
		return model.Experiment{}, errBatchNotResumable
	}
	spec, err := scenario.Parse(raw)
	if err != nil {
		return model.Experiment{}, fmt.Errorf("%w: saved scenario: %v", errBatchNotResumable, err)
	}
	if err := parent.Err(); err != nil {
		return model.Experiment{}, err
	}
	// Publish every queued manifest before admitting work. Roll back a partial
	// write failure; no worker or in-memory state is exposed before this succeeds.
	for i, experiment := range pending {
		if err := writeAnalysisJSON(roots[i], "experiment.json", experiment); err != nil {
			for j := 0; j < i; j++ {
				err = errors.Join(err, writeAnalysisJSON(roots[j], "experiment.json", originals[j]))
			}
			return model.Experiment{}, err
		}
	}
	plan := newTimingPlan(spec)
	s.state.mu.Lock()
	for _, experiment := range pending {
		s.state.experiments[experiment.ID] = experiment
		s.state.runTimings[experiment.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, len(spec.Phases))}
	}
	s.state.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	batch.cancel, batch.repetitions = cancel, expected
	if s.repeatBatches == nil {
		s.repeatBatches = make(map[string]*repeatBatch)
	}
	for _, memberID := range batch.members {
		s.repeatBatches[memberID] = batch
		s.cancels[memberID] = cancel
	}
	s.state.notify()
	s.runs.Add(1)
	go s.runRepeatedScenarios(ctx, batch, pending, spec)
	return pending[0], nil
}

func readResumeFile(root *os.Root, name string, limit int) ([]byte, error) {
	file, err := openResultFile(root, name)
	if err != nil {
		return nil, err
	}
	defer file.file.Close()
	if file.size > int64(limit) {
		return nil, fmt.Errorf("%s exceeds %d bytes", name, limit)
	}
	return io.ReadAll(io.NewSectionReader(file.file, 0, file.size))
}

func (s *Server) cleanupBeforeResume(ctx context.Context, runIDs []string, spec scenario.Scenario) error {
	timeout, _ := time.ParseDuration(spec.JobShutdownTimeout)
	if timeout <= 0 {
		timeout = 3 * time.Minute
	}
	for _, runID := range runIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		cleanupCtx, cancel := context.WithTimeout(ctx, timeout)
		// Attempted run IDs will never run again. Fence every possible generation,
		// including generations whose state was lost in a Controller restart.
		err := s.stopRunGeneration(cleanupCtx, runID, ^uint64(0))
		cancel()
		if err != nil {
			return fmt.Errorf("cleanup previous run %s: %w", runID, err)
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.refreshAgentState(cleanupCtx)
}
