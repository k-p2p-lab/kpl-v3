package controller

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
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

var errBatchNotResumable = errors.New("batch has no eligible runs to continue")

func (s *Server) handleBatchResume(ctx context.Context, retry bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		experiment, err := s.resumeScenarioBatch(ctx, r.PathValue("batchID"), retry)
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
	return s.resumeScenarioBatch(parent, id, false)
}

// RetryScenarioBatch restarts unfinished iterations from their first phase,
// preserving completed iterations, the batch ID, iteration numbers and seeds.
// Fresh IDs isolate retries from old Agent fences, late events and saved logs.
func (s *Server) RetryScenarioBatch(parent context.Context, id string) (model.Experiment, error) {
	return s.resumeScenarioBatch(parent, id, true)
}

func (s *Server) resumeScenarioBatch(parent context.Context, id string, retry bool) (model.Experiment, error) {
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
	allMembers, err := s.allBatchMembers(parent, id)
	if err != nil {
		return model.Experiment{}, err
	}
	members := currentBatchMembers(allMembers)
	sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()

	hasFailure, lastAttempt, expected := false, 0, 0
	seen := make(map[int]bool)
	batch := &repeatBatch{}
	for _, member := range allMembers {
		if s.resultDeletionBusyLocked(member.ID) {
			return model.Experiment{}, errResultBusy
		}
		if job := s.analysisJobs[member.ID]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
			return model.Experiment{}, fmt.Errorf("%w: run analysis is still active", errResultBusy)
		}
		batch.members = append(batch.members, member.ID)
		if member.State != "completed" && (!member.StartedAt.IsZero() || member.State == "failed" || retry) {
			batch.cleanupRuns = append(batch.cleanupRuns, member.ID)
		}
		// Keep cleanup ancestry even when an older attempt's result is deleted.
		batch.cleanupRuns = append(batch.cleanupRuns, member.PreviousRunIDs...)
	}
	minimum := 2
	if retry {
		minimum = 1
	}
	for _, member := range members {
		if expected == 0 {
			expected = member.Repetitions
		}
		if expected < minimum || expected > maxScenarioRepetitions || member.Repetitions != expected || member.Iteration < 1 || member.Iteration > expected || seen[member.Iteration] || member.State == "unreadable" {
			return model.Experiment{}, fmt.Errorf("%w: inconsistent batch metadata", errBatchNotResumable)
		}
		seen[member.Iteration] = true
		if !member.StartedAt.IsZero() || member.State == "failed" || member.State == "completed" {
			lastAttempt = max(lastAttempt, member.Iteration)
		}
		hasFailure = hasFailure || member.State == "failed" || member.State == "interrupted" && !member.StartedAt.IsZero()
	}
	if job := s.batchAnalysisJobs[id]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
		return model.Experiment{}, fmt.Errorf("%w: batch analysis is still active", errResultBusy)
	}
	if !retry && !hasFailure {
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
		if retry {
			if member.State == "completed" {
				continue
			}
			if member.State != "failed" && member.State != "canceled" && member.State != "interrupted" {
				return model.Experiment{}, errBatchNotResumable
			}
		} else if member.Iteration <= lastAttempt || !member.StartedAt.IsZero() || (member.State != "canceled" && member.State != "interrupted") {
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
		if original.ID != member.ID || original.BatchID != id || original.Iteration != member.Iteration || original.Repetitions != expected || !retry && !original.StartedAt.IsZero() {
			return model.Experiment{}, errBatchNotResumable
		}
		if !retry {
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
		next.StartedAt, next.FinishedAt, next.Timing = time.Time{}, time.Time{}, nil
		if retry {
			var nonce [16]byte
			if _, err := cryptorand.Read(nonce[:]); err != nil {
				return model.Experiment{}, err
			}
			next.ID = fmt.Sprintf("run-%s-%x", time.Now().UTC().Format("20060102T150405Z"), nonce)
			next.PreviousRunIDs = append(append([]string{}, original.PreviousRunIDs...), original.ID)
		}
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
	if retry {
		if err := s.reserveRepeatedResultsLocked(pending, raw); err != nil {
			return model.Experiment{}, err
		}
		for _, experiment := range pending {
			batch.members = append(batch.members, experiment.ID)
		}
	} else {
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
	seen := make(map[string]bool)
	for _, runID := range runIDs {
		if !validResultID(runID) {
			return fmt.Errorf("invalid cleanup run ID %q", runID)
		}
		if seen[runID] {
			continue
		}
		seen[runID] = true
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
