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

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

var errBatchNotResumable = errors.New("batch has no eligible runs to continue")
var errBatchAttemptsChanged = errors.New("Batch attempts changed; refresh results before retrying.")

func decodeRetryExpectedRuns(w http.ResponseWriter, r *http.Request) ([]string, error) {
	if r.Body == nil {
		return nil, nil
	}
	var request *struct {
		ExpectedRunIDs json.RawMessage `json:"expectedRunIds"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
		// Existing clients send no body. They retain the legacy unguarded API.
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("invalid retry request: %w", err)
	}
	if request == nil {
		return nil, errors.New("retry request must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("retry request must contain one JSON object")
	}
	if len(request.ExpectedRunIDs) == 0 {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(request.ExpectedRunIDs, &ids); err != nil || len(ids) == 0 || len(ids) > maxScenarioRepetitions {
		return nil, fmt.Errorf("expectedRunIds must contain 1 through %d run IDs", maxScenarioRepetitions)
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !validResultID(id) || seen[id] {
			return nil, errors.New("expectedRunIds must contain distinct valid run IDs")
		}
		seen[id] = true
	}
	return ids, nil
}

func (s *Server) handleBatchResume(ctx context.Context, retry bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var expected []string
		if retry {
			var err error
			expected, err = decodeRetryExpectedRuns(w, r)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		experiment, err := s.resumeScenarioBatchExpected(ctx, r.PathValue("batchID"), retry, expected)
		if err != nil {
			switch {
			case errors.Is(err, errResultNotFound):
				http.NotFound(w, r)
			case errors.Is(err, errBatchAttemptsChanged):
				writeJSON(w, http.StatusConflict, map[string]string{"error": errBatchAttemptsChanged.Error(), "code": "batch-attempt-changed"})
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
	return s.resumeScenarioBatchExpected(parent, id, retry, nil)
}

func (s *Server) resumeScenarioBatchExpected(parent context.Context, id string, retry bool, expectedRunIDs []string) (model.Experiment, error) {
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
	s.state.persistMu.Lock()
	recoveryErr := s.rollbackBatchExtensionLocked(id)
	s.state.persistMu.Unlock()
	if recoveryErr != nil {
		return model.Experiment{}, recoveryErr
	}
	allMembers, err := s.allBatchMembers(parent, id)
	if err != nil && !(retry && errors.Is(err, errResultNotFound)) {
		return model.Experiment{}, err
	}
	if retry {
		missing, err := s.missingBatchMembers(parent, id, allMembers)
		if err != nil {
			return model.Experiment{}, err
		}
		allMembers = append(allMembers, missing...)
		if len(allMembers) == 0 {
			return model.Experiment{}, errResultNotFound
		}
	}
	members := currentBatchMembers(allMembers)
	sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	// A lost manifest can hide an active member from the filesystem listing,
	// including legacy groups without a durable Current ID for that slot.
	s.state.mu.RLock()
	for runID, live := range s.state.experiments {
		if live.BatchID == id && (live.State == "running" || live.State == "queued" || s.cancels[runID] != nil || s.repeatBatches[runID] != nil) {
			s.state.mu.RUnlock()
			return model.Experiment{}, errResultBusy
		}
	}
	s.state.mu.RUnlock()
	if retry && expectedRunIDs != nil {
		// File descriptors captured while listing can precede a final state
		// write. Under persistMu, use the live state where available so a just
		// completed iteration is neither part of the precondition nor retried.
		s.state.mu.RLock()
		for i := range allMembers {
			if live, exists := s.state.experiments[allMembers[i].ID]; exists && live.BatchID == id && !allMembers[i].Missing {
				allMembers[i].State = live.State
			}
		}
		s.state.mu.RUnlock()
		members = currentBatchMembers(allMembers)
		sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
		remaining := make(map[string]bool, len(expectedRunIDs))
		for _, runID := range expectedRunIDs {
			remaining[runID] = true
		}
		count := 0
		for _, member := range members {
			if member.State == "completed" {
				continue
			}
			if !remaining[member.ID] {
				return model.Experiment{}, errBatchAttemptsChanged
			}
			delete(remaining, member.ID)
			count++
		}
		if len(remaining) != 0 || count != len(expectedRunIDs) {
			return model.Experiment{}, errBatchAttemptsChanged
		}
	}

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
		if !member.missingUnknownID {
			batch.members = append(batch.members, member.ID)
		}
		if !member.missingUnknownID && member.State != "completed" && (!member.StartedAt.IsZero() || member.State == "failed" || retry) {
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
	executionID := cryptorand.Text()
	var originals, pending []model.Experiment
	var roots []*os.Root
	defer func() {
		for _, root := range roots {
			_ = root.Close()
		}
	}()
	var raw []byte
	var replay *batchReplay
	for _, member := range members {
		if member.Missing {
			if member.RetryUnavailable != "" {
				return model.Experiment{}, fmt.Errorf("%w: %s", errBatchNotResumable, member.RetryUnavailable)
			}
		}
	}
	for _, member := range members {
		if member.Missing {
			actual := make([]savedResult, 0, len(allMembers))
			for _, run := range allMembers {
				if !run.Missing {
					actual = append(actual, run)
				}
			}
			replay, err = s.batchReplayInputs(parent, id, actual)
			if err != nil {
				return model.Experiment{}, fmt.Errorf("%w: %v", errBatchNotResumable, err)
			}
			break
		}
	}
	for _, member := range members {
		if retry {
			if member.State == "completed" {
				continue
			}
			if member.State != "failed" && member.State != "canceled" && member.State != "interrupted" && member.State != "missing" {
				return model.Experiment{}, errBatchNotResumable
			}
		} else if member.Iteration <= lastAttempt || !member.StartedAt.IsZero() || (member.State != "canceled" && member.State != "interrupted") {
			continue
		}
		if !retry {
			fenced, err := s.state.resultWriteFencedLocked(member.ID)
			if err != nil {
				return model.Experiment{}, err
			}
			if fenced {
				return model.Experiment{}, fmt.Errorf("%w: run %s rejects runtime writes; use Retry to allocate a fresh Run ID", errBatchNotResumable, member.ID)
			}
		}
		var original model.Experiment
		var yaml []byte
		if member.Missing {
			yaml = []byte(replay.ScenarioYAML)
			spec, err := scenario.Parse(yaml)
			if err != nil {
				return model.Experiment{}, fmt.Errorf("%w: saved scenario: %v", errBatchNotResumable, err)
			}
			seed, known := replay.Seeds[member.Iteration]
			if !known {
				if spec.Seed == 0 {
					return model.Experiment{}, fmt.Errorf("%w: missing generated seed for iteration %d", errBatchNotResumable, member.Iteration)
				}
				seed = spec.Seed
			}
			original = model.Experiment{ID: member.ID, BatchID: id, Iteration: member.Iteration, Repetitions: expected, Name: spec.Name, Seed: seed, TotalPhases: len(spec.Phases)}
			previous := map[string]bool{}
			for _, run := range allMembers {
				if run.Iteration != member.Iteration || run.ID == member.ID || run.Missing {
					continue
				}
				for _, old := range append(append([]string{}, run.PreviousRunIDs...), run.ID) {
					if !previous[old] {
						original.PreviousRunIDs = append(original.PreviousRunIDs, old)
						previous[old] = true
					}
				}
			}
		} else {
			root, err := openResultDirectory(runs, member.ID)
			if err != nil {
				return model.Experiment{}, err
			}
			roots = append(roots, root)
			metadata, err := readResumeFile(root, "experiment.json", resultMetadataLimit)
			if err != nil {
				return model.Experiment{}, err
			}
			if err := json.Unmarshal(metadata, &original); err != nil {
				return model.Experiment{}, err
			}
			logical := savedResult{ID: original.ID, BatchID: original.BatchID, Repetitions: original.Repetitions}
			if err := s.applyBatchExtension(&logical); err != nil {
				return model.Experiment{}, err
			}
			original.Repetitions = logical.Repetitions
			if original.ID != member.ID || original.BatchID != id || original.Iteration != member.Iteration || original.Repetitions != expected || !retry && !original.StartedAt.IsZero() {
				return model.Experiment{}, errBatchNotResumable
			}
			if !retry {
				manifest, err := readRunArchive(root, member.ID)
				if err != nil {
					return model.Experiment{}, err
				}
				for _, name := range []string{"events.jsonl", "observations.jsonl"} {
					file, err := captureRunSource(root, manifest, name)
					if errors.Is(err, os.ErrNotExist) {
						continue
					}
					if err != nil {
						return model.Experiment{}, err
					}
					file.close()
					if file.size != 0 {
						return model.Experiment{}, fmt.Errorf("%w: run %s already has observations", errBatchNotResumable, member.ID)
					}
				}
			}
			yaml, err = readResumeFile(root, "scenario.yaml", scenarioYAMLLimit)
			if err != nil {
				return model.Experiment{}, err
			}
		}
		if raw == nil {
			raw = yaml
		} else if !bytes.Equal(raw, yaml) {
			return model.Experiment{}, fmt.Errorf("%w: remaining scenarios differ", errBatchNotResumable)
		}
		next := original
		next.ExecutionID = executionID
		next.StopRequested = false
		next.ControllerVersion = ""
		next.CleanupState, next.DataState, next.IntegrityError, next.CleanupError = "pending", "pending", "", ""
		next.TelemetryPending = false
		next.Agents = nil
		next.State, next.Error, next.PhaseName = "queued", "", ""
		next.Phase, next.ActiveJobs, next.CompletedJobs, next.FailedJobs, next.CanceledJobs = 0, 0, 0, 0, 0
		next.StartedAt, next.FinishedAt, next.Timing = time.Time{}, time.Time{}, nil
		if retry {
			var nonce [16]byte
			if _, err := cryptorand.Read(nonce[:]); err != nil {
				return model.Experiment{}, err
			}
			next.ID = fmt.Sprintf("run-%s-%x", time.Now().UTC().Format("20060102T150405Z"), nonce)
			next.PreviousRunIDs = append([]string{}, original.PreviousRunIDs...)
			if !member.missingUnknownID {
				next.PreviousRunIDs = append(next.PreviousRunIDs, original.ID)
			}
		}
		originals, pending = append(originals, original), append(pending, next)
	}
	if len(pending) == 0 {
		return model.Experiment{}, errBatchNotResumable
	}
	spec, err := scenario.Parse(raw)
	if err != nil {
		return model.Experiment{}, fmt.Errorf("%w: saved scenario: %v", errBatchNotResumable, err)
	}
	sharedScenario := string(raw)
	for i := range pending {
		pending[i].ScenarioYAML = sharedScenario
	}
	if err := parent.Err(); err != nil {
		return model.Experiment{}, err
	}
	before := originals
	if retry {
		before = nil
	}
	for _, member := range members {
		if member.Missing && !member.missingUnknownID {
			absent, err := s.missingResultSourceAbsent(member.ID)
			if err != nil {
				return model.Experiment{}, fmt.Errorf("%w: inspect missing result: %v", errBatchNotResumable, err)
			}
			if !absent {
				return model.Experiment{}, errBatchAttemptsChanged
			}
			// The old result is unavailable. Fence late ingress without creating
			// replacement metadata, journals, or scientific summaries for it.
			// Existing partial files remain untouched; the retry has a fresh ID.
			if err := s.markResultWriteFencedLocked(member.ID); err != nil {
				return model.Experiment{}, err
			}
		}
	}
	if err := s.commitBatchAdmissionLocked(parent, id, pending, raw, expected, members, before); err != nil {
		return model.Experiment{}, err
	}
	if retry {
		for _, run := range pending {
			batch.members = append(batch.members, run.ID)
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
	batch.cancel, batch.repetitions, batch.executionID = cancel, expected, executionID
	if s.repeatBatches == nil {
		s.repeatBatches = make(map[string]*repeatBatch)
	}
	for _, memberID := range batch.members {
		s.repeatBatches[memberID] = batch
		s.cancels[memberID] = cancel
	}
	s.state.notify()
	s.enqueueExperimentsLocked(ctx, batch, pending, spec)
	first, _ := s.state.experimentWithTiming(pending[0].ID)
	return first, nil
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
	if err := s.waitControllerWorkers(ctx, runIDs); err != nil {
		return err
	}
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
		participants, participantErr := s.cleanupParticipants(runID)
		if participantErr == nil && len(participants) == 0 {
			for _, phase := range spec.Phases {
				if phase.Action == "join" {
					participantErr = errors.New("cannot verify previous Peer cleanup before Agents reconnect")
					break
				}
			}
		}
		if participantErr != nil {
			cancel()
			return fmt.Errorf("cleanup previous run %s: %w", runID, participantErr)
		}
		// Attempted run IDs will never run again. Fence every possible generation,
		// including generations whose state was lost in a Controller restart.
		err := s.stopRunGeneration(cleanupCtx, runID, ^uint64(0))
		cancel()
		if err == nil {
			drainCtx, drainCancel := context.WithTimeout(ctx, timeout)
			if s.config.RunWorkers {
				err = s.rebuildJournalIndex(drainCtx, runID)
			}
			if err == nil {
				err = s.drainRunTelemetry(drainCtx, runID)
			}
			drainCancel()
		}
		if err != nil {
			return fmt.Errorf("cleanup previous run %s: %w", runID, err)
		}
		// A successful strict drain ACK resolves delivery debt. Preserve the
		// earlier scientific failure; terminal Peer evidence is checked below.
		if s.config.RunWorkers {
			s.state.persistMu.Lock()
			deleted, fenceErr := s.state.resultWriteFencedLocked(runID)
			s.state.persistMu.Unlock()
			if fenceErr != nil {
				return fenceErr
			}
			if deleted {
				s.state.mu.Lock()
				if run, exists := s.state.experiments[runID]; exists {
					run.TelemetryPending = false
					s.state.experiments[runID] = run
				}
				s.state.mu.Unlock()
			} else {
				s.updateExperiment(runID, func(run *model.Experiment) { run.TelemetryPending = false })
			}
		}
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := s.refreshAgentState(cleanupCtx, runIDs...); err != nil {
		return err
	}
	if s.config.RunWorkers {
		for _, id := range runIDs {
			if err := s.waitRunTerminalEvidence(cleanupCtx, id); err != nil {
				return err
			}
			s.state.persistMu.Lock()
			deleted, err := s.state.resultWriteFencedLocked(id)
			s.state.persistMu.Unlock()
			if err != nil {
				return err
			}
			if _, ok := s.state.workerMetricSummary(id); !deleted && !ok {
				if err := s.computeWorkerSummary(cleanupCtx, id); err != nil {
					return err
				}
			}
			if err := s.retireRunRuntime(cleanupCtx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
