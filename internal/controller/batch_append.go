package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const batchExtensionFile = "batch.json"

var errBatchNotAppendable = errors.New("only a complete, idle group can accept additional runs")

// A single local record commits the new total. Historical run files retain their
// original metadata; readers project the current total from this record. Pending
// IDs remain invisible until every new result is reserved and the record commits.
type batchExtension struct {
	Version     int      `json:"version"`
	BatchID     string   `json:"batchId"`
	Repetitions int      `json:"repetitions"`
	Pending     []string `json:"pending,omitempty"`
}

func (s *Server) readBatchExtension(id string) (*batchExtension, error) {
	if id == "" {
		return nil, nil
	}
	root, err := s.resultGroupDirectory(id, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := openResultFile(root, batchExtensionFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.close()
	if file.size > resultMetadataLimit {
		return nil, errors.New("batch extension record is too large")
	}
	var record batchExtension
	decoder := json.NewDecoder(file.reader())
	if err := decoder.Decode(&record); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid batch extension record")
	}
	if record.Version != 1 || record.BatchID != id || record.Repetitions < 1 || record.Repetitions > maxScenarioRepetitions || record.Repetitions+len(record.Pending) > maxScenarioRepetitions {
		return nil, errors.New("invalid batch extension identity or count")
	}
	seen := map[string]bool{}
	for _, runID := range record.Pending {
		if !validResultID(runID) || seen[runID] {
			return nil, errors.New("invalid pending extension run")
		}
		seen[runID] = true
	}
	return &record, nil
}

func (s *Server) applyBatchExtension(result *savedResult) error {
	record, err := s.readBatchExtension(result.BatchID)
	if err != nil || record == nil {
		return err
	}
	if slices.Contains(record.Pending, result.ID) {
		return errResultNotFound
	}
	if result.Repetitions < 1 || result.Repetitions > record.Repetitions {
		return errors.New("run count exceeds committed batch extension")
	}
	result.Repetitions = record.Repetitions
	return nil
}

// Caller holds cancelMu and persistMu. Only uncommitted, never-started IDs named
// in the local journal are removed. Committed queued runs survive for retry.
func (s *Server) rollbackBatchExtensionLocked(id string) error {
	record, err := s.readBatchExtension(id)
	if err != nil || record == nil || len(record.Pending) == 0 {
		return err
	}
	runs, err := s.openResultRuns()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if runs != nil {
		defer runs.Close()
		for _, runID := range record.Pending {
			root, err := openResultDirectory(runs, runID)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			file, openErr := openResultFile(root, "experiment.json")
			if openErr == nil {
				result, readErr := readResultMetadata(file, runID, false)
				file.close()
				if readErr != nil || result.BatchID != id || result.Repetitions != record.Repetitions+len(record.Pending) || result.storedState != "queued" || !result.StartedAt.IsZero() {
					root.Close()
					return fmt.Errorf("refuse to remove unexpected pending run %s", runID)
				}
			} else if !errors.Is(openErr, os.ErrNotExist) {
				root.Close()
				return openErr
			}
			root.Close()
			if err := removeResultDirectory(runs, runID); err != nil {
				return err
			}
		}
		if err := syncRunDirectory(runs); err != nil {
			return err
		}
	}
	root, err := s.resultGroupDirectory(id, false)
	if err != nil {
		return err
	}
	defer root.Close()
	record.Pending = nil
	if err := writeAnalysisJSON(root, batchExtensionFile, record); err != nil {
		return err
	}
	return syncRunDirectory(root)
}

func (s *Server) recoverBatchExtensions(ctx context.Context) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	data, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer data.Close()
	groups, err := openResultDirectory(data, resultGroupsDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer groups.Close()
	entries, err := localRunEntries(groups)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() || !validResultID(entry.Name()) {
			continue
		}
		if err := s.rollbackBatchExtensionLocked(entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handleBatchAppend(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			AdditionalRuns      int `json:"additionalRuns"`
			ExpectedRepetitions int `json:"expectedRepetitions"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if request.AdditionalRuns < 1 || request.ExpectedRepetitions < 1 || request.ExpectedRepetitions > maxScenarioRepetitions || request.AdditionalRuns > maxScenarioRepetitions-request.ExpectedRepetitions {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("additionalRuns must be positive and the group total cannot exceed %d; expectedRepetitions is required", maxScenarioRepetitions))
			return
		}
		run, err := s.AppendScenarioBatch(ctx, r.PathValue("batchID"), request.AdditionalRuns, request.ExpectedRepetitions)
		if err != nil {
			switch {
			case errors.Is(err, errResultNotFound):
				http.NotFound(w, r)
			case errors.Is(err, errBatchNotAppendable), errors.Is(err, errResultBusy):
				writeError(w, http.StatusConflict, err.Error())
			case errors.Is(err, errRunStorageFull):
				writeError(w, http.StatusInsufficientStorage, err.Error())
			default:
				s.logger.Error("append experiment batch", "batch", r.PathValue("batchID"), "error", err)
				writeError(w, http.StatusInternalServerError, "cannot append runs; refresh results before retrying: "+err.Error())
			}
			return
		}
		writeJSON(w, http.StatusAccepted, run)
	}
}

func (s *Server) AppendScenarioBatch(parent context.Context, id string, additional, expected int) (model.Experiment, error) {
	if !validResultID(id) {
		return model.Experiment{}, errResultNotFound
	}
	if additional < 1 || expected < 1 || expected > maxScenarioRepetitions || additional > maxScenarioRepetitions-expected {
		return model.Experiment{}, fmt.Errorf("invalid extension: group limit is %d", maxScenarioRepetitions)
	}
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
	err := s.rollbackBatchExtensionLocked(id)
	s.state.persistMu.Unlock()
	if err != nil {
		return model.Experiment{}, err
	}
	all, err := s.allBatchMembers(parent, id)
	if err != nil {
		return model.Experiment{}, err
	}
	members := currentBatchMembers(all)
	sort.Slice(members, func(i, j int) bool { return members[i].Iteration < members[j].Iteration })
	if len(members) != expected {
		return model.Experiment{}, fmt.Errorf("%w: group size changed or runs are missing; refresh results", errBatchNotAppendable)
	}
	for i, member := range members {
		if member.State != "completed" || member.Repetitions != expected || member.Iteration != i+1 {
			return model.Experiment{}, errBatchNotAppendable
		}
	}
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	batch := &repeatBatch{repetitions: expected + additional}
	for _, member := range all {
		if s.resultDeletionBusyLocked(member.ID) {
			return model.Experiment{}, errResultBusy
		}
		if job := s.analysisJobs[member.ID]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
			return model.Experiment{}, errResultBusy
		}
		batch.members = append(batch.members, member.ID)
		batch.cleanupRuns = append(batch.cleanupRuns, member.ID)
		batch.cleanupRuns = append(batch.cleanupRuns, member.PreviousRunIDs...)
	}
	if job := s.batchAnalysisJobs[id]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
		return model.Experiment{}, errResultBusy
	}
	if err := s.checkRunStorage(); err != nil {
		return model.Experiment{}, err
	}
	var raw []byte
	for _, member := range members {
		root, err := s.analysisDirectory(member.ID)
		if err != nil {
			return model.Experiment{}, err
		}
		yaml, readErr := readResumeFile(root, "scenario.yaml", scenarioYAMLLimit)
		root.Close()
		if readErr != nil {
			return model.Experiment{}, readErr
		}
		if raw == nil {
			raw = yaml
		} else if !bytes.Equal(raw, yaml) {
			return model.Experiment{}, fmt.Errorf("%w: saved scenarios differ", errBatchNotAppendable)
		}
	}
	spec, err := scenario.Parse(raw)
	if err != nil {
		return model.Experiment{}, fmt.Errorf("%w: saved scenario: %v", errBatchNotAppendable, err)
	}
	pending := make([]model.Experiment, additional)
	record := batchExtension{Version: 1, BatchID: id, Repetitions: expected}
	for i := range pending {
		now := time.Now().UTC()
		pending[i] = model.Experiment{ID: fmt.Sprintf("run-%s-%s", now.Format("20060102T150405Z"), rand.Text()), BatchID: id, Iteration: expected + i + 1, Repetitions: expected + additional, Name: spec.Name, State: "queued", Seed: spec.Seed, TotalPhases: len(spec.Phases), ScenarioYAML: string(raw)}
		if spec.Seed == 0 {
			pending[i].Seed = now.UnixNano() + int64(i)
		}
		record.Pending = append(record.Pending, pending[i].ID)
	}
	root, err := s.resultGroupDirectory(id, true)
	if err != nil {
		return model.Experiment{}, err
	}
	defer root.Close()
	if err := writeAnalysisJSON(root, batchExtensionFile, record); err != nil {
		return model.Experiment{}, err
	}
	if err := syncRunDirectory(root); err != nil {
		return model.Experiment{}, err
	}
	// Make the journal's parent directories durable before creating any runs.
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return model.Experiment{}, err
	}
	groups, err := openResultDirectory(data, resultGroupsDirectory)
	if err == nil {
		err = syncRunDirectory(groups)
		groups.Close()
	}
	if err == nil {
		err = syncRunDirectory(data)
	}
	data.Close()
	if err != nil {
		return model.Experiment{}, err
	}
	if err := s.reserveRepeatedFilesLocked(pending, raw); err != nil {
		return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
	}
	if err := parent.Err(); err != nil {
		return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
	}
	// File contents and directory entries must reach local storage before commit.
	for _, run := range pending {
		dir, err := s.analysisDirectory(run.ID)
		if err == nil {
			for _, name := range []string{"experiment.json", "scenario.yaml"} {
				f, openErr := dir.Open(name)
				if openErr != nil {
					err = openErr
					break
				}
				err = errors.Join(f.Sync(), f.Close())
				if err != nil {
					break
				}
			}
			if err == nil {
				err = syncRunDirectory(dir)
			}
			dir.Close()
		}
		if err != nil {
			return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
		}
	}
	runs, err := s.openResultRuns()
	if err != nil {
		return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
	}
	err = syncRunDirectory(runs)
	runs.Close()
	if err != nil {
		return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
	}
	record.Repetitions += additional
	record.Pending = nil
	if err := writeAnalysisJSON(root, batchExtensionFile, record); err != nil {
		return model.Experiment{}, errors.Join(err, s.rollbackBatchExtensionLocked(id))
	}
	// From this point the admission is committed; preserve runs even if a final
	// directory sync fails. A refresh/restart exposes them for the retry workflow.
	if err := syncRunDirectory(root); err != nil {
		return model.Experiment{}, err
	}
	plan := newTimingPlan(spec)
	s.state.mu.Lock()
	for runID, run := range s.state.experiments {
		if run.BatchID == id {
			run.Repetitions = record.Repetitions
			s.state.experiments[runID] = run
		}
	}
	for _, run := range pending {
		s.state.experiments[run.ID] = run
		s.state.runTimings[run.ID] = &runTiming{plan: plan, phases: make([]phaseTiming, len(spec.Phases))}
		batch.members = append(batch.members, run.ID)
	}
	s.state.estimateRunFinishesLocked(pending, time.Now().UTC())
	s.state.mu.Unlock()
	ctx, cancel := context.WithCancel(parent)
	batch.cancel = cancel
	if s.repeatBatches == nil {
		s.repeatBatches = make(map[string]*repeatBatch)
	}
	for _, runID := range batch.members {
		s.repeatBatches[runID] = batch
		s.cancels[runID] = cancel
	}
	s.state.notify()
	s.runs.Add(1)
	go s.runRepeatedScenarios(ctx, batch, pending, spec)
	return pending[0], nil
}
