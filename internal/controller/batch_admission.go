package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Caller holds cancelMu and persistMu. Every admission uses the same durable
// prepare/commit boundary. A reused ID has a rollback image; a new ID is removed
// on recovery. Current survives result deletion and never falls back to an old attempt.
func (s *Server) commitBatchAdmissionLocked(ctx context.Context, id string, pending []model.Experiment, raw []byte, total int, members []savedResult, before []model.Experiment) error {
	record, err := s.readBatchExtension(id)
	if err != nil {
		return err
	}
	if record == nil {
		previousTotal := total
		if len(members) > 0 {
			previousTotal = members[0].Repetitions
		}
		record = &batchExtension{Version: 1, BatchID: id, Repetitions: previousTotal}
	}
	if record.Current == nil {
		record.Current = map[int]string{}
	}
	for _, member := range currentBatchMembers(members) {
		if record.Current[member.Iteration] == "" && !member.Missing {
			record.Current[member.Iteration] = member.ID
		}
	}
	actual := make([]savedResult, 0, len(members))
	for _, member := range members {
		if !member.Missing {
			actual = append(actual, member)
		}
	}
	inputs, err := s.batchReplayInputs(ctx, id, actual)
	if err != nil {
		return err
	}
	seeds := make([]model.Experiment, 0, len(inputs.Seeds)+len(pending))
	for iteration, seed := range inputs.Seeds {
		if iteration <= record.Repetitions {
			seeds = append(seeds, model.Experiment{BatchID: id, Iteration: iteration, Seed: seed})
		}
	}
	seeds = append(seeds, pending...)
	if err = s.retainBatchReplayLocked(id, raw, seeds, record.Repetitions); err != nil {
		return err
	}
	record.PendingTotal, record.Before = total, before
	for _, run := range pending {
		record.Pending = append(record.Pending, run.ID)
	}
	root, err := s.resultGroupDirectory(id, true)
	if err != nil {
		return err
	}
	defer root.Close()
	if err = writeBatchRecord(root, record); err != nil {
		return err
	}
	if err = syncRunDirectory(root); err != nil {
		return err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
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
		return err
	}
	rollback := func(err error) error { return errors.Join(err, s.rollbackBatchExtensionLocked(id)) }
	if len(before) == 0 {
		if err = s.reserveRepeatedFilesLocked(pending, raw); err != nil {
			return rollback(err)
		}
	} else {
		for _, run := range pending {
			dir, e := s.analysisDirectory(run.ID)
			if e != nil {
				return rollback(e)
			}
			e = writeAnalysisJSON(dir, "experiment.json", run)
			dir.Close()
			if e != nil {
				return rollback(e)
			}
			s.state.markRunArchiveDirty(run.ID)
		}
	}
	for _, run := range pending {
		if err = ctx.Err(); err != nil {
			return rollback(err)
		}
		dir, e := s.analysisDirectory(run.ID)
		if e != nil {
			return rollback(e)
		}
		for _, name := range []string{"experiment.json", "scenario.yaml"} {
			file, openErr := dir.Open(name)
			if openErr != nil {
				e = openErr
				break
			}
			e = errors.Join(file.Sync(), file.Close())
			if e != nil {
				break
			}
		}
		if e == nil {
			e = syncRunDirectory(dir)
		}
		dir.Close()
		if e != nil {
			return rollback(e)
		}
	}
	runs, err := s.openResultRuns()
	if err != nil {
		return rollback(err)
	}
	err = syncRunDirectory(runs)
	runs.Close()
	if err != nil {
		return rollback(err)
	}
	record.Repetitions = total
	if record.Retired == nil {
		record.Retired = map[string]bool{}
	}
	for _, run := range pending {
		for _, old := range run.PreviousRunIDs {
			record.Retired[old] = true
		}
	}
	for _, run := range pending {
		record.Current[run.Iteration] = run.ID
	}
	record.Pending, record.Before, record.PendingTotal = nil, nil, 0
	if err = writeBatchRecord(root, record); err != nil {
		return rollback(err)
	}
	s.state.mu.Lock()
	for id, run := range s.state.experiments {
		if run.BatchID == record.BatchID && record.Retired[id] {
			run.Superseded = true
			s.state.experiments[id] = run
		}
	}
	s.state.mu.Unlock()
	// Once published, an uncertain sync must preserve all committed runs.
	return syncRunDirectory(root)
}

// Upgrade historical groups before deletion, while all membership mutations are
// fenced. This also protects retries made before Current was introduced.
func (s *Server) retainBatchCurrentLocked(runs *os.Root, id string) error {
	dir, err := openResultDirectory(runs, id)
	if err != nil {
		return err
	}
	file, err := openResultFile(dir, "experiment.json")
	if err != nil {
		dir.Close()
		return err
	}
	result, err := readResultMetadata(file, id, false)
	file.close()
	if err != nil || result.BatchID == "" {
		dir.Close()
		return err
	}
	metadata, err := readResumeFile(dir, "experiment.json", resultMetadataLimit)
	if err != nil {
		dir.Close()
		return err
	}
	raw, err := readResumeFile(dir, "scenario.yaml", scenarioYAMLLimit)
	dir.Close()
	if err != nil {
		// Deleting a damaged result remains possible. Prefer its independently
		// retained input; an old result with no saved YAML cannot provide one.
		replay, readErr := s.readBatchReplay(result.BatchID)
		if readErr == nil && replay != nil {
			raw, err = []byte(replay.ScenarioYAML), nil
		} else {
			s.logger.Warn("cannot retain deleted result replay scenario", "run", id, "error", errors.Join(err, readErr))
			raw = nil
		}
	}
	var original model.Experiment
	if err = json.Unmarshal(metadata, &original); err != nil {
		return err
	}
	if len(raw) > 0 {
		if err = s.retainBatchReplayLocked(result.BatchID, raw, []model.Experiment{original}, result.Repetitions); err != nil {
			return err
		}
	}
	record, err := s.readBatchExtension(result.BatchID)
	if err != nil {
		return err
	}

	if record == nil {
		record = &batchExtension{Version: 1, BatchID: result.BatchID, Repetitions: result.Repetitions}
	}
	if len(record.Current) == 0 {
		entries, err := localRunEntries(runs)
		if err != nil {
			return err
		}
		var members []savedResult
		for _, entry := range entries {
			if !entry.IsDir() || !validResultID(entry.Name()) {
				continue
			}
			root, e := openResultDirectory(runs, entry.Name())
			if e != nil {
				continue
			}
			f, e := openResultFile(root, "experiment.json")
			if e == nil {
				m, e := readResultMetadata(f, entry.Name(), false)
				f.close()
				if e == nil && m.BatchID == result.BatchID {
					members = append(members, m)
				}
			}
			root.Close()
		}
		record.Current = map[int]string{}
		record.Retired = map[string]bool{}
		for _, member := range members {
			for _, old := range member.PreviousRunIDs {
				record.Retired[old] = true
			}
		}
		for _, member := range currentBatchMembers(members) {
			record.Current[member.Iteration] = member.ID
		}
	}
	if record.Deleted == nil {
		record.Deleted = map[string]attemptRecord{}
	}
	record.Deleted[id] = attemptOf(result)
	root, err := s.resultGroupDirectory(result.BatchID, true)
	if err != nil {
		return err
	}
	defer root.Close()
	if err = writeBatchRecord(root, record); err != nil {
		return err
	}
	if err = syncRunDirectory(root); err != nil {
		return err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	groups, err := openResultDirectory(data, resultGroupsDirectory)
	if err != nil {
		return err
	}
	err = syncRunDirectory(groups)
	groups.Close()
	if err != nil {
		return err
	}
	if err = syncRunDirectory(data); err != nil {
		return err
	}
	s.state.mu.Lock()
	for key, run := range s.state.experiments {
		if run.BatchID == record.BatchID && record.Retired[key] {
			run.Superseded = true
			s.state.experiments[key] = run
		}
	}
	s.state.mu.Unlock()
	return nil
}

func writeBatchRecord(root *os.Root, record any) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(data)+1 > resultMetadataLimit {
		return errors.New("batch history exceeds metadata limit")
	}
	return writeAnalysisJSON(root, batchExtensionFile, record)
}
