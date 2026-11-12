package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const batchReplayFile = "replay.json"

// Replay inputs are independent of result files. The larger scenario-record
// limit accounts for JSON escaping without enlarging the batch history limit.
// A slot records its resolved seed, including seeds generated from seed: 0.
type batchReplay struct {
	Version      int           `json:"version"`
	BatchID      string        `json:"batchId"`
	ScenarioYAML string        `json:"scenarioYaml"`
	ScenarioHash string        `json:"scenarioHash"`
	Seeds        map[int]int64 `json:"seeds"`
}

func replayScenarioHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Server) readBatchReplay(id string) (*batchReplay, error) {
	root, err := s.resultGroupDirectory(id, false)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	raw, err := readResumeFile(root, batchReplayFile, scenarioRecordLimit)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var replay batchReplay
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(&replay); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("invalid batch replay snapshot")
	}
	if replay.Version != 1 || replay.BatchID != id || len(replay.ScenarioYAML) == 0 || len(replay.ScenarioYAML) > scenarioYAMLLimit || replay.ScenarioHash != replayScenarioHash([]byte(replay.ScenarioYAML)) {
		return nil, errors.New("invalid batch replay scenario identity")
	}
	for iteration := range replay.Seeds {
		if iteration < 1 || iteration > maxScenarioRepetitions {
			return nil, errors.New("invalid batch replay iteration")
		}
	}
	return &replay, nil
}

// Caller holds persistMu. Admission snapshots only inputs, never observed state
// or logs. Extra slots from a rolled-back extension are overwritten on its next
// admission and are never visible beyond the committed repetition count.
func (s *Server) retainBatchReplayLocked(id string, raw []byte, runs []model.Experiment, committedTotal int) error {
	replay, err := s.readBatchReplay(id)
	if err != nil {
		return err
	}
	if replay == nil {
		replay = &batchReplay{Version: 1, BatchID: id, ScenarioYAML: string(raw), ScenarioHash: replayScenarioHash(raw), Seeds: map[int]int64{}}
	} else if !bytes.Equal([]byte(replay.ScenarioYAML), raw) {
		return fmt.Errorf("%w: saved batch scenario differs from replay snapshot", errBatchNotResumable)
	}
	if replay.Seeds == nil {
		replay.Seeds = map[int]int64{}
	}
	for _, run := range runs {
		if run.BatchID != id || run.Iteration < 1 || run.Iteration > maxScenarioRepetitions {
			return errors.New("invalid batch replay member")
		}
		if seed, exists := replay.Seeds[run.Iteration]; exists && run.Iteration <= committedTotal && seed != run.Seed {
			return fmt.Errorf("%w: saved batch seeds differ", errBatchNotResumable)
		}
		replay.Seeds[run.Iteration] = run.Seed
	}
	encoded, err := json.Marshal(replay)
	if err != nil {
		return err
	}
	if len(encoded)+1 > scenarioRecordLimit {
		return errors.New("batch replay snapshot exceeds scenario record limit")
	}
	root, err := s.resultGroupDirectory(id, true)
	if err != nil {
		return err
	}
	defer root.Close()
	if err = writeAnalysisJSON(root, batchReplayFile, replay); err != nil {
		return err
	}
	return syncRunDirectory(root)
}

// Old groups can recover from their saved result scenarios, including previous
// attempts. Never consult the editable scenario library. An unknown generated
// seed is deliberately not guessed from another iteration.
func (s *Server) batchReplayInputs(ctx context.Context, id string, members []savedResult) (*batchReplay, error) {
	replay, err := s.readBatchReplay(id)
	if err != nil {
		return nil, err
	}
	if replay == nil {
		replay = &batchReplay{Version: 1, BatchID: id, Seeds: map[int]int64{}}
	}
	if replay.Seeds == nil {
		replay.Seeds = map[int]int64{}
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) && len(members) == 0 {
		return replay, nil
	}
	if err != nil {
		return nil, err
	}
	defer runs.Close()
	for _, member := range members {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		root, err := openResultDirectory(runs, member.ID)
		if err != nil {
			return nil, err
		}
		metadata, metaErr := readResumeFile(root, "experiment.json", resultMetadataLimit)
		raw, yamlErr := readResumeFile(root, "scenario.yaml", scenarioYAMLLimit)
		root.Close()
		if metaErr != nil {
			return nil, metaErr
		}
		if yamlErr != nil {
			return nil, yamlErr
		}
		var original model.Experiment
		if err := json.Unmarshal(metadata, &original); err != nil {
			return nil, err
		}
		original.BatchID, original.Iteration, original.Repetitions = normalizeLegacySingleRunBatch(original.ID, original.BatchID, original.Iteration, original.Repetitions)
		if original.ID != member.ID || original.BatchID != id || original.Iteration != member.Iteration {
			return nil, errors.New("invalid saved replay identity")
		}
		if replay.ScenarioYAML == "" {
			replay.ScenarioYAML, replay.ScenarioHash = string(raw), replayScenarioHash(raw)
		} else if !bytes.Equal([]byte(replay.ScenarioYAML), raw) {
			return nil, errors.New("saved batch scenarios differ")
		}
		if seed, exists := replay.Seeds[member.Iteration]; exists && seed != original.Seed {
			return nil, errors.New("saved batch seeds differ")
		}
		replay.Seeds[member.Iteration] = original.Seed
	}
	return replay, nil
}

func missingIterationID(id string, iteration int) string {
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("missing-%x-%d", sum[:12], iteration)
}

func (s *Server) missingResultSourceAbsent(id string) (bool, error) {
	deleted, err := s.state.resultDeletedLocked(id)
	if err != nil || deleted {
		return deleted, err
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer runs.Close()
	root, err := openResultDirectory(runs, id)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer root.Close()
	_, err = root.Lstat("experiment.json")
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	return false, err
}

// Missing rows describe unavailable results. They are not result directories,
// downloadable data, completed samples, or historical attempts.
func (s *Server) missingBatchMembers(ctx context.Context, id string, all []savedResult) ([]savedResult, error) {
	record, err := s.readBatchExtension(id)
	if err != nil {
		return nil, err
	}
	current, total, name := currentBatchMembers(all), 0, id
	if record != nil {
		total = record.Repetitions
	}
	// An interrupted first admission may leave a journal with no committed IDs.
	if len(all) == 0 && (record == nil || len(record.Current) == 0) {
		return nil, nil
	}
	present := map[int]bool{}
	for _, run := range current {
		total = max(total, run.Repetitions)
		name = run.Name
		if run.Iteration < 1 || run.Iteration > maxScenarioRepetitions || present[run.Iteration] {
			return nil, errors.New("inconsistent batch iteration metadata")
		}
		present[run.Iteration] = true
	}
	if total < 1 || total > maxScenarioRepetitions {
		return nil, errors.New("invalid batch repetition count")
	}
	if len(present) == total {
		return nil, nil
	}
	replay, replayErr := s.batchReplayInputs(ctx, id, all)
	var spec scenario.Scenario
	if replayErr == nil && replay.ScenarioYAML != "" {
		spec, replayErr = scenario.Parse([]byte(replay.ScenarioYAML))
		name = spec.Name
	}
	missing := []savedResult{}
	for iteration := 1; iteration <= total; iteration++ {
		if present[iteration] {
			continue
		}
		runID := ""
		if record != nil {
			runID = record.Current[iteration]
		}
		unknown := runID == ""
		if unknown {
			runID = missingIterationID(id, iteration)
		}
		row := savedResult{ID: runID, BatchID: id, Iteration: iteration, Repetitions: total, Name: name, State: "missing", Missing: true, missingUnknownID: unknown}
		s.state.mu.RLock()
		for _, live := range s.state.experiments {
			if live.BatchID == id && (live.State == "queued" || live.State == "running") {
				row.Active = true
				break
			}
		}
		s.state.mu.RUnlock()
		switch {
		case replayErr != nil:
			row.RetryUnavailable = "Cannot retry missing run: saved replay inputs cannot be verified: " + replayErr.Error()
		case replay.ScenarioYAML == "":
			row.RetryUnavailable = "Cannot retry missing run: no saved scenario snapshot remains. Restore the original saved scenario; the scenario library is not used."
		default:
			if _, known := replay.Seeds[iteration]; !known && spec.Seed == 0 {
				row.RetryUnavailable = "Cannot retry missing run: its generated seed was not retained. Restore its experiment.json or batch replay snapshot."
			}
		}
		if !unknown {
			absent, err := s.missingResultSourceAbsent(runID)
			if err != nil {
				row.RetryUnavailable = "Cannot inspect missing result: " + err.Error()
			} else if !absent {
				row.RetryUnavailable = "Cannot retry unavailable run metadata: experiment.json still exists but could not be read. Repair its permissions or contents before retrying."
			}
		}
		missing = append(missing, row)
	}
	return missing, nil
}

func (s *Server) withMissingSavedResults(ctx context.Context, results []savedResult) ([]savedResult, error) {
	groups := map[string][]savedResult{}
	indices := map[string]int{}
	for i, run := range results {
		indices[run.ID] = i
		if run.BatchID != "" {
			groups[run.BatchID] = append(groups[run.BatchID], run)
		}
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	root, err := openResultDirectory(data, resultGroupsDirectory)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if root != nil {
		defer root.Close()
		entries, err := localRunEntries(root)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() && validResultID(entry.Name()) {
				if _, exists := groups[entry.Name()]; !exists {
					groups[entry.Name()] = nil
				}
			}
		}
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		missing, err := s.missingBatchMembers(ctx, id, groups[id])
		if err != nil {
			s.logger.Warn("describe missing saved runs", "batch", id, "error", err)
			continue
		}
		for _, row := range missing {
			if i, exists := indices[row.ID]; exists {
				if results[i].State == "unreadable" {
					results[i] = row
				}
				continue
			}
			indices[row.ID] = len(results)
			results = append(results, row)
		}
	}
	return results, nil
}
