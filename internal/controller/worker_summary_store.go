package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// The summary is a durable projection, not a retention cache. Historical Runs
// remain visible after their in-memory projection has been released. Iteration
// holds at most one decoded Run and a small directory page, without replaying
// telemetry or touching the archive mount.
const workerSummaryLimit = 16 << 20

func (s *state) readWorkerSummary(id string) (runWorkerSummary, error) {
	if !validResultID(id) {
		return runWorkerSummary{}, errors.New("invalid summary Run identity")
	}
	if deleted, err := s.resultDeletedLocked(id); err != nil {
		return runWorkerSummary{}, err
	} else if deleted {
		return runWorkerSummary{}, os.ErrNotExist
	}
	parent, err := os.OpenRoot(filepath.Join(s.dataDir, currentRunsDirectory))
	if err != nil {
		return runWorkerSummary{}, err
	}
	defer parent.Close()
	root, err := openResultDirectory(parent, id)
	if err != nil {
		return runWorkerSummary{}, err
	}
	defer root.Close()
	return readWorkerSummaryRoot(root, id)
}

func readWorkerSummaryRoot(root *os.Root, id string) (runWorkerSummary, error) {
	data, err := readResumeFile(root, workerSummaryFile, workerSummaryLimit)
	if err != nil {
		return runWorkerSummary{}, err
	}
	var summary runWorkerSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		return summary, err
	}
	if summary.RunID != id || summary.Metrics.RunID != id {
		return summary, errors.New("persisted summary Run identity mismatch")
	}
	for _, row := range summary.Counters {
		labels := 4
		switch row.Family {
		case "events", "controlRPCs", "controlEntries", "controlMessageIDs", "controlPXRecords":
		case "messageBytes":
			labels = 5
		case "operationFailures":
			labels = 3
		case "droppedEvents":
			labels = 2
		default:
			return summary, errors.New("unknown persisted counter family")
		}
		if len(row.Labels) != labels || row.Labels[0] != id || row.Value < 0 {
			return summary, errors.New("persisted counter identity mismatch")
		}
	}
	return summary, nil
}

func (s *state) walkWorkerSummaries(skip map[string]bool, visit func(runWorkerSummary) error) error {
	root, err := os.OpenRoot(filepath.Join(s.dataDir, currentRunsDirectory))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(128)
		for _, entry := range entries {
			id := entry.Name()
			if !entry.IsDir() || !validResultID(id) || skip[id] {
				continue
			}
			// A deletion can be committed before local cleanup fails. Its
			// remaining projection is evidence to remove, never a live series.
			if deleted, err := s.resultDeletedLocked(id); err != nil {
				return err
			} else if deleted {
				continue
			}
			run, err := openResultDirectory(root, id)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			summary, err := readWorkerSummaryRoot(run, id)
			run.Close()
			// A Run can be queued, still acquiring its first summary, or deleted
			// concurrently. Missing projections are not fabricated as zeroes.
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return fmt.Errorf("read runtime summary %s: %w", id, err)
			}
			if err := visit(summary); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (s *state) visitWorkerSummaries(visit func(runWorkerSummary) error) error {
	s.mu.RLock()
	cached := make([]runWorkerSummary, 0, len(s.workerSummaries))
	skip := make(map[string]bool, len(s.workerSummaries))
	for id, summary := range s.workerSummaries {
		cached = append(cached, summary)
		skip[id] = true
	}
	s.mu.RUnlock()
	for _, summary := range cached {
		if deleted, err := s.resultDeletedLocked(summary.RunID); err != nil {
			return err
		} else if deleted {
			continue
		}
		if err := visit(summary); err != nil {
			return err
		}
	}
	return s.walkWorkerSummaries(skip, visit)
}

// Caller holds persistMu. The immutable summary has already been synced by
// writeAnalysisJSON; only the redundant RAM memo is discarded.
func (s *state) releaseRetiredSummary(id string) {
	s.mu.Lock()
	if summary, ok := s.workerSummaries[id]; ok && summary.Retired {
		delete(s.workerSummaries, id)
	}
	s.mu.Unlock()
	s.metrics.lifecycleMu.Lock()
	delete(s.metrics.sealed, id)
	s.metrics.lifecycleMu.Unlock()
}
