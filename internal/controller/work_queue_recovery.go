package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func needsRecoveredCleanup(run model.Experiment) bool {
	if run.CleanupState == "complete" {
		return false
	}
	if run.CleanupState == "retained" || run.DataState == "collecting" {
		return true
	}
	return (!run.StartedAt.IsZero() || len(run.Agents) > 0) &&
		(run.State == "running" || run.CleanupState == "failed" || run.CleanupState == "running" || run.CleanupState == "pending")
}

// Controller-only restart does not stop Agent-owned Peers. Recover cleanup from
// retained local manifests and observed live inventory before analysis claims
// isolation. Metadata is read without Controller locks and never resolved on NAS.
func (s *Server) recoverWorkCleanup(ctx context.Context) error {
	candidates := make(map[string]bool)
	restored := make(map[string]model.Experiment)
	// The current/future submissions already have live schedulers and must
	// never receive a recovery fence for their newly reserved Run IDs.
	scheduled := make(map[string]bool)
	s.cancelMu.Lock()
	for _, slot := range s.experimentQueue {
		if slot.batch != nil {
			for _, id := range slot.batch.queuedIDs {
				scheduled[id] = true
			}
		}
	}
	s.cancelMu.Unlock()
	pending, err := s.pendingWorkCleanupIDs()
	if err != nil {
		return err
	}
	for _, id := range pending {
		candidates[id] = true
	}
	s.state.mu.RLock()
	for _, node := range s.state.nodes {
		if node.RunID != "" && !scheduled[node.RunID] && node.State != model.NodeStopped && !(node.State == model.NodeFailed && node.Metadata["cleanupComplete"] == "true") {
			candidates[node.RunID] = true
		}
	}
	for id, run := range s.state.experiments {
		if !scheduled[id] && needsRecoveredCleanup(run) {
			candidates[id] = true
		}
	}
	s.state.mu.RUnlock()
	runs, err := s.openResultRuns()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("restore Peer cleanup metadata: %w", err)
	}
	if runs != nil {
		defer runs.Close()
		directory, err := runs.Open(".")
		if err != nil {
			return err
		}
		defer directory.Close()
		for {
			entries, readErr := directory.ReadDir(128)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			for _, entry := range entries {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !entry.IsDir() || !validResultID(entry.Name()) || scheduled[entry.Name()] {
					continue
				}
				root, e := openResultDirectory(runs, entry.Name())
				if e != nil {
					if errors.Is(e, os.ErrNotExist) {
						continue
					}
					return e
				}
				data, e := readResumeFile(root, "experiment.json", resultMetadataLimit)
				root.Close()
				if errors.Is(e, os.ErrNotExist) {
					continue
				}
				var run model.Experiment
				if e == nil {
					e = json.Unmarshal(data, &run)
				}
				if e != nil || run.ID != entry.Name() || run.State == "" {
					// Corrupt historical results remain unreadable; do not invent
					// replacement manifests. Observed live Peers still force their
					// independent cleanup barrier above.
					s.logger.Warn("cannot read historical Peer cleanup metadata", "run", entry.Name(), "error", e)
					continue
				}
				if needsRecoveredCleanup(run) {
					candidates[run.ID], restored[run.ID] = true, run
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	s.state.mu.Lock()
	for id := range candidates {
		if !validResultID(id) || scheduled[id] {
			continue
		}
		if _, exists := s.state.experiments[id]; !exists {
			if run, found := restored[id]; found {
				// Rehydrate known history solely for cleanup/status sealing. A
				// persisted active state never starts a new scenario scheduler.
				if run.State == "running" || run.State == "queued" {
					run.State = "interrupted"
				}
				run.Timing, run.QueuePosition = nil, 0
				s.state.experiments[id] = run
			}
		}
		s.rememberQueueCleanupLocked(id)
	}
	s.state.mu.Unlock()
	return s.persistQueueCleanupLocked()
}
