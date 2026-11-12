package controller

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sort"
	"strings"
)

func (s *Server) handleResultBatchAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/result-batches/")
	if id, found := strings.CutSuffix(path, "/note"); found {
		s.handleStoredNote(w, r, id, func(text *string, revision string) (resultNote, error) {
			return s.resultGroupNote(r.Context(), id, text, revision)
		})
		return
	}
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/result-batches/")
	deleted, err := s.deleteSavedBatch(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, errResultNotFound):
			http.NotFound(w, r)
		case errors.Is(err, errResultBusy):
			writeError(w, http.StatusConflict, err.Error())
		default:
			s.logger.Error("delete saved batch", "batch", id, "error", err)
			writeError(w, http.StatusInternalServerError, "cannot finish deleting the group; some results may already be deleted. Refresh and retry after resolving the storage error")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deletedIds": deleted})
}

func (s *Server) deleteSavedBatch(ctx context.Context, id string) (deleted []string, resultErr error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	defer func() {
		keys := make([]string, 0, len(deleted)+1)
		for _, runID := range deleted {
			keys = append(keys, "run:"+runID)
		}
		if resultErr == nil {
			keys = append(keys, "batch:"+id)
		}
		s.removeLibraryMembers(keys...)
	}()
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	record, err := s.readBatchExtension(id)
	if err != nil {
		return nil, err
	}
	// Membership cannot change through admission or another deletion while
	// these locks are held. Read members before persistMu: batchMembers takes it.
	members, err := s.allBatchMembers(ctx, id)
	if err != nil && !errors.Is(err, errResultNotFound) {
		return nil, err
	}
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	// Check every member before removing anything, including the mean artifact.
	// This also freezes download leases until deletion has finished.
	memberIDs := make(map[string]bool, len(members))
	cacheIDs := make(map[string]bool, len(members))
	rememberCache := func(runID string) {
		if validResultID(runID) {
			cacheIDs[runID] = true
		}
	}
	for _, member := range members {
		memberIDs[member.ID] = true
		rememberCache(member.ID)
		for _, previous := range member.PreviousRunIDs {
			rememberCache(previous)
		}
	}
	// Source directories can disappear independently of the durable group
	// history. Retain those identities for cache cleanup and reader preflight,
	// without treating missing sources as deletable result directories.
	if record != nil {
		for _, runID := range record.Current {
			rememberCache(runID)
		}
		for runID := range record.Deleted {
			rememberCache(runID)
		}
		for runID := range record.Retired {
			rememberCache(runID)
		}
	}
	// An interrupted deletion can leave a fenced directory without readable
	// experiment.json. It is intentionally absent from allBatchMembers/listing,
	// but the durable batch history still proves this cleanup belongs here.
	if record != nil && len(record.Deleted) > 0 {
		runs, err := s.openResultRuns()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if runs != nil {
			defer runs.Close()
			for runID := range record.Deleted {
				if memberIDs[runID] {
					continue
				}
				fenced, err := s.state.resultDeletedLocked(runID)
				if err != nil {
					return nil, err
				}
				if !fenced {
					continue
				}
				directory, err := openResultDirectory(runs, runID)
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				if err != nil {
					return nil, err
				}
				directory.Close()
				memberIDs[runID] = true
				members = append(members, savedResult{ID: runID, BatchID: id})
			}
		}
	}
	for runID := range cacheIDs {
		memberIDs[runID] = true
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	s.state.mu.RLock()
	for runID, run := range s.state.experiments {
		if run.BatchID == id {
			memberIDs[runID] = true
		}
	}
	s.state.mu.RUnlock()
	for runID := range memberIDs {
		if s.resultDeletionBusyLocked(runID) {
			return nil, errResultBusy
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A retry may only have the independently stored batch artifact left.
	root, err := s.batchAnalysisDirectory(id, false)
	hasAnalysis := err == nil
	if root != nil {
		_ = root.Close()
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	groupRoot, err := s.resultGroupDirectory(id, false)
	hasGroup := err == nil
	if groupRoot != nil {
		_ = groupRoot.Close()
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(members) == 0 && !hasAnalysis && !hasGroup {
		return nil, errResultNotFound
	}
	if job := s.batchAnalysisJobs[id]; job != nil && job.cancel != nil {
		job.cancel()
	}
	delete(s.batchAnalysisJobs, id)
	// Remove the mean first so a failed run deletion cannot leave a reusable
	// aggregate referring to already removed members.
	if hasAnalysis {
		data, err := os.OpenRoot(s.config.DataDir)
		if err != nil {
			return nil, err
		}
		defer data.Close()
		parent, err := openResultDirectory(data, "batch-analyses")
		if err != nil {
			return nil, err
		}
		defer parent.Close()
		if err := removeResultDirectory(parent, id); err != nil {
			return nil, err
		}
	}
	deleted = make([]string, 0, len(members))
	for _, member := range members {
		if err := s.deleteSavedResultLocked(member.ID); err != nil {
			return deleted, err
		}
		deleted = append(deleted, member.ID)
	}
	// The group record remains available if physical cleanup fails, so a retry
	// can recover missing and retired Run cache identities. persistMu prevents
	// cache publication from racing this cleanup.
	for runID := range cacheIDs {
		if err := s.removeBatchRunAnalysisCacheLocked(runID); err != nil {
			return deleted, err
		}
	}
	// Keep the group note if any member deletion fails, allowing a safe retry.
	if hasGroup || len(members) > 0 {
		data, err := os.OpenRoot(s.config.DataDir)
		if err != nil {
			return deleted, err
		}
		defer data.Close()
		parent, err := openResultDirectory(data, resultGroupsDirectory)
		if err != nil {
			return deleted, err
		}
		defer parent.Close()
		if err := removeResultDirectory(parent, id); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}
