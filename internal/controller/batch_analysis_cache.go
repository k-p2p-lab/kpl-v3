package controller

import (
	"context"
	"errors"
	"os"
)

// Keep one source descriptor per contributor. A cache hit pins the same Run
// identity while exposing only its local derived analysis to the worker.
func (s *Server) captureBatchAnalysisSource(ctx context.Context, job *batchAnalysisJob, member savedResult) (*resultSnapshot, error) {
	if !job.force {
		cached, hit, err := s.captureBatchRunAnalysisCache(ctx, member)
		if err != nil {
			return nil, err
		}
		if hit {
			return cached, nil
		}
	}
	return s.captureAnalysisSource(ctx, job.slot, member.ID)
}

// Deletion holds persistMu and has already committed the Run tombstone. Cache
// publishers take that same lock before committing, so late work cannot restore
// derived data after deletion.
func (s *Server) removeBatchRunAnalysisCacheLocked(id string) error {
	if !validResultID(id) {
		return errResultNotFound
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	parent, err := openResultDirectory(data, batchRunCacheDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer parent.Close()
	if err := parent.RemoveAll(id); err != nil {
		return err
	}
	return syncRunDirectory(parent)
}
