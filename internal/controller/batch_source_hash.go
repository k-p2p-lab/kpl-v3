package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Read one captured run at a time: keeping snapshots open across the entire
// batch would exhaust the bounded NAS reader pool. Hashing shares the analysis
// worker limit and never holds Controller locks while reading source data.
func (s *Server) hashBatchAnalysisSources(ctx context.Context, job *batchAnalysisJob, selected []savedResult) (map[string]string, string, error) {
	hashes := make(map[string]string, len(selected))
	for i, member := range selected {
		err := func() error {
			select {
			case s.analysisSlots <- struct{}{}:
				defer func() { <-s.analysisSlots }()
			case <-ctx.Done():
				return ctx.Err()
			}
			snapshot, err := s.captureResultFilesContext(ctx, member.ID, false)
			if err != nil {
				return err
			}
			defer snapshot.close()
			if snapshot.active || snapshot.result.State != "completed" || snapshot.result.BatchID != job.status.BatchID || snapshot.result.SourceRevision != member.SourceRevision {
				return errors.New("batch sources changed; retry after all runs stop")
			}
			var total int64
			for _, file := range snapshot.files {
				if file.name != resultNoteFile {
					total += file.size
				}
			}
			s.analysisJobMu.Lock()
			job.status.State, job.status.Phase = "running", fmt.Sprintf("Run %d/%d · checking-sources", i+1, len(selected))
			job.status.TotalBytes, job.status.ProcessedBytes = total, 0
			if job.status.StartedAt.IsZero() {
				job.status.StartedAt = time.Now().UTC()
			}
			job.status.UpdatedAt = time.Now().UTC()
			err = s.persistBatchAnalysis(job.status)
			s.analysisJobMu.Unlock()
			if err != nil {
				return err
			}
			var processed int64
			var last time.Time
			hash, err := analysisSourceHash(ctx, snapshot.files, func(n int64) {
				processed += n
				now := time.Now().UTC()
				if now.Sub(last) < time.Second {
					return
				}
				last = now
				s.analysisJobMu.Lock()
				job.status.ProcessedBytes, job.status.UpdatedAt = processed, now
				s.analysisJobMu.Unlock()
			})
			if err == nil {
				hashes[member.ID] = hash
			}
			return err
		}()
		if err != nil {
			return nil, "", fmt.Errorf("run %s: %w", member.ID, err)
		}
	}
	members, err := s.allBatchMembers(ctx, job.status.BatchID)
	if err != nil {
		return nil, "", err
	}
	if batchMembership(members) != job.status.Membership {
		return nil, "", errors.New("saved batch members changed during source verification; retry to capture the current batch")
	}
	sourceHash, err := s.batchSourceHash(ctx, job.status.BatchID, members, hashes)
	return hashes, sourceHash, err
}

func (s *Server) batchSourceHash(ctx context.Context, id string, members []savedResult, hashes map[string]string) (string, error) {
	// Completed runs contribute source bytes; excluded/historical attempts
	// contribute their identity and outcome, not their unused event logs.
	for i := range members {
		members[i].SourceRevision = hashes[members[i].ID]
	}
	attempts, err := s.batchReliability(ctx, id, nil)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(struct {
		Version    int
		Membership string
		Attempts   batchReliability
	}{1, batchMembership(members), attempts})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (s *Server) reuseBatchAnalysis(ctx context.Context, job *batchAnalysisJob) error {
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.batchAnalysisJobs[job.status.BatchID] != job {
		return context.Canceled
	}
	members, err := s.allBatchMembers(ctx, job.status.BatchID)
	if err != nil {
		return err
	}
	if batchMembership(members) != job.status.Membership {
		return errors.New("saved batch members changed during source verification; retry to capture the current batch")
	}
	status := *job.previous
	status.Membership, status.Stale, status.Reused = job.status.Membership, false, true
	status.UpdatedAt = time.Now().UTC()
	if err := s.persistBatchAnalysis(status); err != nil {
		return err
	}
	job.status = status
	return nil
}
