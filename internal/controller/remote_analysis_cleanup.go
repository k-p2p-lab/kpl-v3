package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"
)

const analysisWorkerCleanupFile = ".analysis-worker-cleanup.json"

func readAnalysisWorkerCleanup(root *os.Root) ([]string, error) {
	data, err := readResumeFile(root, analysisWorkerCleanupFile, resultMetadataLimit)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var endpoints []string
	if err := json.Unmarshal(data, &endpoints); err != nil || endpoints == nil {
		return nil, errors.New("invalid analysis worker cleanup ledger")
	}
	seen := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		if _, err := remoteAnalysisURL(endpoint); err != nil || seen[endpoint] {
			return nil, errors.New("invalid worker endpoint in analysis cleanup ledger")
		}
		seen[endpoint] = true
	}
	return endpoints, nil
}

func writeAnalysisWorkerCleanup(ctx context.Context, root *os.Root, endpoints []string) error {
	data, err := json.Marshal(endpoints)
	if err != nil {
		return err
	}
	if len(data)+1 > resultMetadataLimit {
		return errors.New("analysis worker cleanup ledger exceeds its size limit")
	}
	staged, err := stageAnalysisJSON(ctx, root, json.RawMessage(data))
	if err != nil {
		return err
	}
	defer staged.discard()
	if err := ctx.Err(); err != nil {
		return err
	}
	return staged.publish(analysisWorkerCleanupFile)
}

// Record admission durably before sending any start message. The scheduler
// assigns at most one active job to an endpoint.
func (s *Server) trackAnalysisWorker(ctx context.Context, endpoint string) error {
	if _, err := remoteAnalysisURL(endpoint); err != nil {
		return err
	}
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	endpoints, err := readAnalysisWorkerCleanup(root)
	if err != nil {
		return err
	}
	if slices.Contains(endpoints, endpoint) {
		return fmt.Errorf("%w: worker %s has an unfinished cleanup record", errRemoteAnalysisCleanupUnconfirmed, endpoint)
	}
	endpoints = append(endpoints, endpoint)
	slices.Sort(endpoints)
	return writeAnalysisWorkerCleanup(ctx, root, endpoints)
}

// Called after a terminal worker response, an idle proof or a failure before
// start. The scheduler cannot reuse an endpoint while its cleanup is unverified.
func (s *Server) clearAnalysisWorker(endpoint string) error {
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	root, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	endpoints, err := readAnalysisWorkerCleanup(root)
	if err != nil {
		return err
	}
	index := slices.Index(endpoints, endpoint)
	if index < 0 {
		return nil
	}
	if len(endpoints) > 1 {
		return writeAnalysisWorkerCleanup(context.Background(), root, slices.Delete(endpoints, index, index+1))
	}
	if err := root.Remove(analysisWorkerCleanupFile); err != nil {
		return err
	}
	return syncRunDirectory(root)
}

func (s *Server) pendingAnalysisWorkerCleanup() ([]string, error) {
	s.analysisWorkerCleanupMu.Lock()
	defer s.analysisWorkerCleanupMu.Unlock()
	root, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	return readAnalysisWorkerCleanup(root)
}

// The queue calls this before opening a new analysis group or admitting the
// next experiment. At this boundary no tracked endpoint can receive a new job.
// Keep probing recorded endpoints even if configuration removed them.
func (s *Server) waitAnalysisWorkerCleanup(ctx context.Context, slot *workSlot) error {
	endpoints, err := s.pendingAnalysisWorkerCleanup()
	if err != nil {
		return fmt.Errorf("read analysis worker cleanup: %w", err)
	}
	if len(endpoints) == 0 {
		return ctx.Err()
	}
	if err := ValidateAnalysisWorkerToken(s.config.AnalysisWorkerToken); err != nil {
		return fmt.Errorf("%w: restore the recorded analysis worker endpoints and their credential to verify cleanup", errRemoteAnalysisCleanupUnconfirmed)
	}
	for _, endpoint := range endpoints {
		s.updateWorkStatus(slot.id, "Waiting for analysis worker cleanup", 0)
		reported := false
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := probeRemoteAnalysisIdle(ctx, endpoint, s.config.AnalysisWorkerToken); err == nil {
				break
			} else if !reported && ctx.Err() == nil {
				s.logger.Warn("waiting for analysis worker cleanup", "worker", endpoint, "error", err)
				reported = true
			}
			if err := sleepContext(ctx, 500*time.Millisecond); err != nil {
				return err
			}
		}
		if err := s.clearAnalysisWorker(endpoint); err != nil {
			return fmt.Errorf("clear verified analysis worker cleanup: %w", err)
		}
	}
	s.updateWorkStatus(slot.id, "", 0)
	return ctx.Err()
}
