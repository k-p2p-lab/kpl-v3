package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"golang.org/x/sys/unix"
)

var errRunStorageFull = errors.New("insufficient local result storage")

func (s *Server) runStorageFree() (uint64, error) {
	if err := os.MkdirAll(s.config.DataDir, 0755); err != nil {
		return 0, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(s.config.DataDir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
func (s *Server) checkRunStorage() error {
	if s.config.RunMinFreeBytes == 0 {
		return nil
	}
	free, err := s.runStorageFree()
	if err != nil {
		return err
	}
	if free < s.config.RunMinFreeBytes {
		return fmt.Errorf("%w: %d bytes available; %d required before starting a run", errRunStorageFull, free, s.config.RunMinFreeBytes)
	}
	return nil
}
func (s *Server) waitRunStorage(ctx context.Context, id string) error {
	waiting := false
	for {
		err := s.checkRunStorage()
		if err == nil {
			return nil
		}
		if !errors.Is(err, errRunStorageFull) {
			return err
		}
		if !waiting {
			s.updateExperiment(id, func(run *model.Experiment) { run.PhaseName = "Waiting for local result storage" })
			s.logger.Warn("batch waiting for local result storage", "run", id, "error", err)
			waiting = true
		}
		if err := sleepContext(ctx, time.Second); err != nil {
			return err
		}
	}
}

type resultStorageOverview struct {
	Checking         bool      `json:"checking"`
	CheckStartedAt   time.Time `json:"checkStartedAt"`
	LocalDirectory   string    `json:"localDirectory"`
	ArchiveDirectory string    `json:"archiveDirectory"`
	AvailableBytes   uint64    `json:"availableBytes"`
	MinFreeBytes     uint64    `json:"minFreeBytes"`
	LastCheckedAt    time.Time `json:"lastCheckedAt"`
	Error            string    `json:"error,omitempty"`
}

func (s *Server) handleResultStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	free, err := s.runStorageFree()
	s.archiveStatusMu.RLock()
	result := resultStorageOverview{Checking: s.archiveChecking, CheckStartedAt: s.archiveCheckStartedAt, LocalDirectory: currentRunsDirectory, ArchiveDirectory: archivedRunsDirectory, AvailableBytes: free, MinFreeBytes: s.config.RunMinFreeBytes, LastCheckedAt: s.archiveCheckedAt, Error: s.archiveError}
	s.archiveStatusMu.RUnlock()
	if err != nil {
		result.Error = "Cannot inspect local result storage: " + err.Error()
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}
