package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const pendingWorkCleanupDirectory = "cleanup-pending"

// Callers hold cancelMu, but never state.mu. The directory is local Controller
// metadata, separate from deletable results and from the external archive.
func (s *Server) writePendingWorkCleanup(id string) error {
	if !validResultID(id) {
		return errors.New("invalid pending cleanup identity")
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	if err := data.Mkdir(pendingWorkCleanupDirectory, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	directory, err := openResultDirectory(data, pendingWorkCleanupDirectory)
	if err != nil {
		return err
	}
	defer directory.Close()
	file, err := directory.OpenFile(id, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		info, checkErr := directory.Lstat(id)
		if checkErr != nil {
			return checkErr
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return errors.New("invalid pending cleanup marker")
		}
	} else if err != nil {
		return err
	} else if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	return errors.Join(syncRunDirectory(directory), syncRunDirectory(data))
}

func (s *Server) pendingWorkCleanupIDs() ([]string, error) {
	data, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer data.Close()
	directory, err := openResultDirectory(data, pendingWorkCleanupDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	entries, err := localRunEntries(directory)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		info, err := directory.Lstat(entry.Name())
		if err != nil {
			return nil, err
		}
		if !validResultID(entry.Name()) || !info.Mode().IsRegular() || info.Size() != 0 {
			return nil, errors.New("invalid pending cleanup marker")
		}
		ids = append(ids, entry.Name())
	}
	slices.Sort(ids)
	return ids, nil
}

func (s *Server) clearPendingWorkCleanup(ids []string) error {
	data, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer data.Close()
	directory, err := openResultDirectory(data, pendingWorkCleanupDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer directory.Close()
	for _, id := range ids {
		if !validResultID(id) {
			return errors.New("invalid completed cleanup identity")
		}
		if err := directory.Remove(id); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return syncRunDirectory(directory)
}

func (s *Server) persistQueueCleanupLocked() error {
	for _, id := range s.queueCleanup {
		if err := s.writePendingWorkCleanup(id); err != nil {
			return fmt.Errorf("persist Peer cleanup for %s: %w", id, err)
		}
	}
	return nil
}

// Delete holds cancelMu and persistMu. Guarantee pending cleanup survives the
// removal of a retained manifest; complete historical ownership alone is not a
// reason to contact old Agents again. No Controller lock is acquired recursively.
func (s *Server) retainWorkCleanupBeforeDelete(id string) error {
	pending := slices.Contains(s.queueCleanup, id)
	s.state.mu.RLock()
	if run, exists := s.state.experiments[id]; exists {
		pending = pending || needsRecoveredCleanup(run)
	}
	for _, node := range s.state.nodes {
		if node.RunID == id && node.State != model.NodeStopped && !(node.State == model.NodeFailed && node.Metadata["cleanupComplete"] == "true") {
			pending = true
			break
		}
	}
	s.state.mu.RUnlock()
	if !pending {
		root, err := s.analysisDirectory(id)
		if err == nil {
			data, readErr := readResumeFile(root, "experiment.json", resultMetadataLimit)
			root.Close()
			var run model.Experiment
			if readErr == nil && json.Unmarshal(data, &run) == nil && run.ID == id {
				pending = needsRecoveredCleanup(run)
			}
		}
	}
	if !pending {
		return nil
	}
	if err := s.writePendingWorkCleanup(id); err != nil {
		return err
	}
	s.rememberQueueCleanupLocked(id)
	return nil
}
