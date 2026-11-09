package controller

import (
	"errors"
	"os"
	"path/filepath"
)

const closedRunCacheEntries = 256

// Only a positive lookup is memoized. Eviction changes no ownership decision:
// the immutable local retirement marker is checked again on every cache miss.
func (s *state) cacheRetiredRunLocked(id string) {
	if s.closedRuns == nil {
		s.closedRuns = make(map[string]bool)
	}
	if s.closedRuns[id] {
		return
	}
	for len(s.closedRunOrder) >= closedRunCacheEntries {
		delete(s.closedRuns, s.closedRunOrder[0])
		s.closedRunOrder[0] = ""
		s.closedRunOrder = s.closedRunOrder[1:]
	}
	s.closedRuns[id] = true
	s.closedRunOrder = append(s.closedRunOrder, id)
}

func (s *state) lookupRetiredRun(id string) (bool, error) {
	if !validResultID(id) {
		return false, nil
	}
	s.mu.RLock()
	closed := s.closedRuns[id]
	lookup := s.retiredLookup
	s.mu.RUnlock()
	if closed || lookup == nil {
		return closed, nil
	}
	closed, err := lookup(id)
	if closed && err == nil {
		s.mu.Lock()
		s.cacheRetiredRunLocked(id)
		s.mu.Unlock()
	}
	return closed, err
}

func (s *Server) isRuntimeRetired(id string) (bool, error) {
	if !validResultID(id) {
		return false, nil
	}
	info, err := os.Lstat(filepath.Join(s.config.DataDir, retiredRuntimeDirectory, id+".done"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != 0 {
		return false, errors.New("invalid runtime retirement fence")
	}
	return true, nil
}

func writeRetiredRuntimeFence(directory *os.Root, id string) error {
	file, err := directory.OpenFile(id+".done", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		info, e := directory.Lstat(id + ".done")
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() || info.Size() != 0 {
			return errors.New("invalid runtime retirement fence")
		}
		return nil
	}
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close(), syncRunDirectory(directory))
}
