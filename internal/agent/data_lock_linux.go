//go:build linux

package agent

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// Keep the inode in place after release: unlinking a lock file would let a new
// opener lock a different inode while another Agent still owns this directory.
func lockAgentDataDirectory(directory string) (*os.File, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(directory, "agent.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.New("data directory is already in use by another Agent")
		}
		return nil, err
	}
	return file, nil
}
