package controller

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
)

const archiveCopyTemporaryPrefix = ".incoming-"

func isArchiveCopyTemporary(name string) bool {
	if !strings.HasPrefix(name, archiveCopyTemporaryPrefix) {
		return false
	}
	secret := strings.TrimPrefix(name, archiveCopyTemporaryPrefix)
	// crypto/rand.Text uses the RFC 4648 base32 alphabet and at least 128
	// random bits (26 characters). Do not treat arbitrary user dot files as
	// copy temporaries, even if their names start with the same prefix.
	if len(secret) < 26 {
		return false
	}
	for _, ch := range secret {
		if !(ch >= 'A' && ch <= 'Z' || ch >= '2' && ch <= '7') {
			return false
		}
	}
	return true
}

// The single NAS worker calls this before creating any copy temporary in the
// held Run directory. Published manifests cannot reference dot files, and
// readers never open these reserved temporary names. No Controller lock is held.
func (s *Server) cleanAbandonedArchiveCopies(ctx context.Context, root *os.Root) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	removed := false
	for {
		entries, readErr := dir.ReadDir(100)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !isArchiveCopyTemporary(entry.Name()) {
				continue
			}
			info, err := root.Lstat(entry.Name())
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			if err := root.Remove(entry.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			removed = true
			s.archiveProgress()
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if removed {
		return syncRunDirectory(root)
	}
	return nil
}
