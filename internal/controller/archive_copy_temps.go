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

func isArchiveManifestTemporary(name string) bool {
	const prefix = ".analysis-"
	const suffix = ".tmp"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	secret := name[len(prefix) : len(name)-len(suffix)]
	// stageAnalysisJSON uses a 16-byte random nonce encoded as lowercase hex.
	// Match that reserved format exactly instead of removing user dot files.
	if len(secret) != 32 {
		return false
	}
	for _, ch := range secret {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

// The single NAS worker calls this before creating copy or manifest-publication
// temporaries in the held Run directory. Published manifests cannot reference
// dot files, and readers never open these reserved names. No Controller lock is held.
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
			if !isArchiveCopyTemporary(entry.Name()) && !isArchiveManifestTemporary(entry.Name()) {
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
