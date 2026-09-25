package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func (s *Server) openArchiveStore() (*os.Root, error) {
	if s.archiveIOCheck != nil {
		s.archiveIOCheck()
	}
	// Remember the destination identity locally. If a NAS mount disappears, do
	// not silently archive onto the empty directory underneath that mount.
	expectedPath := filepath.Join(s.config.DataDir, ".archive-store-id")
	expected, err := os.ReadFile(expectedPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	if len(expected) == 0 {
		if err := data.Mkdir(archivedRunsDirectory, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	root, err := openResultDirectory(data, archivedRunsDirectory)
	if err != nil {
		return nil, err
	}
	actual, readErr := readArchiveIdentity(root)
	if len(expected) > 0 {
		if readErr != nil || !bytesEqual(expected, actual) {
			root.Close()
			return nil, errors.New("archive storage identity is missing or changed; check the runs mount")
		}
	} else {
		if errors.Is(readErr, os.ErrNotExist) {
			actual = []byte(rand.Text())
			readErr = root.WriteFile(".kpl-store-id", actual, 0600)
		}
		if readErr != nil {
			root.Close()
			return nil, readErr
		}
		if len(actual) == 0 || len(actual) > 128 {
			root.Close()
			return nil, errors.New("invalid archive storage identity")
		}
		if err := writeFileAtomic(expectedPath, actual, 0600); err != nil {
			root.Close()
			return nil, err
		}
	}
	return root, nil
}
func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func (s *Server) importArchivedRuns(ctx context.Context) error {
	archive, err := s.openArchiveStore()
	if err != nil {
		return err
	}
	defer archive.Close()
	entries, err := localRunEntries(archive)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() || !validResultID(entry.Name()) {
			continue
		}
		id := entry.Name()
		// Once indexed, the Controller's local metadata is authoritative. No NAS
		// stat/read is performed per result list refresh, note edit, or phase.
		s.state.persistMu.Lock()
		deleted, checkErr := s.state.resultDeletedLocked(id)
		indexed := false
		if current, openErr := s.analysisDirectory(id); openErr == nil {
			_, statErr := current.Lstat(runArchiveManifest)
			_, importErr := current.Lstat(runArchiveImportFile)
			indexed = statErr == nil && errors.Is(importErr, os.ErrNotExist)
			current.Close()
		}
		s.state.persistMu.Unlock()
		if checkErr != nil {
			return checkErr
		}
		if deleted || indexed {
			continue
		}
		remote, err := openResultDirectory(archive, id)
		if err != nil {
			continue
		}
		manifest, err := readRunArchive(remote, id)
		if err != nil {
			remote.Close()
			s.logger.Warn("read archive index", "run", id, "error", err)
			continue
		}
		_, manifestErr := remote.Lstat(runArchiveManifest)
		legacy := errors.Is(manifestErr, os.ErrNotExist)
		small := map[string][]byte{}
		for _, name := range []string{"experiment.json", "scenario.yaml", resultNoteFile, analysisJobFile} {
			file, readErr := openResultFile(remote, name)
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			if readErr != nil {
				err = readErr
				break
			}
			if file.size > resultMetadataLimit {
				file.close()
				err = fmt.Errorf("archive %s metadata is too large", name)
				break
			}
			small[name], readErr = io.ReadAll(file.reader())
			file.close()
			if readErr != nil {
				err = readErr
				break
			}
		}
		if err == nil && legacy {
			for _, name := range append(append([]string{}, resultSourceFiles[:]...), analysisJobFile, analysisResultFile, analysisSummaryFile) {
				file, readErr := openResultFile(remote, name)
				if errors.Is(readErr, os.ErrNotExist) {
					continue
				}
				if readErr != nil {
					err = readErr
					break
				}
				stored := storedRunFile{Object: name, Size: file.size}
				file.close()
				if isRunLog(name) {
					manifest.Logs[name] = []storedRunFile{stored}
				} else {
					manifest.Files[name] = stored
				}
			}
		}
		remote.Close()
		if err != nil {
			s.logger.Warn("index saved archive", "run", id, "error", err)
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// No untrusted archive may replace an already present local record.
		err = func() error {
			s.state.persistMu.Lock()
			defer s.state.persistMu.Unlock()
			if deleted, err := s.state.resultDeletedLocked(id); err != nil || deleted {
				return err
			}
			parentPath := filepath.Join(s.config.DataDir, currentRunsDirectory)
			if err := os.MkdirAll(parentPath, 0755); err != nil {
				return err
			}
			parent, err := s.openResultRuns()
			if err != nil {
				return err
			}
			defer parent.Close()
			if err := parent.Mkdir(id, 0755); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			local, err := openResultDirectory(parent, id)
			if err != nil {
				return err
			}
			defer local.Close()
			_, existingMetadata := local.Lstat("experiment.json")
			_, importing := local.Lstat(runArchiveImportFile)
			if importing != nil && !errors.Is(importing, os.ErrNotExist) {
				return importing
			}
			if legacy && existingMetadata == nil && errors.Is(importing, os.ErrNotExist) {
				return nil
			}
			// Persist intent before creating metadata so a crash halfway through
			// legacy discovery can resume without mistaking it for a local run.
			if err := writeImportedMetadata(local, runArchiveImportFile, []byte("pending\n")); err != nil {
				return err
			}
			if err := syncRunDirectory(local); err != nil {
				return err
			}
			for name, data := range small {
				if _, err := local.Lstat(name); err == nil {
					continue
				} else if !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if err := writeImportedMetadata(local, name, data); err != nil {
					return err
				}
			}
			for name, data := range small {
				file, err := openResultFile(local, name)
				if err != nil {
					return err
				}
				if file.size <= resultMetadataLimit {
					current, readErr := io.ReadAll(file.reader())
					if readErr != nil {
						file.close()
						return readErr
					}
					if bytesEqual(current, data) {
						stored := manifest.Files[name]
						stored.Object = name
						stored.Size = file.size
						stored.ModifiedAt = file.info.ModTime().UnixNano()
						digest := sha256.Sum256(current)
						stored.SHA256 = hex.EncodeToString(digest[:])
						manifest.Files[name] = stored
					}
				}
				file.close()
			}
			if err := writeRunArchive(local, manifest); err != nil {
				return err
			}
			if err := syncRunDirectory(local); err != nil {
				return err
			}
			if err := local.Remove(runArchiveImportFile); err != nil {
				return err
			}
			s.state.markRunArchiveDirty(id)
			return writeAnalysisJSON(local, runArchiveStatusFile, runArchiveStatus{State: "archived", UpdatedAt: time.Now().UTC()})
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) cleanDeletedArchives(ctx context.Context) error {
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	markers, err := openResultDirectory(data, ".deleted-results")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer markers.Close()
	entries, err := localRunEntries(markers)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	if err := data.Mkdir(".archived-deletions", 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	done, err := openResultDirectory(data, ".archived-deletions")
	if err != nil {
		return err
	}
	defer done.Close()
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := entry.Name()
		if !validResultID(id) || !entry.Type().IsRegular() {
			continue
		}
		if _, err := done.Lstat(id); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		archive, err := s.openArchiveStore()
		if err != nil {
			return err
		}
		err = removeResultDirectory(archive, id)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err == nil {
			stageErr := removeResultDirectory(archive, ".incoming-"+id)
			if stageErr != nil && !errors.Is(stageErr, os.ErrNotExist) {
				err = stageErr
			}
		}
		archive.Close()
		if err != nil {
			return err
		}
		if err := done.WriteFile(id, []byte("removed\n"), 0600); err != nil {
			return err
		}
	}
	return nil
}

func readArchiveIdentity(root *os.Root) ([]byte, error) {
	file, err := openResultFile(root, ".kpl-store-id")
	if err != nil {
		return nil, err
	}
	defer file.close()
	if file.size == 0 || file.size > 128 {
		return nil, errors.New("invalid archive storage identity")
	}
	return io.ReadAll(file.reader())
}

// Publish whole metadata files; interrupted imports never leave truncated JSON
// or YAML that a later import would treat as an existing authoritative record.
func writeImportedMetadata(root *os.Root, name string, data []byte) error {
	temp := ".kpl-metadata-" + rand.Text()
	file, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer root.Remove(temp)
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	return root.Rename(temp, name)
}
