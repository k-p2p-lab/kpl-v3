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
	// Remember the destination identity locally. If a NAS mount disappears, do
	// not silently archive onto the empty directory underneath that mount.
	expectedPath := filepath.Join(s.config.DataDir, ".archive-store-id")
	expected, err := os.ReadFile(expectedPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, &archiveAccessError{kind: "local", err: fmt.Errorf("read local archive identity: %w", err)}
	}
	if len(expected) == 0 {
		// Readers can arrive before the background worker has initialized the
		// store. Serialize only initialization, never routine NAS reads.
		s.archiveIdentityInitMu.Lock()
		defer s.archiveIdentityInitMu.Unlock()
		expected, err = os.ReadFile(expectedPath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, &archiveAccessError{kind: "local", err: fmt.Errorf("read local archive identity: %w", err)}
		}
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, &archiveAccessError{kind: "local", err: err}
	}
	defer data.Close()
	if len(expected) == 0 {
		s.archiveStatusMu.RLock()
		reconnectState := s.archiveReconnectState
		s.archiveStatusMu.RUnlock()
		if reconnectState == "queued" || reconnectState == "running" || reconnectState == "failed" {
			return nil, &archiveAccessError{kind: "unavailable", err: errors.New("local archive identity is missing; complete archive reconnection before automatic initialization")}
		}
		referenced, err := localArchiveReferencesPresent(data)
		if err != nil {
			return nil, &archiveAccessError{kind: "local", err: fmt.Errorf("inspect local archive references: %w", err)}
		}
		if referenced {
			return nil, &archiveAccessError{kind: "unavailable", err: errors.New("local archive identity is missing but archived results remain; verify the runs mount before reconnecting")}
		}
	}
	if s.archiveIOCheck != nil {
		s.archiveIOCheck()
	}
	if len(expected) == 0 {
		if err := data.Mkdir(archivedRunsDirectory, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, &archiveAccessError{kind: "unavailable", err: fmt.Errorf("prepare archive directory: %w", err)}
		}
	}
	root, err := openResultDirectory(data, archivedRunsDirectory)
	if err != nil {
		return nil, &archiveAccessError{kind: "unavailable", err: fmt.Errorf("open archive directory: %w", err)}
	}
	actual, readErr := readArchiveIdentity(root)
	if len(expected) > 0 {
		if readErr != nil {
			root.Close()
			if errors.Is(readErr, os.ErrNotExist) {
				return nil, &archiveAccessError{kind: "unavailable", err: errors.New("archive storage identity is missing; check the runs mount before reconnecting")}
			}
			return nil, &archiveAccessError{kind: "unavailable", err: fmt.Errorf("read archive storage identity: %w", readErr)}
		}
		if !bytesEqual(expected, actual) {
			root.Close()
			return nil, &archiveAccessError{kind: "unavailable", err: errors.New("archive storage identity changed; verify the runs mount before reconnecting")}
		}
	} else {
		if errors.Is(readErr, os.ErrNotExist) {
			actual, readErr = createArchiveIdentity(root)
		}
		if readErr != nil {
			root.Close()
			return nil, &archiveAccessError{kind: "unavailable", err: fmt.Errorf("initialize archive storage identity: %w", readErr)}
		}
		if len(actual) == 0 || len(actual) > 128 {
			root.Close()
			return nil, &archiveAccessError{kind: "unavailable", err: errors.New("invalid archive storage identity")}
		}
		if err := writeImportedMetadata(data, ".archive-store-id", actual); err != nil {
			root.Close()
			return nil, &archiveAccessError{kind: "local", err: fmt.Errorf("save local archive identity: %w", err)}
		}
		if err := syncRunDirectory(data); err != nil {
			root.Close()
			return nil, &archiveAccessError{kind: "local", err: fmt.Errorf("synchronize local archive identity: %w", err)}
		}
	}
	return root, nil
}

// A lost local identity is not a first-time store when local manifests still
// refer to remote bytes. This local-only scan runs before any NAS operation and
// never holds experiment or persistence locks. Empty, partially imported indexes
// remain eligible for the original first-time archive discovery path.
func localArchiveReferencesPresent(data *os.Root) (bool, error) {
	parent, err := openResultDirectory(data, currentRunsDirectory)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer parent.Close()
	entries, err := localRunEntries(parent)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !validResultID(entry.Name()) {
			continue
		}
		local, err := openResultDirectory(parent, entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue // A local-only result may have been deleted after the scan.
		}
		if err != nil {
			return false, err
		}
		manifest, err := readRunArchive(local, entry.Name())
		local.Close()
		if err != nil {
			return false, fmt.Errorf("read archive references for %s: %w", entry.Name(), err)
		}
		if len(manifest.Files) > 0 {
			return true, nil
		}
		for _, segments := range manifest.Logs {
			if len(segments) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func createArchiveIdentity(root *os.Root) ([]byte, error) {
	identity := []byte(rand.Text())
	file, err := root.OpenFile(".kpl-store-id", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return readArchiveIdentity(root)
	}
	if err != nil {
		return nil, err
	}
	if _, err = file.Write(identity); err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		_ = root.Remove(".kpl-store-id")
		return nil, err
	}
	if err := syncRunDirectory(root); err != nil {
		return nil, err
	}
	return identity, nil
}
func bytesEqual(a, b []byte) bool { return string(a) == string(b) }

func (s *Server) importArchivedRuns(ctx context.Context) error {
	s.setArchivePhase("discovering")
	archive, err := s.openArchiveStore()
	if err != nil {
		return err
	}
	var firstError error
	recordError := func(id string, err error) {
		s.logger.Warn("index saved archive deferred", "run", id, "error", err)
		if firstError == nil {
			firstError = fmt.Errorf("index archive %s: %w", id, err)
		}
	}
	defer archive.Close()
	s.archiveProgress()
	entries, err := localRunEntries(archive)
	if err != nil {
		return &archiveAccessError{kind: "unavailable", err: fmt.Errorf("list archive directory: %w", err)}
	}
	for _, entry := range entries {
		s.archiveProgress()
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
			recordError(id, checkErr)
			continue
		}
		if deleted || indexed {
			continue
		}
		remote, err := openResultDirectory(archive, id)
		if err != nil {
			recordError(id, err)
			continue
		}
		s.archiveProgress()
		manifest, err := readRunArchive(remote, id)
		if err != nil {
			remote.Close()
			recordError(id, err)
			continue
		}
		s.archiveProgress()
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
			s.archiveProgress()
			small[name], readErr = io.ReadAll(file.reader())
			file.close()
			if readErr != nil {
				err = readErr
				break
			}
			s.archiveProgress()
		}
		if err == nil && legacy {
			for _, name := range append(append([]string{}, resultSourceFiles[:]...), analysisJobFile, analysisResultFile, analysisSummaryFile, analysisArtifactFile(analysisResultFile, 1), analysisArtifactFile(analysisSummaryFile, 1)) {
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
				s.archiveProgress()
				if isRunLog(name) {
					manifest.Logs[name] = []storedRunFile{stored}
				} else {
					manifest.Files[name] = stored
				}
			}
		}
		remote.Close()
		if err != nil {
			recordError(id, err)
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
			return s.writeRunArchiveStatus(local, id, runArchiveStatus{State: "archived", UpdatedAt: time.Now().UTC()})
		}()
		if err != nil {
			recordError(id, err)
		}
	}
	return firstError
}

func (s *Server) cleanDeletedArchives(ctx context.Context) error {
	s.setArchivePhase("preparing")
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
	var firstError error
	recordError := func(id string, err error) {
		s.logger.Warn("delete saved archive deferred", "run", id, "error", err)
		if firstError == nil {
			firstError = fmt.Errorf("delete archive %s: %w", id, err)
		}
	}
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
			recordError(id, err)
			continue
		}
		s.setArchivePhase("deleting")
		archive, err := s.openArchiveStore()
		if err != nil {
			return err
		}
		s.archiveProgress()
		err = removeResultDirectory(archive, id)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err == nil {
			s.archiveProgress()
			stageErr := removeResultDirectory(archive, ".incoming-"+id)
			if stageErr != nil && !errors.Is(stageErr, os.ErrNotExist) {
				err = stageErr
			}
		}
		archive.Close()
		if err != nil {
			recordError(id, err)
			continue
		}
		s.archiveProgress()
		s.setArchivePhase("preparing")
		if err := done.WriteFile(id, []byte("removed\n"), 0600); err != nil {
			recordError(id, err)
		}
	}
	return firstError
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
