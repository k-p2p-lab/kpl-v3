package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// There is exactly one background NAS worker. A blocked mount consumes this
// worker, not a new goroutine per tick or any Controller control/persistence lock.
func (s *Server) runArchiveLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.archiveStatusMu.Lock()
		s.archiveChecking = true
		s.archiveCheckStartedAt = time.Now().UTC()
		s.archiveStatusMu.Unlock()
		err := s.archiveMaintenance(ctx)
		s.archiveStatusMu.Lock()
		s.archiveChecking = false
		s.archiveCheckedAt = time.Now().UTC()
		s.archiveError = ""
		if err != nil {
			s.archiveError = err.Error()
		}
		s.archiveStatusMu.Unlock()
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("result archive unavailable", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (s *Server) archiveIdle() bool {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	for _, run := range s.state.experiments {
		if run.State == "running" {
			return false
		}
	}
	return true
}
func (s *Server) waitArchiveIdle(ctx context.Context) error {
	for !s.archiveIdle() {
		if err := sleepContext(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (s *Server) archiveMaintenance(ctx context.Context) error {
	if !s.archiveQueueLoaded {
		root, err := s.openResultRuns()
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if root != nil {
			entries, err := localRunEntries(root)
			root.Close()
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.IsDir() && validResultID(entry.Name()) {
					s.state.markRunArchiveDirty(entry.Name())
				}
			}
		}
		s.archiveQueueLoaded = true
	}
	// Avoid rescanning a large NAS directory on every local status poll or run.
	if s.archiveLastScan.IsZero() || time.Since(s.archiveLastScan) >= time.Minute {
		if err := s.importArchivedRuns(ctx); err != nil {
			return err
		}
		s.archiveLastScan = time.Now()
	}
	if err := s.cleanDeletedArchives(ctx); err != nil {
		return err
	}
	if !s.archiveIdle() {
		return nil
	}
	s.state.archiveQueueMu.Lock()
	pending := map[string]uint64{}
	for id := range s.state.archivePending {
		pending[id] = s.state.archiveVersions[id]
	}
	s.state.archiveQueueMu.Unlock()
	var firstError error
	for id, version := range pending {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := s.archiveRun(ctx, id, archiveQuietPeriod)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil || errors.Is(err, errResultNotFound) {
			s.state.archiveQueueMu.Lock()
			if s.state.archiveVersions[id] == version {
				delete(s.state.archivePending, id)
			}
			s.state.archiveQueueMu.Unlock()
		} else if !errors.Is(err, errResultBusy) {
			s.setRunArchiveStatus(id, "pending", err)
			s.logger.Warn("archive result deferred", "run", id, "error", err)
			if firstError == nil {
				firstError = err
			}
		}
	}
	return firstError
}

func (s *state) markRunArchiveDirty(id string) {
	s.archiveQueueMu.Lock()
	defer s.archiveQueueMu.Unlock()
	if s.archiveVersions == nil {
		s.archiveVersions = map[string]uint64{}
		s.archivePending = map[string]bool{}
	}
	s.archiveVersions[id]++
	s.archivePending[id] = true
}

func hashRunFile(file resultFile) (string, error) {
	hash := sha256.New()
	if _, err := io.Copy(hash, file.reader()); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func (s *Server) copyArchiveFile(ctx context.Context, root *os.Root, name string, source resultFile) (storedRunFile, error) {
	stored := storedRunFile{Object: name, Size: source.size, ModifiedAt: source.info.ModTime().UnixNano()}
	temp := ".incoming-" + rand.Text()
	output, err := root.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return stored, err
	}
	defer root.Remove(temp)
	hash := sha256.New()
	reader := source.reader()
	buffer := make([]byte, 64<<10)
	for {
		if err = s.waitArchiveIdle(ctx); err != nil {
			break
		}
		started := time.Now()
		var n int
		n, err = reader.Read(buffer)
		if n > 0 {
			if _, writeErr := output.Write(buffer[:n]); writeErr != nil {
				err = writeErr
				break
			}
			_, _ = hash.Write(buffer[:n])
		}
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}
		// Bound background traffic even while idle (8 MiB/s). A new run pauses the
		// next chunk instead of waiting for a potentially stalled NAS syscall.
		if err = sleepContext(ctx, time.Duration(n)*time.Second/(8<<20)-time.Since(started)); err != nil {
			break
		}
	}
	if err == nil {
		err = output.Sync()
	}
	err = errors.Join(err, output.Close())
	if err != nil {
		return stored, err
	}
	stored.SHA256 = hex.EncodeToString(hash.Sum(nil))
	// Verify the destination before publishing and before freeing local bytes.
	verify, err := openResultFile(root, temp)
	if err != nil {
		return stored, err
	}
	digest, err := s.hashArchiveFile(ctx, verify)
	verify.close()
	if err != nil {
		return stored, err
	}
	if digest != stored.SHA256 {
		return stored, errors.New("archive checksum mismatch")
	}
	if err := ctx.Err(); err != nil {
		return stored, err
	}
	if err = root.Rename(temp, name); err != nil {
		return stored, err
	}
	return stored, nil
}

func (s *Server) archiveRun(ctx context.Context, id string, quiet time.Duration) (resultErr error) {
	var files []resultFile
	var manifest runArchiveManifestData
	defer func() {
		for _, file := range files {
			file.close()
		}
	}()
	err := func() error {
		s.analysisJobMu.Lock()
		defer s.analysisJobMu.Unlock()
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		if s.resultActive(id) {
			return errResultBusy
		}
		if job := s.analysisJobs[id]; job != nil && (job.status.State == "queued" || job.status.State == "running") {
			return errResultBusy
		}
		s.state.mu.RLock()
		live := false
		for node := range s.state.activeNodesLocked() {
			if node.RunID == id {
				live = true
				break
			}
		}
		s.state.mu.RUnlock()
		if live {
			return errResultBusy
		}
		root, err := s.analysisDirectory(id)
		if err != nil {
			return err
		}
		defer root.Close()
		if _, err := root.Lstat(runArchiveImportFile); err == nil {
			return errResultBusy
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Interrupted runs are recoverable, but a newly arriving tail must settle
		// before rotation. Completed runs retaining Peers are deliberately kept local.
		metadata, err := openResultFile(root, "experiment.json")
		if err != nil {
			return err
		}
		result, err := readResultMetadata(metadata, id, false)
		metadata.close()
		if err != nil {
			return err
		}
		if err := s.applyBatchExtension(&result); err != nil {
			return err
		}
		manifest, err = readRunArchive(root, id)
		if err != nil {
			return err
		}
		for _, name := range []string{"events.jsonl", "observations.jsonl"} {
			info, err := root.Lstat(name)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("unsafe local log")
			}
			if time.Since(info.ModTime()) < quiet {
				return errResultBusy
			}
			segment := strings.TrimSuffix(name, ".jsonl") + "-segment-" + time.Now().UTC().Format("20060102T150405.000000000") + "-" + rand.Text() + ".jsonl"
			if err := root.Rename(name, segment); err != nil {
				return err
			}
		}
		entries, err := localRunEntries(root)
		if err != nil {
			return err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, entry := range entries {
			name := entry.Name()
			if (strings.HasPrefix(name, ".analysis-") && strings.HasSuffix(name, ".tmp") || strings.HasPrefix(name, ".kpl-metadata-")) && entry.Type().IsRegular() {
				if err := root.Remove(name); err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(name, ".") {
				continue
			}
			file, err := openResultFile(root, name)
			if err != nil {
				return err
			}
			files = append(files, file)
		}
		return nil
	}()
	if err != nil {
		return err
	}
	// Hashing, NAS opening, copying, verification and publication all run outside
	// Controller locks. Logs are sealed segments and metadata uses pinned inodes.
	type upload struct {
		file    resultFile
		logical string
		segment bool
	}
	uploads := []upload{}
	cleanup := []resultFile{}
	for _, file := range files {
		logical := ""
		for _, name := range []string{"events.jsonl", "observations.jsonl"} {
			if logSegment(file.name, name) {
				logical = name
				break
			}
		}
		if logical != "" {
			known := false
			for _, stored := range manifest.Logs[logical] {
				if stored.Segment == file.name {
					if err := verifyArchivedSegment(file, stored); err != nil {
						return err
					}
					known = true
					break
				}
			}
			if known {
				cleanup = append(cleanup, file)
			} else {
				uploads = append(uploads, upload{file: file, logical: logical, segment: true})
			}
		} else {
			digest, err := hashRunFile(file)
			if err != nil {
				return err
			}
			if previous, ok := manifest.Files[file.name]; ok && previous.SHA256 == digest {
				if !retainedRunFile(file.name) {
					cleanup = append(cleanup, file)
				}
			} else {
				uploads = append(uploads, upload{file: file, logical: file.name})
			}
		}
	}
	if len(uploads) == 0 && len(cleanup) == 0 {
		return nil
	}
	s.setRunArchiveStatus(id, "archiving", nil)
	if err := s.waitArchiveIdle(ctx); err != nil {
		return err
	}
	parent, err := s.openArchiveStore()
	if err != nil {
		return err
	}
	defer parent.Close()
	destination := id
	_, err = parent.Lstat(id)
	fresh := errors.Is(err, os.ErrNotExist)
	if err != nil && !fresh {
		return err
	}
	if fresh {
		destination = ".incoming-" + id
		if err := parent.Mkdir(destination, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	remote, err := openResultDirectory(parent, destination)
	if err != nil {
		return err
	}
	defer remote.Close()
	published, err := readRunArchive(remote, id)
	if err != nil {
		return err
	}
	for name, parts := range published.Logs {
		known := map[string]bool{}
		for _, part := range manifest.Logs[name] {
			known[part.Object] = true
		}
		for _, part := range parts {
			if !known[part.Object] {
				manifest.Logs[name] = append(manifest.Logs[name], part)
			}
		}
	}
	for name, stored := range published.Files {
		if _, ok := manifest.Files[name]; !ok {
			manifest.Files[name] = stored
		}
	}

	for _, upload := range uploads {
		if upload.segment {
			already := false
			for _, stored := range manifest.Logs[upload.logical] {
				if stored.Segment == upload.file.name {
					if err := verifyArchivedSegment(upload.file, stored); err != nil {
						return err
					}
					already = true
					break
				}
			}
			if already {
				cleanup = append(cleanup, upload.file)
				continue
			}
		}
		object := upload.logical
		if upload.segment {
			object = upload.file.name
			// Keep the conventional first log filename in the NAS archive.
			if len(manifest.Logs[upload.logical]) == 0 {
				object = upload.logical
			}
		} else if !retainedRunFile(upload.logical) {
			object = strings.TrimSuffix(upload.logical, ".json") + "-" + rand.Text() + ".json"
		}
		stored, err := s.copyArchiveFile(ctx, remote, object, upload.file)
		if err != nil {
			return err
		}
		if upload.segment {
			stored.Segment = upload.file.name
			manifest.Logs[upload.logical] = append(manifest.Logs[upload.logical], stored)
			cleanup = append(cleanup, upload.file)
		} else {
			manifest.Files[upload.logical] = stored
			if !retainedRunFile(upload.logical) {
				cleanup = append(cleanup, upload.file)
			}
		}
	}
	if err := writeRunArchive(remote, manifest); err != nil {
		return err
	}
	if err := syncRunDirectory(remote); err != nil {
		return err
	}
	if fresh {
		if err := parent.Rename(destination, id); err != nil {
			return err
		}
		if err := syncRunDirectory(parent); err != nil {
			return err
		}
	}
	// Persist the authoritative object map locally BEFORE unlinking any copied
	// bytes. If interrupted, local segments named by that map are deduplicated.
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	local, err := s.analysisDirectory(id)
	if err != nil {
		return err
	}
	defer local.Close()
	if err := writeRunArchive(local, manifest); err != nil {
		return err
	}
	if err := syncRunDirectory(local); err != nil {
		return err
	}
	for _, file := range cleanup {
		info, err := local.Lstat(file.name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if os.SameFile(info, file.info) && info.Size() == file.size && info.ModTime().Equal(file.info.ModTime()) {
			if err := local.Remove(file.name); err != nil {
				return err
			}
		}
	}
	return writeAnalysisJSON(local, runArchiveStatusFile, runArchiveStatus{State: "archived", UpdatedAt: time.Now().UTC()})
}
func syncRunDirectory(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (s *Server) hashArchiveFile(ctx context.Context, file resultFile) (string, error) {
	digest := sha256.New()
	reader := file.reader()
	buffer := make([]byte, 64<<10)
	for {
		if err := s.waitArchiveIdle(ctx); err != nil {
			return "", err
		}
		started := time.Now()
		n, err := reader.Read(buffer)
		if n > 0 {
			_, _ = digest.Write(buffer[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if err := sleepContext(ctx, time.Duration(n)*time.Second/(8<<20)-time.Since(started)); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func verifyArchivedSegment(file resultFile, stored storedRunFile) error {
	if file.size != stored.Size || stored.SHA256 == "" {
		return errors.New("local log differs from committed archive segment")
	}
	digest, err := hashRunFile(file)
	if err != nil {
		return err
	}
	if digest != stored.SHA256 {
		return errors.New("local log differs from committed archive segment")
	}
	return nil
}
