package controller

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// Reconnection is explicitly requested by an operator. Admission only updates
// local state; the existing single archive worker performs all NAS operations.
func (s *Server) handleArchiveReconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request struct {
		ConfirmCurrentArchive bool `json:"confirmCurrentArchive"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || !request.ConfirmCurrentArchive {
		writeError(w, http.StatusBadRequest, "confirmCurrentArchive must be true")
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid archive reconnect request")
		return
	}
	s.cancelMu.Lock()
	if s.shuttingDown {
		s.cancelMu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "controller is shutting down")
		return
	}
	s.archiveStatusMu.Lock()
	if s.archiveReconnectState != "queued" && s.archiveReconnectState != "running" {
		s.archiveReconnectState = "queued"
		s.archiveReconnectError = ""
	}
	state := s.archiveReconnectState
	s.archiveStatusMu.Unlock()
	s.cancelMu.Unlock()
	writeJSON(w, http.StatusAccepted, map[string]string{"state": state})
}

// Called only by runArchiveLoop, before ordinary imports, transfers or deletion.
// A failed request is never retried automatically against a different mount.
func (s *Server) processArchiveReconnect(ctx context.Context) error {
	s.archiveStatusMu.Lock()
	if s.archiveReconnectState != "queued" {
		s.archiveStatusMu.Unlock()
		return nil
	}
	s.archiveReconnectState = "running"
	s.archiveReconnectError = ""
	s.archiveStatusMu.Unlock()
	err := s.reconnectArchiveStore(ctx)
	s.archiveStatusMu.Lock()
	s.archiveReconnectState = "completed"
	if err != nil {
		s.archiveReconnectState = "failed"
		s.archiveReconnectError = err.Error()
	}
	s.archiveStatusMu.Unlock()
	if err == nil {
		s.archiveLastScan = time.Time{}
	}
	return err
}

type archiveReconnectRun struct {
	id       string
	manifest runArchiveManifestData
}

// Pin local manifests before accessing the archive. Result deletion checks
// these same pins; no Controller lock remains held while remote bytes are read.
func (s *Server) captureArchiveReconnectRuns(ctx context.Context) (runs []archiveReconnectRun, release func(), resultErr error) {
	var pinned []string
	release = func() {
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		for _, id := range pinned {
			s.resultReadPins[id]--
			if s.resultReadPins[id] == 0 {
				delete(s.resultReadPins, id)
			}
		}
	}
	defer func() {
		if resultErr != nil {
			release()
		}
	}()
	parent, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return nil, release, nil
	}
	if err != nil {
		return nil, release, err
	}
	defer parent.Close()
	entries, err := localRunEntries(parent)
	if err != nil {
		return nil, release, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, release, err
		}
		id := entry.Name()
		if !validResultID(id) {
			continue
		}
		err := func() error {
			s.state.persistMu.Lock()
			defer s.state.persistMu.Unlock()
			deleted, err := s.state.resultDeletedLocked(id)
			if err != nil || deleted {
				return err
			}
			local, err := openResultDirectory(parent, id)
			if errors.Is(err, os.ErrNotExist) {
				return nil // Concurrent deletion finished before the pin.
			}
			if err != nil {
				return err
			}
			defer local.Close()
			if _, err := local.Lstat(runArchiveImportFile); !errors.Is(err, os.ErrNotExist) {
				if err != nil {
					return err
				}
				return errors.New("unfinished archive import; restore the original archive before reconnecting")
			}
			if _, err := local.Lstat(runArchiveManifest); errors.Is(err, os.ErrNotExist) {
				return nil
			} else if err != nil {
				return err
			}
			manifest, err := readRunArchive(local, id)
			if err != nil {
				return err
			}
			s.resultReadPins[id]++
			pinned = append(pinned, id)
			runs = append(runs, archiveReconnectRun{id: id, manifest: manifest})
			return nil
		}()
		if err != nil {
			return nil, release, fmt.Errorf("read local archive references for %s: %w", id, err)
		}
	}
	return runs, release, nil
}

func readReconnectLocalIdentity(data *os.Root) ([]byte, bool, error) {
	file, err := openResultFile(data, ".archive-store-id")
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.close()
	if file.size > 128 {
		return nil, true, errors.New("invalid local archive identity")
	}
	identity, err := io.ReadAll(file.reader())
	return identity, true, err
}

func (s *Server) reconnectArchiveStore(ctx context.Context) error {
	s.setArchivePhase("preparing")
	if err := s.waitArchiveIdle(ctx); err != nil {
		return err
	}
	runs, release, err := s.captureArchiveReconnectRuns(ctx)
	if err != nil {
		return &archiveAccessError{kind: "local", err: err}
	}
	defer release()
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return &archiveAccessError{kind: "local", err: err}
	}
	defer data.Close()
	expected, existed, err := readReconnectLocalIdentity(data)
	if err != nil {
		return &archiveAccessError{kind: "local", err: fmt.Errorf("read local archive identity: %w", err)}
	}
	if s.archiveIOCheck != nil {
		s.archiveIOCheck()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	archive, err := openResultDirectory(data, archivedRunsDirectory)
	if err != nil {
		return &archiveAccessError{kind: "unavailable", err: fmt.Errorf("open archive reconnect candidate: %w", err)}
	}
	defer archive.Close()
	info, err := archive.Stat(".")
	if err != nil {
		return &archiveAccessError{kind: "unavailable", err: err}
	}
	identity, identityErr := readArchiveIdentity(archive)
	if identityErr != nil && !errors.Is(identityErr, os.ErrNotExist) {
		return &archiveAccessError{kind: "unavailable", err: fmt.Errorf("read archive reconnect identity: %w", identityErr)}
	}
	for _, run := range runs {
		if err := s.verifyArchiveReconnectRun(ctx, archive, run); err != nil {
			return &archiveAccessError{kind: "unavailable", err: fmt.Errorf("verify archive run %s: %w", run.id, err)}
		}
	}
	if err := s.waitArchiveIdle(ctx); err != nil {
		return err
	}
	// Coordinate only the final identity commit with first-time initialization.
	// Existing readers keep their already-open directory descriptors, while the
	// single archive worker prevents overlapping imports or transfers.
	s.archiveIdentityInitMu.Lock()
	defer s.archiveIdentityInitMu.Unlock()
	current, currentExists, err := readReconnectLocalIdentity(data)
	if err != nil {
		return &archiveAccessError{kind: "local", err: err}
	}
	if currentExists != existed || !bytesEqual(current, expected) {
		return errors.New("local archive identity changed during reconnect; retry")
	}
	if s.archiveIOCheck != nil {
		s.archiveIOCheck()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	currentInfo, err := data.Lstat(archivedRunsDirectory)
	if err != nil || !os.SameFile(info, currentInfo) {
		return &archiveAccessError{kind: "unavailable", err: errors.New("archive directory changed during reconnect; verify the runs mount and retry")}
	}
	currentID, currentErr := readArchiveIdentity(archive)
	if errors.Is(identityErr, os.ErrNotExist) {
		if !errors.Is(currentErr, os.ErrNotExist) {
			return &archiveAccessError{kind: "unavailable", err: errors.New("archive identity changed during reconnect; retry")}
		}
		currentID, currentErr = createArchiveIdentity(archive)
	} else if currentErr == nil && !bytesEqual(identity, currentID) {
		return &archiveAccessError{kind: "unavailable", err: errors.New("archive identity changed during reconnect; retry")}
	}
	if currentErr != nil {
		return &archiveAccessError{kind: "unavailable", err: fmt.Errorf("confirm archive reconnect identity: %w", currentErr)}
	}
	// Recheck after identity creation/sync, which may itself wait on the NAS.
	currentInfo, err = data.Lstat(archivedRunsDirectory)
	if err != nil || !os.SameFile(info, currentInfo) {
		return &archiveAccessError{kind: "unavailable", err: errors.New("archive directory changed during reconnect; verify the runs mount and retry")}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, currentExists, err = readReconnectLocalIdentity(data)
	if err != nil {
		return &archiveAccessError{kind: "local", err: err}
	}
	if currentExists != existed || !bytesEqual(current, expected) {
		return errors.New("local archive identity changed during reconnect; retry")
	}
	if err := writeImportedMetadata(data, ".archive-store-id", currentID); err != nil {
		return &archiveAccessError{kind: "local", err: fmt.Errorf("save reconnected archive identity: %w", err)}
	}
	if err := syncRunDirectory(data); err != nil {
		return &archiveAccessError{kind: "local", err: fmt.Errorf("synchronize reconnected archive identity: %w", err)}
	}
	s.archiveProgress()
	return nil
}

func (s *Server) verifyArchiveReconnectRun(ctx context.Context, archive *os.Root, run archiveReconnectRun) error {
	objects := make(map[string]storedRunFile)
	add := func(stored storedRunFile) error {
		digest, err := hex.DecodeString(stored.SHA256)
		if err != nil || len(digest) != 32 {
			return fmt.Errorf("object %s has no valid SHA256 evidence; restore the original archive before reconnecting", stored.Object)
		}
		if previous, ok := objects[stored.Object]; ok && (previous.Size != stored.Size || !strings.EqualFold(previous.SHA256, stored.SHA256)) {
			return fmt.Errorf("conflicting archive references for object %s", stored.Object)
		}
		objects[stored.Object] = stored
		return nil
	}
	for _, stored := range run.manifest.Files {
		if err := add(stored); err != nil {
			return err
		}
	}
	for _, segments := range run.manifest.Logs {
		for _, stored := range segments {
			if err := add(stored); err != nil {
				return err
			}
		}
	}
	if len(objects) == 0 {
		return nil
	}
	if err := s.waitArchiveIdle(ctx); err != nil {
		return err
	}
	remote, err := openResultDirectory(archive, run.id)
	if err != nil {
		return err
	}
	defer remote.Close()
	names := make([]string, 0, len(objects))
	for name := range objects {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := s.waitArchiveIdle(ctx); err != nil {
			return err
		}
		stored := objects[name]
		file, err := openResultFile(remote, name)
		if err != nil {
			return fmt.Errorf("open object %s: %w", name, err)
		}
		if file.size != stored.Size {
			file.close()
			return fmt.Errorf("object %s size differs from the local archive manifest", name)
		}
		digest, err := s.hashArchiveFile(ctx, file)
		file.close()
		if err != nil {
			return fmt.Errorf("read object %s: %w", name, err)
		}
		if !strings.EqualFold(digest, stored.SHA256) {
			return fmt.Errorf("object %s SHA256 differs from the local archive manifest", name)
		}
	}
	return nil
}
