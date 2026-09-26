package controller

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	resultMetadataLimit        = 1 << 20
	resultArchiveCacheTTL      = 5 * time.Second
	resultArchiveMeasureLimit  = 2
	resultArchiveResponseGrace = time.Second
	resultExportTimeLayout     = "2006-01-02T15:04:05.000000000Z"
	resultSizeMaxAgeHeader     = "X-KPL-Result-Size-Max-Age-Ms"
)

var (
	errResultNotFound = errors.New("saved result not found")
	errResultBusy     = errors.New("saved result is still active, queued, finalizing, or being downloaded")
)

type savedResult struct {
	StopRequested     bool          `json:"stopRequested,omitempty"`
	ControllerVersion string        `json:"controllerVersion,omitempty"`
	CleanupState      string        `json:"cleanupState,omitempty"`
	DataState         string        `json:"dataState,omitempty"`
	IntegrityError    string        `json:"integrityError,omitempty"`
	CleanupError      string        `json:"cleanupError,omitempty"`
	Agents            []model.Agent `json:"agents,omitempty"`

	SourceHash             string               `json:"sourceHash,omitempty"`
	SourceRevision         string               `json:"sourceRevision,omitempty"`
	ExecutionID            string               `json:"executionId,omitempty"`
	Superseded             bool                 `json:"superseded,omitempty"`
	GroupNote              *resultNoteSummary   `json:"groupNote,omitempty"`
	Storage                *runArchiveStatus    `json:"storage,omitempty"`
	Note                   *resultNoteSummary   `json:"note,omitempty"`
	PreviousRunIDs         []string             `json:"previousRunIds,omitempty"`
	BatchAnalysis          *batchAnalysisStatus `json:"batchAnalysis,omitempty"`
	Analysis               *analysisJobStatus   `json:"analysis,omitempty"`
	ID                     string               `json:"id"`
	Name                   string               `json:"name"`
	State                  string               `json:"state"`
	StartedAt              time.Time            `json:"startedAt"`
	FinishedAt             time.Time            `json:"finishedAt"`
	Active                 bool                 `json:"active"`
	BatchID                string               `json:"batchId,omitempty"`
	Iteration              int                  `json:"iteration,omitempty"`
	Repetitions            int                  `json:"repetitions,omitempty"`
	SourceBytes            *int64               `json:"sourceBytes,omitempty"`
	DownloadBytes          *int64               `json:"downloadBytes,omitempty"`
	DownloadSizeMaxAgeMS   *int64               `json:"downloadSizeMaxAgeMs,omitempty"`
	storedState            string
	downloadArchiveVersion resultArchiveVersion
	downloadSizePending    bool
}

type resultFile struct {
	parts  []resultFile
	remote *storedRunFile
	name   string
	file   *os.File
	size   int64
	info   os.FileInfo
}

type resultArchiveVersion struct {
	files       []resultFileVersion
	active      bool
	state       string
	storedState string
}

type resultFileVersion struct {
	name string
	size int64
	info os.FileInfo
}

type resultArchiveInfo struct {
	version             resultArchiveVersion
	bytes               int64
	pendingPublications int
	exportedAt          time.Time
	expiresAt           time.Time
	listGraceExtended   bool
}

type resultArchiveFlight struct {
	done chan struct{}
}

type resultSnapshot struct {
	files       []resultFile
	exportedAt  time.Time
	active      bool
	result      savedResult
	storedState string
	release     func()
}

func (snapshot *resultSnapshot) close() {
	for _, file := range snapshot.files {
		file.close()
	}
	if snapshot.release != nil {
		snapshot.release()
		snapshot.release = nil
	}
}

// Result IDs are directory names, never paths or names normalized into paths.
func validResultID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, ch := range id {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' {
			continue
		}
		return false
	}
	return true
}

// Root confines resolution even if a path is replaced during these checks.
// Reject links inside that boundary too: a run must not alias another run.
func openResultDirectory(parent *os.Root, name string) (*os.Root, error) {
	info, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s is not a regular result directory", name)
	}
	root, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = root.Close()
		return nil, fmt.Errorf("result directory changed while opening %s", name)
	}
	return root, nil
}

func openResultFile(root *os.Root, name string) (resultFile, error) {
	result := resultFile{name: name}
	info, err := root.Lstat(name)
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() {
		return result, fmt.Errorf("%s is not a regular result file", name)
	}
	file, err := root.Open(name)
	if err != nil {
		return result, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		_ = file.Close()
		return result, fmt.Errorf("result file changed while opening %s", name)
	}
	result.file, result.size, result.info = file, opened.Size(), opened
	return result, nil
}

func readResultMetadata(file resultFile, id string, active bool) (savedResult, error) {
	var result savedResult
	if file.size > resultMetadataLimit {
		return result, fmt.Errorf("experiment.json exceeds %d bytes", resultMetadataLimit)
	}
	decoder := json.NewDecoder(io.NewSectionReader(file.file, 0, file.size))
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode experiment.json: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return result, errors.New("experiment.json contains trailing data")
	}
	if result.ID != id || result.State == "" {
		return result, errors.New("experiment.json has an invalid ID or state")
	}
	// Persisted data cannot claim ownership by the current Controller process.
	result.storedState = result.State
	result.Active = active && result.State == "running"
	if (result.State == "running" || result.State == "queued") && !active {
		result.State = "interrupted"
	}
	return result, nil
}

func (s *Server) resultActive(id string) bool {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	experiment, exists := s.state.experiments[id]
	return exists && (experiment.State == "running" || experiment.State == "queued")
}

func (s *Server) openResultRuns() (*os.Root, error) {
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	return openResultDirectory(data, currentRunsDirectory)
}

// The marker survives Controller restarts. Persistence callers hold persistMu
// so a late Agent batch cannot recreate a directory after its deletion.
func (s *state) resultDeletedLocked(id string) (bool, error) {
	if !validResultID(id) {
		return false, errors.New("invalid result id")
	}
	data, err := os.OpenRoot(s.dataDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer data.Close()
	markers, err := openResultDirectory(data, ".deleted-results")
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer markers.Close()
	info, err := markers.Lstat(id)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("deletion marker is not a regular file")
	}
	return true, nil
}

func (s *Server) markResultDeletedLocked(id string) error {
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	if err := data.Mkdir(".deleted-results", 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	markers, err := openResultDirectory(data, ".deleted-results")
	if err != nil {
		return err
	}
	defer markers.Close()
	file, err := markers.OpenFile(id, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		_, err = s.state.resultDeletedLocked(id)
		return err
	}
	if err != nil {
		return err
	}
	writeErr := json.NewEncoder(file).Encode(map[string]any{"id": id, "deletedAt": time.Now().UTC()})
	return errors.Join(writeErr, file.Close())
}

// Remove entries relative to held directory descriptors. Symlinks are unlinked,
// never traversed. This uses only Root methods available on the supported Go toolchain.
func removeResultDirectory(parent *os.Root, id string) error {
	directory, err := openResultDirectory(parent, id)
	if err != nil {
		return err
	}
	file, err := directory.Open(".")
	if err != nil {
		_ = directory.Close()
		return err
	}
	entries, readErr := file.ReadDir(-1)
	_ = file.Close()
	if readErr != nil {
		_ = directory.Close()
		return readErr
	}
	for _, entry := range entries {
		if entry.IsDir() {
			err = removeResultDirectory(directory, entry.Name())
		} else {
			err = directory.Remove(entry.Name())
		}
		if err != nil {
			_ = directory.Close()
			return err
		}
	}
	if err := directory.Close(); err != nil {
		return err
	}
	return parent.Remove(id)
}

func (s *Server) handleResultAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/results/")
	if id, found := strings.CutSuffix(path, "/note"); found {
		s.handleResultNote(w, r, id)
		return
	}
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/results/")
	if !validResultID(id) {
		http.NotFound(w, r)
		return
	}
	if err := s.deleteSavedResult(id); err != nil {
		switch {
		case errors.Is(err, errResultNotFound):
			http.NotFound(w, r)
		case errors.Is(err, errResultBusy):
			writeError(w, http.StatusConflict, err.Error())
		default:
			s.logger.Error("delete saved result", "run", id, "error", err)
			writeError(w, http.StatusInternalServerError, "cannot delete saved result; retry after resolving the storage error")
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteSavedResult(id string) error {
	if !validResultID(id) {
		return errResultNotFound
	}
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	s.analysisJobMu.Lock()
	defer s.analysisJobMu.Unlock()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	return s.deleteSavedResultLocked(id)
}

// Caller holds cancelMu, analysisJobMu, and state.persistMu in that order.
func (s *Server) resultDeletionBusyLocked(id string) bool {
	s.state.mu.RLock()
	experiment := s.state.experiments[id]
	s.state.mu.RUnlock()
	return experiment.State == "running" || experiment.State == "queued" || s.cancels[id] != nil || s.repeatBatches[id] != nil || s.resultDownloads[id] > 0 || s.resultReadPins[id] > 0
}

// Caller holds the same locks as resultDeletionBusyLocked.
func (s *Server) deleteSavedResultLocked(id string) error {
	if s.resultDeletionBusyLocked(id) {
		return errResultBusy
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return errResultNotFound
	}
	if err != nil {
		return err
	}
	defer runs.Close()
	directory, err := openResultDirectory(runs, id)
	if errors.Is(err, os.ErrNotExist) {
		return errResultNotFound
	}
	if err != nil {
		return err
	}
	_ = directory.Close()
	if err := s.retainBatchCurrentLocked(runs, id); err != nil {
		return err
	}
	if err := s.markResultDeletedLocked(id); err != nil {
		return fmt.Errorf("persist deletion marker: %w", err)
	}
	if job := s.analysisJobs[id]; job != nil && job.cancel != nil {
		job.cancel()
	}
	delete(s.analysisJobs, id)
	if err := removeResultDirectory(runs, id); err != nil {
		return err
	}
	s.state.mu.Lock()
	delete(s.state.experiments, id)
	delete(s.state.runMetrics, id)
	events := s.state.events[:0]
	for _, event := range s.state.events {
		if event.RunID != id {
			events = append(events, event)
		}
	}
	s.state.events = events
	s.state.mu.Unlock()
	s.state.notify()
	s.resultArchiveMu.Lock()
	delete(s.resultArchives, id)
	s.resultArchiveMu.Unlock()
	return nil
}

func (s *Server) handleResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	results := make([]savedResult, 0)
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		writeJSON(w, http.StatusOK, results)
		return
	}
	if err != nil {
		s.logger.Warn("list saved results", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot read saved results")
		return
	}
	defer runs.Close()
	directory, err := runs.Open(".")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot read saved results")
		return
	}
	defer directory.Close()
	for {
		entries, readErr := directory.ReadDir(100)
		for _, entry := range entries {
			if r.Context().Err() != nil {
				return
			}
			if !validResultID(entry.Name()) || !entry.IsDir() {
				s.logger.Warn("skip unsafe saved result entry", "entry", entry.Name())
				continue
			}
			id := entry.Name()
			result, err := s.readSavedResult(runs, id)
			if errors.Is(err, errResultNotFound) {
				continue
			}
			if err != nil {
				s.logger.Warn("read saved result metadata", "run", id, "error", err)
				result = savedResult{ID: id, Name: id, State: "unreadable", SourceBytes: result.SourceBytes, Note: result.Note}
			} else if !result.Active && result.State != "queued" && (result.Storage == nil || result.Storage.State == "local") && s.hasPreparedResultArchive(id) {
				snapshot, snapshotErr := s.captureResultFiles(id, false)
				if snapshotErr == nil {
					archiveInfo, ready := s.cachedResultArchiveInfo(snapshot, time.Now().UTC())
					snapshot.close()
					if ready {
						archiveBytes := archiveInfo.bytes
						result.DownloadBytes = &archiveBytes
						result.downloadArchiveVersion = archiveInfo.version
						result.downloadSizePending = archiveInfo.pendingPublications > 0
					}
				} else if !errors.Is(snapshotErr, errResultNotFound) {
					s.logger.Warn("capture saved result for archive size", "run", id, "error", snapshotErr)
				}
			}
			if job, err := s.analysisJobStatus(id); err == nil && job.State != "idle" {
				result.Analysis = &job
			}
			results = append(results, result)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			s.logger.Warn("list saved results", "error", readErr)
			writeError(w, http.StatusInternalServerError, "cannot read saved results")
			return
		}
		if err := r.Context().Err(); err != nil {
			return
		}
	}
	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if !a.StartedAt.Equal(b.StartedAt) {
			return a.StartedAt.After(b.StartedAt)
		}
		// Queued runs share a zero start time. Their IDs do not encode the
		// batch execution order; use a complete group/iteration tie break.
		aGroup, bGroup := a.BatchID, b.BatchID
		if aGroup == "" {
			aGroup = a.ID
		}
		if bGroup == "" {
			bGroup = b.ID
		}
		if aGroup != bGroup {
			return aGroup < bGroup
		}
		if a.Iteration != b.Iteration {
			return a.Iteration < b.Iteration
		}
		return a.ID < b.ID
	})
	batchMembers := map[string][]savedResult{}
	for _, result := range results {
		if result.BatchID != "" {
			batchMembers[result.BatchID] = append(batchMembers[result.BatchID], result)
		}
	}
	// Attach one small preview per group, rather than repeating it for every run.
	notedGroups := map[string]bool{}
	for i := range results {
		id := results[i].BatchID
		results[i].GroupNote = nil
		if id == "" || notedGroups[id] {
			continue
		}
		notedGroups[id] = true
		note, err := s.resultGroupNoteSummary(id)
		if err != nil {
			s.logger.Warn("read group note preview", "batch", id, "error", err)
			continue
		}
		results[i].GroupNote = note
	}
	batchStatuses := map[string]*batchAnalysisStatus{}
	for i := range results {
		id := results[i].BatchID
		if id == "" || results[i].Repetitions < 2 {
			continue
		}
		status, exists := batchStatuses[id]
		if !exists {
			job, err := s.batchAnalysisStatus(id)
			if err == nil && job.State != "idle" {
				if job.State == "completed" && (job.Membership != batchMembership(batchMembers[id]) || job.SourceHash == "") {
					job.State, job.Stale = "idle", true
				}
				status = &job
			}
			batchStatuses[id] = status
		}
		results[i].BatchAnalysis = status
	}
	s.extendPendingResultArchiveCache(results, time.Now().UTC())
	writeJSON(w, http.StatusOK, results)
}

// Keep a pending result's measured boundary available for a short interval
// after the complete list is ready, including time spent inspecting other rows.
// The advertised lifetime reserves response grace so clients never receive a
// cache value that is already too close to the server-side boundary.
func (s *Server) extendPendingResultArchiveCache(results []savedResult, now time.Time) {
	s.resultArchiveMu.Lock()
	defer s.resultArchiveMu.Unlock()
	for index := range results {
		result := &results[index]
		if result.DownloadBytes == nil || !result.downloadSizePending {
			continue
		}
		cached, found := s.resultArchives[result.ID]
		if !found || cached.pendingPublications == 0 || cached.bytes != *result.DownloadBytes ||
			!sameResultArchiveVersion(cached.version, result.downloadArchiveVersion) {
			result.DownloadBytes = nil
			result.DownloadSizeMaxAgeMS = nil
			continue
		}
		if !cached.listGraceExtended {
			cached.expiresAt = now.Add(resultArchiveCacheTTL)
			cached.listGraceExtended = true
			s.resultArchives[result.ID] = cached
		}
		maxAgeMS, usable := resultArchiveSizeMaxAgeMS(cached, now)
		if !usable {
			result.DownloadBytes = nil
			result.DownloadSizeMaxAgeMS = nil
			continue
		}
		result.DownloadSizeMaxAgeMS = &maxAgeMS
	}
}

// Only persisted source inputs count here. Derived analysis caches, generated
// ZIP entries and filesystem allocation overhead are not original result data.
var resultSourceFiles = [...]string{"scenario.yaml", "experiment.json", "events.jsonl", "observations.jsonl", resultNoteFile}

func resultSourceBytes(root *os.Root) (*int64, error) {
	manifest, err := readRunArchive(root, "")
	if err != nil {
		return nil, err
	}
	var total int64
	for _, name := range resultSourceFiles {
		file, err := captureRunSource(root, manifest, name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		size := file.size
		file.close()
		if size < 0 || size > math.MaxInt64-total {
			return nil, errors.New("source file size is out of range")
		}
		total += size
	}
	return &total, nil
}

func (s *Server) hasPreparedResultArchive(id string) bool {
	s.resultArchiveMu.Lock()
	defer s.resultArchiveMu.Unlock()
	_, found := s.resultArchives[id]
	return found
}

func (s *Server) readSavedResult(runs *os.Root, id string) (savedResult, error) {
	s.state.persistMu.Lock()
	deleted, err := s.state.resultDeletedLocked(id)
	if err != nil || deleted {
		s.state.persistMu.Unlock()
		if err != nil {
			return savedResult{}, err
		}
		return savedResult{}, errResultNotFound
	}
	root, err := openResultDirectory(runs, id)
	var file resultFile
	var sourceBytes *int64
	var sourceRevision string
	var noteFile resultFile
	var noteErr error
	var storage runArchiveStatus
	if err == nil {
		var sizeErr error
		sourceBytes, sizeErr = resultSourceBytes(root)
		sourceRevision, _ = sourceRevisionAt(root, id)
		storage = resultStorageStatus(root, id)
		if sizeErr != nil {
			s.logger.Warn("stat saved result sources", "run", id, "error", sizeErr)
		}
		noteFile, noteErr = openResultFile(root, resultNoteFile)
		file, err = openResultFile(root, "experiment.json")
		_ = root.Close()
	}
	active := s.resultActive(id)
	s.state.persistMu.Unlock()
	var note resultNote
	if noteFile.file != nil {
		note, noteErr = decodeResultNote(noteFile, id)
	}
	if noteErr != nil && !errors.Is(noteErr, os.ErrNotExist) {
		s.logger.Warn("read saved result note", "run", id, "error", noteErr)
	}
	if err != nil {
		return savedResult{SourceBytes: sourceBytes, Note: note.summary()}, err
	}
	defer file.file.Close()
	result, err := readResultMetadata(file, id, active)
	if err == nil {
		err = s.applyBatchExtension(&result)
	}
	// Never trust a size supplied by experiment.json; stat the current inputs.
	result.SourceBytes = sourceBytes
	result.SourceRevision = sourceRevision
	result.Note = note.summary()
	result.Storage = &storage
	return result, err
}

func (s *Server) captureResult(id string) (*resultSnapshot, error) {
	return s.captureResultFiles(id, true)
}

// Inspections (list and HEAD) keep open file descriptors but do not prevent
// deletion. Only an actual ZIP download needs a deletion lease. On the Swarm
// Linux hosts, captured descriptors remain readable after files are unlinked.
func (s *Server) captureResultFiles(id string, download bool) (*resultSnapshot, error) {
	return s.captureResultFilesContext(context.Background(), id, download)
}
func (s *Server) captureResultFilesContext(ctx context.Context, id string, download bool) (*resultSnapshot, error) {
	if !validResultID(id) {
		return nil, errResultNotFound
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return nil, errResultNotFound
	}
	if err != nil {
		return nil, err
	}
	defer runs.Close()
	snapshot := &resultSnapshot{}
	// Event appends and metadata replacements use this same lock. Only open,
	// stat and the in-memory ownership check belong in the critical section.
	err = func() error {
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		deleted, err := s.state.resultDeletedLocked(id)
		if err != nil {
			return err
		}
		if deleted {
			return errResultNotFound
		}
		root, err := openResultDirectory(runs, id)
		if errors.Is(err, os.ErrNotExist) {
			return errResultNotFound
		}
		if err != nil {
			return err
		}
		defer root.Close()
		manifest, err := readRunArchive(root, id)
		if err != nil {
			return err
		}
		for _, name := range resultSourceFiles {
			file, err := captureRunSource(root, manifest, name)
			if err != nil && !((name == "events.jsonl" || name == "observations.jsonl" || name == resultNoteFile) && errors.Is(err, os.ErrNotExist)) {
				return fmt.Errorf("open %s: %w", name, err)
			}
			if (name == "observations.jsonl" || name == resultNoteFile) && file.file == nil && len(file.parts) == 0 && file.remote == nil {
				continue
			}
			snapshot.files = append(snapshot.files, file)
		}
		snapshot.active = s.resultActive(id)
		snapshot.exportedAt = time.Now().UTC()
		if download {
			if s.resultDownloads == nil {
				s.resultDownloads = make(map[string]int)
			}
			s.resultDownloads[id]++
			snapshot.release = func() {
				s.state.persistMu.Lock()
				s.resultDownloads[id]--
				if s.resultDownloads[id] == 0 {
					delete(s.resultDownloads, id)
				}
				s.state.persistMu.Unlock()
			}
		}
		return nil
	}()
	if err != nil {
		snapshot.close()
		return nil, err
	}
	revision := sourceRevisionOf(snapshot.files)
	releaseRemote, err := s.resolveArchivedFiles(ctx, id, snapshot.files)
	if err != nil {
		snapshot.close()
		return nil, err
	}
	if releaseRemote != nil {
		previousRelease := snapshot.release
		snapshot.release = func() {
			releaseRemote()
			if previousRelease != nil {
				previousRelease()
			}
		}
	}
	result, err := readResultMetadata(snapshot.files[1], id, snapshot.active)
	if err == nil {
		err = s.applyBatchExtension(&result)
	}
	if err == nil {
		// Probe both ends before committing HTTP headers. Later I/O failures
		// still abort the response instead of finishing a misleading ZIP.
		for _, file := range snapshot.files {
			if file.size == 0 {
				continue
			}
			var probe [1]byte
			if _, err = file.reader().ReadAt(probe[:], 0); err != nil {
				break
			}
			if _, err = file.reader().ReadAt(probe[:], file.size-1); err != nil {
				break
			}
		}
	}
	if err != nil {
		snapshot.close()
		return nil, err
	}
	result.SourceRevision = revision
	snapshot.result = result
	snapshot.storedState = result.storedState
	return snapshot, nil
}

func (s *Server) handleResultDownload(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		methodNotAllowed(w)
		return
	}
	snapshot, err := s.captureResultFilesContext(r.Context(), id, r.Method == http.MethodGet)
	if err != nil {
		if errors.Is(err, errResultNotFound) {
			http.NotFound(w, r)
			return
		}
		s.logger.Warn("capture saved result", "run", id, "error", err)
		writeError(w, http.StatusInternalServerError, "saved result files are unreadable")
		return
	}
	defer snapshot.close()
	var archiveInfo resultArchiveInfo
	var sizeMaxAgeMS int64
	for {
		archiveInfo, err = s.prepareResultArchiveInfo(r.Context(), snapshot)
		if err != nil || archiveInfo.pendingPublications == 0 {
			break
		}
		var usable bool
		sizeMaxAgeMS, usable = resultArchiveSizeMaxAgeMS(archiveInfo, time.Now().UTC())
		if usable {
			break
		}
		// The cache crossed the response-safety boundary after lookup but before
		// headers were ready. Rebuild from a current export time.
		snapshot.exportedAt = time.Now().UTC()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || r.Context().Err() != nil {
			return
		}
		s.logger.Warn("measure saved result archive", "run", id, "error", err)
		writeError(w, http.StatusInternalServerError, "saved result files are unreadable")
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.zip\"", id))
	w.Header().Set("Content-Length", strconv.FormatInt(archiveInfo.bytes, 10))
	if archiveInfo.pendingPublications > 0 {
		w.Header().Set(resultSizeMaxAgeHeader, strconv.FormatInt(sizeMaxAgeMS, 10))
	}
	if r.Method == http.MethodHead {
		return
	}
	written := &resultCountingWriter{writer: w}
	if err := snapshot.writeZIP(r.Context(), written); err != nil || written.bytes != archiveInfo.bytes {
		if err == nil {
			err = fmt.Errorf("archive size changed from %d to %d bytes", archiveInfo.bytes, written.bytes)
		}
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && r.Context().Err() == nil {
			s.logger.Warn("stream saved result", "run", id, "error", err)
		}
		// Headers are already committed. Abort the HTTP stream so a short body
		// or size invariant failure cannot be reported as a successful response.
		// ErrAbortHandler suppresses the standard server panic log.
		panic(http.ErrAbortHandler)
	}
}

func (snapshot *resultSnapshot) archiveVersion() resultArchiveVersion {
	version := resultArchiveVersion{active: snapshot.result.Active, state: snapshot.result.State, storedState: snapshot.storedState}
	for _, file := range snapshot.files {
		version.files = append(version.files, resultFileVersion{name: file.name, size: file.size, info: file.info})
		for i, part := range file.parts {
			version.files = append(version.files, resultFileVersion{name: fmt.Sprintf("%s/%d/%s", file.name, i, part.name), size: part.size, info: part.info})
		}
	}
	return version
}

func sameResultArchiveVersion(left, right resultArchiveVersion) bool {
	if left.active != right.active || left.state != right.state || left.storedState != right.storedState || len(left.files) != len(right.files) {
		return false
	}
	for index := range left.files {
		leftFile, rightFile := left.files[index], right.files[index]
		if leftFile.name != rightFile.name || leftFile.size != rightFile.size {
			return false
		}
		if leftFile.info == nil || rightFile.info == nil {
			if leftFile.info != nil || rightFile.info != nil {
				return false
			}
			continue
		}
		if !os.SameFile(leftFile.info, rightFile.info) || !leftFile.info.ModTime().Equal(rightFile.info.ModTime()) {
			return false
		}
	}
	return true
}

func resultArchiveCacheUsable(cached resultArchiveInfo, version resultArchiveVersion, now time.Time) bool {
	if !sameResultArchiveVersion(cached.version, version) {
		return false
	}
	if cached.pendingPublications == 0 {
		return true
	}
	_, usable := resultArchiveSizeMaxAgeMS(cached, now)
	return usable
}

func resultArchiveSizeMaxAgeMS(cached resultArchiveInfo, now time.Time) (int64, bool) {
	remaining := cached.expiresAt.Sub(now) - resultArchiveResponseGrace
	maxAgeMS := remaining.Milliseconds()
	return maxAgeMS, maxAgeMS > 0
}

func (s *Server) cachedResultArchiveInfo(snapshot *resultSnapshot, now time.Time) (resultArchiveInfo, bool) {
	version := snapshot.archiveVersion()
	s.resultArchiveMu.Lock()
	defer s.resultArchiveMu.Unlock()
	cached, found := s.resultArchives[snapshot.result.ID]
	if !found || !resultArchiveCacheUsable(cached, version, now) {
		return resultArchiveInfo{}, false
	}
	return cached, true
}

func useResultArchiveInfo(snapshot *resultSnapshot, cached resultArchiveInfo) {
	if cached.pendingPublications > 0 {
		snapshot.exportedAt = cached.exportedAt
	}
}

func (s *Server) acquireResultArchiveSlot(ctx context.Context) error {
	select {
	case s.resultArchiveSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) releaseResultArchiveSlot() {
	<-s.resultArchiveSlots
}

func (s *Server) finishResultArchiveFlight(runID string, flight *resultArchiveFlight, candidate *resultArchiveInfo) {
	if candidate != nil {
		// A size inspection may finish after deletion. Coordinate with the
		// deletion marker so it cannot repopulate the removed result's cache.
		s.state.persistMu.Lock()
		defer s.state.persistMu.Unlock()
		if deleted, err := s.state.resultDeletedLocked(runID); err != nil || deleted {
			candidate = nil
		}
	}
	s.resultArchiveMu.Lock()
	defer s.resultArchiveMu.Unlock()
	if candidate != nil {
		s.resultArchives[runID] = *candidate
	}
	if s.resultArchiveFlights[runID] == flight {
		delete(s.resultArchiveFlights, runID)
		close(flight.done)
	}
}

// prepareResultArchive caches only the exact byte count, not the archive. A
// file version with no pending publications keeps the same encoded length even
// with a fresh export time. Pending metrics briefly pin their measured boundary.
// One flight per run prevents concurrent cache misses from repeating the work.
func (s *Server) prepareResultArchive(ctx context.Context, snapshot *resultSnapshot) (int64, error) {
	info, err := s.prepareResultArchiveInfo(ctx, snapshot)
	return info.bytes, err
}

func (s *Server) prepareResultArchiveInfo(ctx context.Context, snapshot *resultSnapshot) (resultArchiveInfo, error) {
	version := snapshot.archiveVersion()
	runID := snapshot.result.ID
	for {
		if err := ctx.Err(); err != nil {
			return resultArchiveInfo{}, err
		}
		now := time.Now().UTC()
		s.resultArchiveMu.Lock()
		if cached, found := s.resultArchives[runID]; found && resultArchiveCacheUsable(cached, version, now) {
			useResultArchiveInfo(snapshot, cached)
			s.resultArchiveMu.Unlock()
			return cached, nil
		}
		if flight := s.resultArchiveFlights[runID]; flight != nil {
			done := flight.done
			s.resultArchiveMu.Unlock()
			select {
			case <-ctx.Done():
				return resultArchiveInfo{}, ctx.Err()
			case <-done:
				continue
			}
		}
		flight := &resultArchiveFlight{done: make(chan struct{})}
		s.resultArchiveFlights[runID] = flight
		s.resultArchiveMu.Unlock()
		if err := s.acquireResultArchiveSlot(ctx); err != nil {
			s.finishResultArchiveFlight(runID, flight, nil)
			return resultArchiveInfo{}, err
		}

		measured := &resultCountingWriter{}
		pendingPublications := 0
		var err error
		func() {
			defer s.releaseResultArchiveSlot()
			err = snapshot.writeZIPMeasured(ctx, measured, &pendingPublications)
		}()
		if err == nil {
			err = ctx.Err()
		}
		candidate := resultArchiveInfo{
			version: version, bytes: measured.bytes, pendingPublications: pendingPublications,
			exportedAt: snapshot.exportedAt,
		}
		if pendingPublications > 0 {
			candidate.expiresAt = time.Now().UTC().Add(resultArchiveCacheTTL)
		}

		if err == nil {
			s.finishResultArchiveFlight(runID, flight, &candidate)
		} else {
			s.finishResultArchiveFlight(runID, flight, nil)
		}
		if err != nil {
			return resultArchiveInfo{}, err
		}
		return candidate, nil
	}
}

func (snapshot *resultSnapshot) writeZIP(ctx context.Context, output io.Writer) error {
	return snapshot.writeZIPMeasured(ctx, output, nil)
}

func (snapshot *resultSnapshot) writeZIPMeasured(ctx context.Context, output io.Writer, pendingPublications *int) error {
	archive := zip.NewWriter(resultContextWriter{ctx: ctx, writer: output})
	sizes := make(map[string]int64, len(snapshot.files))
	for _, file := range snapshot.files {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := archive.Create(file.name)
		if err != nil {
			return err
		}
		if file.file != nil || len(file.parts) > 0 {
			if _, err := io.CopyN(entry, file.reader(), file.size); err != nil {
				return fmt.Errorf("copy %s: %w", file.name, err)
			}
		}
		sizes[file.name] = file.size
	}
	var eventLog io.Reader = strings.NewReader("")
	for _, file := range snapshot.files {
		if file.name == "events.jsonl" && (file.file != nil || len(file.parts) > 0) {
			eventLog = file.reader()
			break
		}
	}
	metrics, err := summarizeRunEventsContext(ctx, snapshot.result.ID, eventLog, snapshot.exportedAt)
	if err != nil {
		return fmt.Errorf("summarize saved events: %w", err)
	}
	if pendingPublications != nil {
		*pendingPublications = metrics.PendingPublications
	}
	metricsJSON, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return err
	}
	metricsJSON = append(metricsJSON, '\n')
	metricsEntry, err := archive.Create("metrics.json")
	if err != nil {
		return err
	}
	if _, err := metricsEntry.Write(metricsJSON); err != nil {
		return err
	}
	sizes["metrics.json"] = int64(len(metricsJSON))
	exported, err := archive.CreateHeader(&zip.FileHeader{Name: "export.json", Method: zip.Store})
	if err != nil {
		return err
	}
	if err := json.NewEncoder(exported).Encode(struct {
		Version     int              `json:"version"`
		RunID       string           `json:"runId"`
		ExportedAt  string           `json:"exportedAt"`
		Active      bool             `json:"active"`
		Partial     bool             `json:"partial"`
		State       string           `json:"state"`
		StoredState string           `json:"storedState"`
		EventBytes  int64            `json:"eventBytes"`
		Files       map[string]int64 `json:"files"`
		Boundary    string           `json:"boundary"`
	}{
		Version: 1, RunID: snapshot.result.ID, ExportedAt: snapshot.exportedAt.UTC().Format(resultExportTimeLayout),
		Active: snapshot.result.Active, Partial: snapshot.result.Active || snapshot.result.State == "interrupted" || snapshot.result.State == "queued",
		State: snapshot.result.State, StoredState: snapshot.storedState,
		EventBytes: sizes["events.jsonl"], Files: sizes,
		Boundary: "Only bytes persisted at the snapshot boundary are included. In-flight or subsequently received telemetry is excluded; a finished experiment may still receive late telemetry.",
	}); err != nil {
		return err
	}
	return archive.Close()
}

type resultContextWriter struct {
	ctx    context.Context
	writer io.Writer
}

type resultCountingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *resultCountingWriter) Write(data []byte) (int, error) {
	if w.writer == nil {
		w.bytes += int64(len(data))
		return len(data), nil
	}
	written, err := w.writer.Write(data)
	w.bytes += int64(written)
	return written, err
}

type resultContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r resultContextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func (w resultContextWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(data)
}
