package controller

// Local result metadata and logical source snapshots. NAS reads are resolved
// after releasing Controller locks; sealed log segments remain immutable.
import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

const currentRunsDirectory = "current-run"
const archivedRunsDirectory = "runs"
const runArchiveManifest = ".kpl-archive.json"
const runArchiveStatusFile = ".archive-status.json"
const runArchiveImportFile = ".archive-import"
const runArchiveManifestLimit = 4 << 20
const archiveQuietPeriod = 5 * time.Second

// Objects are immutable after publication. Segment identifies a local sealed
// log file, allowing a crash between manifest publication and unlink to recover
// without counting the same bytes twice.
type storedRunFile struct {
	Object     string `json:"object"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256,omitempty"`
	ModifiedAt int64  `json:"localModifiedAt,omitempty"`
	Segment    string `json:"segment,omitempty"`
}
type runArchiveManifestData struct {
	Version int                        `json:"version"`
	RunID   string                     `json:"runId"`
	Files   map[string]storedRunFile   `json:"files"`
	Logs    map[string][]storedRunFile `json:"logs"`
}
type runArchiveStatus struct {
	State     string    `json:"state"`
	Error     string    `json:"error,omitempty"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func isRunLog(name string) bool { return name == "events.jsonl" || name == "observations.jsonl" }
func logSegment(name, log string) bool {
	return strings.HasPrefix(name, strings.TrimSuffix(log, ".jsonl")+"-segment-") && strings.HasSuffix(name, ".jsonl") && safeArchiveObject(name)
}
func safeArchiveObject(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\\\x00") && !strings.HasPrefix(name, ".")
}
func retainedRunFile(name string) bool {
	return name == "experiment.json" || name == "scenario.yaml" || name == resultNoteFile || name == analysisJobFile
}

func readRunArchive(root *os.Root, id string) (runArchiveManifestData, error) {
	manifest := runArchiveManifestData{Version: 1, RunID: id, Files: map[string]storedRunFile{}, Logs: map[string][]storedRunFile{}}
	f, err := openResultFile(root, runArchiveManifest)
	if errors.Is(err, os.ErrNotExist) {
		return manifest, nil
	}
	if err != nil {
		return manifest, err
	}
	defer f.file.Close()
	if f.size > runArchiveManifestLimit {
		return manifest, errors.New("run archive manifest is too large")
	}
	decoder := json.NewDecoder(io.NewSectionReader(f.file, 0, f.size))
	if err = decoder.Decode(&manifest); err != nil {
		return manifest, err
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return manifest, errors.New("invalid run archive manifest")
	}
	if manifest.Version != 1 || (id != "" && manifest.RunID != id) || manifest.Files == nil || manifest.Logs == nil {
		return manifest, errors.New("invalid run archive identity")
	}
	for name, file := range manifest.Files {
		if !safeArchiveObject(name) || !safeArchiveObject(file.Object) || file.Size < 0 {
			return manifest, errors.New("invalid archived file")
		}
	}
	for name, files := range manifest.Logs {
		if !isRunLog(name) {
			return manifest, errors.New("invalid archived log")
		}
		seen := map[string]bool{}
		for _, file := range files {
			if !safeArchiveObject(file.Object) || file.Size < 0 || seen[file.Object] || file.Segment != "" && !logSegment(file.Segment, name) {
				return manifest, errors.New("invalid archived segment")
			}
			seen[file.Object] = true
		}
	}
	return manifest, nil
}

// Do not publish an index that cannot be read after restart. The caller keeps
// local bytes when this bound is reached.
func writeRunArchive(root *os.Root, manifest runArchiveManifestData) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if len(data)+1 > runArchiveManifestLimit {
		return errors.New("run archive manifest is too large")
	}
	return writeAnalysisJSON(root, runArchiveManifest, manifest)
}

func localRunEntries(root *os.Root) ([]os.DirEntry, error) {
	dir, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	return dir.ReadDir(-1)
}

// Called under persistMu. Returned remote placeholders are opened only AFTER
// releasing it, so a stalled NAS can never hold up ingestion or phase changes.
func captureRunSource(root *os.Root, manifest runArchiveManifestData, name string) (resultFile, error) {
	if !isRunLog(name) {
		file, err := openResultFile(root, name)
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return file, err
		}
		if stored, ok := manifest.Files[name]; ok {
			return resultFile{name: name, size: stored.Size, remote: &stored}, nil
		}
		return file, err
	}
	result := resultFile{name: name}
	known := map[string]bool{}
	for _, stored := range manifest.Logs[name] {
		known[stored.Segment] = true
		part := resultFile{name: name, size: stored.Size, remote: &stored}
		if stored.Segment != "" {
			local, err := openResultFile(root, stored.Segment)
			if err == nil {
				if local.size != stored.Size {
					local.file.Close()
					result.close()
					return resultFile{}, errors.New("archived segment size changed")
				}
				part = local
			} else if !errors.Is(err, os.ErrNotExist) {
				result.close()
				return resultFile{}, err
			}
		}
		result.parts = append(result.parts, part)
		result.size += part.size
	}
	entries, err := localRunEntries(root)
	if err != nil {
		result.close()
		return resultFile{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if !logSegment(entry.Name(), name) || known[entry.Name()] {
			continue
		}
		part, err := openResultFile(root, entry.Name())
		if err != nil {
			result.close()
			return resultFile{}, err
		}
		result.parts = append(result.parts, part)
		result.size += part.size
	}
	tail, err := openResultFile(root, name)
	if err == nil {
		result.parts = append(result.parts, tail)
		result.size += tail.size
	} else if !errors.Is(err, os.ErrNotExist) {
		result.close()
		return resultFile{}, err
	}
	return result, nil
}

func (file resultFile) reader() *io.SectionReader {
	if len(file.parts) > 0 {
		return io.NewSectionReader(joinedResultReader{parts: file.parts}, 0, file.size)
	}
	return io.NewSectionReader(file.file, 0, file.size)
}
func (file resultFile) close() {
	if file.file != nil {
		_ = file.file.Close()
	}
	for _, part := range file.parts {
		part.close()
	}
}
func (file resultFile) hasRemote() bool {
	if file.remote != nil {
		return true
	}
	for _, part := range file.parts {
		if part.hasRemote() {
			return true
		}
	}
	return false
}

type joinedResultReader struct{ parts []resultFile }

func (reader joinedResultReader) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative read offset")
	}
	read := 0
	for _, part := range reader.parts {
		if offset >= part.size {
			offset -= part.size
			continue
		}
		length := min(int64(len(data)-read), part.size-offset)
		n, err := part.file.ReadAt(data[read:read+int(length)], offset)
		read += n
		if err != nil && !(err == io.EOF && int64(n) == length) {
			return read, err
		}
		if int64(n) != length {
			return read, io.ErrUnexpectedEOF
		}
		offset = 0
		if read == len(data) {
			return read, nil
		}
	}
	if read < len(data) {
		return read, io.EOF
	}
	return read, nil
}
func (file *resultFile) resolveRemote(root *os.Root) error {
	if file.remote != nil {
		opened, err := openResultFile(root, file.remote.Object)
		if err != nil {
			return err
		}
		if opened.size != file.size {
			opened.close()
			return errors.New("archived file size changed")
		}
		file.file, file.info = opened.file, opened.info
		file.remote = nil
	}
	for i := range file.parts {
		if err := file.parts[i].resolveRemote(root); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) openNASRun(id string) (*os.Root, error) {
	runs, err := s.openArchiveStore()
	if err != nil {
		return nil, err
	}
	defer runs.Close()
	return openResultDirectory(runs, id)
}

// Persisted status is intentionally small and local, including when the NAS is
// unavailable. It is not part of the original experiment outcome.
func (s *Server) setRunArchiveStatus(id, state string, cause error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	root, err := s.analysisDirectory(id)
	if err != nil {
		return
	}
	defer root.Close()
	status := runArchiveStatus{State: state, UpdatedAt: time.Now().UTC()}
	if cause != nil {
		status.Error = cause.Error()
	}
	if err := s.writeRunArchiveStatus(root, id, status); err != nil {
		s.logger.Warn("save archive status", "run", id, "error", err)
	}
}

// The storage overview exposes only this cheap revision. Browsers reload the
// local result index when it changes, without scanning the NAS or polling every
// completed result continuously. Publish after the local status is committed.
func (s *Server) writeRunArchiveStatus(root *os.Root, id string, status runArchiveStatus) error {
	if err := writeAnalysisJSON(root, runArchiveStatusFile, status); err != nil {
		return err
	}
	s.state.archiveQueueMu.Lock()
	if status.State == "archived" {
		delete(s.state.archiveDirtyNotified, id)
	}
	s.state.resultsRevision.Add(1)
	s.state.archiveQueueMu.Unlock()
	return nil
}

func readRunArchiveStatus(root *os.Root) runArchiveStatus {
	var status runArchiveStatus
	file, err := openResultFile(root, runArchiveStatusFile)
	if err != nil {
		return status
	}
	defer file.close()
	if file.size <= resultMetadataLimit {
		_ = json.NewDecoder(file.reader()).Decode(&status)
	}
	return status
}

func resultStorageStatus(root *os.Root, id string) runArchiveStatus {
	status := readRunArchiveStatus(root)
	manifest, err := readRunArchive(root, id)
	if err != nil {
		return runArchiveStatus{State: "pending", Error: err.Error()}
	}
	if len(manifest.Files) == 0 && len(manifest.Logs) == 0 {
		if status.State == "" {
			status.State = "local"
		}
		return status
	}
	pending := false
	entries, err := localRunEntries(root)
	if err != nil {
		return runArchiveStatus{State: "pending", Error: err.Error()}
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if isRunLog(name) || logSegment(name, "events.jsonl") || logSegment(name, "observations.jsonl") {
			pending = true
			continue
		}
		info, err := entry.Info()
		if err != nil {
			pending = true
			continue
		}
		stored, ok := manifest.Files[name]
		if !ok || stored.Size != info.Size() || stored.ModifiedAt != info.ModTime().UnixNano() {
			pending = true
		}
	}
	if pending {
		if status.State != "archiving" {
			status.State = "pending"
		}
	} else {
		status.State = "archived"
		status.Error = ""
	}
	return status
}

// Limit outstanding NAS readers. A disconnected hard mount may not honor Go
// context cancellation once inside a syscall; it must never exhaust goroutines
// doing more NAS opens or hold the local persistence lock while blocked.
func (s *Server) resolveArchivedFiles(ctx context.Context, id string, files []resultFile) (func(), error) {
	remote := false
	for _, file := range files {
		remote = remote || file.hasRemote()
	}
	if !remote {
		return nil, nil
	}
	s.state.persistMu.Lock()
	deleted, err := s.state.resultDeletedLocked(id)
	if err == nil && !deleted {
		s.resultReadPins[id]++
	}
	s.state.persistMu.Unlock()
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, errResultNotFound
	}
	defer func() {
		s.state.persistMu.Lock()
		s.resultReadPins[id]--
		if s.resultReadPins[id] == 0 {
			delete(s.resultReadPins, id)
		}
		s.state.persistMu.Unlock()
	}()
	select {
	case s.archiveReadSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := func() { <-s.archiveReadSlots }
	root, err := s.openNASRun(id)
	if err != nil {
		release()
		return nil, err
	}
	defer root.Close()
	for i := range files {
		if err := files[i].resolveRemote(root); err != nil {
			release()
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
