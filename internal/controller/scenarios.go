package controller

import (
	"bytes"
	"crypto/rand"
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
	"unicode"
	"unicode/utf8"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const (
	scenarioYAMLLimit = 1 << 20
	// A JSON string can encode one source byte as a six-byte Unicode escape.
	// This envelope therefore accepts every valid 1 MiB scenario regardless
	// of the JSON escaping chosen by the client.
	scenarioJSONRequestBodyLimit = 6*scenarioYAMLLimit + (64 << 10)
	scenarioNameRuneLimit        = 128
	// JSON can expand control characters to six-byte escape sequences. The
	// stored limit must accept every YAML document admitted by the API.
	scenarioRecordLimit = 6*scenarioYAMLLimit + (64 << 10)
)

var errScenarioNotFound = errors.New("saved scenario not found")

var errScenarioRecordChanged = errors.New("scenario record changed while reading")

type savedScenarioSummary struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type savedScenario struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	YAML      string    `json:"yaml"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type scenarioSubmission struct {
	Name string `json:"name"`
	YAML string `json:"yaml"`
}

type scenarioSummaryCacheEntry struct {
	info    os.FileInfo
	summary savedScenarioSummary
}

type scenarioSummaryFlight struct {
	info    os.FileInfo
	done    chan struct{}
	summary savedScenarioSummary
	err     error
}

func (s *Server) handleScenarios(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items, err := s.listSavedScenarios()
		if err != nil {
			s.logger.Error("list saved scenarios", "error", err)
			writeError(w, http.StatusInternalServerError, "cannot list saved scenarios")
			return
		}
		writeJSON(w, http.StatusOK, items)
	case http.MethodPost:
		submission, err := decodeScenarioSubmission(w, r)
		if err != nil {
			writeScenarioRequestError(w, err)
			return
		}
		item, err := s.createSavedScenario(submission)
		if err != nil {
			s.logger.Error("save scenario", "error", err)
			writeError(w, http.StatusInternalServerError, "cannot save scenario")
			return
		}
		writeJSON(w, http.StatusCreated, item)
	default:
		methodNotAllowed(w)
	}
}

func (s *Server) handleScenarioAction(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/scenarios/")
	if !validScenarioID(id) {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		item, err := s.getSavedScenario(id)
		if err != nil {
			s.writeScenarioStorageError(w, id, "load", err)
			return
		}
		writeJSON(w, http.StatusOK, item)
	case http.MethodPut:
		submission, err := decodeScenarioSubmission(w, r)
		if err != nil {
			writeScenarioRequestError(w, err)
			return
		}
		item, err := s.updateSavedScenario(id, submission)
		if err != nil {
			s.writeScenarioStorageError(w, id, "update", err)
			return
		}
		writeJSON(w, http.StatusOK, item)
	case http.MethodDelete:
		if err := s.deleteSavedScenario(id); err != nil {
			s.writeScenarioStorageError(w, id, "delete", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w)
	}
}

func (s *Server) writeScenarioStorageError(w http.ResponseWriter, id, operation string, err error) {
	if errors.Is(err, errScenarioNotFound) {
		writeError(w, http.StatusNotFound, errScenarioNotFound.Error())
		return
	}
	s.logger.Error(operation+" saved scenario", "scenario", id, "error", err)
	writeError(w, http.StatusInternalServerError, "cannot "+operation+" saved scenario")
}

func writeScenarioRequestError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "scenario request is too large")
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func decodeScenarioSubmission(w http.ResponseWriter, r *http.Request) (scenarioSubmission, error) {
	var submission scenarioSubmission
	raw, err := readLimitedRequestBody(w, r, scenarioJSONRequestBodyLimit)
	if err != nil {
		return submission, err
	}
	if !utf8.Valid(raw) {
		return submission, errors.New("scenario request must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&submission); err != nil {
		return submission, fmt.Errorf("invalid JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return submission, errors.New("scenario request contains trailing data")
		}
		return submission, fmt.Errorf("invalid JSON: %w", err)
	}
	name, err := validateScenarioSubmission(submission.Name, submission.YAML)
	if err != nil {
		return submission, err
	}
	submission.Name = name
	return submission, nil
}

func readLimitedRequestBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	return io.ReadAll(r.Body)
}

func validateScenarioSubmission(name, source string) (string, error) {
	name, err := validateScenarioName(name)
	if err != nil {
		return "", err
	}
	if err := validateScenarioSource(source, func(data []byte) error {
		_, err := scenario.Parse(data)
		return err
	}); err != nil {
		return "", err
	}
	return name, nil
}

func validateScenarioName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("scenario name is required")
	}
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > scenarioNameRuneLimit {
		return "", fmt.Errorf("scenario name must be valid UTF-8 and at most %d characters", scenarioNameRuneLimit)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", errors.New("scenario name cannot contain control characters")
		}
	}
	return name, nil
}

func validateScenarioSource(source string, check func([]byte) error) error {
	if len(source) == 0 {
		return errors.New("scenario YAML is required")
	}
	if len(source) > scenarioYAMLLimit {
		return fmt.Errorf("scenario YAML cannot exceed %d bytes", scenarioYAMLLimit)
	}
	if err := check([]byte(source)); err != nil {
		return fmt.Errorf("invalid scenario YAML: %w", err)
	}
	return nil
}

func validScenarioID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, ch := range id {
		if ch < '0' || ch > '9' {
			if ch < 'a' || ch > 'f' {
				return false
			}
		}
	}
	return true
}

func newScenarioID() (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(rand.Reader, value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (s *Server) createSavedScenario(submission scenarioSubmission) (savedScenario, error) {
	s.scenarioMu.Lock()
	defer s.scenarioMu.Unlock()
	root, err := s.openScenarioDirectory(true)
	if err != nil {
		return savedScenario{}, err
	}
	defer root.Close()
	for attempts := 0; attempts < 4; attempts++ {
		id, err := newScenarioID()
		if err != nil {
			return savedScenario{}, fmt.Errorf("generate scenario id: %w", err)
		}
		now := time.Now().UTC()
		item := savedScenario{ID: id, Name: submission.Name, YAML: submission.YAML, CreatedAt: now, UpdatedAt: now}
		info, err := writeScenarioRecord(root, item, true)
		if errors.Is(err, os.ErrExist) {
			continue
		} else if err != nil {
			return savedScenario{}, err
		}
		s.cacheScenarioSummary(item, info)
		return item, nil
	}
	return savedScenario{}, errors.New("cannot allocate a unique scenario id")
}

func (s *Server) listSavedScenarios() ([]savedScenarioSummary, error) {
	root, err := s.openScenarioDirectory(false)
	if errors.Is(err, os.ErrNotExist) {
		return []savedScenarioSummary{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	items := make([]savedScenarioSummary, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		id := strings.TrimSuffix(name, ".json")
		if !validScenarioID(id) {
			continue
		}
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect scenario %s: %w", id, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("inspect scenario %s: record is not a regular file", id)
		}
		// Each list still checks the file's identity, length, and modification
		// time. Only unchanged, previously parsed records use the summary cache;
		// atomic replacements by this or another Controller invalidate it. Cold
		// readers of that same identity share one validation flight.
		summary, err := s.loadScenarioSummary(root, id, info)
		if errors.Is(err, errScenarioNotFound) {
			// A concurrent delete after ReadDir simply omits the item from this
			// snapshot. Every record that is returned was fully validated.
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read scenario %s: %w", id, err)
		}
		items = append(items, summary)
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		return items[i].ID < items[j].ID
	})
	return items, nil
}

func (s *Server) getSavedScenario(id string) (savedScenario, error) {
	s.scenarioMu.Lock()
	defer s.scenarioMu.Unlock()
	root, err := s.openScenarioDirectory(false)
	if errors.Is(err, os.ErrNotExist) {
		return savedScenario{}, errScenarioNotFound
	}
	if err != nil {
		return savedScenario{}, err
	}
	defer root.Close()
	item, info, err := s.readScenarioRecord(root, id)
	if err == nil {
		s.cacheScenarioSummary(item, info)
	}
	return item, err
}

func (s *Server) updateSavedScenario(id string, submission scenarioSubmission) (savedScenario, error) {
	s.scenarioMu.Lock()
	defer s.scenarioMu.Unlock()
	root, err := s.openScenarioDirectory(false)
	if errors.Is(err, os.ErrNotExist) {
		return savedScenario{}, errScenarioNotFound
	}
	if err != nil {
		return savedScenario{}, err
	}
	defer root.Close()
	current, _, err := s.readScenarioRecord(root, id)
	if err != nil {
		return savedScenario{}, err
	}
	current.Name = submission.Name
	current.YAML = submission.YAML
	now := time.Now().UTC()
	if !now.After(current.UpdatedAt) {
		now = current.UpdatedAt.Add(time.Nanosecond)
	}
	current.UpdatedAt = now
	info, err := writeScenarioRecord(root, current, false)
	if err != nil {
		return savedScenario{}, err
	}
	s.cacheScenarioSummary(current, info)
	return current, nil
}

func (s *Server) deleteSavedScenario(id string) error {
	s.scenarioMu.Lock()
	defer s.scenarioMu.Unlock()
	root, err := s.openScenarioDirectory(false)
	if errors.Is(err, os.ErrNotExist) {
		return errScenarioNotFound
	}
	if err != nil {
		return err
	}
	defer root.Close()
	info, err := root.Lstat(id + ".json")
	if errors.Is(err, os.ErrNotExist) {
		return errScenarioNotFound
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("scenario record is not a regular file")
	}
	if err := root.Remove(id + ".json"); errors.Is(err, os.ErrNotExist) {
		return errScenarioNotFound
	} else {
		if err == nil {
			s.removeCachedScenarioSummary(id)
		}
		return err
	}
}

func scenarioSummary(item savedScenario) savedScenarioSummary {
	return savedScenarioSummary{ID: item.ID, Name: item.Name, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt}
}

func sameScenarioRecord(left, right os.FileInfo) bool {
	return left != nil && right != nil && left.Mode().IsRegular() && right.Mode().IsRegular() &&
		left.Size() == right.Size() && left.ModTime().Equal(right.ModTime()) && os.SameFile(left, right)
}

func (s *Server) loadScenarioSummary(root *os.Root, id string, info os.FileInfo) (savedScenarioSummary, error) {
	for attempts := 0; attempts < 4; attempts++ {
		if !info.Mode().IsRegular() {
			return savedScenarioSummary{}, errors.New("scenario record is not a regular file")
		}
		summary, flight, leader := s.acquireScenarioSummaryFlight(id, info)
		if flight == nil {
			return summary, nil
		}
		var err error
		if leader {
			summary, err = s.runScenarioSummaryFlight(root, id, flight)
		} else {
			<-flight.done
			summary, err = flight.summary, flight.err
		}
		if !errors.Is(err, errScenarioRecordChanged) {
			return summary, err
		}
		current, statErr := root.Lstat(id + ".json")
		if errors.Is(statErr, os.ErrNotExist) {
			return savedScenarioSummary{}, errScenarioNotFound
		}
		if statErr != nil {
			return savedScenarioSummary{}, statErr
		}
		info = current
	}
	return savedScenarioSummary{}, errScenarioRecordChanged
}

// acquireScenarioSummaryFlight holds the cache mutex only for map access. The
// leader performs all file I/O and YAML parsing after this function returns.
func (s *Server) acquireScenarioSummaryFlight(id string, info os.FileInfo) (savedScenarioSummary, *scenarioSummaryFlight, bool) {
	s.scenarioCacheMu.Lock()
	defer s.scenarioCacheMu.Unlock()
	for _, entry := range s.scenarioSummaries[id] {
		if sameScenarioRecord(entry.info, info) {
			return entry.summary, nil, false
		}
	}
	for _, flight := range s.scenarioSummaryFlights[id] {
		if sameScenarioRecord(flight.info, info) {
			return savedScenarioSummary{}, flight, false
		}
	}
	flight := &scenarioSummaryFlight{info: info, done: make(chan struct{})}
	s.scenarioSummaryFlights[id] = append(s.scenarioSummaryFlights[id], flight)
	return savedScenarioSummary{}, flight, true
}

func (s *Server) runScenarioSummaryFlight(root *os.Root, id string, flight *scenarioSummaryFlight) (summary savedScenarioSummary, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			summary = savedScenarioSummary{}
			err = fmt.Errorf("scenario record validation panicked: %v", recovered)
		}
		s.finishScenarioSummaryFlight(id, flight, summary, err)
	}()
	item, recordInfo, err := s.readScenarioRecordExpected(root, id, flight.info)
	if err != nil {
		return savedScenarioSummary{}, err
	}
	if !sameScenarioRecord(recordInfo, flight.info) {
		return savedScenarioSummary{}, errScenarioRecordChanged
	}
	// Parsing a large YAML document can take long enough for an atomic replace
	// to complete after readScenarioRecordExpected's final path check.
	current, statErr := root.Lstat(id + ".json")
	if errors.Is(statErr, os.ErrNotExist) {
		return savedScenarioSummary{}, errScenarioNotFound
	}
	if statErr != nil {
		return savedScenarioSummary{}, statErr
	}
	if !sameScenarioRecord(recordInfo, current) {
		return savedScenarioSummary{}, errScenarioRecordChanged
	}
	return scenarioSummary(item), nil
}

func (s *Server) finishScenarioSummaryFlight(id string, flight *scenarioSummaryFlight, summary savedScenarioSummary, flightErr error) {
	s.scenarioCacheMu.Lock()
	defer s.scenarioCacheMu.Unlock()
	flight.summary, flight.err = summary, flightErr
	if flightErr == nil {
		s.storeScenarioSummaryLocked(id, scenarioSummaryCacheEntry{info: flight.info, summary: summary})
	}
	flights := s.scenarioSummaryFlights[id]
	for index, current := range flights {
		if current != flight {
			continue
		}
		flights = append(flights[:index], flights[index+1:]...)
		break
	}
	if len(flights) == 0 {
		delete(s.scenarioSummaryFlights, id)
	} else {
		s.scenarioSummaryFlights[id] = flights
	}
	close(flight.done)
}

func (s *Server) cacheScenarioSummary(item savedScenario, info os.FileInfo) {
	if info == nil {
		return
	}
	s.scenarioCacheMu.Lock()
	s.storeScenarioSummaryLocked(item.ID, scenarioSummaryCacheEntry{info: info, summary: scenarioSummary(item)})
	s.scenarioCacheMu.Unlock()
}

func (s *Server) storeScenarioSummaryLocked(id string, entry scenarioSummaryCacheEntry) {
	entries := s.scenarioSummaries[id]
	for index := range entries {
		if sameScenarioRecord(entries[index].info, entry.info) {
			entries[index] = entry
			s.scenarioSummaries[id] = entries
			return
		}
	}
	// Multiple identities can be validated concurrently while another
	// Controller atomically replaces a record. Retaining a small version set
	// prevents completion order from making the current identity cold again.
	entries = append(entries, entry)
	if len(entries) > 4 {
		entries = entries[len(entries)-4:]
	}
	s.scenarioSummaries[id] = entries
}

func (s *Server) removeCachedScenarioSummary(id string) {
	s.scenarioCacheMu.Lock()
	delete(s.scenarioSummaries, id)
	s.scenarioCacheMu.Unlock()
}

func (s *Server) openScenarioDirectory(create bool) (*os.Root, error) {
	if create {
		if err := os.MkdirAll(s.config.DataDir, 0o755); err != nil {
			return nil, err
		}
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	info, err := data.Lstat("scenarios")
	if errors.Is(err, os.ErrNotExist) && create {
		if err := data.Mkdir("scenarios", 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		info, err = data.Lstat("scenarios")
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("scenario storage is not a regular directory")
	}
	root, err := data.OpenRoot("scenarios")
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = root.Close()
		return nil, errors.New("scenario storage changed while opening")
	}
	return root, nil
}

func (s *Server) readScenarioRecord(root *os.Root, id string) (savedScenario, os.FileInfo, error) {
	for attempts := 0; attempts < 3; attempts++ {
		item, info, err := s.readScenarioRecordOnce(root, id, nil)
		if !errors.Is(err, errScenarioRecordChanged) {
			return item, info, err
		}
	}
	return savedScenario{}, nil, errScenarioRecordChanged
}

func (s *Server) readScenarioRecordExpected(root *os.Root, id string, expected os.FileInfo) (savedScenario, os.FileInfo, error) {
	return s.readScenarioRecordOnce(root, id, expected)
}

func (s *Server) readScenarioRecordOnce(root *os.Root, id string, expected os.FileInfo) (savedScenario, os.FileInfo, error) {
	var item savedScenario
	filename := id + ".json"
	info, err := root.Lstat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return item, nil, errScenarioNotFound
	}
	if err != nil {
		return item, nil, err
	}
	if !info.Mode().IsRegular() {
		return item, nil, errors.New("scenario record is not a regular file")
	}
	if expected != nil && !sameScenarioRecord(expected, info) {
		return item, nil, errScenarioRecordChanged
	}
	if info.Size() < 0 || info.Size() > scenarioRecordLimit {
		return item, nil, errors.New("scenario record is too large")
	}
	file, err := root.Open(filename)
	if err != nil {
		return item, nil, err
	}
	opened, err := file.Stat()
	if err != nil || !sameScenarioRecord(info, opened) {
		_ = file.Close()
		return item, nil, errScenarioRecordChanged
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, scenarioRecordLimit+1))
	finished, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return item, nil, readErr
	}
	if statErr != nil || !sameScenarioRecord(opened, finished) {
		return item, nil, errScenarioRecordChanged
	}
	if closeErr != nil {
		return item, nil, closeErr
	}
	current, err := root.Lstat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return item, nil, errScenarioNotFound
	}
	if err != nil {
		return item, nil, err
	}
	if !sameScenarioRecord(finished, current) {
		return item, nil, errScenarioRecordChanged
	}
	if len(raw) > scenarioRecordLimit {
		return item, nil, errors.New("scenario record is too large")
	}
	if !utf8.Valid(raw) {
		return item, nil, errors.New("scenario record must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&item)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if decodeErr != nil {
		return item, nil, fmt.Errorf("decode scenario record: %w", decodeErr)
	}
	if !errors.Is(trailingErr, io.EOF) {
		return item, nil, errors.New("scenario record contains trailing data")
	}
	if item.ID != id || item.CreatedAt.IsZero() || item.UpdatedAt.IsZero() || item.UpdatedAt.Before(item.CreatedAt) {
		return item, nil, errors.New("scenario record has invalid metadata")
	}
	name, err := validateScenarioName(item.Name)
	if err != nil || name != item.Name {
		return item, nil, errors.New("scenario record has invalid name")
	}
	check := s.scenarioRecordCheck
	if check == nil {
		check = func(data []byte) error {
			_, err := scenario.Parse(data)
			return err
		}
	}
	if err := validateScenarioSource(item.YAML, check); err != nil {
		return item, nil, fmt.Errorf("scenario record has invalid YAML: %w", err)
	}
	return item, current, nil
}

func writeScenarioRecord(root *os.Root, item savedScenario, create bool) (os.FileInfo, error) {
	data, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	tempID, err := newScenarioID()
	if err != nil {
		return nil, err
	}
	tempName := ".kpl-scenario-" + tempID
	file, err := root.OpenFile(tempName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = root.Remove(tempName)
	}()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, err
	}
	written, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	target := item.ID + ".json"
	if create {
		// Linking a complete temporary record provides atomic create-if-absent
		// semantics, so even another Controller cannot overwrite an ID collision.
		if err := root.Link(tempName, target); err != nil {
			return nil, err
		}
	} else if err := root.Rename(tempName, target); err != nil {
		return nil, err
	}
	committed, err := root.Lstat(target)
	if err != nil {
		return nil, err
	}
	if !sameScenarioRecord(written, committed) {
		return nil, errScenarioRecordChanged
	}
	return committed, nil
}
