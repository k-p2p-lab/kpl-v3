package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	libraryGroupsFile         = "library-groups.json"
	libraryGroupsFileLimit    = 4 << 20
	libraryGroupsRequestLimit = 2 << 20
	libraryGroupLimit         = 1000
	libraryMembershipLimit    = 20000
	libraryMoveLimit          = 2000
	libraryGroupNameLimit     = 128
)

var (
	errLibraryGroupsConflict  = errors.New("groups changed in another window; refresh before saving")
	errLibraryGroupNameExists = errors.New("a group with this name already exists")
	errLibraryGroupNotFound   = errors.New("library group not found")
	errLibraryMemberNotFound  = errors.New("a selected item no longer exists; refresh before moving items")
)

type libraryGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type libraryGroupsSnapshot struct {
	Revision    string            `json:"revision"`
	Groups      []libraryGroup    `json:"groups"`
	Memberships map[string]string `json:"memberships"`
}

type libraryGroupsRecord struct {
	Version int `json:"version"`
	libraryGroupsSnapshot
}

type libraryGroupsRequest struct {
	Name     *string  `json:"name,omitempty"`
	GroupID  *string  `json:"groupId,omitempty"`
	Keys     []string `json:"keys,omitempty"`
	Revision string   `json:"revision"`
}

func emptyLibraryGroups() libraryGroupsSnapshot {
	return libraryGroupsSnapshot{Revision: "0", Groups: []libraryGroup{}, Memberships: map[string]string{}}
}

func validLibraryMemberKey(key string) bool {
	kind, id, ok := strings.Cut(key, ":")
	if !ok {
		return false
	}
	if kind == "scenario" {
		return validScenarioID(id)
	}
	return (kind == "batch" || kind == "run") && validResultID(id)
}

func validateLibraryGroupName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > libraryGroupNameLimit {
		return "", fmt.Errorf("group name must contain 1 to %d UTF-8 characters", libraryGroupNameLimit)
	}
	for _, ch := range name {
		if unicode.IsControl(ch) {
			return "", errors.New("group name cannot contain control characters")
		}
	}
	return name, nil
}

// Library metadata lives directly in the local data directory. It never becomes
// an experiment input, archive member, or analysis hash input. All accesses use
// libraryGroupsMu. Group endpoints do not acquire experiment, archive or
// scenario locks; initial admission can hold cancelMu/persistMu before this lock.
func (s *Server) readLibraryGroups() (libraryGroupsSnapshot, error) {
	data, err := os.OpenRoot(s.config.DataDir)
	if errors.Is(err, os.ErrNotExist) {
		return emptyLibraryGroups(), nil
	}
	if err != nil {
		return libraryGroupsSnapshot{}, err
	}
	defer data.Close()
	file, err := openResultFile(data, libraryGroupsFile)
	if errors.Is(err, os.ErrNotExist) {
		return emptyLibraryGroups(), nil
	}
	if err != nil {
		return libraryGroupsSnapshot{}, err
	}
	defer file.file.Close()
	if file.size < 0 || file.size > libraryGroupsFileLimit {
		return libraryGroupsSnapshot{}, errors.New("library metadata is too large")
	}
	raw, err := io.ReadAll(io.LimitReader(file.file, libraryGroupsFileLimit+1))
	if err != nil {
		return libraryGroupsSnapshot{}, err
	}
	if len(raw) > libraryGroupsFileLimit || !utf8.Valid(raw) {
		return libraryGroupsSnapshot{}, errors.New("invalid library metadata encoding or size")
	}
	var record libraryGroupsRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return libraryGroupsSnapshot{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return libraryGroupsSnapshot{}, errors.New("invalid library metadata JSON")
	}
	if record.Version != 1 || record.Revision == "" || record.Revision == "0" || len(record.Revision) > 128 || record.Groups == nil || record.Memberships == nil || len(record.Groups) > libraryGroupLimit || len(record.Memberships) > libraryMembershipLimit {
		return libraryGroupsSnapshot{}, errors.New("invalid library metadata")
	}
	ids := map[string]bool{}
	for i, group := range record.Groups {
		name, err := validateLibraryGroupName(group.Name)
		if err != nil || name != group.Name || !validScenarioID(group.ID) || ids[group.ID] {
			return libraryGroupsSnapshot{}, errors.New("invalid saved library group")
		}
		for _, prior := range record.Groups[:i] {
			if strings.EqualFold(prior.Name, name) {
				return libraryGroupsSnapshot{}, errors.New("duplicate saved library group name")
			}
		}
		ids[group.ID] = true
	}
	for key, id := range record.Memberships {
		if !validLibraryMemberKey(key) || !ids[id] {
			return libraryGroupsSnapshot{}, errors.New("invalid saved library membership")
		}
	}
	return record.libraryGroupsSnapshot, nil
}

func (s *Server) writeLibraryGroups(snapshot libraryGroupsSnapshot) error {
	record := libraryGroupsRecord{Version: 1, libraryGroupsSnapshot: snapshot}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if len(raw)+1 > libraryGroupsFileLimit {
		return errors.New("library metadata is too large")
	}
	if err := os.MkdirAll(s.config.DataDir, 0755); err != nil {
		return err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	return writeAnalysisJSON(data, libraryGroupsFile, record)
}

func decodeLibraryGroupsRequest(w http.ResponseWriter, r *http.Request, action string) (libraryGroupsRequest, error) {
	var request libraryGroupsRequest
	raw, err := readLimitedRequestBody(w, r, libraryGroupsRequestLimit)
	if err != nil {
		return request, err
	}
	if !utf8.Valid(raw) {
		return request, errors.New("group request must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return request, errors.New("group request contains trailing data")
	}
	if request.Revision == "" || len(request.Revision) > 128 {
		return request, errors.New("revision is required; load groups before editing")
	}
	switch action {
	case "create", "rename":
		if request.Name == nil || request.GroupID != nil || request.Keys != nil {
			return request, errors.New("name and revision are required")
		}
		name, err := validateLibraryGroupName(*request.Name)
		if err != nil {
			return request, err
		}
		request.Name = &name
	case "move":
		if request.Name != nil || request.GroupID == nil || (*request.GroupID != "" && !validScenarioID(*request.GroupID)) || len(request.Keys) == 0 || len(request.Keys) > libraryMoveLimit {
			return request, fmt.Errorf("groupId and 1 to %d keys are required", libraryMoveLimit)
		}
		for _, key := range request.Keys {
			if !validLibraryMemberKey(key) {
				return request, errors.New("invalid library member key")
			}
		}
	case "delete":
		if request.Name != nil || request.GroupID != nil || request.Keys != nil {
			return request, errors.New("only revision is accepted when deleting a group")
		}
	}
	return request, nil
}

func (s *Server) handleLibraryGroups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/library-groups")
	action, id := "", ""
	switch {
	case path == "":
		switch r.Method {
		case http.MethodGet:
			action = "list"
		case http.MethodPost:
			action = "create"
		default:
			w.Header().Set("Allow", "GET, POST")
			methodNotAllowed(w)
			return
		}
	case path == "/members":
		if r.Method != http.MethodPut {
			w.Header().Set("Allow", "PUT")
			methodNotAllowed(w)
			return
		}
		action = "move"
	default:
		id = strings.TrimPrefix(path, "/")
		if !validScenarioID(id) {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodPatch:
			action = "rename"
		case http.MethodDelete:
			action = "delete"
		default:
			w.Header().Set("Allow", "PATCH, DELETE")
			methodNotAllowed(w)
			return
		}
	}
	var request libraryGroupsRequest
	if action != "list" {
		var err error
		request, err = decodeLibraryGroupsRequest(w, r, action)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "group request is too large")
			} else {
				writeError(w, http.StatusBadRequest, err.Error())
			}
			return
		}
	}
	snapshot, err := s.changeLibraryGroups(r.Context(), action, id, request)
	if err != nil {
		switch {
		case errors.Is(err, errLibraryGroupsConflict), errors.Is(err, errLibraryGroupNameExists):
			writeError(w, http.StatusConflict, err.Error())
		case errors.Is(err, errLibraryGroupNotFound), errors.Is(err, errLibraryMemberNotFound):
			writeError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, errLibraryGroupsLimit):
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			s.logger.Error("access library groups", "action", action, "error", err)
			writeError(w, http.StatusInternalServerError, "cannot read or save groups; check local Controller storage and retry")
		}
		return
	}
	status := http.StatusOK
	if action == "create" {
		status = http.StatusCreated
	}
	writeJSON(w, status, snapshot)
}

var errLibraryGroupsLimit = errors.New("library group or membership limit reached")

func (s *Server) changeLibraryGroups(ctx context.Context, action, id string, request libraryGroupsRequest) (libraryGroupsSnapshot, error) {
	s.libraryGroupsMu.Lock()
	defer s.libraryGroupsMu.Unlock()
	snapshot, err := s.readLibraryGroups()
	if err != nil || action == "list" {
		return snapshot, err
	}
	if snapshot.Revision != request.Revision {
		return libraryGroupsSnapshot{}, errLibraryGroupsConflict
	}
	if err := ctx.Err(); err != nil {
		return libraryGroupsSnapshot{}, err
	}
	index := -1
	for i, group := range snapshot.Groups {
		if group.ID == id {
			index = i
		}
		if request.Name != nil && group.ID != id && strings.EqualFold(group.Name, *request.Name) {
			return libraryGroupsSnapshot{}, errLibraryGroupNameExists
		}
	}
	changed := false
	switch action {
	case "create":
		if len(snapshot.Groups) >= libraryGroupLimit {
			return libraryGroupsSnapshot{}, errLibraryGroupsLimit
		}
		id, err := newScenarioID()
		if err != nil {
			return libraryGroupsSnapshot{}, err
		}
		snapshot.Groups = append(snapshot.Groups, libraryGroup{ID: id, Name: *request.Name})
		changed = true
	case "rename":
		if index < 0 {
			return libraryGroupsSnapshot{}, errLibraryGroupNotFound
		}
		changed = snapshot.Groups[index].Name != *request.Name
		snapshot.Groups[index].Name = *request.Name
	case "delete":
		if index < 0 {
			return libraryGroupsSnapshot{}, errLibraryGroupNotFound
		}
		snapshot.Groups = append(snapshot.Groups[:index], snapshot.Groups[index+1:]...)
		for key, group := range snapshot.Memberships {
			if group == id {
				delete(snapshot.Memberships, key)
			}
		}
		changed = true
	case "move":
		group := *request.GroupID
		if group != "" {
			found := false
			for _, item := range snapshot.Groups {
				if item.ID == group {
					found = true
					break
				}
			}
			if !found {
				return libraryGroupsSnapshot{}, errLibraryGroupNotFound
			}
			if err := s.checkLibraryMembers(ctx, request.Keys); err != nil {
				return libraryGroupsSnapshot{}, err
			}
		}
		for _, key := range request.Keys {
			if snapshot.Memberships[key] == group {
				continue
			}
			if group == "" {
				delete(snapshot.Memberships, key)
			} else {
				snapshot.Memberships[key] = group
			}
			changed = true
		}
		if len(snapshot.Memberships) > libraryMembershipLimit {
			return libraryGroupsSnapshot{}, errLibraryGroupsLimit
		}
	}
	if !changed {
		return snapshot, nil
	}
	if err := ctx.Err(); err != nil {
		return libraryGroupsSnapshot{}, err
	}
	snapshot.Revision = rand.Text()
	if err := s.writeLibraryGroups(snapshot); err != nil {
		return libraryGroupsSnapshot{}, err
	}
	return snapshot, nil
}

// The result list and legacy archive importer both retain experiment.json in
// current-run. Checking only these local identities avoids reading logs or NAS
// storage, and does not need any experiment lock. Deletion removes memberships
// afterwards under libraryGroupsMu, closing the check/delete race.
func (s *Server) checkLibraryMembers(ctx context.Context, keys []string) error {
	wantedRuns, wantedBatches := map[string]bool{}, map[string]bool{}
	var scenarios *os.Root
	defer func() {
		if scenarios != nil {
			_ = scenarios.Close()
		}
	}()
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		kind, id, _ := strings.Cut(key, ":")
		switch kind {
		case "scenario":
			if scenarios == nil {
				var err error
				scenarios, err = s.openScenarioDirectory(false)
				if errors.Is(err, os.ErrNotExist) {
					return errLibraryMemberNotFound
				}
				if err != nil {
					return err
				}
			}
			info, err := scenarios.Lstat(id + ".json")
			if errors.Is(err, os.ErrNotExist) {
				return errLibraryMemberNotFound
			}
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return errors.New("scenario member is not a regular file")
			}
		case "run":
			wantedRuns[id] = true
		case "batch":
			wantedBatches[id] = true
		}
	}
	if len(wantedRuns)+len(wantedBatches) == 0 {
		return nil
	}
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return errLibraryMemberNotFound
	}
	if err != nil {
		return err
	}
	defer runs.Close()
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	defer data.Close()
	deleted, err := openResultDirectory(data, ".deleted-results")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if deleted != nil {
		defer deleted.Close()
	}
	dir, err := runs.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(100)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !entry.IsDir() || !validResultID(entry.Name()) || (len(wantedBatches) == 0 && !wantedRuns[entry.Name()]) {
				continue
			}
			if deleted != nil {
				info, err := deleted.Lstat(entry.Name())
				if err == nil {
					if !info.Mode().IsRegular() {
						return errors.New("result deletion marker is not a regular file")
					}
					continue
				}
				if !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			root, err := openResultDirectory(runs, entry.Name())
			if err != nil {
				continue
			}
			file, err := openResultFile(root, "experiment.json")
			_ = root.Close()
			var item savedResult
			if err == nil {
				item, err = readResultMetadata(file, entry.Name(), false)
				_ = file.file.Close()
			}
			if err != nil {
				// The results list presents unreadable metadata as an individual
				// run. Its safe local directory still supports library grouping.
				delete(wantedRuns, entry.Name())
			} else if item.BatchID == "" {
				delete(wantedRuns, item.ID)
			} else {
				delete(wantedBatches, item.BatchID)
			}
			if len(wantedRuns)+len(wantedBatches) == 0 {
				return nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return errLibraryMemberNotFound
		}
		if readErr != nil {
			return readErr
		}
	}
}

// Called after releasing source-storage locks. A cleanup error must not turn a
// successful source deletion into a reported failure or recreate any source.
func (s *Server) removeLibraryMembers(keys ...string) {
	s.removeLibraryMembersAndEmptyBatch("", keys...)
}

func (s *Server) removeLibraryMembersAndEmptyBatch(batchID string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	s.libraryGroupsMu.Lock()
	defer s.libraryGroupsMu.Unlock()
	snapshot, err := s.readLibraryGroups()
	if err != nil {
		s.logger.Warn("clean deleted library memberships", "error", err)
		return
	}
	changed := false
	if batchID != "" && snapshot.Memberships["batch:"+batchID] != "" {
		empty, checkErr := s.libraryBatchEmpty(batchID)
		if checkErr != nil {
			s.logger.Warn("check deleted library batch membership", "batch", batchID, "error", checkErr)
		} else if empty {
			keys = append(keys, "batch:"+batchID)
		}
	}
	for _, key := range keys {
		if _, ok := snapshot.Memberships[key]; ok {
			delete(snapshot.Memberships, key)
			changed = true
		}
	}
	if !changed {
		return
	}
	snapshot.Revision = rand.Text()
	if err := s.writeLibraryGroups(snapshot); err != nil {
		s.logger.Warn("clean deleted library memberships", "error", err)
	}
}

// The caller already holds source-deletion locks. Reading only the local
// manifest captures the batch identity before its last run can be removed.
func (s *Server) libraryResultBatchID(id string) string {
	runs, err := s.openResultRuns()
	if err != nil {
		return ""
	}
	defer runs.Close()
	root, err := openResultDirectory(runs, id)
	if err != nil {
		return ""
	}
	defer root.Close()
	file, err := openResultFile(root, "experiment.json")
	if err != nil {
		return ""
	}
	defer file.file.Close()
	item, err := readResultMetadata(file, id, false)
	if err != nil {
		return ""
	}
	return item.BatchID
}

// Admission and deletion are serialized by cancelMu. Once the last member has
// been deleted, append/retry cannot admit new members for that batch. If an
// admission preceded deletion, its members already exist here. Unreadable or
// disappearing records are inconclusive and retain the membership.
func (s *Server) libraryBatchEmpty(batchID string) (bool, error) {
	runs, err := s.openResultRuns()
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer runs.Close()
	dir, err := runs.Open(".")
	if err != nil {
		return false, err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(100)
		for _, entry := range entries {
			if !entry.IsDir() || !validResultID(entry.Name()) {
				continue
			}
			root, err := openResultDirectory(runs, entry.Name())
			if err != nil {
				return false, err
			}
			file, err := openResultFile(root, "experiment.json")
			_ = root.Close()
			if err != nil {
				return false, err
			}
			item, err := readResultMetadata(file, entry.Name(), false)
			_ = file.file.Close()
			if err != nil {
				return false, err
			}
			if item.BatchID == batchID {
				return false, nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return true, nil
		}
		if readErr != nil {
			return false, readErr
		}
	}
}
