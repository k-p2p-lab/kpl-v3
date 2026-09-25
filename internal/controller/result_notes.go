package controller

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

const resultNoteFile = "note.json"
const resultNoteTextLimit = 16 << 10

// JSON escaping can expand each input byte to six bytes.
const resultNoteFileLimit = 6*resultNoteTextLimit + 4096

var errResultNoteConflict = errors.New("this note changed in another window; load the latest note before saving")

type resultNote struct {
	RunID     string     `json:"runId,omitempty"`
	BatchID   string     `json:"batchId,omitempty"`
	Text      string     `json:"text"`
	Revision  string     `json:"revision"`
	UpdatedAt *time.Time `json:"updatedAt,omitempty"`
}

type resultNoteSummary struct {
	Preview   string     `json:"preview"`
	UpdatedAt *time.Time `json:"updatedAt,omitempty"`
}

func (note resultNote) summary() *resultNoteSummary {
	if strings.TrimSpace(note.Text) == "" {
		return nil
	}
	preview := []rune(strings.Join(strings.Fields(note.Text), " "))
	if len(preview) > 160 {
		preview = append(preview[:160], '…')
	}
	return &resultNoteSummary{Preview: string(preview), UpdatedAt: note.UpdatedAt}
}

func readResultNote(root *os.Root, id string) (resultNote, error) {
	return readScopedNote(root, id, false)
}

func emptyResultNote(id string, group bool) resultNote {
	note := resultNote{Revision: "0"}
	if group {
		note.BatchID = id
	} else {
		note.RunID = id
	}
	return note
}

func readScopedNote(root *os.Root, id string, group bool) (resultNote, error) {
	file, err := openResultFile(root, resultNoteFile)
	if errors.Is(err, os.ErrNotExist) {
		return emptyResultNote(id, group), nil
	}
	if err != nil {
		return resultNote{}, err
	}
	return decodeScopedNote(file, id, group)
}

// The caller owns the captured descriptor; decode it after releasing persistMu
// when listing results, so note previews do not delay experiment writes.
func decodeResultNote(file resultFile, id string) (resultNote, error) {
	return decodeScopedNote(file, id, false)
}

func decodeScopedNote(file resultFile, id string, group bool) (resultNote, error) {
	defer file.file.Close()
	if file.size > resultNoteFileLimit {
		return resultNote{}, errors.New("saved note is too large")
	}
	var note resultNote
	decoder := json.NewDecoder(io.NewSectionReader(file.file, 0, file.size))
	if err := decoder.Decode(&note); err != nil {
		return resultNote{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return resultNote{}, errors.New("invalid saved note JSON")
	}
	validIdentity := note.RunID == id && note.BatchID == ""
	if group {
		validIdentity = note.BatchID == id && note.RunID == ""
	}
	if !validIdentity || note.Revision == "" || note.Revision == "0" || note.UpdatedAt == nil || note.UpdatedAt.IsZero() || len(note.Text) > resultNoteTextLimit {
		return resultNote{}, errors.New("invalid saved note")
	}
	return note, nil
}

// Serialize note changes with run deletion and manifest writes. Notes live in a
// separate file so finalizing or retrying an experiment cannot overwrite them.
func (s *Server) resultNote(id string, text *string, revision string) (resultNote, error) {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	root, err := s.analysisDirectory(id)
	if err != nil {
		return resultNote{}, err
	}
	defer root.Close()
	note, err := saveScopedNote(root, id, false, text, revision)
	if err == nil && text != nil {
		s.state.markRunArchiveDirty(id)
	}
	return note, err
}

func saveScopedNote(root *os.Root, id string, group bool, text *string, revision string) (resultNote, error) {
	note, err := readScopedNote(root, id, group)
	if err != nil || text == nil {
		return note, err
	}
	if note.Revision != revision {
		return resultNote{}, errResultNoteConflict
	}
	if note.Text == *text {
		return note, nil
	}
	now := time.Now().UTC()
	note = emptyResultNote(id, group)
	note.Text, note.Revision, note.UpdatedAt = *text, rand.Text(), &now
	// Retain a revision after clearing so stale editors cannot restore old text.
	if err := writeAnalysisJSON(root, resultNoteFile, note); err != nil {
		return resultNote{}, err
	}
	return note, nil
}

func (s *Server) handleResultNote(w http.ResponseWriter, r *http.Request, id string) {
	s.handleStoredNote(w, r, id, func(text *string, revision string) (resultNote, error) {
		return s.resultNote(id, text, revision)
	})
}

func (s *Server) handleStoredNote(w http.ResponseWriter, r *http.Request, id string, access func(*string, string) (resultNote, error)) {
	if !validResultID(id) {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		w.Header().Set("Allow", "GET, PUT")
		methodNotAllowed(w)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	var request struct {
		Text     *string `json:"text"`
		Revision *string `json:"revision"`
	}
	if r.Method == http.MethodPut {
		r.Body = http.MaxBytesReader(w, r.Body, resultNoteFileLimit)
		if err := decodeJSON(w, r, &request); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "note request is too large")
			} else {
				writeError(w, http.StatusBadRequest, err.Error())
			}
			return
		}
		if request.Text == nil || request.Revision == nil || *request.Revision == "" {
			writeError(w, http.StatusBadRequest, "text and revision are required; load the note before editing")
			return
		}
		if !utf8.ValidString(*request.Text) || len(*request.Text) > resultNoteTextLimit {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("note must be valid UTF-8 and at most %d bytes", resultNoteTextLimit))
			return
		}
		if strings.TrimSpace(*request.Text) == "" {
			*request.Text = ""
		}
	}
	revision := ""
	if request.Revision != nil {
		revision = *request.Revision
	}
	note, err := access(request.Text, revision)
	if err != nil {
		switch {
		case errors.Is(err, errResultNotFound):
			http.NotFound(w, r)
		case errors.Is(err, errResultNoteConflict):
			writeError(w, http.StatusConflict, err.Error())
		default:
			s.logger.Error("access result note", "id", id, "error", err)
			writeError(w, http.StatusInternalServerError, "cannot read or save the note; check result storage and retry")
		}
		return
	}
	writeJSON(w, http.StatusOK, note)
}
