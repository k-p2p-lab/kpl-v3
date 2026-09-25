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
	RunID     string     `json:"runId"`
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
	file, err := openResultFile(root, resultNoteFile)
	if errors.Is(err, os.ErrNotExist) {
		return resultNote{RunID: id, Revision: "0"}, nil
	}
	if err != nil {
		return resultNote{}, err
	}
	return decodeResultNote(file, id)
}

// The caller owns the captured descriptor; decode it after releasing persistMu
// when listing results, so note previews do not delay experiment writes.
func decodeResultNote(file resultFile, id string) (resultNote, error) {
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
	if note.RunID != id || note.Revision == "" || note.Revision == "0" || note.UpdatedAt == nil || note.UpdatedAt.IsZero() || len(note.Text) > resultNoteTextLimit {
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
	note, err := readResultNote(root, id)
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
	note = resultNote{RunID: id, Text: *text, Revision: rand.Text(), UpdatedAt: &now}
	// Keep an empty note's revision as well: a stale editor must not resurrect
	// text after somebody has cleared it. Publish atomically for ZIP snapshots.
	if err := writeAnalysisJSON(root, resultNoteFile, note); err != nil {
		return resultNote{}, err
	}
	s.state.markRunArchiveDirty(id)
	return note, nil
}

func (s *Server) handleResultNote(w http.ResponseWriter, r *http.Request, id string) {
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
	note, err := s.resultNote(id, request.Text, revision)
	if err != nil {
		switch {
		case errors.Is(err, errResultNotFound):
			http.NotFound(w, r)
		case errors.Is(err, errResultNoteConflict):
			writeError(w, http.StatusConflict, err.Error())
		default:
			s.logger.Error("access result note", "run", id, "error", err)
			writeError(w, http.StatusInternalServerError, "cannot read or save the note; check result storage and retry")
		}
		return
	}
	writeJSON(w, http.StatusOK, note)
}
