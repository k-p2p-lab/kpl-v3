package controller

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const libraryDownloadLimit = 2000
const libraryDownloadTTL = 10 * time.Minute
const libraryDownloadTicketLimit = 16

type libraryDownload struct {
	Kind      string    `json:"kind"`
	IDs       []string  `json:"ids"`
	ExpiresAt time.Time `json:"-"`
}

// Keep only bounded selections, never open files or complete ZIPs, between the
// authenticated POST and the browser's native streaming download.
func (s *Server) pruneLibraryDownloadsLocked(now time.Time) {
	for id, request := range s.libraryDownloads {
		if !now.Before(request.ExpiresAt) {
			delete(s.libraryDownloads, id)
		}
	}
}

func (s *Server) handleLibraryDownloadRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 512<<10)
	var request libraryDownload
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid download selection")
		return
	}
	if (request.Kind != "scenarios" && request.Kind != "results") || len(request.IDs) == 0 || len(request.IDs) > libraryDownloadLimit {
		writeError(w, http.StatusBadRequest, "select 1–2,000 scenarios or runs per download")
		return
	}
	seen := make(map[string]bool)
	ids, keys := make([]string, 0, len(request.IDs)), make([]string, 0, len(request.IDs))
	for _, id := range request.IDs {
		valid, prefix := validResultID(id), "run:"
		if request.Kind == "scenarios" {
			valid, prefix = validScenarioID(id), "scenario:"
		}
		if !valid {
			writeError(w, http.StatusBadRequest, "invalid download item ID")
			return
		}
		if !seen[id] {
			ids, keys = append(ids, id), append(keys, prefix+id)
			seen[id] = true
		}
	}
	if err := s.checkLibraryDownloadItems(r.Context(), request.Kind, ids, keys); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errLibraryMemberNotFound) || errors.Is(err, errResultNotFound) || errors.Is(err, os.ErrNotExist) {
			status = http.StatusNotFound
		}
		writeError(w, status, "selected items are missing or unreadable; refresh the list and try again")
		return
	}
	now := time.Now().UTC()
	request.IDs, request.ExpiresAt = ids, now.Add(libraryDownloadTTL)
	id := rand.Text()
	s.libraryDownloadsMu.Lock()
	s.pruneLibraryDownloadsLocked(now)
	if len(s.libraryDownloads) >= libraryDownloadTicketLimit {
		s.libraryDownloadsMu.Unlock()
		writeError(w, http.StatusTooManyRequests, "too many pending downloads; start an existing download or try again later")
		return
	}
	if s.libraryDownloads == nil {
		s.libraryDownloads = make(map[string]libraryDownload)
	}
	s.libraryDownloads[id] = request
	s.libraryDownloadsMu.Unlock()
	writeJSON(w, http.StatusCreated, map[string]any{
		"url": "/api/v1/downloads/" + id, "count": len(ids), "expiresAt": request.ExpiresAt,
	})
}

func (s *Server) checkLibraryDownloadItems(ctx context.Context, kind string, ids, keys []string) error {
	if kind == "scenarios" {
		return s.checkLibraryMembers(ctx, keys)
	}
	runs, err := s.openResultRuns()
	if err != nil {
		return err
	}
	defer runs.Close()
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A bulk export selects exact Run IDs, including members of a batch.
		// Group membership validation deliberately treats batches as one key.
		if _, err := s.readSavedResult(runs, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handleLibraryDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/downloads/")
	// Bound active streams as well as idle tickets. Backpressure never creates
	// a goroutine, open snapshot or large buffer for another waiting download.
	select {
	case s.libraryDownloadSlots <- struct{}{}:
		defer func() { <-s.libraryDownloadSlots }()
	default:
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusTooManyRequests, "two bulk downloads are already in progress; try again after one finishes")
		return
	}
	s.libraryDownloadsMu.Lock()
	s.pruneLibraryDownloadsLocked(time.Now())
	selection, found := s.libraryDownloads[id]
	delete(s.libraryDownloads, id)
	s.libraryDownloadsMu.Unlock()
	if !found {
		writeError(w, http.StatusGone, "download selection expired or was used; select the items and download again")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	var archive *zip.Writer
	names := make(map[string]bool)
	for _, itemID := range selection.IDs {
		err := s.writeLibraryDownloadItem(r.Context(), selection.Kind, itemID, names, func() *zip.Writer {
			if archive == nil {
				w.Header().Set("Content-Type", "application/zip")
				w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%s.zip"`, selection.Kind, time.Now().UTC().Format("20060102T150405Z")))
				archive = zip.NewWriter(resultContextWriter{ctx: r.Context(), writer: w})
			}
			return archive
		})
		if err != nil {
			s.logger.Warn("bulk download item failed", "kind", selection.Kind, "item", itemID, "error", err)
			if archive != nil {
				// Never finish a valid-looking ZIP with silently missing items.
				panic(http.ErrAbortHandler)
			}
			status := http.StatusInternalServerError
			if errors.Is(err, errScenarioNotFound) || errors.Is(err, errResultNotFound) {
				status = http.StatusNotFound
			}
			writeError(w, status, "selected item is missing or unreadable; refresh the list and try again")
			return
		}
	}
	if err := archive.Close(); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) writeLibraryDownloadItem(ctx context.Context, kind, id string, names map[string]bool, archive func() *zip.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if kind == "scenarios" {
		item, err := s.getSavedScenario(id)
		if err != nil {
			return err
		}
		name := uniqueDownloadName(downloadFilePart(item.Name, "scenario"), ".yaml", names)
		entry, err := archive().Create(name)
		if err != nil {
			return err
		}
		_, err = io.WriteString(entry, item.YAML)
		return err
	}
	snapshot, err := s.captureResultFilesContext(ctx, id, true)
	if err != nil {
		return err
	}
	defer snapshot.close()
	if err := s.acquireResultArchiveSlot(ctx); err != nil {
		return err
	}
	defer s.releaseResultArchiveSlot()
	name := uniqueDownloadName(fmt.Sprintf("[%s]-%s", downloadFilePart(snapshot.result.Name, id), id), ".zip", names)
	// The inner archive already compresses each source file. Stream it directly
	// into a stored outer entry, with only one Run's descriptors open at a time.
	entry, err := archive().CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
	if err != nil {
		return err
	}
	return snapshot.writeZIP(ctx, entry)
}

func uniqueDownloadName(base, extension string, names map[string]bool) string {
	name := base + extension
	for suffix := 2; names[strings.ToLower(name)]; suffix++ {
		name = fmt.Sprintf("%s (%d)%s", base, suffix, extension)
	}
	names[strings.ToLower(name)] = true
	return name
}

func downloadFilePart(name, fallback string) string {
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || strings.ContainsRune(`/\:*?"<>|`, r) || r >= '\u202a' && r <= '\u202e' || r >= '\u2066' && r <= '\u2069' {
			return '_'
		}
		return r
	}, name)
	name = strings.Trim(name, " .")
	if name == "" {
		name = fallback
	}
	// Leave room for the full Run ID and extension within a filesystem component.
	var short strings.Builder
	for _, r := range name {
		if short.Len()+utf8.RuneLen(r) > 96 {
			break
		}
		short.WriteRune(r)
	}
	name = strings.TrimRight(short.String(), " .")
	base := strings.ToUpper(strings.SplitN(name, ".", 2)[0])
	if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9' {
		name = "_" + name
	}
	return name
}
