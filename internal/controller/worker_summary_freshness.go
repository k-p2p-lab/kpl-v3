package controller

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Runtime summaries read only the event stream. Use the same mechanical file
// identity, size, timestamp and archived-object evidence as source revisions;
// metadata changes cannot make two different event inputs compare equal.
func eventRevisionOf(files []resultFile) string {
	var events []resultFile
	for _, file := range files {
		if file.name == "events.jsonl" {
			events = append(events, file)
		}
	}
	return "events-v1:" + sourceRevisionOf(events)
}

func eventRevisionAt(root *os.Root, id string) (string, error) {
	revision, _, err := eventInputAt(root, id)
	return revision, err
}

func eventInputAt(root *os.Root, id string) (string, int64, error) {
	manifest, err := readRunArchive(root, id)
	if err != nil {
		return "", 0, err
	}
	events, err := captureRunSource(root, manifest, "events.jsonl")
	if errors.Is(err, os.ErrNotExist) {
		return eventRevisionOf(nil), 0, nil
	}
	if err != nil {
		return "", 0, err
	}
	defer events.close()
	return eventRevisionOf([]resultFile{events}), events.size, nil
}

// Caller holds persistMu, serializing the event check, metadata revision and
// atomic summary replacement with every accepted append. A legacy summary or a
// due delivery window still requires calculation by an isolated worker.
func (s *state) rebaseRetiredWorkerSummaryLocked(id string, now time.Time) (bool, error) {
	s.mu.RLock()
	run, known := s.experiments[id]
	s.mu.RUnlock()
	if known && (run.State == "running" || run.State == "queued" || run.CleanupState == "running") {
		return false, nil
	}
	if fenced, err := s.resultWriteFencedLocked(id); err != nil || fenced {
		return false, err
	}
	root, err := os.OpenRoot(filepath.Join(s.dataDir, currentRunsDirectory, id))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer root.Close()
	summary, err := readWorkerSummaryRoot(root, id)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !summary.Retired || summary.EventRevision == "" || !summary.EventsComplete || !summary.NextRefreshAt.IsZero() && !now.Before(summary.NextRefreshAt) {
		return false, nil
	}
	events, size, err := eventInputAt(root, id)
	if err != nil || summary.Cursor != size {
		return false, err
	}
	current, err := sourceRevisionAt(root, id)
	if err != nil {
		return false, err
	}
	if summary.EventRevision != events && summary.SourceRevision != current {
		return false, nil
	}
	// A verified archive move preserves the canonical source revision while
	// changing its physical layout. An authoritative complete binding permits
	// that retag without reading the same archived bytes again, even offline.
	if summary.SourceRevision != current || summary.EventRevision != events {
		summary.SourceRevision = current
		summary.EventRevision = events
		if err := writeAnalysisJSON(root, workerSummaryFile, summary); err != nil {
			return false, err
		}
	}
	s.mu.Lock()
	delete(s.workerSummaries, id)
	delete(s.summaryDirty, id)
	if summary.NextRefreshAt.IsZero() {
		delete(s.summaryDue, id)
	} else {
		if s.summaryDue == nil {
			s.summaryDue = make(map[string]time.Time)
		}
		s.summaryDue[id] = summary.NextRefreshAt
	}
	s.mu.Unlock()
	return true, nil
}
