package controller

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotReleaseDoesNotWaitForUnrelatedPersistence(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, s, "snapshot-release-owner", "completed", time.Now())
	first, err := s.captureResultFilesContext(t.Context(), run.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()
	second, err := s.captureResultFilesContext(t.Context(), run.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	path := "/api/v1/results/" + run.ID
	if response := resultRequest(s, http.MethodDelete, path); response.Code != http.StatusConflict {
		t.Fatalf("open snapshots did not protect deletion: %d %s", response.Code, response.Body)
	}
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() { first.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		s.state.persistMu.Unlock()
		locked = false
		<-done
		t.Fatal("closed snapshot retained its download lease behind an unrelated durable write")
	}
	s.resultDownloadMu.Lock()
	remaining := s.resultDownloads[run.ID]
	s.resultDownloadMu.Unlock()
	if remaining != 1 {
		t.Fatalf("closing one snapshot released another owner's deletion protection: %d", remaining)
	}
	for _, file := range first.files {
		if file.file != nil {
			if _, err := file.file.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("snapshot released its lease before closing its file: %v", err)
			}
		}
	}
	s.state.persistMu.Unlock()
	locked = false
	if response := resultRequest(s, http.MethodDelete, path); response.Code != http.StatusConflict {
		t.Fatalf("remaining snapshot lost deletion protection: %d %s", response.Code, response.Body)
	}
	second.close()
	// Download accounting must remain independent of the archive reader pin.
	s.state.persistMu.Lock()
	s.resultReadPins[run.ID]++
	s.state.persistMu.Unlock()
	if response := resultRequest(s, http.MethodDelete, path); response.Code != http.StatusConflict {
		t.Fatalf("download release cleared the separate archive read pin: %d %s", response.Code, response.Body)
	}
	s.state.persistMu.Lock()
	delete(s.resultReadPins, run.ID)
	s.state.persistMu.Unlock()
	if response := resultRequest(s, http.MethodDelete, path); response.Code != http.StatusNoContent {
		t.Fatalf("fully closed result could not be deleted: %d %s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("successful deletion retained the closed result: %v", err)
	}
}
