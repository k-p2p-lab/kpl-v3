package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestUnchangedExperimentPersistenceKeepsSourceRevisionAndArchiveStatus(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "run", State: "completed", Name: "unchanged"}
	if err := s.persistManifest(run, []byte("version: 1\nname: unchanged\nphases:\n - action: stop-all\n")); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, run.ID)
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "experiment.json")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := s.runSourceRevision(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	storageRevision := s.state.resultsRevision.Load()
	if err := s.persistExperiment(run); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("unchanged metadata was replaced: %v", err)
	}
	current, err := s.runSourceRevision(run.ID)
	if err != nil || current != revision || s.state.resultsRevision.Load() != storageRevision {
		t.Fatalf("unchanged metadata invalidated archive/analysis: %q %q %v", current, revision, err)
	}
	run.State = "failed"
	if err := s.persistExperiment(run); err != nil {
		t.Fatal(err)
	}
	current, err = s.runSourceRevision(run.ID)
	if err != nil || current == revision || s.state.resultsRevision.Load() == storageRevision {
		t.Fatalf("changed metadata was ignored: %q %q %v", current, revision, err)
	}
}

func TestAtomicMetadataReplacementAndTemporaryFileCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "experiment.json")
	for _, data := range []string{`{"state":"running"}`, `{"state":"completed"}`} {
		if err := writeFileAtomic(path, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		stored, err := os.ReadFile(path)
		if err != nil || string(stored) != data {
			t.Fatalf("metadata = %s, read error = %v", stored, err)
		}
	}
	targetDir := filepath.Join(dir, "target-directory")
	if err := os.Mkdir(targetDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(targetDir, []byte("cannot replace a directory"), 0o644); err == nil {
		t.Fatal("replaced a directory with metadata")
	}
	files, err := filepath.Glob(filepath.Join(dir, ".kpl-metadata-*"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary metadata files = %v, error = %v", files, err)
	}
}

func TestLinuxMetadataReadersNeverSeePartialJSON(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("validates Linux rename visibility under concurrent reads")
	}
	path := filepath.Join(t.TempDir(), "experiment.json")
	data := []byte(`{"state":"running","scenario":"` + strings.Repeat("x", 128*1024) + `"}`)
	if err := writeFileAtomic(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			current, err := os.ReadFile(path)
			if err != nil || !json.Valid(current) {
				t.Errorf("read incomplete metadata: bytes=%d, error=%v", len(current), err)
				return
			}
		}
	}()
	for i := 0; i < 30; i++ {
		if err := writeFileAtomic(path, data, 0o644); err != nil {
			t.Error(err)
			break
		}
	}
	close(done)
	readers.Wait()
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o644 {
		t.Fatalf("metadata permissions: %v %v", info, err)
	}
}
