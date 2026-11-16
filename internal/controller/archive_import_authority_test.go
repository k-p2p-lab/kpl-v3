package controller

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func copyArchiveForDiscovery(t *testing.T, from, to *Server, id string, legacy bool) {
	t.Helper()
	archive, err := to.openArchiveStore()
	if err != nil {
		t.Fatal(err)
	}
	archive.Close()
	destination := filepath.Join(to.config.DataDir, archivedRunsDirectory, id)
	if err := os.CopyFS(destination, os.DirFS(filepath.Join(from.config.DataDir, archivedRunsDirectory, id))); err != nil {
		t.Fatal(err)
	}
	if legacy {
		if err := os.Remove(filepath.Join(destination, runArchiveManifest)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestArchiveImportPreservesUnindexedLocalAuthority(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, state := range []string{"completed", "running", "queued"} {
			t.Run(fmt.Sprintf("legacy=%t/state=%s", legacy, state), func(t *testing.T) {
				const id = "local-import-collision"
				remote := New(ServerConfig{DataDir: t.TempDir()}, nil)
				resultFixture(t, remote, id, "completed", time.Unix(100, 0))
				if err := remote.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "remote-only")}); err != nil {
					t.Fatal(err)
				}
				archiveTestRun(t, remote, id)
				local := New(ServerConfig{DataDir: t.TempDir()}, nil)
				run, _ := resultFixture(t, local, id, state, time.Unix(200, 0))
				if state == "running" || state == "queued" {
					local.state.experiments[id] = run
				}
				if err := local.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "local-only")}); err != nil {
					t.Fatal(err)
				}
				before := storageZIP(t, local, id)
				copyArchiveForDiscovery(t, remote, local, id, legacy)
				if err := local.importArchivedRuns(t.Context()); err != nil {
					t.Fatal(err)
				}
				after := storageZIP(t, local, id)
				if !bytes.Equal(before["events.jsonl"], after["events.jsonl"]) || !bytes.Equal(before["experiment.json"], after["experiment.json"]) {
					t.Fatalf("discovery merged another record into local authority: before=%v after=%v", storedEventIDs(t, before["events.jsonl"]), storedEventIDs(t, after["events.jsonl"]))
				}
				if _, err := os.Stat(filepath.Join(local.config.DataDir, currentRunsDirectory, id, runArchiveManifest)); !os.IsNotExist(err) {
					t.Fatalf("discovery published an archive index over local authority: %v", err)
				}
			})
		}
	}
}

func TestModernArchiveImportResumesExplicitPartialIndex(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprint("manifest-published=", published), func(t *testing.T) {
			const id = "partial-modern-import"
			remote := New(ServerConfig{DataDir: t.TempDir()}, nil)
			_, metadata := resultFixture(t, remote, id, "completed", time.Unix(100, 0))
			if err := remote.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "retained")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, remote, id)
			local := New(ServerConfig{DataDir: t.TempDir()}, nil)
			copyArchiveForDiscovery(t, remote, local, id, false)
			path := filepath.Join(local.config.DataDir, currentRunsDirectory, id)
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			root, err := os.OpenRoot(path)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if err := writeImportedMetadata(root, runArchiveImportFile, []byte("pending\n")); err != nil {
				t.Fatal(err)
			}
			if err := writeImportedMetadata(root, "experiment.json", metadata); err != nil {
				t.Fatal(err)
			}
			if published {
				if err := writeRunArchive(root, runArchiveManifestData{Version: 1, RunID: id, Files: map[string]storedRunFile{}, Logs: map[string][]storedRunFile{}}); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(local.config, nil)
			if err := restarted.importArchivedRuns(t.Context()); err != nil {
				t.Fatal(err)
			}
			files := storageZIP(t, restarted, id)
			if got := fmt.Sprint(storedEventIDs(t, files["events.jsonl"])); got != "[retained]" {
				t.Fatalf("partial import did not recover its archived log: %s", got)
			}
			if _, err := root.Lstat(runArchiveImportFile); !os.IsNotExist(err) {
				t.Fatalf("partial import intent remains: %v", err)
			}
		})
	}
}

func TestArchiveWorkerRecoversMetadataOnlyRemoteCommitAfterDiscovery(t *testing.T) {
	const id = "metadata-only-commit"
	remote := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, remote, id, "completed", time.Unix(100, 0))
	archiveTestRun(t, remote, id)
	local := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, local, id, "completed", time.Unix(100, 0))
	copyArchiveForDiscovery(t, remote, local, id, false)
	if err := local.importArchivedRuns(t.Context()); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, local, id)
	if _, err := os.Stat(filepath.Join(local.config.DataDir, currentRunsDirectory, id, runArchiveManifest)); err != nil {
		t.Fatalf("archive worker did not recover the committed metadata: %v", err)
	}
	if files := storageZIP(t, local, id); len(files["scenario.yaml"]) == 0 || len(files["experiment.json"]) == 0 {
		t.Fatal("metadata-only recovery lost source files")
	}
}

func TestArchiveDiscoveryPreservesAlreadyOffloadedSourceAndLocalTail(t *testing.T) {
	for _, tail := range []bool{false, true} {
		t.Run(fmt.Sprint("late-tail=", tail), func(t *testing.T) {
			const id = "indexed-source"
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			resultFixture(t, s, id, "completed", time.Unix(100, 0))
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "offloaded")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			if tail {
				if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "late-local")}); err != nil {
					t.Fatal(err)
				}
			}
			before := storageZIP(t, s, id)
			restarted := New(s.config, nil)
			if err := restarted.importArchivedRuns(t.Context()); err != nil {
				t.Fatal(err)
			}
			after := storageZIP(t, restarted, id)
			if !bytes.Equal(before["events.jsonl"], after["events.jsonl"]) || !bytes.Equal(before["experiment.json"], after["experiment.json"]) {
				t.Fatalf("discovery changed indexed source: before=%v after=%v", storedEventIDs(t, before["events.jsonl"]), storedEventIDs(t, after["events.jsonl"]))
			}
			archiveTestRun(t, restarted, id)
			if final := storageZIP(t, restarted, id); !bytes.Equal(after["events.jsonl"], final["events.jsonl"]) {
				t.Fatal("normal archival after discovery duplicated indexed source")
			}
		})
	}
}
