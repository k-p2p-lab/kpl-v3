package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// A raw tail has no committed sealed-segment identity. Even equal bytes and
// mtime do not prove it is the old source: a restored index may coexist with
// independently written data. Preserve both logical sources and offload the
// tail only after verifying its own new archive object.
func TestArchiveRecoveryPreservesRawLogWithoutSealedIdentity(t *testing.T) {
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "published", true: "incoming"}[staged], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "raw-log-crash"
			resultFixture(t, s, id, "completed", time.Now().UTC())
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "once")}); err != nil {
				t.Fatal(err)
			}
			old := time.Now().Add(-time.Minute)
			originalRaw := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")
			if err := os.Chtimes(originalRaw, old, old); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			manifest := archiveReuseManifest(t, s, id)
			stored := manifest.Logs["events.jsonl"][0]
			remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, id)
			original, err := os.ReadFile(filepath.Join(remote, stored.Object))
			if err != nil {
				t.Fatal(err)
			}
			if staged {
				if err := os.Rename(remote, filepath.Join(s.config.DataDir, archivedRunsDirectory, ".incoming-"+id)); err != nil {
					t.Fatal(err)
				}
			}
			local := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			if err := os.Remove(filepath.Join(local, runArchiveManifest)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(local, "events.jsonl"), original, 0600); err != nil {
				t.Fatal(err)
			}
			originalModified := time.Unix(0, stored.ModifiedAt)
			if err := os.Chtimes(filepath.Join(local, "events.jsonl"), originalModified, originalModified); err != nil {
				t.Fatal(err)
			}
			restarted := New(s.config, nil)
			if err := restarted.archiveMaintenance(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := storedEventIDs(t, storageZIP(t, restarted, id)["events.jsonl"])
			if strings.Join(got, ",") != "once,once" {
				t.Fatalf("recovery inferred away a raw tail without committed identity: %v", got)
			}
			manifest = archiveReuseManifest(t, restarted, id)
			if len(manifest.Logs["events.jsonl"]) != 2 || manifest.Logs["events.jsonl"][0].Object != stored.Object {
				t.Fatal("recovery lost the published object or distinct raw tail")
			}
			entries, err := os.ReadDir(local)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if entry.Name() == "events.jsonl" || logSegment(entry.Name(), "events.jsonl") {
					t.Fatalf("verified duplicate log remains local: %s", entry.Name())
				}
			}
		})
	}
}

func TestArchiveRecoveryDoesNotFoldFreshRawTailIntoOlderIdenticalObject(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "fresh-raw-tail"
	resultFixture(t, s, id, "completed", time.Now().UTC())
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "retained")}); err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(filepath.Join(localPath, "events.jsonl"), old, old); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	manifest := archiveReuseManifest(t, s, id)
	stored := manifest.Logs["events.jsonl"][0]
	remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, id)
	data, err := os.ReadFile(filepath.Join(remote, stored.Object))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(remote, filepath.Join(s.config.DataDir, archivedRunsDirectory, ".incoming-"+id)); err != nil {
		t.Fatal(err)
	}
	local, err := s.analysisDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	blank := runArchiveManifestData{Version: 1, RunID: id, Files: map[string]storedRunFile{}, Logs: map[string][]storedRunFile{}}
	err = writeRunArchive(local, blank)
	local.Close()
	if err != nil {
		t.Fatal(err)
	}
	// The raw tail is a separate later local write. Equal bytes alone are not
	// proof that it is the original source left by an interrupted cleanup.
	if err := os.WriteFile(filepath.Join(localPath, "events.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	archiveTestRun(t, restarted, id)
	manifest = archiveReuseManifest(t, restarted, id)
	if len(manifest.Logs["events.jsonl"]) != 2 {
		t.Fatal("a distinct later raw tail was silently folded into the older object")
	}
	if got := strings.Join(storedEventIDs(t, storageZIP(t, restarted, id)["events.jsonl"]), ","); got != "retained,retained" {
		t.Fatalf("later local bytes were discarded: %s", got)
	}
}
