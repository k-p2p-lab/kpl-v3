package controller

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestArchiveRetryReclaimsAbandonedCopyTemporaryFiles(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial publication", true: "published run"}[published], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "copy-retry"
			resultFixture(t, s, id, "completed", time.Now())
			if published {
				archiveTestRun(t, s, id)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(id, "after interruption")}}); err != nil {
				t.Fatal(err)
			}
			archive, err := s.openArchiveStore()
			if err != nil {
				t.Fatal(err)
			}
			archive.Close()
			remoteID := id
			if !published {
				remoteID = ".incoming-" + id
				if err := os.Mkdir(filepath.Join(s.config.DataDir, archivedRunsDirectory, remoteID), 0755); err != nil {
					t.Fatal(err)
				}
			}
			remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, remoteID)
			abandoned := []string{archiveCopyTemporaryPrefix + rand.Text(), ".analysis-" + strings.Repeat("a", 32) + ".tmp"}
			preserved := []string{"user.json", ".user-note", ".incoming-user-note", ".incoming-ABCDEFGHIJKLMNOPQRSTUVWXYZ0", ".analysis-user-note.tmp", ".analysis-" + strings.Repeat("a", 31) + ".tmp", ".analysis-" + strings.Repeat("G", 32) + ".tmp"}
			for _, name := range append(preserved, abandoned...) {
				if err := os.WriteFile(filepath.Join(remote, name), []byte("retained evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			link, directory := archiveCopyTemporaryPrefix+rand.Text(), archiveCopyTemporaryPrefix+rand.Text()
			target := filepath.Join(t.TempDir(), "link-target")
			if err := os.WriteFile(target, []byte("outside evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(remote, link)); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(remote, directory), 0755); err != nil {
				t.Fatal(err)
			}
			metadataLink := ".analysis-" + strings.Repeat("b", 32) + ".tmp"
			metadataDirectory := ".analysis-" + strings.Repeat("c", 32) + ".tmp"
			if err := os.Symlink(target, filepath.Join(remote, metadataLink)); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(remote, metadataDirectory), 0755); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			final := filepath.Join(s.config.DataDir, archivedRunsDirectory, id)
			for _, name := range abandoned {
				if _, err := os.Lstat(filepath.Join(final, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("archive retry retained abandoned staging bytes %q: %v", name, err)
				}
			}
			for _, name := range preserved {
				if data, err := os.ReadFile(filepath.Join(final, name)); err != nil || string(data) != "retained evidence" {
					t.Fatalf("retry changed unrelated file %q: %q %v", name, data, err)
				}
			}
			if info, err := os.Lstat(filepath.Join(final, link)); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("retry removed reserved-looking symlink: %v", err)
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "outside evidence" {
				t.Fatalf("retry changed symlink target: %q %v", data, err)
			}
			if info, err := os.Stat(filepath.Join(final, directory)); err != nil || !info.IsDir() {
				t.Fatalf("retry removed reserved-looking directory: %v", err)
			}
			if info, err := os.Lstat(filepath.Join(final, metadataLink)); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("retry removed reserved-looking manifest symlink: %v", err)
			}
			if info, err := os.Stat(filepath.Join(final, metadataDirectory)); err != nil || !info.IsDir() {
				t.Fatalf("retry removed reserved-looking manifest directory: %v", err)
			}
			if ids := storedEventIDs(t, storageZIP(t, s, id)["events.jsonl"]); len(ids) != 1 || ids[0] != "after interruption" {
				t.Fatalf("retry damaged published source: %v", ids)
			}
		})
	}
}
