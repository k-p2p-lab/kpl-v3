package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type archiveLocalRewriteContext struct {
	context.Context
	check func() error
}

func (ctx archiveLocalRewriteContext) Err() error {
	if err := ctx.Context.Err(); err != nil {
		return err
	}
	return ctx.check()
}

func TestArchiveKeepsInPlaceArtifactRewriteAfterRemoteCopy(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "archive-local-rewrite"
	resultFixture(t, s, id, "completed", time.Now().UTC())
	local := filepath.Join(s.config.DataDir, currentRunsDirectory, id, analysisResultFile)
	before := []byte("{\"value\":\"before\"}\n")
	after := []byte("{\"value\":\"change\"}\n")
	if err := os.WriteFile(local, before, 0600); err != nil {
		t.Fatal(err)
	}
	identity, err := os.Stat(local)
	if err != nil {
		t.Fatal(err)
	}
	mutated := false
	ctx := archiveLocalRewriteContext{Context: t.Context(), check: func() error {
		if mutated {
			return nil
		}
		// The first artifact is sorted before retained metadata. Its final
		// archive object exists only after copying and checksum verification;
		// the next context check creates a deterministic pre-unlink window.
		destination := filepath.Join(s.config.DataDir, archivedRunsDirectory, ".incoming-"+id)
		entries, err := os.ReadDir(destination)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "analysis-result-") || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			copied, err := os.ReadFile(filepath.Join(destination, entry.Name()))
			if err != nil {
				return err
			}
			if !bytes.Equal(copied, before) {
				return errors.New("archive fixture did not copy the original artifact")
			}
			if err := os.WriteFile(local, after, 0600); err != nil {
				return err
			}
			if err := os.Chtimes(local, identity.ModTime(), identity.ModTime()); err != nil {
				return err
			}
			updated, err := os.Stat(local)
			if err != nil {
				return err
			}
			if !os.SameFile(identity, updated) || updated.Size() != identity.Size() || !updated.ModTime().Equal(identity.ModTime()) {
				return errors.New("archive fixture did not preserve inode, size and mtime")
			}
			if sourceFileIdentity(identity.Sys()) == sourceFileIdentity(updated.Sys()) {
				return errors.New("filesystem did not expose the changed source identity")
			}
			mutated = true
			break
		}
		return nil
	}}
	if err := s.archiveRun(ctx, id, 0); err != nil {
		t.Fatal(err)
	}
	if !mutated {
		t.Fatal("archive never reached the verified remote-copy boundary")
	}
	current, err := os.ReadFile(local)
	if err != nil || !bytes.Equal(current, after) {
		t.Fatalf("archival discarded a newer local artifact: content=%q err=%v", current, err)
	}
	s.state.archiveQueueMu.Lock()
	pending, version := s.state.archivePending[id], s.state.archiveVersions[id]
	s.state.archiveQueueMu.Unlock()
	if !pending || version == 0 {
		t.Fatalf("changed local artifact was not queued for archival: pending=%t version=%d", pending, version)
	}
	root, err := s.analysisDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	status := readRunArchiveStatus(root)
	root.Close()
	if status.State != "pending" {
		t.Fatalf("changed local artifact was reported as %q", status.State)
	}
	if got := listedStorageState(t, s, id); got != "pending" {
		t.Fatalf("result index hid the retained changed artifact as %q", got)
	}
	archiveTestRun(t, s, id)
	stored := archiveReuseManifest(t, s, id).Files[analysisResultFile]
	archived, err := os.ReadFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, id, stored.Object))
	if err != nil || !bytes.Equal(archived, after) {
		t.Fatalf("later archive pass did not preserve the new artifact: content=%q err=%v", archived, err)
	}
	if _, err := os.Stat(local); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified local payload was not removed on the next pass: %v", err)
	}
	if got := listedStorageState(t, s, id); got != "archived" {
		t.Fatalf("verified latest artifact remained %q", got)
	}
}
