package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArchiveWriteProbeCleansUpWithoutPublishingIdentity(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	path := t.TempDir()
	root, err := os.OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := s.checkArchiveWriteAccess(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		t.Fatalf("probe published identity or left temporary files: %v %v", entries, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.checkArchiveWriteAccess(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe: %v", err)
	}
	entries, err = os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled probe left files: %v %v", entries, err)
	}
}

func TestArchiveIdentityPermissionErrorPreservesCauseAndShowsIdentity(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	wrapped := archiveIdentityIOError(root, "create", os.ErrPermission)
	if !errors.Is(wrapped, os.ErrPermission) {
		t.Fatal("permission classification was lost")
	}
	for _, part := range []string{filepath.Join(root.Name(), ".kpl-store-id"), fmt.Sprintf("uid=%d gid=%d", os.Geteuid(), os.Getegid()), "create", "NAS ACL"} {
		if !strings.Contains(wrapped.Error(), part) {
			t.Fatalf("missing %q in %v", part, wrapped)
		}
	}
}

func TestArchiveReconnectDeniedIdentityCreationRecoversAfterPermissionRepair(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory mode denial requires an unprivileged process")
	}
	s, original, oldPin := reconnectFixture(t)
	archive := filepath.Join(s.config.DataDir, archivedRunsDirectory)
	marker := filepath.Join(archive, ".kpl-store-id")
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(archive, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(archive, 0755) })
	reconnectRequest(s, `{"confirmCurrentArchive":true,"allowMissingRuns":true}`)
	err := s.processArchiveReconnect(t.Context())
	if !errors.Is(err, os.ErrPermission) || archiveFailureKind(err) != "unavailable" {
		t.Fatalf("wrong failure classification: %v", err)
	}
	for _, part := range []string{"create archive write probe", ".kpl-write-check-", fmt.Sprintf("uid=%d gid=%d", os.Geteuid(), os.Getegid())} {
		if !strings.Contains(s.archiveReconnectError, part) {
			t.Fatalf("diagnostic missing %q: %s", part, s.archiveReconnectError)
		}
	}
	pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
	if err != nil || !bytes.Equal(pin, oldPin) {
		t.Fatalf("failed create replaced local identity: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed create published a marker: %v", err)
	}
	if len(s.resultReadPins) != 0 {
		t.Fatal("permission failure leaked pins")
	}
	if err := os.Chmod(archive, 0755); err != nil {
		t.Fatal(err)
	}
	reconnectRequest(s, `{"confirmCurrentArchive":true,"allowMissingRuns":true}`)
	if err := s.processArchiveReconnect(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireReconnectSourcesEqual(t, original, storageZIP(t, s, "archived"))
}
