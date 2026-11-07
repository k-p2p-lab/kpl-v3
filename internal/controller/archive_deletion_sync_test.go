package controller

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveDeletionRequiresDirectorySyncBeforeCompletion(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	directory := t.TempDir()
	archive, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	for _, id := range []string{"deleted", ".incoming-deleted"} {
		if err := archive.Mkdir(id, 0700); err != nil {
			t.Fatal(err)
		}
		if err := archive.WriteFile(id+"/events.jsonl", []byte("retained until deletion"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// A held descriptor can still unlink child directories with write/search
	// permission. Synchronizing the parent requires opening it for reading.
	if err := os.Chmod(directory, 0300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	if probe, err := archive.Open("."); err == nil {
		probe.Close()
		t.Skip("directory read permissions are not enforced for this user")
	} else if !errors.Is(err, os.ErrPermission) {
		t.Fatal(err)
	}
	err = s.removeArchivedResult(archive, "deleted")
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("archive deletion acknowledged without durable directory entries: %v", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"deleted", ".incoming-deleted"} {
		if _, err := os.Stat(filepath.Join(directory, id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fixture did not reach removal before sync failure: %s %v", id, err)
		}
	}
	if err := s.removeArchivedResult(archive, "deleted"); err != nil {
		t.Fatalf("cleanup retry failed after restoring sync permission: %v", err)
	}
}
