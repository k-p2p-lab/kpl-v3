package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestArchiveRecoveryPreservesLocalCopyWhenCommittedObjectIsUnavailable(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "same-size-corruption"} {
		t.Run(damage, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "recover-local"
			resultFixture(t, s, id, "completed", time.Now().UTC())
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "retained")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			local, err := s.analysisDirectory(id)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := readRunArchive(local, id)
			local.Close()
			if err != nil {
				t.Fatal(err)
			}
			stored := manifest.Logs["events.jsonl"][0]
			remotePath := filepath.Join(s.config.DataDir, archivedRunsDirectory, id, stored.Object)
			saved, err := os.ReadFile(remotePath)
			if err != nil {
				t.Fatal(err)
			}
			// Restore the local sealed segment left by a crash after the remote
			// commit but before unlink, then simulate loss on the reconnected NAS.
			localPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, stored.Segment)
			if err := os.WriteFile(localPath, saved, 0600); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				err = os.Remove(remotePath)
			case "truncated":
				err = os.WriteFile(remotePath, saved[:len(saved)/2], 0600)
			default:
				corrupt := bytes.Clone(saved)
				corrupt[0] ^= 1
				err = os.WriteFile(remotePath, corrupt, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			restarted := New(s.config, nil)
			if err := restarted.archiveRun(context.Background(), id, 0); err == nil {
				t.Error("accepted unavailable committed archive object")
			}
			if current, err := os.ReadFile(localPath); err != nil || !bytes.Equal(current, saved) {
				t.Fatalf("recovery lost the remaining valid local copy: %v", err)
			}
			if err := os.WriteFile(remotePath, saved, 0600); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, restarted, id)
			if _, err := os.Stat(localPath); !os.IsNotExist(err) {
				t.Fatalf("verified recovery did not release the local segment: %v", err)
			}
			if got := storedEventIDs(t, storageZIP(t, restarted, id)["events.jsonl"]); len(got) != 1 || got[0] != "retained" {
				t.Fatalf("recovery duplicated or lost the event: %v", got)
			}
		})
	}
}
