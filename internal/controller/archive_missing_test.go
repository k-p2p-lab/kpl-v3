package controller

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArchiveReconnectMissingRunDoesNotBlockOtherTransfers(t *testing.T) {
	for _, damage := range []string{"directory", "object"} {
		t.Run(damage, func(t *testing.T) {
			s, original, oldPin := reconnectFixture(t)
			remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived")
			backup := filepath.Join(t.TempDir(), "backup")
			var restore func()
			if damage == "directory" {
				if err := os.Rename(remote, backup); err != nil {
					t.Fatal(err)
				}
				restore = func() {
					if err := os.Rename(backup, remote); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				local, err := s.analysisDirectory("archived")
				if err != nil {
					t.Fatal(err)
				}
				manifest, err := readRunArchive(local, "archived")
				local.Close()
				if err != nil {
					t.Fatal(err)
				}
				object := filepath.Join(remote, manifest.Logs["events.jsonl"][0].Object)
				data, err := os.ReadFile(object)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(object); err != nil {
					t.Fatal(err)
				}
				restore = func() {
					if err := os.WriteFile(object, data, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := os.Remove(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")); err != nil {
				t.Fatal(err)
			}
			resultFixture(t, s, "pending", "completed", time.Now())
			before := storageZIP(t, s, "pending")
			response := reconnectRequest(s, `{"confirmCurrentArchive":true,"allowMissingRuns":true}`)
			if response.Code != http.StatusAccepted {
				t.Fatal(response.Body)
			}
			if err := s.processArchiveReconnect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if s.archiveReconnectState != "completed" {
				t.Fatal(s.archiveReconnectState)
			}
			pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
			if err != nil || bytes.Equal(pin, oldPin) {
				t.Fatalf("identity was not reconnected: %v", err)
			}
			if got := listedStorageState(t, s, "archived"); got != "unavailable" {
				t.Fatalf("missing bytes labeled %s", got)
			}
			if err := s.archiveMaintenance(t.Context()); err == nil || archiveFailureKind(err) != "maintenance" {
				t.Fatalf("expected isolated missing-run failure, got %v", err)
			}
			if got := listedStorageState(t, s, "pending"); got != "archived" {
				t.Fatalf("unrelated transfer blocked: %s", got)
			}
			requireReconnectSourcesEqual(t, before, storageZIP(t, s, "pending"))
			if got := listedStorageState(t, s, "archived"); got != "unavailable" {
				t.Fatalf("worker erased missing status: %s", got)
			}
			if !s.state.archivePending["archived"] {
				t.Fatal("missing result lost its recovery queue entry")
			}
			if len(s.resultReadPins) != 0 {
				t.Fatalf("leaked reconnect pins: %v", s.resultReadPins)
			}

			// The quarantine and recovery queue survive Controller restart.
			restarted := New(s.config, nil)
			if got := listedStorageState(t, restarted, "archived"); got != "unavailable" {
				t.Fatalf("restart erased missing status: %s", got)
			}
			if err := restarted.archiveMaintenance(t.Context()); err == nil || archiveFailureKind(err) != "maintenance" {
				t.Fatalf("restart bypassed missing data: %v", err)
			}
			response = resultRequest(restarted, http.MethodGet, "/api/v1/experiments/archived/download")
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("missing source download: %d %s", response.Code, response.Body)
			}

			restore()
			if err := restarted.archiveMaintenance(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := listedStorageState(t, restarted, "archived"); got != "archived" {
				t.Fatalf("restored result not recovered: %s", got)
			}
			requireReconnectSourcesEqual(t, original, storageZIP(t, restarted, "archived"))
			if len(restarted.resultReadPins) != 0 {
				t.Fatal("download left pins")
			}
		})
	}
}

func TestArchiveReconnectAllowMissingStillRejectsCorruption(t *testing.T) {
	for _, damage := range []string{"checksum", "legacy"} {
		t.Run(damage, func(t *testing.T) {
			s, _, oldPin := reconnectFixture(t)
			local, err := s.analysisDirectory("archived")
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := readRunArchive(local, "archived")
			if err != nil {
				t.Fatal(err)
			}
			segment := manifest.Logs["events.jsonl"][0]
			remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived")
			if err := os.Remove(filepath.Join(remote, segment.Object)); err != nil {
				t.Fatal(err)
			}
			if damage == "checksum" {
				stored := manifest.Files["scenario.yaml"]
				if err := os.WriteFile(filepath.Join(remote, stored.Object), bytes.Repeat([]byte("x"), int(stored.Size)), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				segment.SHA256 = ""
				manifest.Logs["events.jsonl"][0] = segment
				if err := writeRunArchive(local, manifest); err != nil {
					t.Fatal(err)
				}
			}
			local.Close()
			if err := os.Remove(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")); err != nil {
				t.Fatal(err)
			}
			reconnectRequest(s, `{"confirmCurrentArchive":true,"allowMissingRuns":true}`)
			if err := s.processArchiveReconnect(t.Context()); err == nil || errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corruption hidden by missing object: %v", err)
			}
			pin, err := os.ReadFile(filepath.Join(s.config.DataDir, ".archive-store-id"))
			if err != nil || !bytes.Equal(pin, oldPin) {
				t.Fatalf("changed pin on verification failure: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, archivedRunsDirectory, ".kpl-store-id")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("published failed identity: %v", err)
			}
		})
	}
}

func TestArchiveMissingQuarantineBlocksUnverifiedRemoteReads(t *testing.T) {
	s, original, _ := reconnectFixture(t)
	if err := s.quarantineMissingArchiveRun("archived", errors.New("missing before restore")); err != nil {
		t.Fatal(err)
	}
	// Even when bytes reappear, readers wait for the worker's checksum check.
	s.archiveIOCheck = func() { t.Fatal("quarantined download probed NAS") }
	response := resultRequest(s, http.MethodGet, "/api/v1/experiments/archived/download")
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "unavailable") {
		t.Fatalf("unverified read: %d %s", response.Code, response.Body)
	}
	if len(s.resultReadPins) != 0 {
		t.Fatal("quarantined read leaked pins")
	}
	s.archiveIOCheck = nil
	if err := s.archiveMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := listedStorageState(t, s, "archived"); got != "archived" {
		t.Fatalf("verification did not clear quarantine: %s", got)
	}
	requireReconnectSourcesEqual(t, original, storageZIP(t, s, "archived"))
}
