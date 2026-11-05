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

	"github.com/k-p2p-lab/kpl-v3/internal/model"
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

func TestArchiveMissingKeepsLateLocalDataUntilVerifiedRecovery(t *testing.T) {
	s, _, _ := reconnectFixture(t)
	local, err := s.analysisDirectory("archived")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readRunArchive(local, "archived")
	local.Close()
	if err != nil {
		t.Fatal(err)
	}
	object := filepath.Join(s.config.DataDir, archivedRunsDirectory, "archived", manifest.Logs["events.jsonl"][0].Object)
	data, err := os.ReadFile(object)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(object); err != nil {
		t.Fatal(err)
	}
	if err := s.quarantineMissingArchiveRun("archived", errors.New("missing source object")); err != nil {
		t.Fatal(err)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("archived", "late-local")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveRun(t.Context(), "archived", 0); err == nil {
		t.Fatal("incomplete run was published")
	}
	entries, err := os.ReadDir(filepath.Join(s.config.DataDir, currentRunsDirectory, "archived"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		if logSegment(entry.Name(), "events.jsonl") {
			bytes, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "archived", entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range storedEventIDs(t, bytes) {
				found = found || id == "late-local"
			}
		}
	}
	if !found {
		t.Fatal("missing archive discarded late local telemetry")
	}
	if err := os.WriteFile(object, bytes.Repeat([]byte("x"), len(data)), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveRun(t.Context(), "archived", 0); err == nil {
		t.Fatal("corrupted restore passed verification")
	}
	if got := listedStorageState(t, s, "archived"); got != "unavailable" {
		t.Fatalf("corrupted restore cleared quarantine: %s", got)
	}
	if err := os.WriteFile(object, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveRun(t.Context(), "archived", 0); err != nil {
		t.Fatal(err)
	}
	ids := storedEventIDs(t, storageZIP(t, s, "archived")["events.jsonl"])
	if len(ids) != 2 || ids[0] != "preserved" || ids[1] != "late-local" {
		t.Fatalf("lost or duplicated telemetry: %v", ids)
	}
}

func TestArchiveMissingPolicyIsFixedAtAdmission(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.archiveIOCheck = func() { t.Fatal("admission performed NAS I/O") }
	for _, body := range []string{`{"confirmCurrentArchive":true,"allowMissingRuns":true}`, `{"confirmCurrentArchive":true}`} {
		if r := reconnectRequest(s, body); r.Code != http.StatusAccepted {
			t.Fatal(r.Body)
		}
		if !s.archiveAllowMissing {
			t.Fatal("duplicate request changed the in-flight policy")
		}
	}
	s.archiveReconnectState = "failed"
	if r := reconnectRequest(s, `{"confirmCurrentArchive":true}`); r.Code != http.StatusAccepted {
		t.Fatal(r.Body)
	}
	if s.archiveAllowMissing {
		t.Fatal("strict API default inherited a previous request's missing-run policy")
	}
}

func TestArchiveMissingPreservesVerifiedCompletedAnalysis(t *testing.T) {
	f := newAnalysisStorageFixture(t, false)
	if err := f.save(map[string]string{"analysisId": "retained-job"}); err != nil {
		t.Fatal(err)
	}
	s := f.server
	archiveTestRun(t, s, "run")
	if err := s.quarantineMissingArchiveRun("run", errors.New("source objects missing")); err != nil {
		t.Fatal(err)
	}
	url := f.path + "/result?jobId=job-old"
	response := resultRequest(s, http.MethodGet, url)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "retained-job") {
		t.Fatalf("completed snapshot unavailable: %d %s", response.Code, response.Body)
	}
	if got := listedStorageState(t, s, "run"); got != "unavailable" {
		t.Fatalf("artifact read cleared source quarantine: %s", got)
	}
	local, err := s.analysisDirectory("run")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readRunArchive(local, "run")
	local.Close()
	if err != nil {
		t.Fatal(err)
	}
	name := analysisArtifactFile(analysisResultFile, f.job.status.ArtifactSlot)
	stored := manifest.Files[name]
	remote := filepath.Join(s.config.DataDir, archivedRunsDirectory, "run", stored.Object)
	if err := os.WriteFile(remote, bytes.Repeat([]byte("x"), int(stored.Size)), 0600); err != nil {
		t.Fatal(err)
	}
	response = resultRequest(s, http.MethodGet, url)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unverified snapshot served: %d", response.Code)
	}
	if len(s.resultReadPins) != 0 || len(s.archiveReadSlots) != 0 {
		t.Fatal("artifact read leaked NAS capacity or result pins")
	}
}
