package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestArchiveRecoveryChecksObjectsDiscoveredInRemoteManifest(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "same-size-corruption"} {
		t.Run(damage, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "remote-commit"
			resultFixture(t, s, id, "completed", time.Now().UTC())
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "first")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			localDir := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			oldManifest, err := os.ReadFile(filepath.Join(localDir, runArchiveManifest))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "second")}); err != nil {
				t.Fatal(err)
			}
			archiveTestRun(t, s, id)
			manifest := archiveReuseManifest(t, s, id)
			if len(manifest.Logs["events.jsonl"]) != 2 {
				t.Fatal("fixture must publish two log segments")
			}
			stored := manifest.Logs["events.jsonl"][1]
			// The remote manifest includes the second segment, but the local
			// manifest still predates the commit that would authorize its unlink.
			if err := os.WriteFile(filepath.Join(localDir, runArchiveManifest), oldManifest, 0600); err != nil {
				t.Fatal(err)
			}
			restarted := exerciseArchiveReuseRecovery(t, s, id, stored, stored.Segment, damage)
			if got := strings.Join(storedEventIDs(t, storageZIP(t, restarted, id)["events.jsonl"]), ","); got != "first,second" {
				t.Fatalf("remote commit recovery duplicated or lost events: %s", got)
			}
		})
	}
}

func TestArchiveRecoveryChecksReusedAnalysisArtifact(t *testing.T) {
	for _, damage := range []string{"missing", "truncated", "same-size-corruption"} {
		t.Run(damage, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			const id = "reused-analysis"
			resultFixture(t, s, id, "completed", time.Now().UTC())
			root, err := s.analysisDirectory(id)
			if err != nil {
				t.Fatal(err)
			}
			status := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "job-done", RunID: id, State: "completed"}
			if err := writeAnalysisJSON(root, analysisJobFile, status); err != nil {
				root.Close()
				t.Fatal(err)
			}
			const saved = `{"analysis":"retained"}`
			if err := root.WriteFile(analysisResultFile, []byte(saved), 0600); err != nil {
				root.Close()
				t.Fatal(err)
			}
			root.Close()
			archiveTestRun(t, s, id)
			stored := archiveReuseManifest(t, s, id).Files[analysisResultFile]
			restarted := exerciseArchiveReuseRecovery(t, s, id, stored, analysisResultFile, damage)
			if got := archiveReuseManifest(t, restarted, id).Files[analysisResultFile].Object; got != stored.Object {
				t.Fatalf("identical analysis artifact was uploaded again: %q != %q", got, stored.Object)
			}
			response := resultRequest(restarted, "GET", "/api/v1/analysis-jobs/"+id+"/result?jobId=job-done")
			if response.Code != 200 || response.Body.String() != saved {
				t.Fatalf("recovered analysis download: %d %s", response.Code, response.Body)
			}
		})
	}
}

func archiveReuseManifest(t *testing.T, s *Server, id string) runArchiveManifestData {
	t.Helper()
	root, err := s.analysisDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	manifest, err := readRunArchive(root, id)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func exerciseArchiveReuseRecovery(t *testing.T, s *Server, id string, stored storedRunFile, localName, damage string) *Server {
	t.Helper()
	remotePath := filepath.Join(s.config.DataDir, archivedRunsDirectory, id, stored.Object)
	saved, err := os.ReadFile(remotePath)
	if err != nil {
		t.Fatal(err)
	}
	localPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, localName)
	if err := os.WriteFile(localPath, saved, 0600); err != nil {
		t.Fatal(err)
	}
	switch damage {
	case "missing":
		err = os.Remove(remotePath)
	case "truncated":
		err = os.WriteFile(remotePath, saved[:len(saved)/2], 0600)
	case "same-size-corruption":
		corrupt := bytes.Clone(saved)
		corrupt[0] ^= 1
		err = os.WriteFile(remotePath, corrupt, 0600)
	default:
		t.Fatalf("unknown damage: %s", damage)
	}
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	if err := restarted.archiveRun(context.Background(), id, 0); err == nil {
		t.Error("reused an unavailable committed object")
	}
	if current, err := os.ReadFile(localPath); err != nil || !bytes.Equal(current, saved) {
		t.Fatalf("failed recovery discarded the valid local copy: %v", err)
	}
	if err := os.WriteFile(remotePath, saved, 0600); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, restarted, id)
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Fatalf("verified recovery retained the redundant local copy: %v", err)
	}
	return restarted
}
