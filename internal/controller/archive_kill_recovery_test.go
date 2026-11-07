package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const archiveKillRunID = "killed-archive"
const archiveKillReady = "archive-remote-committed\n"

func archiveKillChild(t *testing.T, directory string) {
	t.Helper()
	s := New(ServerConfig{DataDir: directory}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	resultFixture(t, s, archiveKillRunID, "completed", time.Now().UTC())
	if err := s.state.persistEvents(archiveKillRunID, []model.TraceEvent{storageEvent(archiveKillRunID, "before-kill")}); err != nil {
		t.Fatal(err)
	}
	putNote(t, s, archiveKillRunID, "Note retained after abrupt exit", "0")
	local, err := s.analysisDirectory(archiveKillRunID)
	if err != nil {
		t.Fatal(err)
	}
	status := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "killed-analysis", RunID: archiveKillRunID, State: "completed"}
	if err := writeAnalysisJSON(local, analysisJobFile, status); err != nil {
		local.Close()
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"analysis": strings.Repeat("retained payload ", 1<<16)})
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	if err := local.WriteFile(analysisResultFile, payload, 0600); err != nil {
		local.Close()
		t.Fatal(err)
	}
	local.Close()

	// This existing NAS boundary is after local source capture and before the
	// remote publication. Retain the local lock so archiveRun cannot commit its
	// new local object map or unlink bytes before the parent kills this process.
	var once sync.Once
	s.archiveIOCheck = func() {
		once.Do(func() {
			s.state.persistMu.Lock()
			go func() {
				manifestPath := filepath.Join(directory, archivedRunsDirectory, archiveKillRunID, runArchiveManifest)
				for {
					data, err := os.ReadFile(manifestPath)
					var manifest runArchiveManifestData
					s.archiveStatusMu.RLock()
					phase := s.archivePhase
					s.archiveStatusMu.RUnlock()
					if phase == "preparing" && err == nil && json.Unmarshal(data, &manifest) == nil && manifest.RunID == archiveKillRunID && len(manifest.Logs["events.jsonl"]) == 1 && manifest.Files[analysisResultFile].SHA256 != "" {
						_, _ = io.WriteString(os.Stdout, archiveKillReady)
						return
					}
					time.Sleep(time.Millisecond)
				}
			}()
		})
	}
	if err := s.archiveRun(t.Context(), archiveKillRunID, 0); err != nil {
		t.Fatal(err)
	}
	t.Fatal("archiveRun passed the held local commit boundary without being killed")
}

func TestArchiveAbruptExitAfterRemoteCommitReleasesVerifiedLocalPayload(t *testing.T) {
	if directory := os.Getenv("KPL_ARCHIVE_KILL_CHILD_DIRECTORY"); directory != "" {
		archiveKillChild(t, directory)
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("Linux abrupt-exit recovery regression")
	}
	directory := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, executable, "-test.run=^TestArchiveAbruptExitAfterRemoteCommitReleasesVerifiedLocalPayload$")
	child.Env = append(os.Environ(), "KPL_ARCHIVE_KILL_CHILD_DIRECTORY="+directory)
	child.WaitDelay = time.Second
	var stderr bytes.Buffer
	child.Stderr = &stderr
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill() })
	reader := bufio.NewReader(output)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			_ = child.Wait()
			t.Fatalf("child did not reach the remote commit boundary: %v: %s", err, &stderr)
		}
		if line == archiveKillReady {
			break
		}
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = child.Wait()
	var exited *exec.ExitError
	if !errors.As(err, &exited) {
		t.Fatalf("child did not exit abruptly: %v", err)
	}
	if status, ok := exited.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child did not stop through the requested SIGKILL: %v", err)
	}

	localDirectory := filepath.Join(directory, currentRunsDirectory, archiveKillRunID)
	remoteDirectory := filepath.Join(directory, archivedRunsDirectory, archiveKillRunID)
	data, err := os.ReadFile(filepath.Join(remoteDirectory, runArchiveManifest))
	if err != nil {
		t.Fatal(err)
	}
	var committed runArchiveManifestData
	if err := json.Unmarshal(data, &committed); err != nil {
		t.Fatal(err)
	}
	artifact := committed.Files[analysisResultFile]
	remoteArtifact := filepath.Join(remoteDirectory, artifact.Object)
	before, err := os.Stat(remoteArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(localDirectory, analysisResultFile)); err != nil {
		t.Fatalf("local analysis was removed before the crash boundary: %v", err)
	}
	if _, err := os.Stat(filepath.Join(localDirectory, committed.Logs["events.jsonl"][0].Segment)); err != nil {
		t.Fatalf("local sealed log was removed before the crash boundary: %v", err)
	}
	restarted := New(ServerConfig{DataDir: directory}, nil)
	// Telemetry arriving after restart remains a separate local tail and must
	// appear exactly once alongside the already committed remote segment.
	if err := restarted.state.persistEvents(archiveKillRunID, []model.TraceEvent{storageEvent(archiveKillRunID, "after-kill")}); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, restarted, archiveKillRunID)
	manifest := archiveReuseManifest(t, restarted, archiveKillRunID)
	if manifest.Files[analysisResultFile].Object != artifact.Object {
		t.Fatalf("restart copied an already committed identical artifact again: %q -> %q", artifact.Object, manifest.Files[analysisResultFile].Object)
	}
	after, err := os.Stat(remoteArtifact)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("restart replaced the verified remote artifact: %v", err)
	}
	if _, err := os.Stat(filepath.Join(localDirectory, analysisResultFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified local artifact still consumes local space: %v", err)
	}
	entries, err := os.ReadDir(localDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if isRunLog(entry.Name()) || logSegment(entry.Name(), "events.jsonl") || logSegment(entry.Name(), "observations.jsonl") {
			t.Fatalf("verified local log still consumes local space: %s", entry.Name())
		}
	}
	for _, name := range []string{"experiment.json", "scenario.yaml", resultNoteFile, analysisJobFile, runArchiveManifest} {
		if _, err := os.Stat(filepath.Join(localDirectory, name)); err != nil {
			t.Fatalf("restart removed retained local metadata %s: %v", name, err)
		}
	}
	if ids := strings.Join(storedEventIDs(t, storageZIP(t, restarted, archiveKillRunID)["events.jsonl"]), ","); ids != "before-kill,after-kill" {
		t.Fatalf("restart lost or double-counted a committed segment or late tail: %s", ids)
	}
	entries, err = os.ReadDir(remoteDirectory)
	if err != nil {
		t.Fatal(err)
	}
	objects := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "analysis-result-") {
			objects++
		}
	}
	if objects != 1 {
		t.Fatalf("restart left %d copies of one committed analysis artifact", objects)
	}
	again := New(ServerConfig{DataDir: directory}, nil)
	archiveTestRun(t, again, archiveKillRunID)
	if got := archiveReuseManifest(t, again, archiveKillRunID).Files[analysisResultFile].Object; got != artifact.Object {
		t.Fatalf("a further restart changed the recovered object identity: %s", got)
	}
	if ids := strings.Join(storedEventIDs(t, storageZIP(t, again, archiveKillRunID)["events.jsonl"]), ","); ids != "before-kill,after-kill" {
		t.Fatalf("a further restart changed logical source contents: %s", ids)
	}
}
