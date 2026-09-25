package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func storageEvent(id, event string) model.TraceEvent {
	return model.TraceEvent{RunID: id, EventID: event, NodeID: "peer", Type: "join", Timestamp: time.Now().UTC()}
}
func archiveTestRun(t *testing.T, s *Server, id string) {
	t.Helper()
	if err := s.archiveRun(context.Background(), id, 0); err != nil {
		t.Fatal(err)
	}
}
func storageZIP(t *testing.T, s *Server, id string) map[string][]byte {
	t.Helper()
	response := resultRequest(s, "GET", "/api/v1/experiments/"+id+"/download")
	if response.Code != 200 {
		t.Fatalf("download %d: %s", response.Code, response.Body)
	}
	return decodeResultZIP(t, response.Body.Bytes())
}
func storedEventIDs(t *testing.T, data []byte) []string {
	t.Helper()
	ids := []string{}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event model.TraceEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, event.EventID)
	}
	return ids
}

func TestRunArchiveOffloadsLogsAndPreservesLateEventsNotesAndRestart(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run-archive", "completed", time.Now())
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("run-archive", "first")}}); err != nil {
		t.Fatal(err)
	}
	note := putNote(t, s, "run-archive", "Observation before archive", "0")
	original := storageZIP(t, s, "run-archive")
	archiveTestRun(t, s, "run-archive")
	for _, name := range []string{"experiment.json", "scenario.yaml", resultNoteFile, runArchiveManifest} {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "run-archive", name)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(s.config.DataDir, currentRunsDirectory, "run-archive"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "events.jsonl" || logSegment(entry.Name(), "events.jsonl") {
			t.Fatalf("copied log retained locally: %s", entry.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "runs", "run-archive", "events.jsonl")); err != nil {
		t.Fatal(err)
	}
	archived := storageZIP(t, s, "run-archive")
	if !bytes.Equal(archived["events.jsonl"], original["events.jsonl"]) {
		t.Fatal("archiving changed event bytes")
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent("run-archive", "late")}}); err != nil {
		t.Fatal(err)
	}
	pending := storageZIP(t, s, "run-archive")
	if got := strings.Join(storedEventIDs(t, pending["events.jsonl"]), ","); got != "first,late" {
		t.Fatalf("combined event prefix: %s", got)
	}
	putNote(t, s, "run-archive", "Edited while archived", note.Revision)
	archiveTestRun(t, s, "run-archive")
	restarted := New(s.config, nil)
	final := storageZIP(t, restarted, "run-archive")
	if got := strings.Join(storedEventIDs(t, final["events.jsonl"]), ","); got != "first,late" {
		t.Fatalf("restart changed events: %s", got)
	}
	var exportedNote resultNote
	if err := json.Unmarshal(final[resultNoteFile], &exportedNote); err != nil || exportedNote.Text != "Edited while archived" {
		t.Fatalf("lost note: %s", final[resultNoteFile])
	}
	response := resultRequest(restarted, "GET", "/api/v1/results")
	var list []savedResult
	if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil || len(list) != 1 {
		t.Fatalf("list: %s", response.Body)
	}
	if list[0].Storage == nil || list[0].Storage.State != "archived" {
		t.Fatalf("storage status: %+v", list[0].Storage)
	}
	var bytes int64
	for _, name := range resultSourceFiles {
		bytes += int64(len(final[name]))
	}
	if list[0].SourceBytes == nil || *list[0].SourceBytes != bytes {
		t.Fatalf("source bytes: %+v want %d", list[0].SourceBytes, bytes)
	}
	snapshot, err := restarted.captureResultFiles("run-archive", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	analysis, err := analyzeResult(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if analysis.EventCount != 2 {
		t.Fatalf("analysis omitted archived or local events: %d", analysis.EventCount)
	}
}

func TestArchiveRecoversRemoteCommitBeforeLocalManifestWithoutDuplicates(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "run-crash"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "first")}); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	dir := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
	oldManifest, err := os.ReadFile(filepath.Join(dir, runArchiveManifest))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "second")}); err != nil {
		t.Fatal(err)
	}
	segment := "events-segment-20990101T000000.000000000-crash.jsonl"
	if err := os.Rename(filepath.Join(dir, "events.jsonl"), filepath.Join(dir, segment)); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(filepath.Join(dir, segment))
	if err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	// Simulate process loss after the NAS commit, before local publication/unlink.
	if err := os.WriteFile(filepath.Join(dir, runArchiveManifest), oldManifest, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, segment), saved, 0600); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	// A same-sized local change must never be silently discarded by recovery.
	changed := append([]byte(nil), saved...)
	changed[0] ^= 1
	if err := os.WriteFile(filepath.Join(dir, segment), changed, 0600); err != nil {
		t.Fatal(err)
	}
	if err := restarted.archiveRun(context.Background(), id, 0); err == nil {
		t.Fatal("recovery discarded a changed local segment")
	}
	if current, err := os.ReadFile(filepath.Join(dir, segment)); err != nil || !bytes.Equal(current, changed) {
		t.Fatal("failed verification removed local data")
	}
	if err := os.WriteFile(filepath.Join(dir, segment), saved, 0600); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, restarted, id)
	result := storageZIP(t, restarted, id)
	if got := strings.Join(storedEventIDs(t, result["events.jsonl"]), ","); got != "first,second" {
		t.Fatalf("recovery duplicated or lost events: %s", got)
	}
}

func TestNASStallCannotBlockLocalIngestionControlResultsOrNotes(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run-old", "completed", time.Now())
	if err := s.state.persistEvents("run-old", []model.TraceEvent{storageEvent("run-old", "old")}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.archiveIOCheck = func() { once.Do(func() { close(entered); <-release }) }
	archiveDone := make(chan error, 1)
	go func() { archiveDone <- s.archiveRun(context.Background(), "run-old", 0) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("archive did not reach injected NAS stall")
	}
	defer func() {
		close(release)
		select {
		case <-archiveDone:
		case <-time.After(3 * time.Second):
			t.Error("archive did not resume")
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var experiment model.Experiment
	storageOperation(t, func() error {
		var err error
		experiment, err = s.StartScenario(ctx, []byte("version: 1\nname: storage-isolation\nphases:\n - action: wait\n   duration: 1s\n"))
		return err
	})
	storageOperation(t, func() error {
		return s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{storageEvent(experiment.ID, "live")}})
	})
	storageOperation(t, func() error {
		response := resultRequest(s, "GET", "/api/v1/results")
		if response.Code != 200 {
			return errors.New(response.Body.String())
		}
		return nil
	})
	storageOperation(t, func() error { text := "local note"; _, err := s.resultNote(experiment.ID, &text, "0"); return err })
	storageOperation(t, func() error { return s.StopScenario(experiment.ID) })
	cancel()
	s.runs.Wait()
}

func TestArchiveDestinationMismatchRetainsLocalData(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "run-mount"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "first")}); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	if err := os.Rename(filepath.Join(s.config.DataDir, "runs"), filepath.Join(s.config.DataDir, "nas-offline")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(s.config.DataDir, "runs"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "late")}); err != nil {
		t.Fatal(err)
	}
	if err := s.archiveRun(context.Background(), id, 0); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing mount accepted: %v", err)
	}
	entries, _ := os.ReadDir(filepath.Join(s.config.DataDir, "runs"))
	if len(entries) != 0 {
		t.Fatal("wrote archive into underlying mount directory")
	}
	local, _ := os.ReadDir(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	found := false
	for _, entry := range local {
		found = found || logSegment(entry.Name(), "events.jsonl")
	}
	if !found {
		t.Fatal("failed transfer discarded local tail")
	}
	if err := os.Remove(filepath.Join(s.config.DataDir, "runs")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(s.config.DataDir, "nas-offline"), filepath.Join(s.config.DataDir, "runs")); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, id)
	if got := strings.Join(storedEventIDs(t, storageZIP(t, s, id)["events.jsonl"]), ","); got != "first,late" {
		t.Fatal(got)
	}
}

func TestLegacyArchiveImportAndDeletionRemainLocalWhileOffline(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "legacy"
	resultFixture(t, s, id, "completed", time.Now())
	if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "legacy-event")}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(s.config.DataDir, currentRunsDirectory), filepath.Join(s.config.DataDir, "runs")); err != nil {
		t.Fatal(err)
	}
	if err := s.importArchivedRuns(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")); !os.IsNotExist(err) {
		t.Fatal("import copied a large log onto local storage")
	}
	if got := strings.Join(storedEventIDs(t, storageZIP(t, s, id)["events.jsonl"]), ","); got != "legacy-event" {
		t.Fatal(got)
	}
	block := func() { t.Fatal("local metadata operation attempted NAS access") }
	s.archiveIOCheck = block
	putNote(t, s, id, "edited offline", "0")
	if response := resultRequest(s, "GET", "/api/v1/results"); response.Code != 200 || !strings.Contains(response.Body.String(), id) {
		t.Fatalf("offline list: %s", response.Body)
	}
	if err := s.deleteSavedResult(id); err != nil {
		t.Fatal(err)
	}
	if response := resultRequest(s, "GET", "/api/v1/results"); response.Code != 200 || strings.Contains(response.Body.String(), id) {
		t.Fatalf("deleted result resurfaced: %s", response.Body)
	}
	s.archiveIOCheck = nil
	if err := s.cleanDeletedArchives(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, "runs", id)); !os.IsNotExist(err) {
		t.Fatal("archived result not cleaned up")
	}
	if err := s.importArchivedRuns(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveKeepsLivePeersAndActiveRunsLocal(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "collecting"
	experiment, _ := resultFixture(t, s, id, "completed", time.Now())
	s.state.mu.Lock()
	s.state.setNodeLocked(model.Node{ID: "live", RunID: id, State: model.NodeReady})
	s.state.mu.Unlock()
	if err := s.archiveRun(context.Background(), id, 0); !errors.Is(err, errResultBusy) {
		t.Fatalf("archived live Peer: %v", err)
	}
	s.state.mu.Lock()
	s.state.setNodeLocked(model.Node{ID: "live", RunID: id, State: model.NodeStopped})
	experiment.State = "running"
	s.state.experiments[id] = experiment
	s.state.mu.Unlock()
	if err := s.archiveRun(context.Background(), id, 0); !errors.Is(err, errResultBusy) {
		t.Fatalf("archived active run: %v", err)
	}
}

func TestRunStorageAdmissionAndCancelableBatchWait(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunMinFreeBytes: math.MaxUint64}, nil)
	scenario := `version: 1
name: storage-guard
phases:
 - action: wait
   duration: 1ms
`
	if _, err := s.StartScenario(context.Background(), []byte(scenario)); !errors.Is(err, errRunStorageFull) {
		t.Fatalf("admitted without space: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(scenario))
	request.Header.Set("Content-Type", "application/yaml")
	response := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusInsufficientStorage {
		t.Fatalf("admission status: %d %s", response.Code, response.Body)
	}
	experiment, _ := resultFixture(t, s, "waiting", "queued", time.Time{})
	s.state.mu.Lock()
	s.state.experiments[experiment.ID] = experiment
	s.state.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.waitRunStorage(ctx, experiment.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	waiting := s.state.experiments[experiment.ID]
	s.state.mu.RUnlock()
	if waiting.State != "queued" || waiting.PhaseName != "Waiting for local result storage" {
		t.Fatalf("misleading wait state: %+v", waiting)
	}
}

func TestArchivedAnalysisArtifactRemainsDownloadable(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	id := "analyzed"
	resultFixture(t, s, id, "completed", time.Now())
	status := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "job-done", RunID: id, State: "completed"}
	root, err := s.analysisDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeAnalysisJSON(root, analysisJobFile, status); err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(analysisResultFile, []byte(`{"analysis":"preserved"}`), 0600); err != nil {
		t.Fatal(err)
	}
	root.Close()
	archiveTestRun(t, s, id)
	restarted := New(s.config, nil)
	result := resultRequest(restarted, "GET", "/api/v1/analysis-jobs/"+id+"/result?jobId=job-done")
	if result.Code != 200 || result.Body.String() != `{"analysis":"preserved"}` {
		t.Fatalf("archived artifact: %d %s", result.Code, result.Body)
	}
}

func TestLegacyArchiveImportRecoversPartialLocalIndex(t *testing.T) {
	for _, manifestPublished := range []bool{false, true} {
		t.Run(fmt.Sprint("manifest-published-", manifestPublished), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			id := "partial-import"
			resultFixture(t, s, id, "completed", time.Now())
			if err := s.state.persistEvents(id, []model.TraceEvent{storageEvent(id, "retained")}); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(filepath.Join(s.config.DataDir, currentRunsDirectory), filepath.Join(s.config.DataDir, archivedRunsDirectory)); err != nil {
				t.Fatal(err)
			}
			localPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			if err := os.MkdirAll(localPath, 0755); err != nil {
				t.Fatal(err)
			}
			local, err := os.OpenRoot(localPath)
			if err != nil {
				t.Fatal(err)
			}
			defer local.Close()
			metadata, err := os.ReadFile(filepath.Join(s.config.DataDir, archivedRunsDirectory, id, "experiment.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := writeImportedMetadata(local, runArchiveImportFile, []byte("pending\n")); err != nil {
				t.Fatal(err)
			}
			if err := writeImportedMetadata(local, "experiment.json", metadata); err != nil {
				t.Fatal(err)
			}
			if manifestPublished {
				manifest, err := readRunArchive(local, id)
				if err != nil {
					t.Fatal(err)
				}
				if err := writeRunArchive(local, manifest); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(s.config, nil)
			if err := restarted.archiveRun(context.Background(), id, 0); !errors.Is(err, errResultBusy) {
				t.Fatalf("uploaded an incomplete import: %v", err)
			}
			if err := restarted.importArchivedRuns(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := local.Lstat(runArchiveImportFile); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("incomplete import marker remains: %v", err)
			}
			if _, err := local.Lstat("events.jsonl"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("recovery copied large archived logs locally")
			}
			files := storageZIP(t, restarted, id)
			if len(files["scenario.yaml"]) == 0 || strings.Join(storedEventIDs(t, files["events.jsonl"]), ",") != "retained" {
				t.Fatal("partial import recovery omitted source files")
			}
		})
	}
}

func TestRunArchiveRejectsOversizedManifestBeforePublication(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	manifest, err := readRunArchive(root, "oversized")
	if err != nil {
		t.Fatal(err)
	}
	manifest.Files["large.json"] = storedRunFile{Object: strings.Repeat("x", runArchiveManifestLimit)}
	if err := writeRunArchive(root, manifest); err == nil {
		t.Fatal("published an unreadable index")
	}
	if _, err := root.Lstat(runArchiveManifest); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("oversized index was published")
	}
}

func TestArchivedBatchResumesOfflineAndRejectsPreviouslyRecordedPendingRun(t *testing.T) {
	for _, observed := range []bool{false, true} {
		t.Run(fmt.Sprint("pending-has-events-", observed), func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 3)
			if observed {
				if err := f.server.state.persistEvents(runs[1].ID, []model.TraceEvent{storageEvent(runs[1].ID, "already-recorded")}); err != nil {
					t.Fatal(err)
				}
			}
			for _, run := range runs {
				archiveTestRun(t, f.server, run.ID)
			}
			restarted := New(f.server.config, nil)
			f.register(t, restarted)
			f.fail.Store(false)
			restarted.archiveIOCheck = func() { t.Fatal("resume attempted NAS access") }
			if err := os.Rename(filepath.Join(restarted.config.DataDir, archivedRunsDirectory), filepath.Join(restarted.config.DataDir, "offline")); err != nil {
				t.Fatal(err)
			}
			_, err := restarted.ResumeScenarioBatch(context.Background(), runs[0].BatchID)
			if observed {
				if !errors.Is(err, errBatchNotResumable) {
					t.Fatalf("resumed a run with archived observations: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			waitRepetitions(t, restarted)
			for _, run := range runs[1:] {
				if persistedExperiment(t, restarted, run.ID).State != "completed" {
					t.Fatal("offline continuation failed")
				}
			}
		})
	}
}
