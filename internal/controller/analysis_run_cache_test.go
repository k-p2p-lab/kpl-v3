package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func batchRunCacheFixture(t *testing.T) (*Server, resultAnalysis) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "cache-batch", "cache-run", "completed", 1, 2, 2)
	snapshot, err := s.captureResultFilesContext(t.Context(), "cache-run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	snapshot.result.SourceHash, err = analysisSourceHash(t.Context(), snapshot.files, nil)
	if err != nil {
		t.Fatal(err)
	}
	analysis, err := analyzeResult(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	compactBatchAnalysis(&analysis)
	return s, analysis
}

func cacheCurrentMember(t *testing.T, s *Server, id string) savedResult {
	t.Helper()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	member, err := s.batchRunCacheMemberLocked(id)
	if err != nil {
		t.Fatal(err)
	}
	return member
}

func TestBatchRunCacheSurvivesRestartAndArchiveWithoutRawReads(t *testing.T) {
	s, analysis := batchRunCacheFixture(t)
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	archiveTestRun(t, s, analysis.Result.ID)
	restarted := New(s.config, nil)
	restarted.archiveIOCheck = func() { t.Fatal("a valid Run cache opened NAS source data") }
	member := cacheCurrentMember(t, restarted, analysis.Result.ID)
	if member.SourceRevision != analysis.Result.SourceRevision {
		t.Fatal("archive move changed the logical source revision")
	}
	snapshot, hit, err := restarted.captureBatchRunAnalysisCache(t.Context(), member)
	if err != nil || !hit || snapshot == nil {
		t.Fatalf("valid persisted cache not reused: hit=%t error=%v", hit, err)
	}
	if snapshot.analysisCacheKind != "batch" || len(snapshot.files) != 1 || snapshot.files[0].name != batchRunCacheSourceName || snapshot.result.SourceHash != analysis.Result.SourceHash {
		t.Fatalf("invalid cached snapshot: %+v", snapshot)
	}
	var cached resultAnalysis
	if err := json.NewDecoder(snapshot.files[0].reader()).Decode(&cached); err != nil {
		t.Fatal(err)
	}
	wantResearch, _ := json.Marshal(analysis.Research)
	gotResearch, _ := json.Marshal(cached.Research)
	wantMetrics, _ := json.Marshal(analysis.Metrics)
	gotMetrics, _ := json.Marshal(cached.Metrics)
	if !bytes.Equal(wantResearch, gotResearch) || !bytes.Equal(wantMetrics, gotMetrics) || !cached.AsOf.Equal(analysis.AsOf) {
		t.Fatal("cache changed compact charts, scientific metrics or their calculation boundary")
	}
	if err := restarted.deleteSavedResult(member.ID); !errors.Is(err, errResultBusy) {
		t.Fatalf("a cache reader did not protect source deletion: %v", err)
	}
	snapshot.close()
	snapshot.close()
	if len(restarted.resultReadPins) != 0 {
		t.Fatalf("cache snapshot retained Run pins: %v", restarted.resultReadPins)
	}
}

func TestBatchRunCacheCorruptionAndVersionDriftAreMisses(t *testing.T) {
	for _, kind := range []string{"payload", "index", "version", "length"} {
		t.Run(kind, func(t *testing.T) {
			s, analysis := batchRunCacheFixture(t)
			if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
				t.Fatal(err)
			}
			root, err := s.batchRunCacheDirectory(analysis.Result.ID, false)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			index, err := readBatchRunCacheIndex(root)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "payload":
				err = root.WriteFile(analysisArtifactFile(batchResultFile, index.ArtifactSlot), []byte("bad"), 0600)
			case "index":
				err = root.WriteFile(batchRunCacheIndexFile, []byte("bad"), 0600)
			case "version":
				index.AnalysisVersion--
				err = writeAnalysisJSON(root, batchRunCacheIndexFile, index)
			case "length":
				index.Bytes = batchRunCacheMaxBytes + 1
				err = writeAnalysisJSON(root, batchRunCacheIndexFile, index)
			}
			if err != nil {
				t.Fatal(err)
			}
			cached, hit, err := s.captureBatchRunAnalysisCache(t.Context(), analysis.Result)
			if err != nil || hit || cached != nil || len(s.resultReadPins) != 0 {
				t.Fatalf("invalid cache blocked fallback or retained ownership: hit=%t error=%v pins=%v", hit, err, s.resultReadPins)
			}
		})
	}
}

func TestBatchRunCacheRejectsChangedSourcesAndUnsettledWindows(t *testing.T) {
	s, analysis := batchRunCacheFixture(t)
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, batchRunCacheDirectory, analysis.Result.ID, batchRunCacheIndexFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: analysis.Result.ID, EventID: "late", Type: "publish", MessageID: "late-message", Topic: "topic"}}}); err != nil {
		t.Fatal(err)
	}
	current := cacheCurrentMember(t, s, analysis.Result.ID)
	if current.SourceRevision == analysis.Result.SourceRevision {
		t.Fatal("late telemetry did not change the source identity")
	}
	if cached, hit, err := s.captureBatchRunAnalysisCache(t.Context(), current); err != nil || hit || cached != nil {
		t.Fatalf("changed source reused an old cache: %t %v", hit, err)
	}
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("stale calculation replaced the prior cache reference: %v", err)
	}
	unsettled, pending := batchRunCacheFixture(t)
	pending.Metrics.PendingPublications = 1
	if err := unsettled.saveBatchRunAnalysisCache(t.Context(), pending); err != nil {
		t.Fatal(err)
	}
	if cached, hit, err := unsettled.captureBatchRunAnalysisCache(t.Context(), pending.Result); err != nil || hit || cached != nil {
		t.Fatalf("a pending publication was frozen in cache: %t %v", hit, err)
	}
}

func TestBatchRunCacheCannotRecreateDeletedResults(t *testing.T) {
	s, analysis := batchRunCacheFixture(t)
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSavedResult(analysis.Result.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); !errors.Is(err, errResultNotFound) {
		t.Fatalf("cache publication ignored a Run deletion fence: %v", err)
	}
	if cached, hit, err := s.captureBatchRunAnalysisCache(t.Context(), analysis.Result); !errors.Is(err, errResultNotFound) || hit || cached != nil {
		t.Fatalf("deleted Run remained cache-addressable: hit=%t error=%v", hit, err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, analysis.Result.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cache publication recreated deleted source data: %v", err)
	}
}

func TestBatchRunCacheSeedsSavedArtifactDuringReplacement(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for iteration, id := range []string{"first", "second"} {
		batchFixture(t, s, "batch", id, "completed", iteration+1, 2, 2)
	}
	if _, err := s.startBatchAnalysis(t.Context(), "batch", false); err != nil {
		t.Fatal(err)
	}
	status := awaitBatch(t, s, "batch")
	s.analysisWorkers.Wait()
	if status.State != "completed" {
		t.Fatalf("initial batch failed: %+v", status)
	}
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, batchRunCacheDirectory)); err != nil {
		t.Fatal(err)
	}
	s.analysisJobMu.Lock()
	job := s.batchAnalysisJobs["batch"]
	job.status.SavedAnalysisID, job.status.ID, job.status.State = job.status.ID, "replacement", "queued"
	s.analysisJobMu.Unlock()
	if err := s.seedBatchRunAnalysisCache(t.Context(), "batch"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		cached, hit, err := s.captureBatchRunAnalysisCache(t.Context(), cacheCurrentMember(t, s, id))
		if err != nil || !hit || cached == nil {
			t.Fatalf("saved batch did not seed %s during replacement: hit=%t error=%v", id, hit, err)
		}
		cached.close()
	}
	if len(s.resultReadPins) != 0 {
		t.Fatal("seeding retained source pins")
	}
}

func TestBatchRunCacheCanceledPublicationKeepsCompletedPayload(t *testing.T) {
	s, analysis := batchRunCacheFixture(t)
	if err := s.saveBatchRunAnalysisCache(t.Context(), analysis); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, batchRunCacheDirectory, analysis.Result.ID, batchRunCacheIndexFile)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.saveBatchRunAnalysisCache(ctx, analysis); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cache publication: %v", err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("canceled publication replaced a completed cache: %v", err)
	}
	if len(s.resultReadPins) != 0 {
		t.Fatal("canceled cache publication retained Run pins")
	}
}
