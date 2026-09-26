package controller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func hashTestFile(t *testing.T, name, contents string) resultFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return resultFile{name: name, file: file, size: info.Size(), info: info}
}

func TestAnalysisSourceHashUsesLogicalContents(t *testing.T) {
	ctx := context.Background()
	whole := hashTestFile(t, "events.jsonl", "first\nsecond\n")
	first := hashTestFile(t, "first", "first\n")
	second := hashTestFile(t, "second", "second\n")
	segmented := resultFile{name: "events.jsonl", size: first.size + second.size, parts: []resultFile{first, second}}
	a, err := analysisSourceHash(ctx, []resultFile{whole}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var read int64
	b, err := analysisSourceHash(ctx, []resultFile{segmented, hashTestFile(t, "note.json", "a changed note")}, func(n int64) { read += n })
	if err != nil || a != b || read != whole.size {
		t.Fatalf("segmentation/notes changed source hash: %q %q read=%d err=%v", a, b, read, err)
	}
	changed := hashTestFile(t, "events.jsonl", "third\nsecond\n")
	c, err := analysisSourceHash(ctx, []resultFile{changed}, nil)
	if err != nil || a == c {
		t.Fatalf("same-sized content change was missed: %v", err)
	}
	// A captured boundary must not silently hash a truncated source as complete.
	if err := os.Truncate(whole.file.Name(), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := analysisSourceHash(ctx, []resultFile{whole}, nil); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncation: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := analysisSourceHash(canceled, []resultFile{segmented}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestAnalysisReusesHashAfterTouchAndForcesManualReanalysis(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Unix(1, 0))
	ctx := context.Background()
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	first := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	if first.State != "completed" || first.SourceHash == "" {
		t.Fatalf("first analysis: %+v", first)
	}
	artifact := resultRequest(s, http.MethodGet, first.ResultURL).Body.String()
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, "run", "scenario.yaml")
	if err := os.Chtimes(path, time.Unix(4000, 0), time.Unix(4000, 0)); err != nil {
		t.Fatal(err)
	}
	stale, err := s.analysisJobStatus("run")
	if err != nil || !stale.Stale {
		t.Fatalf("metadata changes must request hash verification: %+v %v", stale, err)
	}
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	reused := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	if reused.State != "completed" || reused.ID != first.ID || !reused.Reused || reused.Stale || reused.SourceHash != first.SourceHash || reused.SourceRevision == first.SourceRevision || reused.SnapshotAt != first.SnapshotAt || reused.CreatedAt != first.CreatedAt {
		t.Fatalf("unchanged bytes did not reuse original snapshot: %+v", reused)
	}
	if resultRequest(s, http.MethodGet, reused.ResultURL).Body.String() != artifact {
		t.Fatal("hash verification rewrote the analysis artifact")
	}
	restarted := New(s.config, nil)
	cached, err := restarted.startAnalysisJob(ctx, "run", false)
	if err != nil || cached.ID != first.ID || cached.State != "completed" {
		t.Fatalf("verified revision did not survive restart: %+v %v", cached, err)
	}
	_, err = restarted.startAnalysisJob(ctx, "run", true)
	if err != nil {
		t.Fatal(err)
	}
	forced := awaitAnalysisJob(t, restarted, "run")
	restarted.analysisWorkers.Wait()
	if forced.State != "completed" || forced.ID == first.ID || forced.Reused || forced.SourceHash != first.SourceHash {
		t.Fatalf("manual reanalysis must recompute identical sources: %+v", forced)
	}
	// Legacy artifacts cannot be trusted by hashing today's source retroactively.
	forced.SourceHash = ""
	if err := restarted.persistAnalysisJob(forced); err != nil {
		t.Fatal(err)
	}
	legacy := New(s.config, nil)
	status, err := legacy.analysisJobStatus("run")
	if err != nil || !status.Stale {
		t.Fatalf("legacy artifact was not marked for baseline analysis: %+v %v", status, err)
	}
	if _, err := legacy.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	baseline := awaitAnalysisJob(t, legacy, "run")
	legacy.analysisWorkers.Wait()
	if baseline.State != "completed" || baseline.ID == forced.ID || baseline.SourceHash == "" || baseline.Reused {
		t.Fatalf("legacy baseline: %+v", baseline)
	}
}

func TestAnalysisDetectsSameSizeRewriteWithPreservedMtime(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "run", "completed", 1, 2, 1)
	ctx := context.Background()
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	first := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, "run", "events.jsonl")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if data[i] == '0' {
			data[i] = '1'
			break
		}
	}
	// Give file change time a distinct value even on coarse test filesystems.
	time.Sleep(2 * time.Millisecond)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	stale, err := s.analysisJobStatus("run")
	if err != nil || !stale.Stale {
		t.Fatalf("same-size preserved-mtime write missed: %+v %v", stale, err)
	}
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	changed := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	if changed.State != "completed" || changed.ID == first.ID || changed.SourceHash == first.SourceHash || changed.Reused {
		t.Fatalf("changed content reused stale metrics: %+v", changed)
	}
}

func TestAnalysisHashSurvivesNASArchival(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "run", "completed", 1, 2, 2)
	ctx := context.Background()
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	first := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	archiveTestRun(t, s, "run")
	if _, err := s.startAnalysisJob(ctx, "run", false); err != nil {
		t.Fatal(err)
	}
	archived := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	if archived.State != "completed" || archived.ID != first.ID || archived.SourceHash != first.SourceHash || !archived.Reused || archived.Stale {
		t.Fatalf("archive relocation regenerated identical analysis: %+v", archived)
	}
}

func TestBatchAnalysisHashReuseChangesAndForce(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "one", "completed", 1, 3, 1)
	batchFixture(t, s, "batch", "two", "completed", 2, 3, 3)
	batchFixture(t, s, "batch", "failed", "failed", 3, 3, 9)
	ctx := context.Background()
	if _, err := s.startBatchAnalysis(ctx, "batch", false); err != nil {
		t.Fatal(err)
	}
	first := awaitBatch(t, s, "batch")
	s.analysisWorkers.Wait()
	if first.State != "completed" || first.SourceHash == "" {
		t.Fatalf("first batch: %+v", first)
	}
	artifact := resultRequest(s, http.MethodGet, first.ResultURL).Body.String()
	archiveTestRun(t, s, "one")
	archiveTestRun(t, s, "two")
	if _, err := s.startBatchAnalysis(ctx, "batch", false); err != nil {
		t.Fatal(err)
	}
	reused := awaitBatch(t, s, "batch")
	s.analysisWorkers.Wait()
	if reused.State != "completed" || reused.ID != first.ID || !reused.Reused || reused.Membership == first.Membership || reused.SourceHash != first.SourceHash {
		t.Fatalf("archived batch did not reuse original analysis: %+v", reused)
	}
	if resultRequest(s, http.MethodGet, reused.ResultURL).Body.String() != artifact {
		t.Fatal("batch hash check rewrote the artifact")
	}
	// Failed cases are excluded from means; appending unused logs cannot change them.
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, "failed", "events.jsonl")
	if err := os.WriteFile(path, []byte("unreadable but excluded\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.startBatchAnalysis(ctx, "batch", false); err != nil {
		t.Fatal(err)
	}
	if got := awaitBatch(t, s, "batch"); got.State != "completed" || got.ID != first.ID {
		t.Fatalf("excluded logs invalidated statistics: %+v", got)
	}
	s.analysisWorkers.Wait()
	restarted := New(s.config, nil)
	cached, err := restarted.startBatchAnalysis(ctx, "batch", false)
	if err != nil || cached.ID != first.ID || cached.State != "completed" {
		t.Fatalf("batch restart: %+v %v", cached, err)
	}
	if _, err := restarted.startBatchAnalysis(ctx, "batch", true); err != nil {
		t.Fatal(err)
	}
	forced := awaitBatch(t, restarted, "batch")
	restarted.analysisWorkers.Wait()
	if forced.State != "completed" || forced.ID == first.ID || forced.SourceHash != first.SourceHash || forced.Reused {
		t.Fatalf("forced batch analysis: %+v", forced)
	}
	// A successful replacement changes the population and must be recomputed.
	batchFixture(t, restarted, "batch", "failed", "completed", 3, 3, 5)
	if _, err := restarted.startBatchAnalysis(ctx, "batch", false); err != nil {
		t.Fatal(err)
	}
	changed := awaitBatch(t, restarted, "batch")
	restarted.analysisWorkers.Wait()
	if changed.State != "completed" || changed.ID == forced.ID || changed.SourceHash == forced.SourceHash || changed.TotalRuns != 3 || changed.Reused {
		t.Fatalf("changed batch population reused old analysis: %+v", changed)
	}
}
