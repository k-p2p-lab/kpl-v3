package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Hold the real encoder inside a large artifact so lock tests do not depend on
// the host's disk speed, scheduler timing, or the size of production logs.
type heldAnalysisJSON struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (v *heldAnalysisJSON) MarshalJSON() ([]byte, error) {
	close(v.entered)
	<-v.release
	return []byte(`{"payload":"` + strings.Repeat("x", 1<<20) + `"}`), nil
}
func (v *heldAnalysisJSON) unblock() { v.once.Do(func() { close(v.release) }) }

func storageOperation(t *testing.T, operation func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- operation() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("operation blocked behind analysis artifact encoding")
	}
}

type analysisStorageFixture struct {
	server         *Server
	ctx            context.Context
	cancel         context.CancelFunc
	job            *analysisJob
	batch          *batchAnalysisJob
	path, artifact string
	save           func(any) error
}

func newAnalysisStorageFixture(t *testing.T, batch bool) analysisStorageFixture {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f := analysisStorageFixture{server: s, ctx: ctx, cancel: cancel}
	status := analysisJobStatus{Version: 1, AnalysisVersion: currentAnalysisVersion, ID: "job-old", State: "running", Phase: "saving", TotalBytes: 100}
	if batch {
		batchFixture(t, s, "batch", "run", "completed", 1, 2, 1)
		batchFixture(t, s, "batch", "second", "completed", 2, 2, 1)
		members, err := s.batchMembers(ctx, "batch")
		if err != nil {
			t.Fatal(err)
		}
		f.batch = &batchAnalysisJob{status: batchAnalysisStatus{analysisJobStatus: status, BatchID: "batch", Membership: batchMembership(members), TotalRuns: 2, ExpectedRuns: 2}, cancel: cancel}
		s.batchAnalysisJobs["batch"] = f.batch
		if err := s.persistBatchAnalysis(f.batch.status); err != nil {
			t.Fatal(err)
		}
		f.path = "/api/v1/batch-analysis-jobs/batch"
		f.artifact = filepath.Join(s.config.DataDir, "batch-analyses", "batch", batchResultFile)
		f.save = func(value any) error { return s.saveBatchAnalysis(ctx, f.batch, value) }
	} else {
		resultFixture(t, s, "run", "completed", time.Unix(1, 0))
		status.RunID = "run"
		status.SourceRevision, _ = s.runSourceRevision("run")
		f.job = &analysisJob{status: status, cancel: cancel}
		s.analysisJobs["run"] = f.job
		if err := s.persistAnalysisJob(status); err != nil {
			t.Fatal(err)
		}
		f.path = "/api/v1/analysis-jobs/run"
		f.artifact = filepath.Join(s.config.DataDir, currentRunsDirectory, "run", analysisResultFile)
		f.save = func(value any) error {
			return s.saveAnalysisJob(ctx, f.job, value, map[string]string{"summary": "saved"})
		}
	}
	return f
}
func holdAnalysisSave(t *testing.T, save func(any) error) (*heldAnalysisJSON, <-chan error) {
	t.Helper()
	value := &heldAnalysisJSON{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(value.unblock)
	done := make(chan error, 1)
	go func() { done <- save(value) }()
	select {
	case <-value.entered:
	case err := <-done:
		t.Fatalf("save stopped before encoding: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("save did not reach encoder")
	}
	return value, done
}
func finishAnalysisSave(t *testing.T, value *heldAnalysisJSON, done <-chan error) error {
	t.Helper()
	value.unblock()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("save did not finish")
		return nil
	}
}
func noAnalysisTemps(t *testing.T, artifact string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(filepath.Dir(artifact), ".analysis-*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files retained: %v %v", files, err)
	}
}

func TestAnalysisArtifactEncodingAllowsStatusTelemetryDeletionAndStop(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%t", batch), func(t *testing.T) {
			f := newAnalysisStorageFixture(t, batch)
			resultFixture(t, f.server, "deletable", "completed", time.Unix(1, 0))
			resultFixture(t, f.server, "live", "completed", time.Unix(1, 0))
			live, stop := context.WithCancel(context.Background())
			defer stop()
			f.server.cancels["live"] = stop
			value, done := holdAnalysisSave(t, f.save)
			for _, url := range []string{f.path, "/api/v1/results"} {
				storageOperation(t, func() error {
					response := resultRequest(f.server, http.MethodGet, url)
					if response.Code != 200 {
						return fmt.Errorf("%s: %d %s", url, response.Code, response.Body)
					}
					return nil
				})
			}
			storageOperation(t, func() error {
				return f.server.state.appendRunEvents("live", []model.TraceEvent{{RunID: "live", NodeID: "peer", EventID: "event", Type: "test", Timestamp: time.Now()}})
			})
			storageOperation(t, func() error { return f.server.deleteSavedResult("deletable") })
			storageOperation(t, func() error { return f.server.StopScenario("live") })
			if live.Err() != context.Canceled {
				t.Fatal("stop did not cancel the live run")
			}
			if err := finishAnalysisSave(t, value, done); err != nil {
				t.Fatal(err)
			}
			response := resultRequest(f.server, http.MethodGet, f.path)
			if !strings.Contains(response.Body.String(), `"state":"completed"`) {
				t.Fatalf("completion: %s", response.Body)
			}
			noAnalysisTemps(t, f.artifact)
		})
	}
}

func TestAnalysisArtifactCommitRejectsCanceledDeletedAndReplacedJobs(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, change := range []string{"cancel", "delete", "replace"} {
			t.Run(fmt.Sprintf("batch=%t/%s", batch, change), func(t *testing.T) {
				f := newAnalysisStorageFixture(t, batch)
				original := []byte(`{"owner":"existing"}`)
				if err := os.WriteFile(f.artifact, original, 0600); err != nil {
					t.Fatal(err)
				}
				value, done := holdAnalysisSave(t, f.save)
				storageOperation(t, func() error {
					switch change {
					case "cancel":
						f.cancel()
					case "delete":
						return f.server.deleteSavedResult("run")
					case "replace":
						f.server.analysisJobMu.Lock()
						defer f.server.analysisJobMu.Unlock()
						if batch {
							newer := &batchAnalysisJob{status: f.batch.status}
							newer.status.ID, newer.status.State = "job-new", "completed"
							f.server.batchAnalysisJobs["batch"] = newer
						} else {
							newer := &analysisJob{status: f.job.status}
							newer.status.ID, newer.status.State = "job-new", "completed"
							f.server.analysisJobs["run"] = newer
						}
					}
					return nil
				})
				err := finishAnalysisSave(t, value, done)
				if err == nil {
					t.Fatal("obsolete analysis was published")
				}
				if change != "delete" && !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel error: %v", err)
				}
				data, readErr := os.ReadFile(f.artifact)
				if change == "delete" && !batch {
					if !errors.Is(readErr, os.ErrNotExist) {
						t.Fatalf("deleted result was restored: %v", readErr)
					}
				} else if readErr != nil || string(data) != string(original) {
					t.Fatalf("previous artifact was replaced: %q %v", data, readErr)
				}
				noAnalysisTemps(t, f.artifact)
			})
		}
	}
}
