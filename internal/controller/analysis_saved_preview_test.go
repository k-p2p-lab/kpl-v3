package controller

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestSavedAnalysisRemainsReadableDuringReplacementAndAfterRestart(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := "run"
		if batch {
			name = "batch"
		}
		t.Run(name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if batch {
				batchFixture(t, s, name, "first", "completed", 1, 2, 1)
				batchFixture(t, s, name, "second", "completed", 2, 2, 2)
			} else {
				resultFixture(t, s, name, "completed", time.Unix(1, 0))
			}
			start := func(ctx context.Context, refresh bool) (analysisJobStatus, error) {
				if batch {
					status, err := s.startBatchAnalysis(ctx, name, refresh)
					return status.analysisJobStatus, err
				}
				return s.startAnalysisJob(ctx, name, refresh)
			}
			await := func() analysisJobStatus {
				if batch {
					return awaitBatch(t, s, name).analysisJobStatus
				}
				return awaitAnalysisJob(t, s, name)
			}
			if _, err := start(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			first := await()
			s.analysisWorkers.Wait()
			if first.State != "completed" {
				t.Fatalf("initial analysis: %+v", first)
			}
			original := resultRequest(s, http.MethodGet, first.ResultURL)
			if original.Code != http.StatusOK {
				t.Fatal(original.Body)
			}

			// Hold the shared analysis slot so the replacement stays pending.
			s.analysisSlots <- struct{}{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			replacement, err := start(ctx, true)
			if err != nil || replacement.SavedAnalysisID != first.ID || replacement.ID == first.ID {
				t.Fatalf("saved identity: %+v %v", replacement, err)
			}
			previewURL := first.ResultURL + "&saved=1"
			assertPreview := func(server *Server) {
				t.Helper()
				preview := resultRequest(server, http.MethodGet, previewURL)
				if preview.Code != http.StatusOK || preview.Body.String() != original.Body.String() {
					t.Fatalf("saved preview: %d %s", preview.Code, preview.Body)
				}
				if response := resultRequest(server, http.MethodHead, previewURL); response.Code != http.StatusOK || response.Body.Len() != 0 {
					t.Fatalf("saved HEAD: %d %s", response.Code, response.Body)
				}
				if response := resultRequest(server, http.MethodGet, first.ResultURL); response.Code != http.StatusConflict {
					t.Fatalf("ordinary endpoint silently returned old results: %d", response.Code)
				}
				if response := resultRequest(server, http.MethodGet, previewURL+"-wrong"); response.Code != http.StatusConflict {
					t.Fatalf("invalid saved selector accepted: %d", response.Code)
				}
			}
			assertPreview(s)
			// Pending job metadata carries the saved identity across restart.
			assertPreview(New(s.config, nil))
			cancel()
			interrupted := await()
			s.analysisWorkers.Wait()
			<-s.analysisSlots
			if interrupted.State != "interrupted" || interrupted.SavedAnalysisID != first.ID {
				t.Fatalf("interrupted replacement discarded saved identity: %+v", interrupted)
			}
			assertPreview(s)

			if _, err := start(context.Background(), true); err != nil {
				t.Fatal(err)
			}
			completed := await()
			s.analysisWorkers.Wait()
			if completed.State != "completed" || completed.ID == first.ID || completed.SavedAnalysisID != "" {
				t.Fatalf("replacement did not take over: %+v", completed)
			}
			for _, server := range []*Server{s, New(s.config, nil)} {
				if response := resultRequest(server, http.MethodGet, previewURL); response.Code != http.StatusConflict {
					t.Fatalf("new artifact served under old ID: %d", response.Code)
				}
				if response := resultRequest(server, http.MethodGet, completed.ResultURL); response.Code != http.StatusOK {
					t.Fatalf("new artifact unavailable: %d %s", response.Code, response.Body)
				}
			}
		})
	}
}

func TestAnalysisSavedArtifactRequiresExplicitMatchingIdentity(t *testing.T) {
	for _, state := range []string{"queued", "running", "failed", "interrupted"} {
		status := analysisJobStatus{ID: "current", State: state, SavedAnalysisID: "previous"}
		if !status.servesArtifact("previous", true) {
			t.Fatalf("saved result unavailable in %s", state)
		}
		for _, id := range []string{"", "current", "unrelated"} {
			if status.servesArtifact(id, true) {
				t.Fatalf("%s accepted unrelated saved ID %q", state, id)
			}
		}
		if status.servesArtifact("previous", false) {
			t.Fatalf("%s returned saved data without opt-in", state)
		}
	}
}
