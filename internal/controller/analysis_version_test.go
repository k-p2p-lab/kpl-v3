package controller

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestOutdatedAnalysisRemainsReadableAndRequestsRecomputation(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name, endpoint := "run", "/api/v1/analysis-jobs/run"
		if batch {
			name, endpoint = "batch", "/api/v1/batch-analysis-jobs/batch"
		}
		t.Run(name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if batch {
				batchFixture(t, s, name, "first", "completed", 1, 2, 1)
				batchFixture(t, s, name, "second", "completed", 2, 2, 2)
			} else {
				resultFixture(t, s, name, "completed", time.Unix(1, 0))
			}
			await := func(server *Server) analysisJobStatus {
				if batch {
					return awaitBatch(t, server, name).analysisJobStatus
				}
				return awaitAnalysisJob(t, server, name)
			}
			if response := resultRequest(s, http.MethodPost, endpoint); response.Code != http.StatusAccepted {
				t.Fatal(response.Body)
			}
			first := await(s)
			s.analysisWorkers.Wait()
			if first.State != "completed" {
				t.Fatalf("initial analysis: %+v", first)
			}
			original := resultRequest(s, http.MethodGet, first.ResultURL)
			if original.Code != http.StatusOK {
				t.Fatal(original.Body)
			}
			// Keep the source hash and revision unchanged across a software upgrade.
			if batch {
				status := s.batchAnalysisJobs[name].status
				status.AnalysisVersion = currentAnalysisVersion - 1
				if err := s.persistBatchAnalysis(status); err != nil {
					t.Fatal(err)
				}
			} else {
				status := first
				status.AnalysisVersion = currentAnalysisVersion - 1
				if err := s.persistAnalysisJob(status); err != nil {
					t.Fatal(err)
				}
			}
			restarted := New(s.config, nil)
			response, status := jobRequest(t, restarted.apiTestHandler(t.Context()), http.MethodGet, endpoint)
			if response.Code != http.StatusOK || !status.Stale || status.ID != first.ID {
				t.Errorf("obsolete analysis not marked stale: %d %+v", response.Code, status)
			}
			list := resultRequest(restarted, http.MethodGet, "/api/v1/results")
			var results []savedResult
			if list.Code != http.StatusOK || json.Unmarshal(list.Body.Bytes(), &results) != nil || len(results) == 0 {
				t.Fatalf("results list: %d %s", list.Code, list.Body)
			}
			for _, result := range results {
				listed := result.Analysis
				if batch && result.BatchAnalysis != nil {
					listed = &result.BatchAnalysis.analysisJobStatus
				}
				if listed == nil || !listed.Stale || listed.ID != first.ID {
					t.Errorf("results list lost obsolete analysis marker: %+v", listed)
				}
			}
			preview := resultRequest(restarted, http.MethodGet, first.ResultURL)
			if preview.Code != http.StatusOK || preview.Body.String() != original.Body.String() {
				t.Fatal("version change removed the saved artifact")
			}
			if response := resultRequest(restarted, http.MethodPost, endpoint); response.Code != http.StatusAccepted {
				t.Fatal(response.Body)
			}
			updated := await(restarted)
			restarted.analysisWorkers.Wait()
			if updated.State != "completed" || updated.ID == first.ID || updated.AnalysisVersion != currentAnalysisVersion || updated.Reused || updated.SourceHash != first.SourceHash || updated.Stale {
				t.Fatalf("version change did not recompute unchanged sources: %+v", updated)
			}
		})
	}
}
