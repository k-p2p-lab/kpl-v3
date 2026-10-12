package controller

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAnalysisPublicationFailurePreservesSavedSnapshot(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, slot := range []int{0, 1} {
			t.Run(fmt.Sprintf("batch=%t/slot=%d", batch, slot), func(t *testing.T) {
				f := newAnalysisStorageFixture(t, batch)
				if err := f.save(map[string]string{"analysisId": "job-old"}); err != nil {
					t.Fatal(err)
				}
				// Exercise replacement of both legacy and previously replaced caches.
				if slot == 1 {
					if batch {
						f.batch.status.ID, f.batch.status.SavedAnalysisID = "job-middle", "job-old"
					} else {
						f.job.status.ID, f.job.status.SavedAnalysisID = "job-middle", "job-old"
					}
					if err := f.save(map[string]string{"analysisId": "job-middle"}); err != nil {
						t.Fatal(err)
					}
				}
				old := analysisJobStatus{}
				if batch {
					old = f.batch.status.analysisJobStatus
					f.batch.status.ID, f.batch.status.SavedAnalysisID, f.batch.status.State = "job-new", old.ID, "running"
					if err := f.server.persistBatchAnalysis(f.batch.status); err != nil {
						t.Fatal(err)
					}
				} else {
					old = f.job.status
					f.job.status.ID, f.job.status.SavedAnalysisID, f.job.status.State = "job-new", old.ID, "running"
					if err := f.server.persistAnalysisJob(f.job.status); err != nil {
						t.Fatal(err)
					}
				}
				url := f.path + "/result?jobId=" + old.ID + "&saved=1"
				original := resultRequest(f.server, http.MethodGet, url)
				if original.Code != http.StatusOK {
					t.Fatal(original.Body)
				}
				blockedName := batchResultFile
				if !batch {
					blockedName = analysisSummaryFile
				}
				blocked := filepath.Join(filepath.Dir(f.artifact), analysisArtifactFile(blockedName, 1-slot))
				// A directory prevents the second publication after the full run
				// artifact has been renamed. Batch failure happens at its sole artifact.
				if err := os.Remove(blocked); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				if err := os.Mkdir(blocked, 0700); err != nil {
					t.Fatal(err)
				}
				if err := f.save(map[string]string{"analysisId": "job-new"}); err == nil {
					t.Fatal("publication unexpectedly succeeded")
				}
				for _, server := range []*Server{f.server, New(f.server.config, nil)} {
					preview := resultRequest(server, http.MethodGet, url)
					if preview.Code != http.StatusOK || preview.Body.String() != original.Body.String() {
						t.Fatalf("saved snapshot lost: %d %s", preview.Code, preview.Body)
					}
					if !batch {
						summary := resultRequest(server, http.MethodGet, strings.Replace(url, "/result?", "/summary?", 1))
						if summary.Code != http.StatusOK || !strings.Contains(summary.Body.String(), `"summary":"saved"`) {
							t.Fatalf("saved summary lost: %d %s", summary.Code, summary.Body)
						}
					}
				}
				if err := os.Remove(blocked); err != nil {
					t.Fatal(err)
				}
				if err := f.save(map[string]string{"analysisId": "job-new"}); err != nil {
					t.Fatal(err)
				}
				for _, server := range []*Server{f.server, New(f.server.config, nil)} {
					response := resultRequest(server, http.MethodGet, f.path+"/result?jobId=job-new")
					if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"analysisId":"job-new"`) {
						t.Fatalf("new snapshot unavailable: %d %s", response.Code, response.Body)
					}
					if response := resultRequest(server, http.MethodGet, url); response.Code != http.StatusConflict {
						t.Fatalf("new snapshot served under old ID: %d", response.Code)
					}
				}
				noAnalysisTemps(t, f.artifact)
			})
		}
	}
}

func TestReplacementAnalysisSurvivesArchivalAndRestart(t *testing.T) {
	f := newAnalysisStorageFixture(t, false)
	if err := f.save(map[string]string{"analysisId": "job-old"}); err != nil {
		t.Fatal(err)
	}
	for round := range 5 {
		id := fmt.Sprintf("job-new-%d", round)
		f.job.status.SavedAnalysisID, f.job.status.ID = f.job.status.ID, id
		if err := f.save(map[string]string{"analysisId": id}); err != nil {
			t.Fatal(err)
		}
		archiveTestRun(t, f.server, "run")
		for _, server := range []*Server{f.server, New(f.server.config, nil)} {
			for _, path := range []string{"/result?jobId=" + id, "/summary?jobId=" + id} {
				response := resultRequest(server, http.MethodGet, f.path+path)
				if response.Code != http.StatusOK || strings.Contains(path, "/result?") && !strings.Contains(response.Body.String(), `"analysisId":"`+id+`"`) {
					t.Fatalf("archived artifact: %d %s", response.Code, response.Body)
				}
			}
		}
	}
}

func TestAnalysisRecoveryRejectsInvalidArtifactSlot(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, slot := range []int{-1, 2} {
			t.Run(fmt.Sprintf("batch=%t/slot=%d", batch, slot), func(t *testing.T) {
				f := newAnalysisStorageFixture(t, batch)
				if err := f.save(map[string]string{"analysisId": "job-old"}); err != nil {
					t.Fatal(err)
				}
				if batch {
					f.batch.status.ArtifactSlot = slot
					if err := f.server.persistBatchAnalysis(f.batch.status); err != nil {
						t.Fatal(err)
					}
				} else {
					f.job.status.ArtifactSlot = slot
					if err := f.server.persistAnalysisJob(f.job.status); err != nil {
						t.Fatal(err)
					}
				}
				response := resultRequest(New(f.server.config, nil), http.MethodGet, f.path+"/result?jobId=job-old")
				if response.Code != http.StatusUnprocessableEntity {
					t.Fatalf("invalid slot accepted: %d %s", response.Code, response.Body)
				}
			})
		}
	}
}
