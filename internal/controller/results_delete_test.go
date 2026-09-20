package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestResultDeleteRequiresAuthenticationAndRejectsDownloadLease(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	experiment, _ := resultFixture(t, server, "run-delete", "completed", time.Now().UTC())
	if response := resultRequest(server, http.MethodDelete, "/api/v1/results/"+experiment.ID); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated delete status=%d", response.Code)
	}
	snapshot, err := server.captureResult(experiment.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.prepareResultArchive(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodDelete, "/api/v1/results/"+experiment.ID, nil)
		authenticateRequest(t, server, r)
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, r)
		return response
	}
	if response := request(); response.Code != http.StatusConflict {
		t.Fatalf("delete during snapshot status=%d: %s", response.Code, response.Body)
	}
	snapshot.close()
	if response := request(); response.Code != http.StatusNoContent {
		t.Fatalf("delete after snapshot status=%d: %s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", experiment.ID)); !os.IsNotExist(err) {
		t.Fatalf("saved directory remains: %v", err)
	}
	server.resultArchiveMu.Lock()
	_, archiveInfoRemains := server.resultArchives[experiment.ID]
	server.resultArchiveMu.Unlock()
	if archiveInfoRemains {
		t.Fatal("deleted result retained its archive-size cache")
	}
	if response := request(); response.Code != http.StatusNotFound {
		t.Fatalf("second delete status=%d", response.Code)
	}
}

func TestResultDeleteBlocksRunningQueuedAndFinalizingRuns(t *testing.T) {
	for _, state := range []string{"running", "queued", "finalizing", "batch-member"} {
		t.Run(state, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			experiment, _ := resultFixture(t, server, "run-busy", "completed", time.Now().UTC())
			switch state {
			case "running", "queued":
				experiment.State = state
				server.state.experiments[experiment.ID] = experiment
			case "finalizing":
				server.cancels[experiment.ID] = func() {}
			case "batch-member":
				server.repeatBatches = map[string]*repeatBatch{experiment.ID: {cancel: func() {}}}
			}
			if err := server.deleteSavedResult(experiment.ID); !errors.Is(err, errResultBusy) {
				t.Fatalf("busy result delete error=%v", err)
			}
			if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", experiment.ID, "experiment.json")); err != nil {
				t.Fatalf("busy result was altered: %v", err)
			}
		})
	}
}

func TestResultDeletionTombstonePreventsLateEventResurrectionAfterRestart(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, _ := resultFixture(t, server, "run-late", "completed", time.Now().UTC())
	server.state.experiments[experiment.ID] = experiment
	batch := model.EventBatch{Events: []model.TraceEvent{{RunID: experiment.ID, Type: "publish", MessageID: "message", Topic: "topic"}}}
	if err := server.state.appendEvents(batch); err != nil {
		t.Fatal(err)
	}
	if err := server.deleteSavedResult(experiment.ID); err != nil {
		t.Fatal(err)
	}
	for _, current := range []*Server{server, New(server.config, nil)} {
		if err := current.state.appendEvents(batch); err != nil {
			t.Fatal(err)
		}
		current.updateExperiment(experiment.ID, func(run *model.Experiment) { run.State = "running" })
		if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", experiment.ID)); !os.IsNotExist(err) {
			t.Fatalf("late telemetry recreated deleted result: %v", err)
		}
		if response := resultRequest(current, http.MethodGet, "/api/v1/experiments/"+experiment.ID+"/download"); response.Code != http.StatusNotFound {
			t.Fatalf("deleted download status=%d", response.Code)
		}
		if response := resultRequest(current, http.MethodGet, "/api/v1/results"); response.Body.String() != "[]\n" {
			t.Fatalf("deleted result reappeared in listing: %s", response.Body)
		}
	}
	if len(server.state.snapshot().Experiments) != 0 || len(server.state.snapshot().Events) != 0 {
		t.Fatal("deleted result remained in current Controller history")
	}
}

func TestResultDeletionRejectsUnsafePathsAndDoesNotFollowLinks(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run-safe", "completed", time.Now().UTC())
	for _, id := range []string{"", "..", "../run-safe", "run-safe/experiment.json", `run-safe\experiment.json`} {
		if err := server.deleteSavedResult(id); !errors.Is(err, errResultNotFound) {
			t.Fatalf("unsafe ID %q error=%v", id, err)
		}
	}
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "preserve")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(server.config.DataDir, "runs", "run-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := server.deleteSavedResult("run-link"); err == nil {
		t.Fatal("accepted symlinked result directory")
	}
	if err := os.Symlink(outside, filepath.Join(server.config.DataDir, "runs", "run-safe", "extra-link")); err != nil {
		t.Fatal(err)
	}
	if err := server.deleteSavedResult("run-safe"); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(outsideFile); err != nil || string(data) != "outside" {
		t.Fatalf("deletion escaped result directory: %q %v", data, err)
	}
}

func TestQueuedSavedResultBecomesInterruptedAfterControllerRestart(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, original := resultFixture(t, server, "run-queued", "queued", time.Time{})
	server.state.experiments[experiment.ID] = experiment
	for index, current := range []*Server{server, New(server.config, nil)} {
		response := resultRequest(current, http.MethodGet, "/api/v1/results")
		var results []savedResult
		if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
			t.Fatal(err)
		}
		wantState := "queued"
		if index == 1 {
			wantState = "interrupted"
		}
		if len(results) != 1 || results[0].State != wantState || results[0].Active {
			t.Fatalf("queued ownership: %+v", results)
		}
		if results[0].DownloadBytes != nil || results[0].DownloadSizeMaxAgeMS != nil {
			t.Fatal("result list performed a cold archive measurement")
		}
	}
	if data, err := os.ReadFile(filepath.Join(server.config.DataDir, "runs", experiment.ID, "experiment.json")); err != nil || string(data) != string(original) {
		t.Fatal("restart listing rewrote queued metadata")
	}
}
