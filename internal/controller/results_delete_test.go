package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
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
	if _, err := os.Stat(filepath.Join(server.config.DataDir, currentRunsDirectory, experiment.ID)); !os.IsNotExist(err) {
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
			if _, err := os.Stat(filepath.Join(server.config.DataDir, currentRunsDirectory, experiment.ID, "experiment.json")); err != nil {
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
		if _, err := os.Stat(filepath.Join(server.config.DataDir, currentRunsDirectory, experiment.ID)); !os.IsNotExist(err) {
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

func TestResultDeletionRetriesExistingMarkerAndRejectsUnsafeMarker(t *testing.T) {
	for _, marker := range []string{"regular", "directory", "symlink"} {
		t.Run(marker, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			run, _ := resultFixture(t, server, "delete-retry", "completed", time.Now())
			markers := filepath.Join(server.config.DataDir, ".deleted-results")
			if err := os.Mkdir(markers, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(markers, run.ID)
			var err error
			switch marker {
			case "regular":
				// Even a crash before writing the timestamp leaves a valid fence.
				err = os.WriteFile(path, nil, 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(filepath.Join("..", currentRunsDirectory, run.ID, "experiment.json"), path)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = server.deleteSavedResult(run.ID)
			_, statErr := os.Stat(filepath.Join(server.config.DataDir, currentRunsDirectory, run.ID, "experiment.json"))
			if marker == "regular" {
				if err != nil || !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("deletion retry failed: delete=%v, source=%v", err, statErr)
				}
			} else if err == nil || statErr != nil {
				t.Fatalf("unsafe marker must preserve source: delete=%v, source=%v", err, statErr)
			}
		})
	}
}

func TestResultDeletionReleasesRunMetricsAndTiming(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run, _ := resultFixture(t, server, "run-forget", "completed", time.Now().UTC())
	server.state.experiments[run.ID] = run
	server.state.runTimings[run.ID] = &runTiming{phases: make([]phaseTiming, 100)}
	agent, err := server.state.registerAgent(model.Agent{ID: "agent", URL: "http://agent"})
	if err != nil {
		t.Fatal(err)
	}
	node := model.Node{ID: "ended", RunID: run.ID, State: model.NodeReady}
	keeper := model.Node{ID: "live", RunID: "other-run", State: model.NodeReady}
	heartbeat := model.AgentHeartbeat{Agent: model.Agent{ID: agent.ID}, Nodes: []model.Node{node, keeper}, Partial: true}
	if err := server.state.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	events := []model.TraceEvent{
		{RunID: keeper.RunID, Type: "publish", Topic: "topic"},
		{RunID: run.ID, Type: "publish", Topic: "topic", Fields: map[string]any{"wireBytes": 123, "payloadEncoding": "raw"}},
		{RunID: run.ID, Type: "send_prune", Fields: map[string]any{"controlEntries": 1, "messageIdCount": 2, "peerExchangeCount": 3}},
		{RunID: run.ID, Type: "phase-operation-failed", Fields: map[string]any{"action": "publish"}},
		{RunID: run.ID, Type: "telemetry_drop", Fields: map[string]any{"count": 4}},
	}
	if err := server.state.appendEvents(model.EventBatch{AgentID: agent.ID, Events: events}); err != nil {
		t.Fatal(err)
	}
	node.State = model.NodeStopped
	heartbeat.Nodes = []model.Node{node}
	if err := server.state.heartbeat(heartbeat); err != nil {
		t.Fatal(err)
	}
	if err := server.deleteSavedResult(run.ID); err != nil {
		t.Fatal(err)
	}
	if _, retained := server.state.runTimings[run.ID]; retained {
		t.Error("deleted result retained phase timing history")
	}
	if _, retained := server.state.runMetrics[run.ID]; retained {
		t.Error("deleted result retained detailed live metrics")
	}
	// Filtering in place must release fields in the unused backing-array tail.
	for _, event := range server.state.events[len(server.state.events):cap(server.state.events)] {
		if event.Fields != nil || event.RunID != "" {
			t.Error("recent-event backing array retained deleted evidence")
			break
		}
	}
	checkCounters := func() {
		t.Helper()
		for key := range gatherTestMetrics(t, server.state) {
			if strings.HasSuffix(strings.SplitN(key, "{", 2)[0], "_total") && strings.Contains(key, `run_id="`+run.ID+`"`) {
				t.Errorf("deleted run retained counter %s", key)
				break
			}
		}
		server.state.metrics.initializedAgents.Range(func(key, _ any) bool {
			if key.([2]string)[0] == run.ID {
				t.Error("deleted run retained Agent metric initialization")
			}
			return true
		})
		server.state.metrics.initializedTopics.Range(func(key, _ any) bool {
			if key.([3]string)[0] == run.ID {
				t.Error("deleted run retained topic metric initialization")
			}
			return true
		})
		requireMetricValue(t, gatherTestMetrics(t, server.state), "kpl_events_total", map[string]string{"run_id": keeper.RunID, "agent_id": agent.ID, "event_type": "publish", "topic": "topic"}, 1)
	}
	checkCounters()
	// A replayed terminal record and an older live report must not reinitialize
	// deleted counters. The terminal inventory fence still rejects revival.
	for _, lateState := range []string{model.NodeStopped, model.NodeReady} {
		node.State = lateState
		heartbeat.Nodes = []model.Node{node}
		if err := server.state.heartbeat(heartbeat); err != nil {
			t.Fatal(err)
		}
		checkCounters()
	}
	// Even an ID absent from Controller inventory is fenced by the durable
	// result marker, including after Controller restart.
	for _, current := range []*Server{server, New(server.config, nil)} {
		if current != server {
			if _, err := current.state.registerAgent(model.Agent{ID: agent.ID, URL: agent.URL}); err != nil {
				t.Fatal(err)
			}
		}
		node.ID, node.State = "late-unknown", model.NodeReady
		heartbeat.Nodes = []model.Node{node}
		if err := current.state.heartbeat(heartbeat); err != nil {
			t.Fatal(err)
		}
		for key := range gatherTestMetrics(t, current.state) {
			if strings.HasPrefix(key, "kpl_events_total{") && strings.Contains(key, `run_id="`+run.ID+`"`) {
				t.Fatalf("late unknown node recreated deleted counters: %s", key)
			}
		}
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
	link := filepath.Join(server.config.DataDir, currentRunsDirectory, "run-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := server.deleteSavedResult("run-link"); err == nil {
		t.Fatal("accepted symlinked result directory")
	}
	if err := os.Symlink(outside, filepath.Join(server.config.DataDir, currentRunsDirectory, "run-safe", "extra-link")); err != nil {
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
	if data, err := os.ReadFile(filepath.Join(server.config.DataDir, currentRunsDirectory, experiment.ID, "experiment.json")); err != nil || string(data) != string(original) {
		t.Fatal("restart listing rewrote queued metadata")
	}
}
