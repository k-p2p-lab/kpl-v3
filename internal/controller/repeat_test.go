package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func waitRepetitions(t *testing.T, server *Server) []model.Experiment {
	t.Helper()
	done := make(chan struct{})
	go func() { server.runs.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("repetition scheduler did not finish")
	}
	experiments := server.state.snapshot().Experiments
	sort.Slice(experiments, func(i, j int) bool { return experiments[i].Iteration < experiments[j].Iteration })
	return experiments
}

func TestStopRespectsSingleRunFinalizationButCancelsRemainingBatch(t *testing.T) {
	for _, repetitions := range []int{1, 3} {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		ctx, cancel := context.WithCancel(context.Background())
		server.repeatBatches = map[string]*repeatBatch{"finalizing": {cancel: cancel, repetitions: repetitions}}
		// runScenario has retired this iteration's handle. Repeated runs still
		// need to cancel later members; a single run has passed its stop gate.
		err := server.StopScenario("finalizing")
		if repetitions == 1 && (err == nil || ctx.Err() != nil) {
			t.Fatalf("single finalizing run accepted a stop: err=%v ctx=%v", err, ctx.Err())
		}
		if repetitions > 1 && (err != nil || ctx.Err() != context.Canceled) {
			t.Fatalf("remaining batch was not canceled: err=%v ctx=%v", err, ctx.Err())
		}
		cancel()
	}
}

func TestRepeatedScenariosReserveUniqueRunsAndContinueWithoutBrowser(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	scenarioYAML := "version: 2\nname: repeated\nseed: 42\nphases:\n  - action: wait\n    duration: 40ms\n"
	body, _ := json.Marshal(map[string]any{"scenario": scenarioYAML, "repetitions": 3})
	uploadCtx, cancelUpload := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", bytes.NewReader(body)).WithContext(uploadCtx)
	request.Header.Set("Content-Type", "application/json")
	authenticateRequest(t, server, request)
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	cancelUpload()
	if response.Code != http.StatusAccepted {
		t.Fatalf("submission: %d %s", response.Code, response.Body)
	}
	var first model.Experiment
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.BatchID != first.ID || first.Iteration != 1 || first.Repetitions != 3 || first.State != "running" {
		t.Fatalf("first response lost compatibility/batch metadata: %+v", first)
	}
	entries, err := os.ReadDir(filepath.Join(server.config.DataDir, "runs"))
	if err != nil || len(entries) != 3 {
		t.Fatalf("all iterations must be reserved before admission returns: %v %v", entries, err)
	}
	experiments := waitRepetitions(t, server)
	seen := make(map[string]bool)
	for index, experiment := range experiments {
		if seen[experiment.ID] || experiment.State != "completed" || experiment.Seed != 42 || experiment.BatchID != first.ID || experiment.Iteration != index+1 {
			t.Fatalf("unexpected independent result: %+v", experiment)
		}
		seen[experiment.ID] = true
		if index > 0 && experiment.StartedAt.Before(experiments[index-1].FinishedAt) {
			t.Fatal("iterations overlapped")
		}
		manifest, err := os.ReadFile(filepath.Join(server.config.DataDir, "runs", experiment.ID, "scenario.yaml"))
		if err != nil || string(manifest) != scenarioYAML {
			t.Fatalf("original scenario changed: %q %v", manifest, err)
		}
	}
}

func TestStoppingQueuedMemberCancelsEntireRepetitionBatch(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	_, err := server.StartScenarioRepeated(context.Background(), []byte("name: stopped\nphases:\n  - action: wait\n    duration: 1h\n"), 3)
	if err != nil {
		t.Fatal(err)
	}
	var queued model.Experiment
	for _, experiment := range server.state.snapshot().Experiments {
		if experiment.Iteration == 3 {
			queued = experiment
		}
	}
	if queued.State != "queued" || !queued.StartedAt.IsZero() {
		t.Fatalf("queued run was prematurely started: %+v", queued)
	}
	if err := server.StopScenario(queued.ID); err != nil {
		t.Fatal(err)
	}
	for _, experiment := range waitRepetitions(t, server) {
		if experiment.State != "canceled" || experiment.FinishedAt.IsZero() {
			t.Fatalf("remaining iteration was not canceled: %+v", experiment)
		}
		if experiment.Iteration > 1 && !experiment.StartedAt.IsZero() {
			t.Fatal("a canceled queued iteration executed")
		}
	}
}

func TestRepeatedScenarioFailureStopsRemainingIterations(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	_, err := server.StartScenarioRepeated(context.Background(), []byte("name: failure\nphases:\n  - action: publish\n    group: absent\n    count: 1\n"), 3)
	if err != nil {
		t.Fatal(err)
	}
	experiments := waitRepetitions(t, server)
	if experiments[0].State != "failed" || !strings.Contains(experiments[0].Error, "no publish-capable") {
		t.Fatalf("first failure missing: %+v", experiments[0])
	}
	for _, experiment := range experiments[1:] {
		if experiment.State != "canceled" || !experiment.StartedAt.IsZero() {
			t.Fatalf("iteration executed after failure: %+v", experiment)
		}
	}
}

func TestRepetitionCannotAdvancePastFailedPeerCleanup(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	var deletes atomic.Int32
	agentAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deletes.Add(1)
			http.Error(w, "cleanup failed", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: model.Agent{ID: "agent", Capacity: 1}})
	}))
	defer agentAPI.Close()
	if _, err := server.state.registerAgent(model.Agent{ID: "agent", URL: agentAPI.URL, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := server.StartScenarioRepeated(context.Background(), []byte("name: cleanup\nphases:\n  - action: wait\n    duration: 1ms\n"), 2)
	if err != nil {
		t.Fatal(err)
	}
	experiments := waitRepetitions(t, server)
	if deletes.Load() == 0 || experiments[0].State != "failed" || experiments[1].State != "canceled" || !experiments[1].StartedAt.IsZero() {
		t.Fatalf("cleanup failure was ignored: deletes=%d runs=%+v", deletes.Load(), experiments)
	}
}

func TestRepetitionValidationAndLegacyYAMLSubmission(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, body := range []string{
		`{"scenario":"name: test\nphases: [{action: wait, duration: 1ms}]", "repetitions":0}`,
		`{"scenario":"name: test\nphases: [{action: wait, duration: 1ms}]", "repetitions":101}`,
		`{"scenario":"name: test\nphases: [{action: wait, duration: 1ms}]", "repetitions":1.5}`,
		`{"scenario":"invalid", "repetitions":3}`,
		`{"scenario":"anything", "unexpected":true}`,
		`{"scenario":"anything"} {}`,
	} {
		request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("accepted invalid repetition request: %s status=%d", body, response.Code)
		}
	}
	if len(server.state.snapshot().Experiments) != 0 {
		t.Fatal("invalid submission reserved experiments")
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs")); !os.IsNotExist(err) {
		t.Fatalf("invalid submission wrote result files: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader("name: legacy\nphases: [{action: wait, duration: 1ms}]\n"))
	request.Header.Set("Content-Type", "application/yaml")
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("legacy YAML failed: %d %s", response.Code, response.Body)
	}
	if experiments := waitRepetitions(t, server); len(experiments) != 1 || experiments[0].State != "completed" {
		t.Fatalf("legacy single run changed: %+v", experiments)
	}
}

func TestExperimentSubmissionLimitsAreBasedOnDecodedScenarioBytes(t *testing.T) {
	base := validSavedScenarioYAML("request-limit")
	exact := base + strings.Repeat("\n", scenarioYAMLLimit-len(base))
	if len(exact) != scenarioYAMLLimit {
		t.Fatalf("exact scenario length=%d", len(exact))
	}

	t.Run("escaped JSON accepts one MiB", func(t *testing.T) {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		body, err := json.Marshal(map[string]any{"scenario": exact, "repetitions": 1})
		if err != nil {
			t.Fatal(err)
		}
		if len(body) <= scenarioYAMLLimit || len(body) > scenarioJSONRequestBodyLimit {
			t.Fatalf("test body does not exercise escaped envelope: %d", len(body))
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("escaped one MiB status=%d body=%s", response.Code, response.Body)
		}
		if experiments := waitRepetitions(t, server); len(experiments) != 1 || experiments[0].State != "completed" {
			t.Fatalf("escaped one MiB experiment failed: %+v", experiments)
		}
	})

	t.Run("decoded JSON over one MiB is invalid", func(t *testing.T) {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		body, err := json.Marshal(map[string]any{"scenario": exact + "\n", "repetitions": 1})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cannot exceed") {
			t.Fatalf("decoded over-limit status=%d body=%s", response.Code, response.Body)
		}
	})

	t.Run("raw JSON envelope over limit is too large", func(t *testing.T) {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(strings.Repeat(" ", scenarioJSONRequestBodyLimit+1)))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("raw JSON over-limit status=%d body=%s", response.Code, response.Body)
		}
	})

	t.Run("raw YAML retains one MiB limit", func(t *testing.T) {
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(exact))
		request.Header.Set("Content-Type", "application/yaml")
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusAccepted {
			t.Fatalf("raw one MiB status=%d body=%s", response.Code, response.Body)
		}
		waitRepetitions(t, server)

		server = New(ServerConfig{DataDir: t.TempDir()}, nil)
		request = httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(exact+"\n"))
		request.Header.Set("Content-Type", "application/yaml")
		response = httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("raw YAML over-limit status=%d body=%s", response.Code, response.Body)
		}
	})
}

func TestExperimentJSONSubmissionRejectsInvalidUTF8BeforeDecode(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	raw := append([]byte(`{"scenario":"version: 2\nname: `), 0xff)
	raw = append(raw, []byte(`\nphases: [{action: wait, duration: 1ms}]","repetitions":1}`)...)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "valid UTF-8") {
		t.Fatalf("invalid UTF-8 status=%d body=%s", response.Code, response.Body)
	}
	if len(server.state.snapshot().Experiments) != 0 {
		t.Fatal("invalid UTF-8 request reserved an experiment")
	}
}
