//go:build linux

package main

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Build and run the production executable: helper test processes alone cannot
// exercise the default /proc/self/exe launch and private CLI dispatcher.
func TestProductionControllerReplacesWorkersAndExportsAfterEachRun(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the production executable and starts local HTTP fixtures")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	directory := t.TempDir()
	binary := filepath.Join(directory, "kpl")
	build := exec.CommandContext(ctx, "go", "build", "-buildvcs=false", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build production executable: %v\n%s", err, output)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	baseURL := "http://" + address
	const user, password = "worker-fixture", "fixture-only-password"
	token := auth.InternalToken(user, password)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 3 * time.Second}
	t.Cleanup(client.CloseIdleConnections)

	var mu sync.Mutex
	nodes := make(map[string]model.Node)
	drained := make(map[string]bool)
	agent := model.Agent{ID: "fixture-agent", StartedAt: time.Now().Add(-time.Minute).UTC(), Capacity: 10, RunDrain: true, RunProcesses: true}
	fakeAgent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/status":
			inventory := []model.Node{}
			active := 0
			for _, node := range nodes {
				inventory = append(inventory, node)
				if node.State == model.NodeReady {
					active++
				}
			}
			snapshot := agent
			snapshot.ActiveNodes = active
			_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{Agent: snapshot, Nodes: inventory})
		case r.Method == "POST" && r.URL.Path == "/api/v1/nodes":
			var request model.CreateNodeRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "invalid fixture request", 400)
				return
			}
			node := model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, Type: request.Type, Role: request.Role, State: model.NodeReady}
			nodes[node.ID] = node
			w.WriteHeader(201)
			_ = json.NewEncoder(w).Encode(node)
		case r.Method == "DELETE" && r.URL.Path == "/api/v1/nodes":
			for id, node := range nodes {
				if node.State != model.NodeStopped {
					http.Error(w, "fixture shutdown found an unremoved Peer "+id, 503)
					return
				}
			}
			w.WriteHeader(204)
		case r.Method == "DELETE" && strings.HasPrefix(r.URL.Path, "/api/v1/runs/"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/nodes")
			for key, node := range nodes {
				if node.RunID == id {
					node.State = model.NodeStopped
					node.Metadata = map[string]string{"cleanupComplete": "true"}
					nodes[key] = node
				}
			}
			w.WriteHeader(202)
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/drain"):
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/drain")
			for _, node := range nodes {
				if node.RunID == id && node.State != model.NodeStopped {
					http.Error(w, "fixture refuses drain before confirmed removal", 503)
					return
				}
			}
			drained[id] = true
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fakeAgent.Close)
	mu.Lock()
	agent.URL = fakeAgent.URL
	mu.Unlock()
	logPath := filepath.Join(directory, "controller.log")
	logs, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logs.Close() })
	process := exec.Command(binary, "run", "controller", "--listen", address, "--data-dir", filepath.Join(directory, "data"), "--run-min-free-bytes", "0")
	process.Env = []string{"PATH=" + os.Getenv("PATH"), "KPL_USER=" + user, "KPL_PASSWORD=" + password}
	process.Stdout, process.Stderr = logs, logs
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- process.Wait() }()
	t.Cleanup(func() {
		_ = process.Process.Signal(syscall.SIGTERM)
		select {
		case err := <-processDone:
			if err != nil && !t.Failed() {
				t.Errorf("production Controller did not exit cleanly: %v", err)
			}
		case <-time.After(8 * time.Second):
			_ = process.Process.Kill()
			<-processDone
			t.Error("production Controller did not stop after graceful cancellation")
		}
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("fixture Controller log:\n%s", data)
		}
	})
	request := func(method, path string, input any, internal bool, output any) int {
		t.Helper()
		var body io.Reader
		if input != nil {
			data, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			body = bytes.NewReader(data)
		}
		r, err := http.NewRequestWithContext(ctx, method, baseURL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-KPL-Request", "dashboard")
		if internal {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(r)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode >= 400 {
			t.Fatalf("%s %s: %d %s", method, path, response.StatusCode, data)
		}
		if output != nil {
			if err := json.Unmarshal(data, output); err != nil {
				t.Fatalf("decode %s: %v", path, err)
			}
		}
		return response.StatusCode
	}
	for {
		response, err := client.Get(baseURL + "/api/v1/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("production Controller did not become healthy")
		case <-time.After(20 * time.Millisecond):
		}
	}
	request("POST", "/api/v1/auth/login", map[string]string{"user": user, "password": password}, false, nil)
	request("POST", "/api/v1/agents/register", agent, true, nil)
	var submitted model.Experiment
	request("POST", "/api/v1/experiments", map[string]any{"scenario": "version: 4\nname: production-worker-fixture\nseed: 42\nphases:\n  - {action: join, group: workers, type: full, count: 2}\n  - {action: wait, duration: 20ms}\n", "repetitions": 3}, false, &submitted)
	var runs []model.Experiment
	for {
		request("GET", "/api/v1/experiments", nil, false, &runs)
		complete := len(runs) == 3
		for _, run := range runs {
			if run.State == "failed" || run.State == "canceled" {
				t.Fatalf("production worker Run failed: %+v", run)
			}
			complete = complete && run.State == "completed" && run.CleanupState == "complete"
		}
		if complete {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("production Runs did not finish: %+v", runs)
		case <-time.After(20 * time.Millisecond):
		}
	}
	mu.Lock()
	drainCount := len(drained)
	mu.Unlock()
	if drainCount != 3 {
		t.Fatalf("completed Runs without confirmed final drain: %d", drainCount)
	}
	var snapshot model.Snapshot
	request("GET", "/api/v1/snapshot", nil, false, &snapshot)
	if len(snapshot.Nodes) != 0 || snapshot.Metrics.RunID != "" {
		t.Fatalf("production live view retained completed runtime: nodes=%d metrics=%+v", len(snapshot.Nodes), snapshot.Metrics)
	}
	// This endpoint queues an export calculation using the same production
	// dispatcher, while preserving its captured source boundary.
	response, err := client.Get(baseURL + "/api/v1/experiments/" + submitted.ID + "/download")
	if err != nil {
		t.Fatal(err)
	}
	archive, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("production export: %d %s", response.StatusCode, archive)
	}
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range reader.File {
		found = found || file.Name == "metrics.json"
	}
	if !found {
		t.Fatal("production ZIP omitted calculated metrics")
	}
	var analysis struct {
		State     string `json:"state"`
		ResultURL string `json:"resultUrl"`
		Error     string `json:"error"`
	}
	analysisPath := "/api/v1/analysis-jobs/" + submitted.ID
	request("POST", analysisPath, map[string]any{}, false, &analysis)
	for analysis.State != "completed" {
		if analysis.State == "failed" || analysis.State == "canceled" {
			t.Fatalf("production analysis worker failed: %+v", analysis)
		}
		select {
		case <-ctx.Done():
			t.Fatal("production analysis did not finish")
		case <-time.After(20 * time.Millisecond):
		}
		request("GET", analysisPath, nil, false, &analysis)
	}
	var artifact struct {
		AnalysisID string `json:"analysisId"`
	}
	request("GET", analysis.ResultURL, nil, false, &artifact)
	if artifact.AnalysisID == "" {
		t.Fatal("production analysis omitted its published artifact identity")
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	pids := make(map[int]bool)
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		var record struct {
			Message string `json:"msg"`
			PID     int    `json:"pid"`
		}
		if json.Unmarshal(line, &record) == nil && record.Message == "experiment worker started" {
			if record.PID == process.Process.Pid || pids[record.PID] {
				t.Fatal("Run workers did not receive distinct OS processes")
			}
			pids[record.PID] = true
			if _, err := os.Stat("/proc/" + strconv.Itoa(record.PID)); !os.IsNotExist(err) {
				t.Fatalf("Run worker %d was not reaped before completion: %v", record.PID, err)
			}
		}
	}
	if len(pids) != 3 {
		t.Fatalf("expected three production child processes, observed %d", len(pids))
	}
}
