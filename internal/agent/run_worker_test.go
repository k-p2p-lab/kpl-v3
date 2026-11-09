package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestAgentRunProcessHelper(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "agent-run-process-helper" {
		return
	}
	if err := workerproc.Serve(context.Background(), "agent-run", NewRunWorkerHandler); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRunProcessInitializationFailureHelper(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "agent-run-init-failure-helper" {
		return
	}
	_ = workerproc.Serve(context.Background(), "agent-run", func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		if err := peer.Call(ctx, "test.hold", nil, nil); err != nil {
			return nil, err
		}
		return nil, errors.New("deliberate initialization failure")
	})
}

func agentRunProcessFactory() RunWorkerFactory {
	return func(ctx context.Context, config RunWorkerConfig, broker workerproc.Handler) (*workerproc.Process, error) {
		return workerproc.Start(ctx, "agent-run", config, broker, workerproc.Options{
			Command: func(string) *exec.Cmd {
				return exec.Command(os.Args[0], "-test.run=^TestAgentRunProcessHelper$", "--", "agent-run-process-helper")
			},
			Stdout: io.Discard, Stderr: io.Discard, GracePeriod: 100 * time.Millisecond, KillWait: time.Second,
		})
	}
}

func drainAgentRunForTest(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	s.handleRunAction(response, httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+id+"/drain?timeout=3s", nil))
	return response
}

func workerControllerFixture(t *testing.T, s *Server) {
	t.Helper()
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); w.WriteHeader(204) }))
	t.Cleanup(controller.Close)
	s.config.ControllerURL = controller.URL
}

func TestRunProcessOwnsHeavyStateAndResetsBetweenRuns(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	workerControllerFixture(t, s)
	startedAt := s.startedAt
	var previousPID int
	for _, runID := range []string{"first", "second"} {
		request := model.CreateNodeRequest{ID: "peer-" + runID, RunID: runID, Group: "workers", AgentStartedAt: startedAt}
		if _, err := s.createNode(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		waitDockerCall(t, dockerLog, "wait")
		s.mu.RLock()
		proc := s.processes[request.ID]
		s.mu.RUnlock()
		deadline := time.Now().Add(5 * time.Second)
		for {
			s.mu.RLock()
			ready := proc.apiURL != ""
			s.mu.RUnlock()
			if ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("container endpoint not published")
			}
			time.Sleep(time.Millisecond)
		}
		worker, err := s.runWorker(runID)
		if err != nil {
			t.Fatal(err)
		}
		pid := worker.process.PID()
		if pid == os.Getpid() || pid == previousPID {
			t.Fatalf("Run reused a process: parent=%d previous=%d current=%d", os.Getpid(), previousPID, pid)
		}
		previousPID = pid
		update := model.Node{ID: request.ID, RunID: runID, State: model.NodeReady, PeerID: "wire-peer", LastSeen: time.Now().UTC(), OverlayObservedAt: time.Now().UTC(), MeshPeers: map[string][]string{"topic": {"a", "b"}}, PeerScores: map[string]float64{"a": 1, "b": 2}, ConnectedPeers: []string{"a", "b"}}
		if err := s.updateNode(update); err != nil {
			t.Fatal(err)
		}
		s.mu.RLock()
		retained := len(proc.node.MeshPeers) + len(proc.node.PeerScores) + len(proc.node.ConnectedPeers)
		s.mu.RUnlock()
		if retained != 0 {
			t.Fatal("supervisor retained experiment topology and score maps")
		}
		var found bool
		for _, node := range s.nodes() {
			if node.ID == request.ID {
				found = len(node.MeshPeers) == 1 && len(node.PeerScores) == 2
			}
		}
		if !found {
			t.Fatal("status did not obtain the experiment process snapshot")
		}
		if response := drainAgentRunForTest(t, s, runID); response.Code != 204 {
			t.Fatalf("drain: %d %s", response.Code, response.Body)
		}
		select {
		case <-worker.process.Done():
		default:
			t.Fatal("drain acknowledged before process Wait")
		}
		if s.startedAt != startedAt || !s.snapshotAgent().RunProcesses {
			t.Fatal("Run reset replaced stable Agent identity")
		}
		if _, err := s.createNode(t.Context(), request); err != nil {
			t.Fatalf("completed create receipt could not replay: %v", err)
		}
	}
	creates := 0
	for _, call := range dockerCalls(t, dockerLog) {
		if call.Args[0] == "create" {
			creates++
		}
	}
	if creates != 2 {
		t.Fatalf("create replay duplicated a Peer: %d", creates)
	}
}

func TestRunProcessWaitsForFinalLogAcknowledgement(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/api/v1/events/batch" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(204)
	}))
	defer controller.Close()
	defer close(release)
	s.config.ControllerURL = controller.URL
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "draining", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "wait")
	worker, err := s.runWorker("draining")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- drainAgentRunForTest(t, s, "draining") }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("final logs not forwarded")
	}
	select {
	case <-worker.process.Done():
		t.Fatal("worker exited before Controller acknowledged logs")
	default:
	}
	response := httptest.NewRecorder()
	s.handleHealth(response, httptest.NewRequest(http.MethodGet, "/api/v1/health", nil))
	if response.Code != 200 {
		t.Fatal("supervisor stopped responding")
	}
	if _, err := s.ensureRunWorker(t.Context(), "next"); err == nil {
		t.Fatal("next Run started while final logs remain")
	}
	release <- struct{}{}
	select {
	case response := <-done:
		if response.Code != 204 {
			t.Fatalf("drain: %d %s", response.Code, response.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish after acknowledgement")
	}
	select {
	case <-worker.process.Done():
	default:
		t.Fatal("worker was not reaped")
	}
}

func TestRunProcessCrashKeepsDockerCreationOwnership(t *testing.T) {
	settings := map[string]string{"DELAY": "create", "DELAY_MS": "300", "FAIL": "rm"}
	s, dockerLog := newContainerTestServer(t, settings)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	workerControllerFixture(t, s)
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "crash", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "create")
	worker, err := s.runWorker("crash")
	if err != nil {
		t.Fatal(err)
	}
	process, err := os.FindProcess(worker.process.PID())
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.RLock()
		proc := s.processes["peer"]
		failed := proc.exited && proc.cleanupErr != nil
		used := s.capacityUsedLocked()
		s.mu.RUnlock()
		if failed {
			if used != 1 {
				t.Fatal("unknown cleanup released capacity")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child failure did not preserve failed container cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	files, err := filepath.Glob(filepath.Join(s.config.DataDir, "runtime-peer-intents", "*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("lost durable creation intent: %v %v", files, err)
	}
	var intent peerCreationIntent
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &intent); err != nil || !strings.HasPrefix(intent.AttemptName, "kpl-peer-") {
		t.Fatalf("invalid intent: %v", err)
	}
	response := drainAgentRunForTest(t, s, "crash")
	var failure model.RunDrainError
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || response.Code != 503 || !failure.WorkerPending || failure.RemainingEvents != 0 {
		t.Fatalf("cleanup did not retain its own barrier: %d %s %v", response.Code, response.Body, err)
	}
	delete(settings, "FAIL")
	response = drainAgentRunForTest(t, s, "crash")
	var recovered model.RunDrainError
	if err := json.Unmarshal(response.Body.Bytes(), &recovered); err != nil || response.Code != 503 || recovered.Code != "run_worker_failed" || recovered.WorkerPending {
		t.Fatalf("recovered ownership hid runtime failure or kept a false barrier: %d %s", response.Code, response.Body)
	}
	if _, err := s.proveRunReset("crash"); err != nil {
		t.Fatalf("runtime failure did not restore resource baseline: %v", err)
	}
	if err := s.runWorkerAdmissionReady("next"); err != nil {
		t.Fatalf("reaped runtime failure permanently blocked next Run: %v", err)
	}
	files, _ = filepath.Glob(filepath.Join(s.config.DataDir, "runtime-peer-intents", "*.json"))
	if len(files) != 0 {
		t.Fatal("confirmed cleanup retained active creation intents")
	}
	creates := 0
	for _, call := range dockerCalls(t, dockerLog) {
		if call.Args[0] == "create" {
			creates++
		}
	}
	if creates != 1 {
		t.Fatal("child crash duplicated Docker admission")
	}
}

func TestRunProcessPeerControlUsesOnlyParentCredentials(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	s.config.Token = "fake-parent-peer-control-key"
	s.config.HostRebootToken = "fake-parent-host-control-key"
	workerControllerFixture(t, s)
	called := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/publish" || r.Header.Get("Authorization") != "Bearer "+s.config.Token || r.Header.Get("X-KPL-Node-ID") != "peer" {
			t.Error("private Peer control lost the parent-owned target or authentication")
			w.WriteHeader(403)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		called <- struct{}{}
		w.WriteHeader(204)
	}))
	defer peer.Close()
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "control", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "wait")
	s.mu.Lock()
	proc := s.processes["peer"]
	proc.apiURL = peer.URL
	s.mu.Unlock()
	s.syncRunWorkerNode(proc)
	if err := s.updateNode(model.Node{ID: "peer", RunID: "control", State: model.NodeReady, PeerID: "wire-peer", LastSeen: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := s.proxyPublish(t.Context(), "peer", model.PublishRequest{PayloadSize: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	default:
		t.Fatal("child did not use the authenticated parent broker")
	}
	worker, err := s.runWorker("control")
	if err != nil {
		t.Fatal(err)
	}
	var page runWorkerSnapshots
	if err := worker.process.Call(t.Context(), "node.snapshots", "", &page); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), s.config.Token) || strings.Contains(string(encoded), s.config.HostRebootToken) {
		t.Fatal("parent credentials entered child runtime state")
	}
	if response := drainAgentRunForTest(t, s, "control"); response.Code != 204 {
		t.Fatalf("drain: %d %s", response.Code, response.Body)
	}
}

func TestRunProcessOwnsLifetimeTimerAfterDockerAdmission(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "100"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	workerControllerFixture(t, s)
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "lifetime", Group: "workers", Lifetime: "0s"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.waitRunContainers(ctx, "lifetime", 0); err != nil {
		t.Fatal(err)
	}
	calls := dockerCalls(t, dockerLog)
	if len(calls) < 2 || calls[0].Args[0] != "create" || calls[len(calls)-1].Args[0] != "rm" {
		t.Fatalf("timer did not preserve Docker admission/cleanup ordering: %v", calls)
	}
	if response := drainAgentRunForTest(t, s, "lifetime"); response.Code != 204 {
		t.Fatalf("drain: %d %s", response.Code, response.Body)
	}
}

func TestRunProcessSupervisorRecoveryRetainsFailedIntents(t *testing.T) {
	settings := map[string]string{"FAIL": "rm"}
	s, dockerLog := newContainerTestServer(t, settings)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	proc := &process{workerEpoch: "saved-epoch", node: model.Node{ID: "orphan", RunID: "old-run", AgentID: s.config.ID, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker"}}}
	if _, err := s.beginPeerIntent(proc, proc.node); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(proc.intentPath)
	if err != nil {
		t.Fatal(err)
	}
	var intent peerCreationIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		t.Fatal(err)
	}
	intent.AdmissionUntil = time.Now().Add(-time.Second)
	data, _ = json.Marshal(intent)
	if err := writeTelemetryFile(proc.intentPath, data); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.recoverPeerIntents(ctx); err == nil {
		t.Fatal("failed Docker removal cleared recovery ownership")
	}
	if _, err := os.Stat(proc.intentPath); err != nil {
		t.Fatal("failed recovery removed the durable intent")
	}
	delete(settings, "FAIL")
	if err := s.recoverPeerIntents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(proc.intentPath); !os.IsNotExist(err) {
		t.Fatalf("confirmed recovery kept intent: %v", err)
	}
	if found, err := s.history.contains("orphan"); err != nil || !found {
		t.Fatalf("recovery lost retired identity: %v %v", found, err)
	}
	if fence, found, err := s.history.fence("old-run"); err != nil || !found || fence != ^uint64(0) {
		t.Fatalf("recovery lost old Run fence: %d %v %v", fence, found, err)
	}
	s.eventsMu.Lock()
	pending := s.pendingEventsLocked()
	s.eventsMu.Unlock()
	if pending != 1 {
		t.Fatalf("recovery lost final termination evidence: %d", pending)
	}
	if err := s.recoverPeerIntents(ctx); err != nil {
		t.Fatal(err)
	}
	s.eventsMu.Lock()
	after := s.pendingEventsLocked()
	s.eventsMu.Unlock()
	if after != pending {
		t.Fatal("completed recovery duplicated termination evidence")
	}
	for _, call := range dockerCalls(t, dockerLog) {
		if call.Args[0] == "create" {
			t.Fatal("supervisor recovery started a new Peer")
		}
	}
}

func TestRunProcessRepeatedResetReturnsToBoundedBaseline(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	started := s.startedAt
	var previousPID int
	for cycle := range 5 {
		runID := fmt.Sprintf("cycle-%d", cycle)
		worker, err := s.ensureRunWorker(t.Context(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if worker.process.PID() == previousPID || worker.process.PID() == os.Getpid() {
			t.Fatal("Run did not receive a fresh OS process")
		}
		previousPID = worker.process.PID()
		for i := range 70 {
			node := model.Node{ID: fmt.Sprintf("peer-%d-%d", cycle, i), RunID: runID, AgentID: s.config.ID, State: model.NodeStopped, StartedAt: time.Now().UTC(), LastSeen: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker", "cleanupComplete": "true"}}
			s.mu.Lock()
			s.processes[node.ID] = &process{node: node, exited: true, workerEpoch: worker.process.Epoch()}
			s.mu.Unlock()
			if err := worker.process.Call(t.Context(), "node.register", node, nil); err != nil {
				t.Fatal(err)
			}
			if err := worker.process.Call(t.Context(), "node.lifecycle", runWorkerNode{Node: node, Exited: true, CleanupDone: true}, nil); err != nil {
				t.Fatal(err)
			}
		}
		if response := drainAgentRunForTest(t, s, runID); response.Code != 204 {
			t.Fatalf("cycle %d drain: %d %s", cycle, response.Code, response.Body)
		}
		proof, err := s.proveRunReset(runID)
		if err != nil {
			t.Fatal(err)
		}
		if proof.HistoryRecords != peerHistoryRecent || proof.ActivePeers != 0 || proof.Reservations != 0 || proof.CleanupPending != 0 || proof.CreationIntents != 0 || proof.TelemetryEvents != 0 || proof.TelemetryIngress != 0 || proof.VolatilePeerFields != 0 || proof.NetworkBound {
			t.Fatalf("cycle %d failed baseline: %+v", cycle, proof)
		}
		s.runWorkersMu.Lock()
		retained := len(s.runWorkers)
		s.runWorkersMu.Unlock()
		if retained != 0 || s.startedAt != started {
			t.Fatal("reset retained an experiment process or replaced supervisor identity")
		}
		select {
		case <-worker.process.Done():
		default:
			t.Fatal("baseline was reported before process Wait")
		}
	}
}

func TestRunProcessResetWaitsForLateIngressAndRejectsDirtyNextRun(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	var receivedMu sync.Mutex
	received := make(map[string]bool)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/events/batch" {
			var batch model.EventBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			receivedMu.Lock()
			for _, event := range batch.Events {
				received[event.EventID] = true
			}
			receivedMu.Unlock()
		}
		w.WriteHeader(204)
	}))
	defer controller.Close()
	s.config.ControllerURL = controller.URL
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "late", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "wait")
	worker, err := s.runWorker("late")
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.networkRequest = model.ExperimentNetworkRequest{RunID: "late", Epoch: "old", RequestedAt: time.Now()}
	s.experimentNetwork = &model.ExperimentNetwork{RunID: "late", Epoch: "old"}
	s.mu.Unlock()
	reader, writer := io.Pipe()
	defer writer.Close()
	telemetryDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", reader))
		telemetryDone <- response
	}()
	data, _ := json.Marshal(model.EventBatch{Events: []model.TraceEvent{{EventID: "late-ingress", RunID: "late", NodeID: "peer", Type: "join", Timestamp: time.Now().UTC()}}})
	if _, err := writer.Write(data[:len(data)/2]); err != nil {
		t.Fatal(err)
	}
	drainDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { drainDone <- drainAgentRunForTest(t, s, "late") }()
	select {
	case response := <-drainDone:
		t.Fatalf("partial ingress bypassed finalization: %d %s", response.Code, response.Body)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-worker.process.Done():
		t.Fatal("worker exited while a valid telemetry body was incomplete")
	default:
	}
	if _, err := writer.Write(data[len(data)/2:]); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	select {
	case response := <-telemetryDone:
		if response.Code != 204 {
			t.Fatalf("late ingress: %d %s", response.Code, response.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late ingress did not finish")
	}
	select {
	case response := <-drainDone:
		if response.Code != 204 {
			t.Fatalf("late drain: %d %s", response.Code, response.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not follow the late acknowledgement")
	}
	receivedMu.Lock()
	found := received["late-ingress"]
	receivedMu.Unlock()
	if !found {
		t.Fatal("reset lost valid late telemetry")
	}
	if err := s.updateNode(model.Node{ID: "peer", State: model.NodeReady, LastSeen: time.Now(), PeerScores: map[string]float64{"stale": 99}}); err != nil {
		t.Fatal(err)
	}
	proof, err := s.proveRunReset("late")
	if err != nil || proof.NetworkBound || proof.VolatilePeerFields != 0 {
		t.Fatalf("late status revived reset state: %+v %v", proof, err)
	}
	if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{EventID: "after-reset", RunID: "late", NodeID: "peer", Type: "join", Timestamp: time.Now().UTC()}}}) {
		t.Fatal("valid late telemetry was discarded")
	}
	if err := s.runWorkerAdmissionReady("next"); !errors.Is(err, errRunResetPending) {
		t.Fatalf("dirty next Run was admitted: %v", err)
	}
	if response := drainAgentRunForTest(t, s, "late"); response.Code != 204 {
		t.Fatalf("late recovery: %d %s", response.Code, response.Body)
	}
	if err := s.runWorkerAdmissionReady("next"); err != nil {
		t.Fatalf("clean baseline did not reopen admission: %v", err)
	}
	if err := s.fenceExperimentNetwork(t.Context(), model.ExperimentNetworkRequest{RunID: "late", Epoch: "stale", RequestedAt: time.Now()}); err == nil {
		t.Fatal("late network callback revived the finalized Run")
	}
}

func TestRunProcessLargeSnapshotsPageWithoutDroppingScoresOrMesh(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	// The race detector instruments all cross-process JSON/map operations.
	// This fixture budget does not alter the production eight-second bound.
	s.runWorkerSnapshotBudget = 45 * time.Second
	var mu sync.Mutex
	heartbeatSeen := make(map[string]bool)
	maxHeartbeatBytes := 0
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/agents/heartbeat" {
			data, err := io.ReadAll(io.LimitReader(r.Body, heartbeatBodyLimit+1))
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			if len(data) > heartbeatBodyLimit {
				t.Error("heartbeat exceeded existing wire limit")
				w.WriteHeader(413)
				return
			}
			var report model.AgentHeartbeat
			if err := json.Unmarshal(data, &report); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			maxHeartbeatBytes = max(maxHeartbeatBytes, len(data))
			for _, node := range report.Nodes {
				if node.State == model.NodeReady {
					if len(node.MeshPeers) != 16 || len(node.PeerScores) != 900 || node.ScoreSample == nil || !node.ScoreSample.Valid() {
						t.Error("heartbeat silently dropped experiment score/mesh fields")
					}
					heartbeatSeen[node.ID] = true
				}
			}
			mu.Unlock()
		}
		w.WriteHeader(204)
	}))
	defer controller.Close()
	s.config.ControllerURL = controller.URL
	// historyTestServer normally stubs HTTP. This fixture intentionally exercises
	// the actual paged IPC -> 10 MiB heartbeat -> loopback HTTP path.
	s.client = &http.Client{Timeout: 10 * time.Second}
	worker, err := s.ensureRunWorker(t.Context(), "large")
	if err != nil {
		t.Fatal(err)
	}
	peers := make([]string, 900)
	scores := make(map[string]float64, 900)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer-%059d", i)
		scores[peers[i]] = 0
	}
	mesh := make(map[string][]string, 16)
	for i := range 16 {
		mesh[fmt.Sprintf("topic-%d", i)] = peers
	}
	const count = 20
	for i := range count {
		node := model.Node{ID: fmt.Sprintf("dense-%02d", i), RunID: "large", AgentID: s.config.ID, State: model.NodeStarting, StartedAt: time.Now().UTC(), Metadata: map[string]string{"runtime": "docker", "pubsubEnabled": "true", "pubsubRouter": "gossipsub"}}
		proc := &process{node: node, workerEpoch: worker.process.Epoch(), apiURL: "http://unused", done: make(chan struct{})}
		s.mu.Lock()
		s.processes[node.ID] = proc
		s.mu.Unlock()
		if err := worker.process.Call(t.Context(), "node.register", node, nil); err != nil {
			t.Fatal(err)
		}
		s.syncRunWorkerNode(proc)
		at := time.Now().UTC()
		if err := s.updateNode(model.Node{ID: node.ID, RunID: "large", State: model.NodeReady, PeerID: "wire-peer", LastSeen: at, OverlayObservedAt: at, MeshPeers: mesh, PeerScores: scores, ScoreSample: auditScoreSample(at, 1, 900)}); err != nil {
			t.Fatal(err)
		}
	}
	view, err := s.nodesWithContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	bytes := 0
	for _, node := range view {
		encoded, err := json.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		bytes += len(encoded)
		if len(node.MeshPeers) != 16 || len(node.PeerScores) != 900 || node.ScoreSample == nil {
			t.Fatal("state view lost dense score/mesh fields")
		}
	}
	if len(view) != count || bytes <= workerproc.MaxMessageBytes {
		t.Fatalf("fixture did not cross the single-frame bound: nodes=%d bytes=%d", len(view), bytes)
	}
	if err := s.heartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	seen, largest := len(heartbeatSeen), maxHeartbeatBytes
	mu.Unlock()
	if seen != count || largest > heartbeatBodyLimit {
		t.Fatalf("incomplete heartbeat pagination: nodes=%d maxBytes=%d", seen, largest)
	}
	endpoint := httptest.NewServer(s.Handler())
	defer endpoint.Close()
	response, err := endpoint.Client().Get(endpoint.URL + "/api/v1/nodes")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("wide state view failed: %d", response.StatusCode)
	}
	decoder := json.NewDecoder(response.Body)
	if _, err := decoder.Token(); err != nil {
		t.Fatal(err)
	}
	stateNodes := 0
	for decoder.More() {
		var node model.Node
		if err := decoder.Decode(&node); err != nil {
			t.Fatal(err)
		}
		if len(node.MeshPeers) != 16 || len(node.PeerScores) != 900 || node.ScoreSample == nil {
			t.Fatal("HTTP state view dropped dense fields")
		}
		stateNodes++
	}
	if _, err := decoder.Token(); err != nil {
		t.Fatal(err)
	}
	if stateNodes != count {
		t.Fatalf("HTTP state view returned %d nodes", stateNodes)
	}
	s.mu.RLock()
	retained := 0
	procs := make([]*process, 0, count)
	for _, proc := range s.processes {
		retained += len(proc.node.MeshPeers) + len(proc.node.PeerScores)
		procs = append(procs, proc)
	}
	s.mu.RUnlock()
	if retained != 0 {
		t.Fatal("supervisor retained dense runtime maps")
	}
	for _, proc := range procs {
		s.finishProcess(proc.node.ID, proc, nil, nil)
	}
	if response := drainAgentRunForTest(t, s, "large"); response.Code != 204 {
		t.Fatalf("dense reset failed: %d %s", response.Code, response.Body)
	}
	proof, err := s.proveRunReset("large")
	if err != nil || proof.VolatilePeerFields != 0 {
		t.Fatalf("dense buffers contaminated reset: %+v %v", proof, err)
	}
	t.Logf("complete dense snapshot=%d bytes; %d Nodes; max heartbeat=%d bytes; reset volatileFields=%d", bytes, count, largest, proof.VolatilePeerFields)
}

type heldRunViewWriter struct {
	header  http.Header
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *heldRunViewWriter) Header() http.Header { return w.header }
func (*heldRunViewWriter) WriteHeader(int)       {}
func (w *heldRunViewWriter) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	return len(data), nil
}

func TestRunProcessResetWaitsForLargeStatusResponseToFinish(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	workerControllerFixture(t, s)
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "reader", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "wait")
	worker, err := s.runWorker("reader")
	if err != nil {
		t.Fatal(err)
	}
	peers := make([]string, 512)
	scores := make(map[string]float64, 512)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer-%059d", i)
		scores[peers[i]] = 0
	}
	mesh := make(map[string][]string, 20)
	for i := range 20 {
		mesh[fmt.Sprintf("topic-%d", i)] = peers
	}
	at := time.Now().UTC()
	if err := s.updateNode(model.Node{ID: "peer", RunID: "reader", State: model.NodeReady, LastSeen: at, OverlayObservedAt: at, MeshPeers: mesh, PeerScores: scores}); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	writer := &heldRunViewWriter{header: make(http.Header), entered: make(chan struct{}), release: release}
	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		s.handleStatus(writer, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("state response did not hold its full snapshot")
	}
	drainDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { drainDone <- drainAgentRunForTest(t, s, "reader") }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.RLock()
		stopped := processCleanupComplete(s.processes["peer"])
		s.mu.RUnlock()
		s.eventsMu.Lock()
		remaining := s.remainingRunEventsLocked("reader")
		s.eventsMu.Unlock()
		if stopped && remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Peer/log finalization did not reach the held-reader boundary")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case response := <-drainDone:
		t.Fatalf("status buffer survived a reported reset: %d %s", response.Code, response.Body)
	case <-time.After(25 * time.Millisecond):
	}
	select {
	case <-worker.process.Done():
		t.Fatal("worker exited while its full HTTP response was held")
	default:
	}
	s.runViewsMu.Lock()
	readers := s.runViews["reader"]
	s.runViewsMu.Unlock()
	if readers != 1 {
		t.Fatalf("snapshot ownership was lost: %d", readers)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-statusDone:
	case <-time.After(5 * time.Second):
		t.Fatal("state response did not finish")
	}
	select {
	case response := <-drainDone:
		if response.Code != 204 {
			t.Fatalf("reset after reader: %d %s", response.Code, response.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not follow reader quiescence")
	}
	proof, err := s.proveRunReset("reader")
	if err != nil || proof.SnapshotReaders != 0 || !proof.WorkerReaped {
		t.Fatalf("reset reader proof failed: %+v %v", proof, err)
	}
}

func TestRunProcessInitializationFailureKeepsUnquiescentHandle(t *testing.T) {
	s := historyTestServer(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s.config.RunWorkerFactory = func(ctx context.Context, config RunWorkerConfig, broker workerproc.Handler) (*workerproc.Process, error) {
		parent := func(ctx context.Context, method string, body json.RawMessage) (any, error) {
			if method == "test.hold" {
				close(entered)
				<-release
				return nil, nil
			}
			return broker(ctx, method, body)
		}
		return workerproc.Start(ctx, "agent-run", config, parent, workerproc.Options{Command: func(string) *exec.Cmd {
			return exec.Command(os.Args[0], "-test.run=^TestAgentRunProcessInitializationFailureHelper$", "--", "agent-run-init-failure-helper")
		}, Stdout: io.Discard, Stderr: io.Discard, HandshakeTimeout: time.Second, GracePeriod: 20 * time.Millisecond, KillWait: 20 * time.Millisecond})
	}
	type result struct {
		worker *agentRunWorker
		err    error
	}
	done := make(chan result, 1)
	go func() { w, err := s.ensureRunWorker(t.Context(), "init-failure"); done <- result{w, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("initialization did not reach its blocked broker")
	}
	var worker *agentRunWorker
	select {
	case output := <-done:
		if output.err == nil || output.worker == nil || output.worker.process == nil {
			t.Fatalf("failed initialization lost its cleanup handle: %v", output.err)
		}
		worker = output.worker
	case <-time.After(5 * time.Second):
		t.Fatal("failed initialization did not return a bounded outcome")
	}
	select {
	case <-worker.process.Done():
		t.Fatal("blocked broker was falsely reported quiescent")
	default:
	}
	if err := s.runWorkerAdmissionReady("next"); !errors.Is(err, errRunResetPending) {
		t.Fatalf("failed initialization admitted another worker: %v", err)
	}
	response := drainAgentRunForTest(t, s, "init-failure")
	var pending model.RunDrainError
	if err := json.Unmarshal(response.Body.Bytes(), &pending); err != nil || response.Code != 503 || !pending.WorkerPending {
		t.Fatalf("unquiescent startup did not retain its barrier: %d %s", response.Code, response.Body)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-worker.process.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("released startup broker did not quiesce")
	}
	response = drainAgentRunForTest(t, s, "init-failure")
	var failed model.RunDrainError
	if err := json.Unmarshal(response.Body.Bytes(), &failed); err != nil || response.Code != 503 || failed.Code != "run_worker_failed" || failed.WorkerPending {
		t.Fatalf("cleaned startup failure lost its outcome or kept a false barrier: %d %s", response.Code, response.Body)
	}
	if err := s.runWorkerAdmissionReady("next"); err != nil {
		t.Fatalf("confirmed initialization cleanup kept admission closed: %v", err)
	}
}

func TestRunProcessServerCloseDoesNotReenterHeartbeat(t *testing.T) {
	s := historyTestServer(t)
	s.config.RunWorkerFactory = agentRunProcessFactory()
	worker, err := s.ensureRunWorker(t.Context(), "closing")
	if err != nil {
		t.Fatal(err)
	}
	s.heartbeatMu.Lock()
	defer s.heartbeatMu.Unlock()
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("local Agent destruction reentered heartbeat/finalization")
	}
	select {
	case <-worker.process.Done():
	default:
		t.Fatal("local Agent destruction did not reap its experiment process")
	}
}
