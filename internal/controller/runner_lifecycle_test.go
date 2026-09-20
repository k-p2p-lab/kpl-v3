package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
	"github.com/k-p2p-lab/v3/internal/scenario"
)

func TestWaitReadyRequiresMinimumNodeCount(t *testing.T) {
	server := newLifecycleTestController(t)
	runID := "run-min-count"
	server.state.mu.Lock()
	server.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline}
	server.state.nodes["worker-1"] = model.Node{
		ID:         "worker-1",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeReady,
	}
	server.state.mu.Unlock()

	phase := scenario.Phase{
		Group:      "workers",
		NodeType:   "full",
		MinCount:   2,
		ReadyRatio: 1,
		Timeout:    "30ms",
	}
	if err := server.waitReady(context.Background(), runID, 1, phase); err == nil {
		t.Fatal("wait-ready passed with one ready node even though minCount is two")
	} else if !strings.Contains(err.Error(), "1/1 ready") {
		t.Fatalf("wait-ready returned %v, want timeout reporting one ready node", err)
	}

	server.state.mu.Lock()
	server.state.nodes["worker-2"] = model.Node{
		ID:         "worker-2",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeReady,
	}
	server.state.mu.Unlock()
	if err := server.waitReady(context.Background(), runID, 1, phase); err != nil {
		t.Fatalf("wait-ready did not pass after minCount was satisfied: %v", err)
	}
}

func TestWaitReadyKeepsFailedAndStoppedNodesInGenerationCohort(t *testing.T) {
	server := newLifecycleTestController(t)
	runID := "run-readiness-cohort"
	server.state.mu.Lock()
	server.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline}
	for i := 0; i < 10; i++ {
		state := model.NodeFailed
		if i == 0 {
			state = model.NodeReady
		}
		id := fmt.Sprintf("worker-%d", i)
		server.state.nodes[id] = model.Node{
			ID:         id,
			RunID:      runID,
			Generation: 1,
			AgentID:    "agent",
			Group:      "workers",
			Type:       "full",
			State:      state,
		}
	}
	server.state.nodes["stopped"] = model.Node{
		ID:         "stopped",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "other-workers",
		Type:       "full",
		State:      model.NodeStopped,
	}
	server.state.nodes["stopping"] = model.Node{
		ID:         "stopping",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "other-workers",
		Type:       "full",
		State:      model.NodeStopping,
	}
	server.state.nodes["other-ready"] = model.Node{
		ID:         "other-ready",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "other-workers",
		Type:       "full",
		State:      model.NodeReady,
	}
	server.state.mu.Unlock()

	ready, total, failed := server.readyCount(runID, 1, "workers", "full")
	if ready != 1 || total != 10 || failed != 9 {
		t.Fatalf("readyCount = (%d, %d, %d), want (1, 10, 9)", ready, total, failed)
	}
	ready, total, failed = server.readyCount(runID, 1, "other-workers", "full")
	if ready != 1 || total != 3 || failed != 0 {
		t.Fatalf("readyCount with stopped nodes = (%d, %d, %d), want (1, 3, 0)", ready, total, failed)
	}

	phase := scenario.Phase{
		Group:      "workers",
		NodeType:   "full",
		MinCount:   10,
		ReadyRatio: 1,
		Timeout:    "30ms",
	}
	if err := server.waitReady(context.Background(), runID, 1, phase); err == nil {
		t.Fatal("wait-ready passed after failed nodes disappeared from the active-node view")
	} else if !strings.Contains(err.Error(), "1/10 ready, 9 failed") {
		t.Fatalf("wait-ready returned %v, want full generation cohort in timeout", err)
	}

	// A failed peer is terminal for this barrier even when the configured ratio
	// would otherwise permit it.
	phase.ReadyRatio = 0.1
	if err := server.waitReady(context.Background(), runID, 1, phase); err == nil {
		t.Fatal("wait-ready passed a cohort containing failed nodes")
	}

	leavePhase := scenario.Phase{
		Group:      "other-workers",
		NodeType:   "full",
		MinCount:   3,
		ReadyRatio: 1,
		Timeout:    "30ms",
	}
	if err := server.waitReady(context.Background(), runID, 1, leavePhase); err == nil {
		t.Fatal("wait-ready dropped explicitly stopped nodes from its generation cohort")
	} else if !strings.Contains(err.Error(), "1/3 ready, 0 failed") {
		t.Fatalf("wait-ready returned %v, want stopped nodes in denominator", err)
	}
	leavePhase.ReadyRatio = 1.0 / 3.0
	if err := server.waitReady(context.Background(), runID, 1, leavePhase); err != nil {
		t.Fatalf("explicit lower readyRatio did not permit the remaining node: %v", err)
	}
}

func TestWaitReadyDoesNotCountReadyNodeOnOfflineAgent(t *testing.T) {
	server := newLifecycleTestController(t)
	runID := "run-offline-agent"
	server.state.mu.Lock()
	server.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOffline}
	server.state.nodes["worker"] = model.Node{
		ID:         "worker",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeReady,
	}
	server.state.mu.Unlock()

	phase := scenario.Phase{
		Group:      "workers",
		NodeType:   "full",
		MinCount:   1,
		ReadyRatio: 1,
		Timeout:    "30ms",
	}
	if err := server.waitReady(context.Background(), runID, 1, phase); err == nil {
		t.Fatal("wait-ready counted a ready node whose agent is offline")
	} else if !strings.Contains(err.Error(), "0/1 ready") {
		t.Fatalf("wait-ready returned %v, want offline node retained only in denominator", err)
	}

	server.state.mu.Lock()
	agent := server.state.agents["agent"]
	agent.State = model.AgentOnline
	server.state.agents["agent"] = agent
	server.state.mu.Unlock()
	if err := server.waitReady(context.Background(), runID, 1, phase); err != nil {
		t.Fatalf("wait-ready did not pass once the owning agent became online: %v", err)
	}
}

func TestWaitReadyGenerationBoundaryIgnoresStoppedPreviousGeneration(t *testing.T) {
	server := newLifecycleTestController(t)
	runID := "run-readiness-generations"
	server.state.mu.Lock()
	server.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline}
	server.state.nodes["old-failed"] = model.Node{
		ID:         "old-failed",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeFailed,
	}
	server.state.nodes["old-stopped"] = model.Node{
		ID:         "old-stopped",
		RunID:      runID,
		Generation: 1,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeStopped,
	}
	server.state.nodes["current-ready"] = model.Node{
		ID:         "current-ready",
		RunID:      runID,
		Generation: 2,
		AgentID:    "agent",
		Group:      "workers",
		Type:       "full",
		State:      model.NodeReady,
	}
	server.state.mu.Unlock()

	phase := scenario.Phase{
		Group:      "workers",
		NodeType:   "full",
		MinCount:   1,
		ReadyRatio: 1,
		Timeout:    "30ms",
	}
	if err := server.waitReady(context.Background(), runID, 2, phase); err != nil {
		t.Fatalf("current generation barrier was polluted by previous generation: %v", err)
	}
}

func TestWaitReadySharesOneTimeoutWithJobBarrier(t *testing.T) {
	server := newLifecycleTestController(t)
	jobs := newPhaseJobs()
	if err := jobs.start("join", func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	phase := scenario.Phase{
		Action:     "wait-ready",
		Group:      "workers",
		Jobs:       []string{"join"},
		ReadyRatio: 1,
		Timeout:    "200ms",
	}
	started := time.Now()
	err := server.runPhase(context.Background(), "run-timeout", 1, phase, rand.New(rand.NewSource(1)), jobs, time.Second)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait-ready returned %v, want deadline exceeded", err)
	}
	if elapsed > 270*time.Millisecond {
		t.Fatalf("wait-ready reused its timeout after the job barrier: elapsed=%s", elapsed)
	}
}

func TestStopAllCancelsAndDrainsBackgroundJoinBeforeDeletingNodes(t *testing.T) {
	controller, agent := newLifecycleTestControllerWithAgent(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runID := "run-stop-all"
	jobs := newPhaseJobs()
	join := scenario.Phase{
		Action:   "join",
		Group:    "workers",
		Role:     "worker",
		NodeType: "full",
		Count:    2,
		Interval: scenario.Distribution{Model: "fixed", Value: "5s"},
	}
	if err := jobs.startContext(ctx, "background-join", func(jobCtx context.Context) error {
		return controller.runJoin(jobCtx, runID, 1, join, rand.New(rand.NewSource(1)))
	}, nil); err != nil {
		t.Fatal(err)
	}

	select {
	case <-agent.firstCreate:
	case <-time.After(2 * time.Second):
		t.Fatal("background join did not create its first node")
	}
	waitForLifecycleNodeCount(t, controller, runID, 1)

	stopAll := scenario.Phase{Action: "stop-all"}
	if err := controller.runPhase(ctx, runID, 1, stopAll, rand.New(rand.NewSource(2)), jobs, time.Second); err != nil {
		t.Fatalf("stop-all failed: %v", err)
	}

	creates, deletes, createAfterDelete := agent.counts()
	if creates != 1 {
		t.Fatalf("agent saw %d creates, want only the create completed before cancellation", creates)
	}
	if deletes != 1 {
		t.Fatalf("agent saw %d deletes, want the one created node to be deleted", deletes)
	}
	if createAfterDelete {
		t.Fatal("a background create reached the agent after stop-all started deleting nodes")
	}

	time.Sleep(30 * time.Millisecond)
	laterCreates, _, laterCreateAfterDelete := agent.counts()
	if laterCreates != creates || laterCreateAfterDelete {
		t.Fatalf("background join continued after stop-all: creates changed from %d to %d", creates, laterCreates)
	}
	controller.state.mu.RLock()
	reservations := len(controller.state.reservations)
	controller.state.mu.RUnlock()
	if reservations != 0 {
		t.Fatalf("stop-all left %d capacity reservations behind", reservations)
	}
}

func TestScenarioOnExitDrainsJobsBeforeFinalJobCounters(t *testing.T) {
	tests := []struct {
		name              string
		onExit            string
		count             int
		interval          string
		wantCompletedJobs int
		wantCanceledJobs  int
	}{
		{
			name:              "drain",
			onExit:            "drain",
			count:             1,
			wantCompletedJobs: 1,
		},
		{
			name:             "cancel",
			onExit:           "cancel",
			count:            2,
			interval:         "5s",
			wantCanceledJobs: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller, agent := newLifecycleTestControllerWithAgent(t)
			interval := ""
			if test.interval != "" {
				interval = "\n    interval:\n      model: fixed\n      value: " + test.interval
			}
			raw := []byte("version: 2\n" +
				"name: on-exit-" + test.name + "\n" +
				"onExit: " + test.onExit + "\n" +
				"jobShutdownTimeout: 1s\n" +
				"phases:\n" +
				"  - name: background join\n" +
				"    job: background-join\n" +
				"    action: join\n" +
				"    group: workers\n" +
				"    type: full\n" +
				"    count: " + strconv.Itoa(test.count) + "\n" +
				"    await: false" + interval + "\n")

			experiment, err := controller.StartScenario(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			finished := waitForLifecycleExperiment(t, controller, experiment.ID)
			if finished.State != "completed" {
				t.Fatalf("experiment state=%q error=%q, want completed", finished.State, finished.Error)
			}
			if finished.ActiveJobs != 0 {
				t.Fatalf("final activeJobs=%d, want zero", finished.ActiveJobs)
			}
			if finished.CompletedJobs != test.wantCompletedJobs || finished.CanceledJobs != test.wantCanceledJobs {
				t.Fatalf("final completedJobs=%d canceledJobs=%d, want %d and %d", finished.CompletedJobs, finished.CanceledJobs, test.wantCompletedJobs, test.wantCanceledJobs)
			}
			if finished.FailedJobs != 0 {
				t.Fatalf("final failedJobs=%d, want zero", finished.FailedJobs)
			}
			if _, fences := agent.generations(); len(fences) != 0 {
				t.Fatalf("natural onExit %s fenced peers with generations %v", test.onExit, fences)
			}
		})
	}
}

func TestScenarioAPIStopDrainsBackgroundJoinBeforeFencingPeers(t *testing.T) {
	controller, agent := newLifecycleTestControllerWithAgent(t)
	raw := []byte(`
version: 2
name: api-cancel-cleanup
jobShutdownTimeout: 1s
phases:
  - name: background join
    job: background-join
    action: join
    group: workers
    count: 2
    await: false
    interval:
      model: fixed
      value: 5s
  - action: wait
    duration: 30s
`)
	experiment, err := controller.StartScenario(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-agent.firstCreate:
	case <-time.After(2 * time.Second):
		t.Fatal("background join did not create its first node")
	}
	waitForLifecycleNodeCount(t, controller, experiment.ID, 1)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments/"+experiment.ID+"/stop", nil)
	authenticateRequest(t, controller, request)
	response := httptest.NewRecorder()
	controller.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("stop API status=%d body=%s, want %d", response.Code, response.Body.String(), http.StatusAccepted)
	}

	finished := waitForLifecycleExperiment(t, controller, experiment.ID)
	if finished.State != "canceled" {
		t.Fatalf("experiment state=%q error=%q, want canceled", finished.State, finished.Error)
	}
	if finished.ActiveJobs != 0 || finished.CanceledJobs != 1 {
		t.Fatalf("final activeJobs=%d canceledJobs=%d, want 0 and 1", finished.ActiveJobs, finished.CanceledJobs)
	}
	creates, deletes, createAfterFence := agent.counts()
	if creates != 1 || deletes != 1 {
		t.Fatalf("agent creates/deletes=%d/%d, want 1/1", creates, deletes)
	}
	if createAfterFence {
		t.Fatal("background join created a fenced-generation peer after cancellation cleanup started")
	}
	_, fences := agent.generations()
	if !reflect.DeepEqual(fences, []uint64{1}) {
		t.Fatalf("cancellation fences = %v, want [1]", fences)
	}
	if remaining := agent.nodeCount(); remaining != 0 {
		t.Fatalf("agent retained %d peers after cancellation cleanup", remaining)
	}
}

func TestScenarioStopAllAdvancesAgentGeneration(t *testing.T) {
	controller, agent := newLifecycleTestControllerWithAgent(t)
	raw := []byte(`
version: 2
name: generation-boundary
phases:
  - action: join
    group: workers
    count: 1
  - action: stop-all
  - action: join
    group: workers
    count: 1
`)
	experiment, err := controller.StartScenario(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	finished := waitForLifecycleExperiment(t, controller, experiment.ID)
	if finished.State != "completed" {
		t.Fatalf("experiment state=%q error=%q", finished.State, finished.Error)
	}
	creates, fences := agent.generations()
	if !reflect.DeepEqual(creates, []uint64{1, 2}) {
		t.Fatalf("create generations = %v, want [1 2]", creates)
	}
	if !reflect.DeepEqual(fences, []uint64{1}) {
		t.Fatalf("stop-all fences = %v, want [1]", fences)
	}
}

func TestScenarioFailureCleansEveryGenerationThroughCurrent(t *testing.T) {
	controller, agent := newLifecycleTestControllerWithAgent(t)
	raw := []byte(`
version: 2
name: failure-after-reset
jobShutdownTimeout: 1s
phases:
  - action: join
    group: first
    count: 1
  - action: stop-all
  - action: join
    group: second
    count: 1
  - action: publish
    group: missing
    count: 1
`)
	experiment, err := controller.StartScenario(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	finished := waitForLifecycleExperiment(t, controller, experiment.ID)
	if finished.State != "failed" || !strings.Contains(finished.Error, "no publish-capable ready nodes") {
		t.Fatalf("experiment state=%q error=%q, want publish failure", finished.State, finished.Error)
	}
	creates, fences := agent.generations()
	if !reflect.DeepEqual(creates, []uint64{1, 2}) {
		t.Fatalf("create generations = %v, want [1 2]", creates)
	}
	if !reflect.DeepEqual(fences, []uint64{1, 2}) {
		t.Fatalf("cleanup fences = %v, want [1 2]", fences)
	}
	if remaining := agent.nodeCount(); remaining != 0 {
		t.Fatalf("agent retained %d peers after failed scenario cleanup", remaining)
	}
}

func TestScenarioCancellationReportsPeerCleanupFailure(t *testing.T) {
	controller, agent := newLifecycleTestControllerWithAgent(t)
	var logs bytes.Buffer
	controller.logger = slog.New(slog.NewTextHandler(&logs, nil))
	agent.mu.Lock()
	agent.fenceStatus = http.StatusInternalServerError
	agent.mu.Unlock()
	raw := []byte(`
version: 2
name: failed-cancel-cleanup
jobShutdownTimeout: 1s
phases:
  - action: join
    group: workers
    count: 1
  - action: wait
    duration: 30s
`)
	experiment, err := controller.StartScenario(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-agent.firstCreate:
	case <-time.After(2 * time.Second):
		t.Fatal("join did not reach the agent")
	}
	waitForLifecycleNodeCount(t, controller, experiment.ID, 1)
	if err := controller.StopScenario(experiment.ID); err != nil {
		t.Fatal(err)
	}

	finished := waitForLifecycleExperiment(t, controller, experiment.ID)
	// Terminal metadata precedes the runner's final log write.
	controller.runs.Wait()
	if finished.State != "canceled" {
		t.Fatalf("experiment state=%q error=%q, want canceled", finished.State, finished.Error)
	}
	if !strings.Contains(finished.Error, "stop scenario peers through generation 1") || !strings.Contains(finished.Error, "500 Internal Server Error") {
		t.Fatalf("cleanup failure missing from experiment error: %q", finished.Error)
	}
	if !strings.Contains(logs.String(), "scenario peer cleanup failed") {
		t.Fatalf("cleanup failure missing from logs: %s", logs.String())
	}
	_, fences := agent.generations()
	if !reflect.DeepEqual(fences, []uint64{1}) {
		t.Fatalf("cleanup fence requests = %v, want [1]", fences)
	}
}

type lifecycleTestAgent struct {
	mu                sync.Mutex
	createCount       int
	deleteCount       int
	createAfterDelete bool
	firstCreate       chan struct{}
	firstCreateOnce   sync.Once
	nodes             map[string]model.Node
	createGenerations []uint64
	fenceGenerations  []uint64
	maxFence          uint64
	fenceStatus       int
}

func (a *lifecycleTestAgent) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes":
		var request model.CreateNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		node := model.Node{
			ID:         request.ID,
			RunID:      request.RunID,
			Generation: request.Generation,
			Group:      request.Group,
			Role:       request.Role,
			Type:       request.Type,
			Profile:    request.Profile,
			State:      model.NodeStarting,
		}
		a.mu.Lock()
		a.createCount++
		if request.Generation <= a.maxFence {
			a.createAfterDelete = true
		}
		a.nodes[node.ID] = node
		a.createGenerations = append(a.createGenerations, request.Generation)
		a.mu.Unlock()
		a.firstCreateOnce.Do(func() { close(a.firstCreate) })
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(node)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
		a.mu.Lock()
		nodes := make([]model.Node, 0, len(a.nodes))
		for _, node := range a.nodes {
			nodes = append(nodes, node)
		}
		a.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(model.AgentHeartbeat{
			Agent: model.Agent{ID: "agent-1", Capacity: 100},
			Nodes: nodes,
		})
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/") && strings.HasSuffix(r.URL.Path, "/nodes"):
		runID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/nodes")
		generation, _ := strconv.ParseUint(r.URL.Query().Get("generation"), 10, 64)
		a.mu.Lock()
		a.fenceGenerations = append(a.fenceGenerations, generation)
		if a.fenceStatus != 0 {
			status := a.fenceStatus
			a.mu.Unlock()
			http.Error(w, "injected fence failure", status)
			return
		}
		if generation > a.maxFence {
			a.maxFence = generation
		}
		for id, node := range a.nodes {
			if node.RunID == runID && node.Generation <= generation {
				a.deleteCount++
				delete(a.nodes, id)
			}
		}
		a.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/nodes/"):
		a.mu.Lock()
		a.deleteCount++
		delete(a.nodes, strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"))
		a.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

func (a *lifecycleTestAgent) counts() (int, int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.createCount, a.deleteCount, a.createAfterDelete
}

func (a *lifecycleTestAgent) generations() ([]uint64, []uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.createGenerations...), append([]uint64(nil), a.fenceGenerations...)
}

func (a *lifecycleTestAgent) nodeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.nodes)
}

func newLifecycleTestController(t *testing.T) *Server {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, logger)
	t.Cleanup(func() {
		server.cancelMu.Lock()
		server.shuttingDown = true
		for _, cancel := range server.cancels {
			cancel()
		}
		server.cancelMu.Unlock()
		// Terminal state becomes visible before the scheduler finishes its last
		// writes. Join it before TempDir cleanup or the mock Agent is closed.
		waitRepetitions(t, server)
	})
	return server
}

func newLifecycleTestControllerWithAgent(t *testing.T) (*Server, *lifecycleTestAgent) {
	t.Helper()
	agent := &lifecycleTestAgent{
		firstCreate: make(chan struct{}),
		nodes:       make(map[string]model.Node),
	}
	agentServer := httptest.NewServer(http.HandlerFunc(agent.serveHTTP))
	t.Cleanup(agentServer.Close)

	controller := newLifecycleTestController(t)
	if _, err := controller.state.registerAgent(model.Agent{
		ID:       "agent-1",
		URL:      agentServer.URL,
		Capacity: 100,
	}); err != nil {
		t.Fatal(err)
	}
	return controller, agent
}

func waitForLifecycleNodeCount(t *testing.T, controller *Server, runID string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(controller.matchingNodes(runID, "", "", "")) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("controller did not record %d nodes for %s", want, runID)
}

func waitForLifecycleExperiment(t *testing.T, controller *Server, runID string) model.Experiment {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		controller.state.mu.RLock()
		experiment := controller.state.experiments[runID]
		controller.state.mu.RUnlock()
		if experiment.State != "running" {
			return experiment
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("experiment %s did not finish", runID)
	return model.Experiment{}
}
