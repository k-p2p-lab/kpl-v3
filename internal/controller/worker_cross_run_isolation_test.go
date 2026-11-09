package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const crossRunTargetScenario = `version: 4
name: cross-run-target-500
seed: 628154
jobShutdownTimeout: 30s
phases:
  - action: join
    group: target
    type: full
    count: 500
    parallel: true
    parallelism: 32
    placement: random
    lifetime: {model: exponential, mean: 45m, min: 30m, max: 60m}
  - {action: wait-ready, group: target, minCount: 500, readyRatio: 1, timeout: 30s}
  - {action: leave, group: target, count: 100, parallel: true, parallelism: 32}
  - {action: join, group: target, type: full, count: 100, parallel: true, parallelism: 32, placement: random, lifetime: {model: exponential, mean: 45m, min: 30m, max: 60m}}
  # The barrier denominator retains stopped Nodes: 500 ready of 600 known.
  - {action: wait-ready, group: target, minCount: 600, readyRatio: 0.8333333333333334, timeout: 30s}
  - {action: leave, group: target, count: 100, parallel: true, parallelism: 32}
  - {action: join, group: target, type: full, count: 100, parallel: true, parallelism: 32, placement: random, lifetime: {model: exponential, mean: 45m, min: 30m, max: 60m}}
  # Each replacement wave must restore 500 ready, now of 700 known Nodes.
  - {action: wait-ready, group: target, minCount: 700, readyRatio: 0.7142857142857143, timeout: 30s}
`
const crossRunDirtyScenario = `version: 4
name: failed-before-target
seed: 99137
jobShutdownTimeout: 30s
phases:
  - {action: join, group: dirty, type: full, count: 500, parallel: true, parallelism: 32, placement: random, lifetime: {model: exponential, mean: 45m, min: 30m, max: 60m}}
  - {action: wait-ready, group: dirty, minCount: 500, readyRatio: 1, timeout: 30s}
  - {action: join, group: dirty-failure, type: full, count: 2, parallel: true, parallelism: 2, placement: random}
`

type crossRunSample struct {
	Suffix, Agent, Group, Lifetime string
	Seed                           int64
}
type crossRunMockAgent struct {
	identity       model.Agent
	nodes, history map[string]model.Node
	fences         map[string]uint64
	drained        map[string]bool
}
type crossRunCluster struct {
	t              *testing.T
	ctx            context.Context
	server         *Server
	mu             sync.Mutex
	agents         map[string]*crossRunMockAgent
	commands       []*exec.Cmd
	samples        map[string]map[string]crossRunSample
	created        map[string]map[string]model.Node
	receipts       map[string][sha256.Size]byte
	events         map[string][]model.TraceEvent
	peak           map[string]int
	left           map[string]int
	unknownID      string
	busyObserved   bool
	targetRun      string
	targetReady    chan struct{}
	releaseTarget  chan struct{}
	releaseOnce    sync.Once
	turnoverReady  [2]chan struct{}
	turnoverACK    [2]chan struct{}
	turnoverOnce   [2]sync.Once
	stopBackground func()
}

func newCrossRunCluster(t *testing.T, ctx context.Context) *crossRunCluster {
	t.Helper()
	f := &crossRunCluster{t: t, ctx: ctx, agents: make(map[string]*crossRunMockAgent), samples: make(map[string]map[string]crossRunSample), created: make(map[string]map[string]model.Node), receipts: make(map[string][sha256.Size]byte), events: make(map[string][]model.TraceEvent), peak: make(map[string]int), left: make(map[string]int), targetReady: make(chan struct{}), releaseTarget: make(chan struct{})}
	for i := range f.turnoverReady {
		f.turnoverReady[i], f.turnoverACK[i] = make(chan struct{}), make(chan struct{})
	}
	f.server = New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	f.server.client.Timeout = time.Minute // The last ACK is explicitly gated, not a latency assertion.
	f.server.workerCommand = func(kind string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		f.mu.Lock()
		f.commands = append(f.commands, cmd)
		f.mu.Unlock()
		return cmd
	}
	for i := range 3 {
		id := fmt.Sprintf("agent-%d", i)
		a := &crossRunMockAgent{nodes: make(map[string]model.Node), history: make(map[string]model.Node), fences: make(map[string]uint64), drained: make(map[string]bool)}
		endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.serveAgent(a, w, r) }))
		t.Cleanup(endpoint.Close)
		a.identity = model.Agent{ID: id, Name: id, URL: endpoint.URL, StartedAt: time.Now().Add(-time.Minute).UTC(), Capacity: 600, RunDrain: true, RunProcesses: true, CreateReplay: true, StartupReconciled: true}
		f.agents[id] = a
		if _, err := f.server.state.registerAgent(a.identity); err != nil {
			t.Fatal(err)
		}
	}
	loopCtx, stop := context.WithCancel(ctx)
	heartbeatDone, summaryDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				_ = f.server.refreshAgentState(loopCtx)
			}
		}
	}()
	go func() { defer close(summaryDone); f.server.runWorkerSummaryLoop(loopCtx) }()
	f.stopBackground = func() {
		stop()
		<-heartbeatDone
		<-summaryDone
	}
	t.Cleanup(func() {
		f.releaseOnce.Do(func() { close(f.releaseTarget) })
		for i := range f.turnoverACK {
			f.turnoverOnce[i].Do(func() { close(f.turnoverACK[i]) })
		}
		f.stopBackground()
		f.server.cancelMu.Lock()
		for _, cancel := range f.server.cancels {
			cancel()
		}
		f.server.cancelMu.Unlock()
		f.server.runs.Wait()
		f.server.workWorkers.Wait()
	})
	return f
}

func (f *crossRunCluster) serveAgent(a *crossRunMockAgent, w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/api/v1/status" {
		f.mu.Lock()
		agent := a.identity
		agent.LastSeen = time.Now().UTC()
		agent.ActiveNodes = len(a.nodes)
		var nodes []model.Node
		for _, n := range a.nodes {
			nodes = append(nodes, n)
		}
		for _, n := range a.history {
			nodes = append(nodes, n)
		}
		f.mu.Unlock()
		writeJSON(w, 200, model.AgentHeartbeat{Agent: agent, Nodes: nodes})
		return
	}
	if r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes" {
		f.createAgentNode(a, w, r)
		return
	}
	if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/nodes/") {
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/")
		f.mu.Lock()
		node, exists := a.nodes[id]
		if exists {
			f.retireMockNode(a, node)
			f.left[node.RunID]++
		}
		_, stopped := a.history[id]
		f.mu.Unlock()
		if !exists && !stopped {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/runs/") {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/nodes")
		f.mu.Lock()
		a.fences[id] = ^uint64(0)
		for _, node := range a.nodes {
			if node.RunID == id {
				f.retireMockNode(a, node)
			}
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/drain") {
		f.drainAgent(a, w, r)
		return
	}
	http.NotFound(w, r)
}

// Caller holds f.mu. Terminal evidence remains available through every later
// full Agent report, including while the next Run is actively creating Peers.
func (f *crossRunCluster) retireMockNode(a *crossRunMockAgent, node model.Node) {
	node.State = model.NodeStopped
	node.LastSeen = time.Now().UTC()
	node.Metadata = map[string]string{"cleanupComplete": "true"}
	node.PeerScores = map[string]float64{}
	for i := range 32 {
		node.PeerScores[fmt.Sprintf("historical-score-%d", i)] = float64(i)
	}
	a.history[node.ID] = node
	delete(a.nodes, node.ID)
}

func (f *crossRunCluster) createAgentNode(a *crossRunMockAgent, w http.ResponseWriter, r *http.Request) {
	var raw json.RawMessage
	var request model.CreateNodeRequest
	if json.NewDecoder(r.Body).Decode(&raw) != nil || json.Unmarshal(raw, &request) != nil {
		http.Error(w, "bad create", 400)
		return
	}
	f.mu.Lock()
	if fence, exists := a.fences[request.RunID]; exists && request.Generation <= fence {
		f.mu.Unlock()
		http.Error(w, "Run generation is fenced", 409)
		return
	}
	if request.Group == "dirty-failure" {
		if f.unknownID == "" {
			f.unknownID = request.ID
		}
		if request.ID != f.unknownID {
			f.mu.Unlock()
			w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
			w.Header().Set("Retry-After", "30")
			writeError(w, 503, "mock Agent telemetry admission is full")
			return
		}
	}
	digest := sha256.Sum256(raw)
	if old, exists := f.receipts[request.ID]; exists && old != digest {
		f.mu.Unlock()
		http.Error(w, "changed original create", 409)
		return
	}
	node, exists := a.nodes[request.ID]
	gate := false
	turnover := -1
	if !exists {
		// An exact replay of an individually stopped Peer must not resurrect it.
		node, exists = a.history[request.ID]
	}
	if !exists {
		now := time.Now().UTC()
		node = model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, AgentID: a.identity.ID, Group: request.Group, Type: request.Type, Role: request.Role, Profile: request.Profile, State: model.NodeReady, StartedAt: now, LastSeen: now}
		a.nodes[node.ID] = node
		f.receipts[node.ID] = digest
		if f.samples[node.RunID] == nil {
			f.samples[node.RunID] = make(map[string]crossRunSample)
			f.created[node.RunID] = make(map[string]model.Node)
		}
		f.samples[node.RunID][node.ID] = crossRunSample{Suffix: strings.TrimPrefix(node.ID, node.RunID), Agent: a.identity.ID, Group: request.Group, Seed: request.Seed, Lifetime: request.Lifetime}
		f.created[node.RunID][node.ID] = node
		live := 0
		for _, owner := range f.agents {
			for _, current := range owner.nodes {
				if current.RunID == node.RunID {
					live++
				}
			}
		}
		f.peak[node.RunID] = max(f.peak[node.RunID], live)
		if request.Group == "target" && len(f.created[node.RunID]) == 500 {
			f.targetRun = node.RunID
			gate = true
			close(f.targetReady)
		}
		if request.Group == "target" && (len(f.created[node.RunID]) == 600 || len(f.created[node.RunID]) == 700) {
			turnover = (len(f.created[node.RunID]) - 600) / 100
			close(f.turnoverReady[turnover])
		}
	}
	unknown := request.ID == f.unknownID
	f.mu.Unlock()
	if unknown {
		// Keep the unknown admission alive until the other parallel operation
		// has installed a real A-owned Retry-After=30s hint in the parent.
		for {
			f.server.state.mu.RLock()
			busy := len(f.server.state.agentAdmissionUntil) > 0
			f.server.state.mu.RUnlock()
			if busy {
				f.mu.Lock()
				f.busyObserved = true
				f.mu.Unlock()
				break
			}
			select {
			case <-f.ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
		dropCreateResponse(f.t, w)
		return
	}
	if gate {
		select {
		case <-f.releaseTarget:
		case <-f.ctx.Done():
			return
		}
	}
	if turnover >= 0 {
		select {
		case <-f.turnoverACK[turnover]:
		case <-f.ctx.Done():
			return
		}
	}
	writeJSON(w, http.StatusCreated, node)
}

func (f *crossRunCluster) drainAgent(a *crossRunMockAgent, w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/runs/"), "/drain")
	f.mu.Lock()
	for _, node := range a.nodes {
		if node.RunID == id {
			f.mu.Unlock()
			http.Error(w, "Peers still running", 503)
			return
		}
	}
	if a.drained[id] {
		f.mu.Unlock()
		w.WriteHeader(204)
		return
	}
	if f.events[id] == nil {
		var nodes []model.Node
		owners := map[string]string{}
		for _, n := range f.created[id] {
			nodes = append(nodes, n)
			owners[n.ID] = n.AgentID
		}
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
		if len(nodes) >= 2 {
			f.events[id] = workerFixtureEvents(id, nodes)
			for i := range f.events[id] {
				f.events[id][i].AgentID = owners[f.events[id][i].NodeID]
			}
		}
	}
	var events []model.TraceEvent
	for _, event := range f.events[id] {
		if event.AgentID == a.identity.ID {
			events = append(events, event)
		}
	}
	f.mu.Unlock()
	if len(events) > 0 {
		if err := f.server.state.appendEvents(model.EventBatch{AgentID: a.identity.ID, Events: events}); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	f.mu.Lock()
	a.drained[id] = true
	f.mu.Unlock()
	w.WriteHeader(204)
}

type crossRunBaseline struct {
	HotNodes, Reservations, Uncertain, RawMetrics, RunTimings, JournalIndices int
	Cooldowns, CooldownOwners, AgentOccupied, ControllersUnreaped             int
	Cancellations, RepeatBatches, WorkSlots, DirtySummaries, DueSummaries     int
	MockLivePeers                                                             int
	SummaryActive                                                             bool
}
type crossRunMetricCounts struct{ Published, Delivered, Expected, Eligible, Duplicates, LatencySamples int }

func crossRunCounts(metrics model.Metrics) crossRunMetricCounts {
	return crossRunMetricCounts{metrics.Published, metrics.Delivered, metrics.ExpectedDeliveries, metrics.EligibleDeliveries, metrics.Duplicates, metrics.LatencySamples}
}

func (f *crossRunCluster) baseline() crossRunBaseline {
	s := f.server
	s.cancelMu.Lock()
	s.state.mu.RLock()
	b := crossRunBaseline{HotNodes: len(s.state.nodes), Reservations: len(s.state.reservations), Uncertain: len(s.state.uncertainCreates), RawMetrics: len(s.state.runMetrics), RunTimings: len(s.state.runTimings), JournalIndices: len(s.state.journalIndices), Cooldowns: len(s.state.agentAdmissionUntil), CooldownOwners: len(s.state.agentAdmissionOwners), Cancellations: len(s.cancels), RepeatBatches: len(s.repeatBatches), WorkSlots: len(s.workSlots), DirtySummaries: len(s.state.summaryDirty), DueSummaries: len(s.state.summaryDue), SummaryActive: s.summaryRefreshActive}
	for _, agent := range s.state.agents {
		b.AgentOccupied += agent.ActiveNodes
	}
	s.state.mu.RUnlock()
	s.cancelMu.Unlock()
	s.workerMu.Lock()
	for _, workers := range s.unreapedWorkers {
		b.ControllersUnreaped += len(workers)
	}
	s.workerMu.Unlock()
	f.mu.Lock()
	for _, agent := range f.agents {
		b.MockLivePeers += len(agent.nodes)
	}
	f.mu.Unlock()
	return b
}

func (f *crossRunCluster) awaitRun(id string) model.Experiment {
	f.t.Helper()
	done := make(chan struct{})
	go func() { f.server.runs.Wait(); close(done) }()
	select {
	case <-done:
	case <-f.ctx.Done():
		f.t.Fatalf("Run %s did not finish: %v", id, f.ctx.Err())
	}
	f.server.state.mu.RLock()
	run := f.server.state.experiments[id]
	f.server.state.mu.RUnlock()
	if run.CleanupState != "complete" || run.TelemetryPending {
		f.t.Fatalf("Run did not confirm cleanup/drain: %+v", run)
	}
	if err := f.server.verifyRetiredRuntime(id); err != nil {
		f.t.Fatal(err)
	}
	f.mu.Lock()
	for _, agent := range f.agents {
		if !agent.drained[id] {
			f.t.Errorf("Agent %s did not drain %s", agent.identity.ID, id)
		}
	}
	f.mu.Unlock()
	for _, name := range []string{filepath.Join(currentRunsDirectory, id, "experiment.json"), filepath.Join(currentRunsDirectory, id, "scenario.yaml"), filepath.Join(ownershipDirectory, id+".json"), filepath.Join(retiredRuntimeDirectory, id+".json")} {
		if _, err := os.Stat(filepath.Join(f.server.config.DataDir, name)); err != nil {
			f.t.Fatalf("immutable history was removed: %s: %v", name, err)
		}
	}
	return run
}

func (f *crossRunCluster) awaitBaseline(label string) crossRunBaseline {
	f.t.Helper()
	for {
		b := f.baseline()
		if b.Cooldowns != 0 || b.CooldownOwners != 0 {
			f.t.Fatalf("%s inherited a finished Run's soft admission hint: %+v", label, b)
		}
		if b == (crossRunBaseline{}) {
			f.t.Logf("%s baseline %+v", label, b)
			return b
		}
		select {
		case <-f.ctx.Done():
			f.t.Fatalf("%s did not return to baseline: %+v: %v", label, b, f.ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (f *crossRunCluster) lateRunPayload(old model.Experiment, current model.Experiment) {
	f.t.Helper()
	// The original A OS process is already reaped. Deliver an A payload over
	// an actual private worker connection to B's broker contract, proving that
	// a stale Run payload cannot cross the independent broker/HTTP boundary.
	b := &runWorkerBroker{server: f.server, runID: current.ID, executionID: current.ExecutionID, generation: 1}
	f.mu.Lock()
	var oldNode model.Node
	for _, node := range f.created[old.ID] {
		oldNode = node
		break
	}
	var owner model.Agent
	for _, agent := range f.agents {
		if agent.identity.ID == oldNode.AgentID {
			owner = agent.identity
		}
	}
	before := len(f.created[current.ID])
	f.mu.Unlock()
	request := model.CreateNodeRequest{ID: oldNode.ID, RunID: old.ID, Group: oldNode.Group, Type: oldNode.Type, Generation: oldNode.Generation, AgentStartedAt: owner.StartedAt}
	raw, _ := json.Marshal(request)
	call := workerAgentCall{BaseURL: owner.URL, Method: http.MethodPost, Path: "/api/v1/nodes", Input: raw, Output: true}
	process, err := workerproc.Start(f.ctx, "controller-create-test", map[string]string{"runId": old.ID}, b.handle, workerproc.Options{Command: func(string) *exec.Cmd {
		return exec.Command(os.Args[0], "-test.run=^TestControllerCreateIPCWorkerHelper$", "--", "create-ipc-helper")
	}, Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		f.t.Fatal(err)
	}
	var proof workerCreateIPCTestProof
	callErr := process.Call(f.ctx, "create", workerCreateIPCTestInput{Call: call}, &proof)
	closeErr := process.Close()
	_ = b.close()
	if callErr != nil || closeErr != nil || proof.Failure == nil || !strings.Contains(proof.Failure.Message, "foreign-run") {
		f.t.Fatalf("late Run payload was not rejected through private IPC: %+v %v %v", proof, callErr, closeErr)
	}
	f.mu.Lock()
	after := len(f.created[current.ID])
	f.mu.Unlock()
	if before != after {
		f.t.Fatal("late RPC changed B's admitted population")
	}
	// Keep A's real source/ownership history. A late scientific event must
	// affect only A's own journal, never B's events or metric accumulators.
	event := model.TraceEvent{RunID: old.ID, AgentID: oldNode.AgentID, NodeID: oldNode.ID, SessionID: oldNode.ID + "-session", Sequence: 99999, EventID: "cross-run-late-A", Type: "publish", MessageID: "old-A-message", Topic: "topic", Timestamp: time.Now().UTC()}
	if err := f.server.state.appendEvents(model.EventBatch{AgentID: oldNode.AgentID, Events: []model.TraceEvent{event}}); err != nil {
		f.t.Fatal(err)
	}
	// An out-of-order historical status page can contain previously unseen
	// nonterminal A records. It must not recreate A's retired runtime indexes,
	// consume B's reservations, or make B's ready population disappear.
	f.mu.Lock()
	owner.LastSeen = time.Now().UTC()
	owner.ActiveNodes = len(f.agents[owner.ID].nodes)
	f.mu.Unlock()
	late := []model.Node{
		{ID: old.ID + "-late-ready-unseen", RunID: old.ID, Generation: 1, AgentID: owner.ID, Group: "dirty", Type: "full", State: model.NodeReady, StartedAt: owner.LastSeen, LastSeen: owner.LastSeen},
		{ID: old.ID + "-late-starting-unseen", RunID: old.ID, Generation: 1, AgentID: owner.ID, Group: "dirty", Type: "full", State: model.NodeStarting, StartedAt: owner.LastSeen, LastSeen: owner.LastSeen},
	}
	if err := f.server.state.heartbeat(model.AgentHeartbeat{Agent: owner, Nodes: late, Partial: true}); err != nil {
		f.t.Fatal(err)
	}
	if err := f.server.verifyRetiredRuntime(old.ID); err != nil {
		f.t.Fatal(err)
	}
	f.server.state.mu.RLock()
	for _, node := range late {
		_, hot := f.server.state.nodes[node.ID]
		_, reported := f.server.state.nodeReportTimes[node.ID]
		_, versioned := f.server.state.nodeVersions[node.ID]
		if hot || reported || versioned {
			f.t.Errorf("late %s A record recreated runtime state: hot=%v report=%v version=%v", node.State, hot, reported, versioned)
		}
	}
	f.server.state.mu.RUnlock()
}

func (f *crossRunCluster) target(dirty *model.Experiment) ([]crossRunSample, crossRunMetricCounts, crossRunBaseline) {
	f.t.Helper()
	first, err := f.server.StartScenario(f.ctx, []byte(crossRunTargetScenario))
	if err != nil {
		f.t.Fatal(err)
	}
	select {
	case <-f.targetReady:
	case <-f.ctx.Done():
		f.t.Fatalf("target did not reach 500 simultaneous Peers: %v", f.ctx.Err())
	}
	if err := f.server.refreshAgentState(f.ctx, first.ID); err != nil {
		f.t.Fatal(err)
	}
	ready, total, failed := f.server.readyCount(first.ID, 1, "target", "full")
	if ready != 500 || total != 500 || failed != 0 {
		f.t.Fatalf("target gate does not contain 500 ready Peers: ready=%d total=%d failed=%d", ready, total, failed)
	}
	f.server.state.mu.RLock()
	beforeReservations, beforeUnknown := len(f.server.state.reservations), len(f.server.state.uncertainCreates)
	f.server.state.mu.RUnlock()
	beforeSummary, _ := f.server.state.workerMetricSummary(first.ID)
	if dirty != nil {
		f.lateRunPayload(*dirty, first)
	}
	if err := f.server.refreshAgentState(f.ctx, first.ID); err != nil {
		f.t.Fatal(err)
	}
	ready, total, failed = f.server.readyCount(first.ID, 1, "target", "full")
	f.server.state.mu.RLock()
	afterReservations, afterUnknown := len(f.server.state.reservations), len(f.server.state.uncertainCreates)
	f.server.state.mu.RUnlock()
	afterSummary, _ := f.server.state.workerMetricSummary(first.ID)
	if ready != 500 || total != 500 || failed != 0 || beforeReservations != afterReservations || beforeUnknown != afterUnknown || crossRunCounts(beforeSummary.Metrics) != crossRunCounts(afterSummary.Metrics) {
		f.t.Fatalf("old Run traffic affected B: ready=%d/%d failed=%d reservation=%d/%d unknown=%d/%d metrics=%+v/%+v", ready, total, failed, beforeReservations, afterReservations, beforeUnknown, afterUnknown, crossRunCounts(beforeSummary.Metrics), crossRunCounts(afterSummary.Metrics))
	}
	f.releaseOnce.Do(func() { close(f.releaseTarget) })
	for wave := range f.turnoverReady {
		select {
		case <-f.turnoverReady[wave]:
		case <-f.ctx.Done():
			f.t.Fatalf("replacement wave %d did not restore 500 Peers: %v", wave+1, f.ctx.Err())
		}
		if err := f.server.refreshAgentState(f.ctx, first.ID); err != nil {
			f.t.Fatal(err)
		}
		ready, total, failed = f.server.readyCount(first.ID, 1, "target", "full")
		f.mu.Lock()
		left, live := f.left[first.ID], 0
		for _, agent := range f.agents {
			for _, node := range agent.nodes {
				if node.RunID == first.ID {
					live++
				}
			}
		}
		f.mu.Unlock()
		wantTotal, wantLeft := 600+100*wave, 100*(wave+1)
		if ready != 500 || total != wantTotal || failed != 0 || live != 500 || left != wantLeft {
			f.t.Fatalf("replacement wave %d clipped or overlapped population: ready=%d total=%d failed=%d live=%d left=%d", wave+1, ready, total, failed, live, left)
		}
		f.t.Logf("B replacement wave=%d ready=%d known=%d live=%d removed=%d", wave+1, ready, total, live, left)
		f.turnoverOnce[wave].Do(func() { close(f.turnoverACK[wave]) })
	}
	run := f.awaitRun(first.ID)
	if run.State != "completed" || run.DataState != "complete" {
		f.t.Fatalf("B did not finish successfully: %+v", run)
	}
	baseline := f.awaitBaseline("after B")
	summary, ok := f.server.state.workerMetricSummary(first.ID)
	if !ok {
		f.t.Fatal("B omitted its scientific metric summary")
	}
	metrics := crossRunCounts(summary.Metrics)
	if metrics.Published != 1 || metrics.Delivered != 1 || metrics.Expected != 1 || metrics.Eligible != 1 {
		f.t.Fatalf("B's scientific data is incomplete or contaminated: %+v", metrics)
	}
	f.mu.Lock()
	var samples []crossRunSample
	for _, sample := range f.samples[first.ID] {
		samples = append(samples, sample)
	}
	peak := f.peak[first.ID]
	commands := append([]*exec.Cmd(nil), f.commands...)
	f.mu.Unlock()
	if peak != 500 || len(samples) != 700 {
		f.t.Fatalf("B population was clipped: peak=%d samples=%d", peak, len(samples))
	}
	for _, cmd := range commands {
		if cmd.ProcessState == nil {
			f.t.Fatal("a Controller experiment or summary OS process was not reaped")
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Suffix < samples[j].Suffix })
	for _, sample := range samples {
		if sample.Lifetime == "" {
			f.t.Fatal("lifetime distribution was not sampled")
		}
	}
	f.t.Logf("B ready=%d peak=%d requests=%d metrics=%+v reapedProcesses=%d", ready, peak, len(samples), metrics, len(commands))
	return samples, metrics, baseline
}

func TestWorkerCrossRunIsolationReaches500AfterFailedDenseRun(t *testing.T) {
	// This is the entire integration fixture's budget, including instrumented
	// OS subprocesses. Scenario readiness and shutdown deadlines remain 30s.
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	fresh := newCrossRunCluster(t, ctx)
	fresh.awaitBaseline("fresh initial")
	freshSamples, freshMetrics, freshBaseline := fresh.target(nil)
	// The independent baseline cluster has finished all work. Preserve its
	// history and measured outcome without replaying its 700 terminal records
	// throughout the separate failed-A comparison's execution.
	fresh.stopBackground()
	dirty := newCrossRunCluster(t, ctx)
	dirty.awaitBaseline("dirty initial")
	first, err := dirty.server.StartScenario(ctx, []byte(crossRunDirtyScenario))
	if err != nil {
		t.Fatal(err)
	}
	failed := dirty.awaitRun(first.ID)
	dirty.mu.Lock()
	peak, busy := dirty.peak[first.ID], dirty.busyObserved
	initial := 0
	for _, sample := range dirty.samples[first.ID] {
		if sample.Group == "dirty" {
			initial++
		}
	}
	dirty.mu.Unlock()
	if failed.State != "failed" || peak < 500 || initial != 500 || !busy {
		t.Fatalf("dirty A did not exercise the intended dense/unknown/backpressure failure: state=%s peak=%d initial=%d busy=%v", failed.State, peak, initial, busy)
	}
	dirty.awaitBaseline("after failed A")
	t.Logf("A state=%s readyWave=%d peak=%d unknownCreate=true retryAfter30sObserved=%v", failed.State, initial, peak, busy)
	dirtySamples, dirtyMetrics, dirtyBaseline := dirty.target(&failed)
	if !reflect.DeepEqual(freshSamples, dirtySamples) {
		t.Fatal("failed A changed B's normalized placement/seed/lifetime samples")
	}
	if freshMetrics != dirtyMetrics || freshBaseline != dirtyBaseline {
		t.Fatalf("failed A changed B outcome or residual state: metrics=%+v/%+v baseline=%+v/%+v", freshMetrics, dirtyMetrics, freshBaseline, dirtyBaseline)
	}
	for _, f := range []*crossRunCluster{fresh, dirty} {
		f.mu.Lock()
		for id, owner := range f.agents {
			if len(owner.history) == 0 {
				t.Errorf("%s lost retained terminal history", id)
			}
		}
		f.mu.Unlock()
	}
	// Preserve byte-for-byte immutable scenario input; cleanup is not deletion.
	raw, err := os.ReadFile(filepath.Join(dirty.server.config.DataDir, currentRunsDirectory, failed.ID, "scenario.yaml"))
	if err != nil || !bytes.Equal(raw, []byte(crossRunDirtyScenario)) {
		t.Fatalf("A's original source was changed by B: %v", err)
	}
}
