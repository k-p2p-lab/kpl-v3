package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func analysisPoolAgent(id, host, endpoint string) model.Agent {
	now := time.Now().UTC()
	return model.Agent{
		ID: id, Name: id, URL: endpoint, Hostname: host, State: model.AgentOnline,
		Labels: map[string]string{"swarmNodeId": host}, Host: &model.AgentHost{NodeID: host},
		StartedAt: now.Add(-time.Minute), LastSeen: now, StartupReconciled: true,
		AnalysisProcesses: true,
	}
}

func registerAnalysisPoolAgent(t *testing.T, s *Server, agent model.Agent) {
	t.Helper()
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticAnalysisPoolUsesDistinctEnabledHostsAfterExperiments(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), ControlNodeID: "control-node", User: "test-user", Password: "test-password"}, nil)
	for _, agent := range []model.Agent{
		analysisPoolAgent("a-first", "host-one", "http://host-one:9091"),
		analysisPoolAgent("b-duplicate", "host-one", "http://duplicate-one:9091"),
		analysisPoolAgent("c-second", "host-two", "http://host-two:9091"),
		analysisPoolAgent("control", "control-node", "http://controller-agent:9091"),
		analysisPoolAgent("disabled", "disabled-host", "http://disabled:9091"),
		analysisPoolAgent("stale", "stale-host", "http://stale:9091"),
	} {
		registerAnalysisPoolAgent(t, s, agent)
	}
	legacy := analysisPoolAgent("legacy", "legacy-host", "http://legacy:9091")
	legacy.AnalysisProcesses = false
	registerAnalysisPoolAgent(t, s, legacy)
	if err := s.state.setHostDisabled("disabled-host", true); err != nil {
		t.Fatal(err)
	}
	s.state.mu.Lock()
	stale := s.state.agents["stale"]
	stale.LastSeen = time.Now().Add(-2 * agentStaleAfter)
	s.state.agents[stale.ID] = stale
	s.state.mu.Unlock()
	first := queueTestEnqueue(t, s, "experiment")
	second := queueTestEnqueue(t, s, "experiment")
	third := queueTestEnqueue(t, s, "experiment")
	local := queueTestEnqueue(t, s, "analysis")
	remoteOne := queueTestEnqueue(t, s, "batch-analysis")
	remoteTwo := queueTestEnqueue(t, s, "analysis")
	overflow := queueTestEnqueue(t, s, "batch-analysis")
	after := queueTestEnqueue(t, s, "experiment")
	for _, experiment := range []*queueTestWork{first, second, third} {
		queueTestAwait(t, experiment.entered, "preceding experiment")
		select {
		case <-local.entered:
			t.Fatal("analysis started before the experiment group finished")
		default:
		}
		experiment.once.Do(func() { close(experiment.release) })
	}
	queueTestAwait(t, local.entered, "Controller analysis")
	queueTestAwait(t, remoteOne.entered, "first Agent analysis")
	queueTestAwait(t, remoteTwo.entered, "second Agent analysis")
	if local.slot.analysisWorkerURL != "" {
		t.Fatal("Controller analysis did not retain the local child slot")
	}
	allowed := map[string]string{
		"http://host-one:9091":      "host-one",
		"http://duplicate-one:9091": "host-one",
		"http://host-two:9091":      "host-two",
	}
	hostOne, oneOK := allowed[remoteOne.slot.analysisWorkerURL]
	hostTwo, twoOK := allowed[remoteTwo.slot.analysisWorkerURL]
	if !oneOK || !twoOK || hostOne == hostTwo {
		t.Fatalf("automatic pool chose ineligible or duplicate hosts: %q, %q", remoteOne.slot.analysisWorkerURL, remoteTwo.slot.analysisWorkerURL)
	}
	select {
	case <-overflow.entered:
		t.Fatal("stale, disabled, control or legacy Agent added an extra analysis slot")
	default:
	}
	local.once.Do(func() { close(local.release) })
	queueTestAwait(t, overflow.entered, "next analysis on the available Controller slot")
	if overflow.slot.analysisWorkerURL != "" {
		t.Fatal("the same Agent host was assigned to simultaneous analyses")
	}
	for _, work := range []*queueTestWork{remoteOne, remoteTwo, overflow} {
		work.once.Do(func() { close(work.release) })
	}
	queueTestAwait(t, after.entered, "next experiment after all analyses")
}

func TestAutomaticAnalysisPoolExecutesRemoteChildWithAgentCredential(t *testing.T) {
	s, localCommands := isolatedAnalysisServer(t)
	s.config.ControlNodeID = "control-node"
	s.config.AnalysisAgentToken = remoteTestToken
	remote, commands := isolatedAnalysisServer(t)
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, remote.workerCommand)
	finished := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == remoteAnalysisPath {
			defer close(finished)
		}
		worker.ServeHTTP(w, r)
	}))
	t.Cleanup(endpoint.Close)
	registerAnalysisPoolAgent(t, s, analysisPoolAgent("registered-agent", "agent-host", endpoint.URL))
	batchFixture(t, s, "batch", "run", "completed", 1, 2, 2)
	local := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, local.entered, "local analysis")
	s.analysisSlots <- struct{}{}
	defer func() { <-s.analysisSlots }()
	if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
		t.Fatal(err)
	}
	status := awaitAnalysisJob(t, s, "run")
	if status.State != "completed" {
		t.Fatalf("automatically assigned analysis did not complete: %+v", status)
	}
	s.analysisWorkers.Wait()
	queueTestAwait(t, finished, "automatic Agent child cleanup")
	processes := commands()
	if len(processes) != 1 || processes[0].ProcessState == nil || len(localCommands()) != 0 {
		t.Fatal("analysis did not run in a separately reclaimed remote child")
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 1 || queue[0].ID != local.slot.id || !strings.Contains(queue[0].Worker, "Controller") {
		t.Fatalf("automatic analysis changed its local predecessor: %+v", queue)
	}
}
