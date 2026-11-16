package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func checkAnalysisAdmissionReleased(t *testing.T, s *Server) {
	t.Helper()
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 0 {
		t.Fatalf("analysis retained cleanup intent: pending=%v error=%v", pending, err)
	}
	s.state.persistMu.Lock()
	pins := len(s.resultReadPins)
	s.state.persistMu.Unlock()
	if pins != 0 {
		t.Fatalf("analysis retained source pins: %d", pins)
	}
}

func TestAnalysisFrozenPoolRechecksAgentBeforeNewAssignment(t *testing.T) {
	for _, change := range []string{"same-instance", "reenabled", "disabled", "offline", "instance", "URL", "host", "capability", "busy", "unreconciled", "new-host"} {
		t.Run(change, func(t *testing.T) {
			s, localCommands := isolatedAnalysisServer(t)
			s.config.ControlNodeID = "control-node"
			s.config.AnalysisAgentToken = remoteTestToken
			remote, remoteCommands := isolatedAnalysisServer(t)
			endpoint := httptest.NewServer(newRemoteAnalysisWorker(remoteTestToken, s.logger, remote.workerCommand))
			defer endpoint.Close()
			other, otherCommands := isolatedAnalysisServer(t)
			otherEndpoint := httptest.NewServer(newRemoteAnalysisWorker(remoteTestToken, s.logger, other.workerCommand))
			defer otherEndpoint.Close()
			agent := analysisPoolAgent("registered-agent", "agent-host", endpoint.URL)
			registerAnalysisPoolAgent(t, s, agent)
			batchFixture(t, s, "batch", "run", "completed", 1, 1, 1)
			local := queueTestEnqueue(t, s, "analysis")
			queueTestAwait(t, local.entered, "prefix leader captured the original eligible Agent")
			expectedRemote := change == "same-instance" || change == "reenabled"
			if change == "disabled" || change == "reenabled" || change == "new-host" {
				setEnabledTest(t, s.state, agent.ID, false)
			}
			if change == "reenabled" {
				setEnabledTest(t, s.state, agent.ID, true)
			}
			s.state.mu.Lock()
			current := s.state.agents[agent.ID]
			switch change {
			case "offline":
				current.State = model.AgentOffline
			case "instance":
				current.StartedAt = current.StartedAt.Add(time.Second)
			case "URL":
				current.URL = otherEndpoint.URL
			case "host":
				current.Host = &model.AgentHost{NodeID: "replacement-host"}
				current.Labels = map[string]string{"swarmNodeId": "replacement-host"}
			case "capability":
				current.AnalysisProcesses = false
			case "busy":
				current.ActiveNodes = 1
			case "unreconciled":
				current.StartupReconciled = false
			}
			s.state.agents[agent.ID] = current
			s.state.mu.Unlock()
			if change == "new-host" {
				registerAnalysisPoolAgent(t, s, analysisPoolAgent("new-agent", "new-host", otherEndpoint.URL))
			}
			if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
				t.Fatal(err)
			}
			local.once.Do(func() { close(local.release) })
			status := awaitAnalysisJob(t, s, "run")
			s.analysisWorkers.Wait()
			wantedRemote := 0
			if expectedRemote {
				wantedRemote = 1
			}
			if status.State != "completed" || len(remoteCommands()) != wantedRemote || len(otherCommands()) != 0 || len(localCommands()) != 1-wantedRemote {
				t.Fatalf("frozen pool used unavailable/replacement/new host or duplicated local slot: change=%s state=%s remote=%d other=%d local=%d", change, status.State, len(remoteCommands()), len(otherCommands()), len(localCommands()))
			}
			for _, commands := range []func() []*exec.Cmd{remoteCommands, otherCommands, localCommands} {
				for _, command := range commands() {
					if command.ProcessState == nil {
						t.Fatal("analysis released assignment before actual OS child reaping")
					}
				}
			}
			checkAnalysisAdmissionReleased(t, s)
		})
	}
}

func waitAnalysisCleanupIntentBoundary(t *testing.T) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	stack := make([]byte, 1<<20)
	for {
		for _, routine := range strings.Split(string(stack[:runtime.Stack(stack, true)]), "\n\n") {
			if strings.Contains(routine, "controller.(*Server).trackAnalysisWorker(") && (strings.Contains(routine, "contextlock.Lock") || strings.Contains(routine, "sync.(*Mutex).Lock")) {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatal("assigned analysis did not reach the cleanup intent publication wait")
		case <-ticker.C:
		}
	}
}

func TestAssignedAnalysisRechecksAfterSourceCaptureAndIntentWait(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	s.config.ControlNodeID = "control-node"
	s.config.AnalysisAgentToken = remoteTestToken
	remote, commands := isolatedAnalysisServer(t)
	var requests atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		newRemoteAnalysisWorker(remoteTestToken, s.logger, remote.workerCommand).ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	agent := analysisPoolAgent("registered-agent", "agent-host", endpoint.URL)
	registerAnalysisPoolAgent(t, s, agent)
	batchFixture(t, s, "batch", "run", "completed", 1, 1, 1)
	local := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, local.entered, "Controller slot before remote admission")
	s.analysisWorkerCleanupMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.analysisWorkerCleanupMu.Unlock()
		}
	}()
	if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
		t.Fatal(err)
	}
	waitAnalysisCleanupIntentBoundary(t)
	setEnabledTest(t, s.state, agent.ID, false)
	s.analysisWorkerCleanupMu.Unlock()
	locked = false
	status := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	if status.State != "failed" || !strings.Contains(status.Error, "unavailable or changed") || requests.Load() != 0 || len(commands()) != 0 {
		t.Fatalf("changed pre-start assignment reached remote RPC: state=%s error=%s requests=%d children=%d", status.State, status.Error, requests.Load(), len(commands()))
	}
	checkAnalysisAdmissionReleased(t, s)
}

func TestAnalysisAdmissionOSWorkerHelper(t *testing.T) {
	if len(os.Args) < 4 || os.Args[len(os.Args)-3] != "analysis-admission-worker-helper" {
		return
	}
	gate := os.Args[len(os.Args)-2]
	kind := os.Args[len(os.Args)-1]
	err := workerproc.Serve(context.Background(), kind, func(ctx context.Context, kind string, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		handler, err := WorkerFactory(ctx, kind, bootstrap, peer)
		if err != nil {
			return nil, err
		}
		return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
			if method == "analyze" {
				if err := os.WriteFile(filepath.Join(gate, "started"), nil, 0600); err != nil {
					return nil, err
				}
				for {
					if _, err := os.Stat(filepath.Join(gate, "release")); err == nil {
						break
					} else if !errors.Is(err, os.ErrNotExist) {
						return nil, err
					}
					if err := sleepContext(callCtx, time.Millisecond); err != nil {
						return nil, err
					}
				}
			}
			return handler(callCtx, method, input)
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestActiveAnalysisKeepsItsAdmittedWorkerAfterDisable(t *testing.T) {
	s, localCommands := isolatedAnalysisServer(t)
	s.config.ControlNodeID = "control-node"
	s.config.AnalysisAgentToken = remoteTestToken
	remote, commands := isolatedAnalysisServer(t)
	originalCommand := remote.workerCommand
	gate := t.TempDir()
	remote.workerCommand = func(kind string) *exec.Cmd {
		cmd := originalCommand(kind)
		cmd.Args = []string{os.Args[0], "-test.run=^TestAnalysisAdmissionOSWorkerHelper$", "--", "analysis-admission-worker-helper", gate, kind}
		return cmd
	}
	endpoint := httptest.NewServer(newRemoteAnalysisWorker(remoteTestToken, s.logger, remote.workerCommand))
	defer endpoint.Close()
	agent := analysisPoolAgent("registered-agent", "agent-host", endpoint.URL)
	registerAnalysisPoolAgent(t, s, agent)
	batchFixture(t, s, "batch", "run", "completed", 1, 1, 1)
	local := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, local.entered, "Controller slot before active remote calculation")
	defer os.WriteFile(filepath.Join(gate, "release"), nil, 0600)
	if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(gate, "started")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual remote OS child did not enter analyze RPC")
		}
		time.Sleep(time.Millisecond)
	}
	setEnabledTest(t, s.state, agent.ID, false)
	// Keep the original real calculation active while a newly registered
	// endpoint advertises the same host. Its URL cannot add another host lease.
	replacement, replacementCommands := isolatedAnalysisServer(t)
	replacementEndpoint := httptest.NewServer(newRemoteAnalysisWorker(remoteTestToken, s.logger, replacement.workerCommand))
	defer replacementEndpoint.Close()
	registerAnalysisPoolAgent(t, s, analysisPoolAgent("replacement-agent", "agent-host", replacementEndpoint.URL))
	batchFixture(t, s, "following-batch", "following-run", "completed", 1, 1, 1)
	if _, err := s.startAnalysisJob(t.Context(), "following-run", false); err != nil {
		t.Fatal(err)
	}
	local.once.Do(func() { close(local.release) })
	following := awaitAnalysisJob(t, s, "following-run")
	if following.State != "completed" || len(localCommands()) != 1 || len(replacementCommands()) != 0 {
		t.Fatalf("replacement endpoint duplicated the active host or local slot: state=%s local=%d replacement=%d", following.State, len(localCommands()), len(replacementCommands()))
	}
	if err := os.WriteFile(filepath.Join(gate, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	status := awaitAnalysisJob(t, s, "run")
	s.analysisWorkers.Wait()
	started := commands()
	if status.State != "completed" || len(started) != 1 || started[0].ProcessState == nil {
		t.Fatalf("disable changed an already admitted real child: state=%s children=%d", status.State, len(started))
	}
	checkAnalysisAdmissionReleased(t, s)
}
