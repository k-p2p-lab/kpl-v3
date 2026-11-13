package controller

import (
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const firstRecoveredRun, secondRecoveredRun = "earlier-recovered-run", "later-recovered-run"

type recoveredCleanupFixture struct {
	*controllerWorkerFixture
	commandsMu   sync.Mutex
	commands     []string
	networkOwner string
	fenceFailure string
	drainFailure string
}

func newRecoveredCleanupFixture(t *testing.T) *recoveredCleanupFixture {
	t.Helper()
	f := newControllerWorkerFixture(t)
	recovery := &recoveredCleanupFixture{controllerWorkerFixture: f}
	for _, id := range []string{firstRecoveredRun, secondRecoveredRun} {
		run, _ := resultFixture(t, f.server, id, "failed", time.Now().Add(-time.Minute))
		run.TelemetryPending = true
		f.server.state.experiments[id] = run
	}
	node := model.Node{ID: secondRecoveredRun + "-peer", RunID: secondRecoveredRun, AgentID: "agent", State: model.NodeReady}
	f.mu.Lock()
	f.nodes[node.ID] = node
	f.mu.Unlock()
	f.server.state.mu.Lock()
	f.server.state.setNodeLocked(node)
	f.server.state.mu.Unlock()
	f.server.client.Transport = discoveryTransport(func(request *http.Request) (*http.Response, error) {
		recovery.commandsMu.Lock()
		var drainID string
		if request.Method == http.MethodDelete {
			id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/api/v1/runs/"), "/nodes")
			recovery.commands = append(recovery.commands, "fence "+id)
			if id == recovery.fenceFailure {
				recovery.commandsMu.Unlock()
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Header: http.Header{}, Body: io.NopCloser(strings.NewReader("generation fence unavailable"))}, nil
			}
		} else if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/drain") {
			id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/api/v1/runs/"), "/drain")
			drainID = id
			recovery.commands = append(recovery.commands, "drain "+id)
			f.mu.Lock()
			otherLive := len(f.nodes) > 0
			f.mu.Unlock()
			if otherLive || (recovery.networkOwner != "" && recovery.networkOwner != id) || id == recovery.drainFailure {
				recovery.commandsMu.Unlock()
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable", Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"code":"` + model.RunWorkerPendingCode + `","workerPending":true,"error":"Run experiment process has not exited: recovered Run retains Peer config or network"}`))}, nil
			}
		}
		recovery.commandsMu.Unlock()
		response, err := http.DefaultTransport.RoundTrip(request)
		if err == nil && drainID != "" && response.StatusCode == http.StatusNoContent {
			recovery.commandsMu.Lock()
			if recovery.networkOwner == drainID {
				recovery.networkOwner = ""
			}
			recovery.commandsMu.Unlock()
		}
		return response, err
	})
	return recovery
}

func (f *recoveredCleanupFixture) cleanup(t *testing.T) error {
	t.Helper()
	return f.server.cleanupBeforeResume(t.Context(), []string{firstRecoveredRun, secondRecoveredRun}, scenario.Scenario{JobShutdownTimeout: "5s"})
}

func (f *recoveredCleanupFixture) requireCommands(t *testing.T, want []string) {
	t.Helper()
	f.commandsMu.Lock()
	defer f.commandsMu.Unlock()
	if strings.Join(f.commands, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected recovery fence/drain order: %v; want %v", f.commands, want)
	}
}

func (f *recoveredCleanupFixture) requireRetired(t *testing.T, want bool) {
	t.Helper()
	for _, id := range []string{firstRecoveredRun, secondRecoveredRun} {
		_, err := os.Stat(filepath.Join(f.server.config.DataDir, retiredRuntimeDirectory, id+".done"))
		if (err == nil) != want || (err != nil && !errors.Is(err, os.ErrNotExist)) {
			t.Fatalf("Run %s retired=%v; want %v; evidence error=%v", id, err == nil, want, err)
		}
		if want {
			if err := f.server.verifyRetiredRuntime(id); err != nil {
				t.Fatalf("retired Run %s retained runtime state: %v", id, err)
			}
		}
	}
}

func TestRecoveredRunsAreAllFencedBeforeFirstAgentReset(t *testing.T) {
	f := newRecoveredCleanupFixture(t)
	if err := f.cleanup(t); err != nil {
		t.Fatalf("recovery reset ran before every recovered Peer was fenced: %v", err)
	}
	f.requireCommands(t, []string{"fence " + firstRecoveredRun, "fence " + secondRecoveredRun, "drain " + firstRecoveredRun, "drain " + secondRecoveredRun})
	f.requireRetired(t, true)
}

func TestRecoveredRunPendingResetStillDrainsLaterNetworkOwner(t *testing.T) {
	f := newRecoveredCleanupFixture(t)
	f.networkOwner = secondRecoveredRun
	if err := f.cleanup(t); err == nil || !hasPendingRunWorker(err) || !strings.Contains(err.Error(), firstRecoveredRun) {
		t.Fatalf("first recovered Run did not preserve its strict reset error: %v", err)
	}
	f.requireCommands(t, []string{"fence " + firstRecoveredRun, "fence " + secondRecoveredRun, "drain " + firstRecoveredRun, "drain " + secondRecoveredRun})
	f.requireRetired(t, false)
	f.commandsMu.Lock()
	owner := f.networkOwner
	f.commands = nil
	f.commandsMu.Unlock()
	if owner != "" {
		t.Fatalf("later recovered Run never released its own network: %s", owner)
	}
	f.server.state.mu.RLock()
	firstPending := f.server.state.experiments[firstRecoveredRun].TelemetryPending
	secondPending := f.server.state.experiments[secondRecoveredRun].TelemetryPending
	f.server.state.mu.RUnlock()
	if !firstPending || secondPending {
		t.Fatalf("strict drain ACK debt was not retained per Run: earlier=%v later=%v", firstPending, secondPending)
	}
	if err := f.cleanup(t); err != nil {
		t.Fatalf("retry stayed blocked after the later network owner drained: %v", err)
	}
	f.requireRetired(t, true)
}

func TestRecoveredRunCleanupCollectsFenceAndDrainErrors(t *testing.T) {
	f := newRecoveredCleanupFixture(t)
	f.fenceFailure = firstRecoveredRun
	f.drainFailure = secondRecoveredRun
	err := f.cleanup(t)
	if err == nil || !strings.Contains(err.Error(), firstRecoveredRun) || !strings.Contains(err.Error(), secondRecoveredRun) || !hasPendingRunWorker(err) {
		t.Fatalf("cleanup did not preserve errors from both recovered Runs: %v", err)
	}
	f.requireCommands(t, []string{"fence " + firstRecoveredRun, "fence " + secondRecoveredRun, "drain " + secondRecoveredRun})
	f.requireRetired(t, false)
}
