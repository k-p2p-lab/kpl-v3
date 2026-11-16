package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func terminalResetTestServer(t *testing.T) *Server {
	t.Helper()
	s := historyTestServer(t)
	s.config.RunWorkerFactory = func(context.Context, RunWorkerConfig, workerproc.Handler) (*workerproc.Process, error) {
		return nil, errors.New("terminal-only reset must not start a worker")
	}
	return s
}

func terminalResetTestProcess(t *testing.T, id, run string) *process {
	t.Helper()
	proc := historyTerminal(id, run, time.Now().UTC())
	proc.configPath = filepath.Join(t.TempDir(), "owned-peer.json")
	if err := os.WriteFile(proc.configPath, []byte("{}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return proc
}

func TestRunResetAndRepeatedDrainDoNotWaitForRetiredReplay(t *testing.T) {
	s := terminalResetTestServer(t)
	const run = "reset-current"
	current := terminalResetTestProcess(t, "current-terminal", run)
	s.processes = map[string]*process{current.node.ID: current}
	cold := historyTerminal("cold-terminal", "older-run", time.Now().Add(-time.Hour).UTC())
	if err := s.history.saveNodes([]model.Node{cold.node}); err != nil {
		t.Fatal(err)
	}
	s.resetHistoryReplay()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, enterOnce sync.Once
	var emptyReports, coldReports, otherReports atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var h model.AgentHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if len(h.Nodes) == 0 {
			emptyReports.Add(1)
		}
		for _, node := range h.Nodes {
			if node.ID == cold.node.ID {
				coldReports.Add(1)
				enterOnce.Do(func() { close(entered) })
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}
			if node.ID == "other-terminal" {
				otherReports.Add(1)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.client, s.config.ControllerURL = controller.Client(), controller.URL
	periodicCtx, cancelPeriodic := context.WithTimeout(t.Context(), 5*time.Second)
	var periodicErr error
	go func() { defer close(done); periodicErr = s.heartbeat(periodicCtx) }()
	defer func() { releaseOnce.Do(func() { close(release) }); cancelPeriodic(); <-done; controller.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("periodic heartbeat did not reach cold replay")
	}
	// A recovered terminal Run arriving after the background snapshot must also
	// be acknowledged, since reclaim and reset verify global retained configs.
	other := terminalResetTestProcess(t, "other-terminal", "other-recovered-run")
	otherPath := other.configPath
	s.mu.Lock()
	s.processes[other.node.ID] = other
	s.mu.Unlock()
	for attempt := range 3 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		err := s.finishRunWorker(ctx, run)
		cancel()
		if err != nil {
			t.Fatalf("reset attempt%d waited for unrelated cold history: %v", attempt, err)
		}
		proof, err := s.proveRunReset(run)
		if err != nil || !proof.WorkerReaped || proof.TemporaryConfigs != 0 {
			t.Fatalf("attempt%d reset proof=%+v error=%v", attempt, proof, err)
		}
	}
	s.mu.RLock()
	currentACK, otherACK := current.heartbeatAcknowledged, other.heartbeatAcknowledged
	s.mu.RUnlock()
	if !currentACK || !otherACK || otherReports.Load() != 1 || coldReports.Load() != 1 || emptyReports.Load() < 3 {
		t.Fatalf("ACKs current=%v other=%v otherReports=%d coldReports=%d emptyReports=%d", currentACK, otherACK, otherReports.Load(), coldReports.Load(), emptyReports.Load())
	}
	if _, err := os.Stat(otherPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("other recovered Run config was not reclaimed: %v", err)
	}
	// Another periodic report can complete while the single old page sender is
	// still blocked, without starting a second backfill send.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err := s.heartbeat(ctx)
	cancel()
	if err != nil || coldReports.Load() != 1 {
		t.Fatalf("concurrent periodic report queued behind cold replay: error=%v sends=%d", err, coldReports.Load())
	}
	releaseOnce.Do(func() { close(release) })
	<-done
	if periodicErr != nil {
		t.Fatal(periodicErr)
	}
}

func TestRunResetStillRequiresCurrentTerminalAcknowledgment(t *testing.T) {
	s := terminalResetTestServer(t)
	const run = "reset-current-ack"
	proc := terminalResetTestProcess(t, "current-terminal", run)
	path := proc.configPath
	s.processes = map[string]*process{proc.node.ID: proc}
	s.networkRequest = model.ExperimentNetworkRequest{RunID: run, Epoch: "retained-network"}
	entered := make(chan struct{})
	var calls atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var h model.AgentHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if len(h.Nodes) != 1 || h.Nodes[0].ID != proc.node.ID {
			t.Error("terminal evidence omitted from reset ACK")
			w.WriteHeader(400)
			return
		}
		if calls.Add(1) == 1 {
			close(entered)
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()
	s.client, s.config.ControllerURL = controller.Client(), controller.URL
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- s.finishRunWorker(ctx, run) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		<-finished
		t.Fatal("reset did not attempt terminal inventory POST")
	}
	cancel()
	err := <-finished
	var pending *runWorkerPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("unacknowledged terminal reset succeeded: %v", err)
	}
	s.mu.RLock()
	ack, bound := proc.heartbeatAcknowledged, s.networkRequest.RunID == run
	s.mu.RUnlock()
	if ack || !bound {
		t.Fatalf("missing ACK released ownership: acknowledged=%v bound=%v", ack, bound)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("unacknowledged Peer config removed: %v", err)
	}
	if err := s.finishRunWorker(t.Context(), run); err != nil {
		t.Fatalf("fresh terminal ACK did not recover reset: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged config not reclaimed: %v", err)
	}
}

func TestRetiredReplayRegistrationFenceCapacityAndFailedPageRetry(t *testing.T) {
	s := terminalResetTestServer(t)
	current := historyTerminal("current-terminal", "current-run", time.Now().UTC())
	s.processes = map[string]*process{current.node.ID: current}
	nodes := make([]model.Node, 400)
	for index := range nodes {
		nodes[index] = historyTerminal(fmt.Sprintf("cold-%04d", index), "older-run", time.Unix(int64(index+1), 0)).node
	}
	if err := s.history.saveNodes(nodes); err != nil {
		t.Fatal(err)
	}
	s.resetHistoryReplay()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var coldCalls atomic.Int32
	var receivedMu sync.Mutex
	received := map[int32][]byte{}
	freshIDs := map[string]bool{}
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/agents/register" {
			io.Copy(io.Discard, r.Body)
			w.Header().Set("X-KPL-Agent-Capacity", "19")
			w.Header().Set("X-KPL-Agent-Capacity-Revision", "registered")
			w.WriteHeader(204)
			return
		}
		var h model.AgentHeartbeat
		if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		w.Header().Set("X-KPL-Agent-Capacity", "23")
		w.Header().Set("X-KPL-Agent-Capacity-Revision", "current")
		if len(h.Nodes) > 0 && h.Nodes[0].RunID == "older-run" {
			call := coldCalls.Add(1)
			encoded, _ := json.Marshal(h.Nodes)
			receivedMu.Lock()
			received[call] = encoded
			if call >= 3 {
				for _, node := range h.Nodes {
					freshIDs[node.ID] = true
				}
			}
			receivedMu.Unlock()
			if call == 1 {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
				}
				w.Header().Set("X-KPL-Agent-Capacity", "3")
				w.Header().Set("X-KPL-Agent-Capacity-Revision", "stale-replay")
			}
			if call == 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	s.client, s.config.ControllerURL = controller.Client(), controller.URL
	periodicCtx, cancelPeriodic := context.WithTimeout(t.Context(), 5*time.Second)
	var periodicErr error
	go func() { defer close(done); periodicErr = s.heartbeat(periodicCtx) }()
	defer func() { releaseOnce.Do(func() { close(release) }); cancelPeriodic(); <-done; controller.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first cold page did not reach Controller")
	}
	s.heartbeatMu.Lock()
	old := s.historyReplay
	s.heartbeatMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	err := s.register(ctx)
	cancel()
	if err != nil {
		t.Fatalf("registration waited for old backfill: %v", err)
	}
	s.heartbeatMu.Lock()
	fresh := s.historyReplay
	s.heartbeatMu.Unlock()
	if fresh == nil || fresh == old {
		t.Fatal("registration did not replace replay generation")
	}
	if err := s.prepareRunReset(t.Context(), "current-run"); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	<-done
	if periodicErr != nil {
		t.Fatal(periodicErr)
	}
	s.heartbeatMu.Lock()
	same := s.historyReplay == fresh
	pending, last := len(fresh.pending), len(fresh.last)
	s.heartbeatMu.Unlock()
	s.mu.RLock()
	capacity, revision := s.capacityLimit, s.capacityRevision
	s.mu.RUnlock()
	if !same || pending != 0 || last != 0 || capacity != 23 || revision != "current" {
		t.Fatalf("old reply changed new generation/capacity: same=%v pending=%d last=%d capacity=%d revision=%s", same, pending, last, capacity, revision)
	}
	if err := s.heartbeat(t.Context()); err == nil {
		t.Fatal("failed page send was reported successful")
	}
	s.heartbeatMu.Lock()
	pending = len(s.historyReplay.pending)
	lastKey := bytes.Clone(s.historyReplay.last)
	s.heartbeatMu.Unlock()
	if pending != historyPruneBatch {
		t.Fatalf("failed page not retained: %d", pending)
	}
	if err := s.heartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	receivedMu.Lock()
	retried := bytes.Equal(received[2], received[3])
	receivedMu.Unlock()
	s.heartbeatMu.Lock()
	sameKey := bytes.Equal(lastKey, s.historyReplay.last)
	s.heartbeatMu.Unlock()
	if !retried || !sameKey {
		t.Fatal("retry skipped or replaced the failed pending page")
	}
	for attempt := 0; attempt < 5; attempt++ {
		s.heartbeatMu.Lock()
		complete := s.historyReplay == nil
		s.heartbeatMu.Unlock()
		if complete {
			break
		}
		if err := s.heartbeat(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	s.heartbeatMu.Lock()
	complete := s.historyReplay == nil
	s.heartbeatMu.Unlock()
	receivedMu.Lock()
	count := len(freshIDs)
	receivedMu.Unlock()
	if !complete || count != len(nodes) {
		t.Fatalf("registration replay lost identities: complete=%v unique=%d/%d", complete, count, len(nodes))
	}
}
