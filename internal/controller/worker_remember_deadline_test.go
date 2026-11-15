package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type rememberDeadlineStart struct {
	Owner          model.Agent
	Request        model.CreateNodeRequest
	DeadlineMillis int
}

type rememberDeadlineResult struct {
	Expired bool
	Error   string
}

func TestRememberDeadlineWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "remember-deadline-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "controller-remember-deadline", func(factoryCtx context.Context, _ string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var start rememberDeadlineStart
		if err := json.Unmarshal(raw, &start); err != nil {
			return nil, err
		}
		child := New(ServerConfig{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		client := &runWorkerClient{peer: peer, server: child, runID: peer.RunID(), ctx: factoryCtx}
		child.worker, child.state.worker = client, client
		child.state.agents[start.Owner.ID] = start.Owner
		child.state.experiments[start.Request.RunID] = model.Experiment{ID: start.Request.RunID, State: "running"}
		return func(callCtx context.Context, _ string, _ json.RawMessage) (any, error) {
			client.ctx = callCtx
			createCtx := callCtx
			if start.DeadlineMillis > 0 {
				var cancel context.CancelFunc
				createCtx, cancel = context.WithTimeout(callCtx, time.Duration(start.DeadlineMillis)*time.Millisecond)
				defer cancel()
			}
			agent, err := child.acquireAgentWithPlacement(createCtx, start.Request.ID, start.Owner.ID, nil)
			if err == nil {
				err = child.createReservedNode(createCtx, start.Request, agent, "", nil)
			}
			result := rememberDeadlineResult{Expired: errors.Is(err, context.DeadlineExceeded)}
			if err != nil {
				result.Error = err.Error()
			}
			return result, nil
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// Exercise the real child reserve -> ownership -> create path. The operation
// deadline is shorter than the execute request, so using the worker lifetime
// instead of the create context cannot accidentally satisfy the first case.
func TestRememberCanceledBeforePersistenceReleasesPrivateCallback(t *testing.T) {
	for _, mode := range []string{"create-deadline", "execution-cancel"} {
		t.Run(mode, func(t *testing.T) { exerciseRememberDeadline(t, mode) })
	}
}

func TestRememberDurableOwnershipTransactionRetainsReclamationBarrier(t *testing.T) {
	for _, mode := range []string{"durable-success", "durable-failure"} {
		t.Run(mode, func(t *testing.T) { exerciseRememberDeadline(t, mode) })
	}
}

func TestRememberSuccessfulWorkerCreatePersistsOwnershipBeforeHTTP(t *testing.T) {
	exerciseRememberDeadline(t, "success")
}

func exerciseRememberDeadline(t *testing.T, mode string) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	const id = "remember-deadline"
	run, _ := resultFixture(t, s, id, "running", time.Now().UTC())
	s.state.experiments[id] = run
	before := run
	var httpCreates atomic.Int64
	var beforeHTTP atomic.Bool
	var owner model.Agent
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		httpCreates.Add(1)
		stored, err := s.readRunCleanupOwnership(id)
		s.state.mu.RLock()
		live := s.state.experiments[id]
		s.state.mu.RUnlock()
		if err == nil && len(stored) == 1 && stored[0].ID == owner.ID && len(live.Agents) == 1 && live.Agents[0].ID == owner.ID {
			beforeHTTP.Store(true)
		}
		var req model.CreateNodeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, 201, model.Node{ID: req.ID, RunID: req.RunID, Generation: req.Generation, AgentID: owner.ID, Group: req.Group, State: model.NodeReady})
	}))
	defer endpoint.Close()
	owner = model.Agent{ID: "agent", URL: endpoint.URL, StartedAt: time.Now().UTC().Add(-time.Minute), LastSeen: time.Now().UTC(), State: model.AgentOnline, Capacity: 2, RunProcesses: true, RunDrain: true, StartupReconciled: true}
	if _, err := s.state.registerAgent(owner); err != nil {
		t.Fatal(err)
	}
	req := model.CreateNodeRequest{ID: id + "-00001", RunID: id, Generation: 1, Group: "peers", Type: "full", AgentStartedAt: owner.StartedAt}
	start := rememberDeadlineStart{Owner: owner, Request: req}
	if mode == "create-deadline" {
		start.DeadlineMillis = 100
	}
	durable := mode == "durable-success" || mode == "durable-failure"
	entered, canceled, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	metadataFailure := errors.New("injected ownership metadata write failure")
	callbackError := make(chan error, 1)
	broker := &runWorkerBroker{server: s, runID: id, executionID: run.ExecutionID, generation: 1}
	process, err := workerproc.Start(t.Context(), "controller-remember-deadline", start, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method != "remember" {
			return broker.handle(ctx, method, raw)
		}
		go func() { <-ctx.Done(); close(canceled) }()
		defer close(callbackDone)
		if durable {
			// The cleanup ownership file is already durable before this
			// metadata-persistence seam is entered. Cancellation must not
			// abandon the transaction or discard its actual storage error.
			err := s.rememberRunAgentWithPersistenceContext(ctx, id, owner, func(saved model.Experiment) error {
				close(entered)
				<-release
				if mode == "durable-failure" {
					return metadataFailure
				}
				return s.persistExperiment(saved)
			})
			callbackError <- err
			return nil, err
		}
		close(entered)
		value, err := broker.handle(ctx, method, raw)
		callbackError <- err
		return value, err
	}, workerproc.Options{RunID: id, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestRememberDeadlineWorkerHelper$", "--", "remember-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	held := mode == "create-deadline" || mode == "execution-cancel"
	var unlock sync.Once
	if held {
		s.state.persistMu.Lock()
	}
	unlockPersistence := func() {
		if held {
			unlock.Do(s.state.persistMu.Unlock)
		}
	}
	defer func() { unlockPersistence(); unblock(); _ = process.Close(); _ = broker.close() }()
	executionCtx, cancelExecution := context.WithCancel(t.Context())
	defer cancelExecution()
	callDone := make(chan error, 1)
	var result rememberDeadlineResult
	go func() { callDone <- process.Call(executionCtx, "execute", nil, &result) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("child create never entered actual parent ownership path")
	}
	if mode == "success" {
		if err := <-callDone; err != nil || result.Error != "" {
			t.Fatalf("successful ownership/create failed: %v %+v", err, result)
		}
		if err := process.Close(); err != nil {
			t.Fatal(err)
		}
		if httpCreates.Load() != 1 || !beforeHTTP.Load() {
			t.Fatalf("ownership was not durable before HTTP: creates=%d durable=%v", httpCreates.Load(), beforeHTTP.Load())
		}
		s.state.mu.RLock()
		node := s.state.nodes[req.ID]
		reservation := s.state.reservations[req.ID]
		occupied := s.state.agents[owner.ID].ActiveNodes
		s.state.mu.RUnlock()
		broker.mu.Lock()
		leases := len(broker.createLeases)
		broker.mu.Unlock()
		if node.State != model.NodeReady || reservation != owner.ID || occupied != 1 || leases != 0 {
			t.Fatalf("successful create lost acknowledgement or physical occupancy: %+v reserved=%q occupied=%d pending=%d", node, reservation, occupied, leases)
		}
		return
	}
	if mode == "create-deadline" {
		select {
		case err := <-callDone:
			if err != nil || !result.Expired {
				t.Fatalf("create deadline lost: call=%v result=%+v", err, result)
			}
		case <-time.After(time.Second):
			cancelExecution()
			<-callDone
			t.Fatal("create deadline did not cancel ownership before worker lifetime")
		}
	} else {
		cancelExecution()
		if err := <-callDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("execute cancellation=%v", err)
		}
	}
	closeCtx, cancelClose := context.WithTimeout(t.Context(), 300*time.Millisecond)
	closeErr := process.CloseContext(closeCtx)
	cancelClose()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("parent callback did not receive cancellation")
	}
	done := false
	select {
	case <-process.Done():
		done = true
	default:
	}
	callbackFinished := false
	select {
	case <-callbackDone:
		callbackFinished = true
	default:
	}
	if durable {
		if closeErr == nil || done || callbackFinished {
			t.Fatal("active durable ownership transaction was reclaimed before completion")
		}
		owners, err := s.readRunCleanupOwnership(id)
		if err != nil || len(owners) != 1 || owners[0].ID != owner.ID {
			t.Fatalf("started transaction lost durable cleanup ownership: %+v %v", owners, err)
		}
	} else {
		if closeErr != nil || !done || !callbackFinished {
			t.Fatalf("canceled pure ownership wait retained private IPC completion: close=%v Done=%v callback=%v", closeErr, done, callbackFinished)
		}
		s.state.mu.RLock()
		after := s.state.experiments[id]
		s.state.mu.RUnlock()
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("precommit cancellation mutated scientific state: before=%+v after=%+v", before, after)
		}
		if _, err := os.Stat(filepath.Join(s.config.DataDir, ownershipDirectory, id+".json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canceled wait published cleanup ownership: %v", err)
		}
	}
	if httpCreates.Load() != 0 {
		t.Fatalf("canceled ownership admitted HTTP: %d", httpCreates.Load())
	}
	unlockPersistence()
	unblock()
	select {
	case <-process.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("ownership callback did not quiesce after fixture release")
	}
	actualErr := <-callbackError
	if mode == "durable-failure" && !errors.Is(actualErr, metadataFailure) {
		t.Fatalf("actual durable failure was masked by cancellation: %v", actualErr)
	}
	if mode == "durable-success" && actualErr != nil {
		t.Fatalf("started durable ownership transaction failed after cancellation: %v", actualErr)
	}
	s.state.mu.RLock()
	after := s.state.experiments[id]
	s.state.mu.RUnlock()
	expectedOwners := 0
	if mode == "durable-success" {
		expectedOwners = 1
	}
	if len(after.Agents) != expectedOwners {
		t.Fatalf("late ownership mutation: mode=%s owners=%+v", mode, after.Agents)
	}
	if mode == "durable-success" {
		saved := persistedExperiment(t, s, id)
		if len(saved.Agents) != 1 || saved.Agents[0].ID != owner.ID {
			t.Fatalf("canceled started transaction lost metadata: %+v", saved.Agents)
		}
	}
	broker.revoke()
	s.state.mu.RLock()
	reservations, unknown, occupied := len(s.state.reservations), len(s.state.uncertainCreates), s.state.agents[owner.ID].ActiveNodes
	s.state.mu.RUnlock()
	if reservations != 0 || unknown != 0 || occupied != 0 {
		t.Fatalf("no HTTP path retained admission debt: reservations=%d unknown=%d occupied=%d", reservations, unknown, occupied)
	}
}
