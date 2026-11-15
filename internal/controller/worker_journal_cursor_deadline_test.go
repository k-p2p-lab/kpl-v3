package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestJournalCursorDeadlineWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "journal-cursor-deadline-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "controller-journal-cursor-deadline", func(factoryCtx context.Context, _ string, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		var directory string
		if err := json.Unmarshal(bootstrap, &directory); err != nil {
			return nil, err
		}
		server := New(ServerConfig{DataDir: directory}, nil)
		client := &runWorkerClient{peer: peer, runID: peer.RunID(), ctx: factoryCtx, server: server}
		return func(callCtx context.Context, _ string, _ json.RawMessage) (any, error) {
			ctx, cancel := context.WithTimeout(callCtx, 100*time.Millisecond)
			defer cancel()
			var cursor workerJournalCursor
			err := client.call(ctx, "journal", nil, &cursor)
			return errors.Is(err, context.DeadlineExceeded), nil
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

// The child call itself expires even when the parent's admitted cursor callback
// ignores its deadline. Assert Process.Done while persistence is still held:
// an unresolved callback must not survive finalization and delay the next Run.
func TestFinalJournalCursorDeadlineReleasesPrivateCallbackBeforePersistence(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "final-journal-cursor-deadline"
	run, _ := resultFixture(t, s, id, "completed", time.Now().UTC().Add(-time.Minute))
	s.state.experiments[id] = run
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent(id, 1)}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	broker := &runWorkerBroker{server: s, runID: id, executionID: run.ExecutionID, generation: 1}
	entered := make(chan struct{})
	callbackDone := make(chan struct{})
	process, err := workerproc.Start(t.Context(), "controller-journal-cursor-deadline", s.config.DataDir, func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == "journal" {
			close(entered)
			defer close(callbackDone)
		}
		return broker.handle(ctx, method, raw)
	}, workerproc.Options{RunID: id, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestJournalCursorDeadlineWorkerHelper$", "--", "journal-cursor-deadline-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	s.state.persistMu.Lock()
	var unlock sync.Once
	defer func() { unlock.Do(s.state.persistMu.Unlock); _ = process.Close(); _ = broker.close() }()
	var expired bool
	if err := process.Call(t.Context(), "exercise", nil, &expired); err != nil {
		t.Fatal(err)
	}
	if !expired {
		t.Fatal("actual child journal call did not preserve its deadline")
	}
	select {
	case <-entered:
	default:
		t.Fatal("child never entered the actual parent journal broker")
	}
	closeCtx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	closeErr := process.CloseContext(closeCtx)
	cancel()
	blocked := closeErr != nil
	select {
	case <-process.Done():
	default:
		blocked = true
	}
	select {
	case <-callbackDone:
	default:
		blocked = true
	}
	// Releasing the fixture owner is cleanup after recording the failure, not the
	// evidence of success: a fixed parent callback and Process.Done already ended.
	unlock.Do(s.state.persistMu.Unlock)
	select {
	case <-process.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("private RPC failed to quiesce after fixture release")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("deadline changed journal evidence: %v", err)
	}
	s.state.mu.RLock()
	afterRun := s.state.experiments[id]
	s.state.mu.RUnlock()
	if !reflect.DeepEqual(run, afterRun) {
		t.Fatalf("canceled cursor changed scientific result: before=%+v after=%+v", run, afterRun)
	}
	if blocked {
		t.Fatalf("expired journal callback retained private IPC/OS completion behind unrelated persistence ownership: %v", closeErr)
	}
}
