package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

func TestReapingOwnerWorkerHelper(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "reaping-owner-helper" {
		return
	}
	err := workerproc.Serve(context.Background(), "reaping-owner-fixture", func(_ context.Context, _ string, _ json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		return func(ctx context.Context, method string, _ json.RawMessage) (any, error) {
			if method == "alive" {
				return os.Getpid(), nil
			}
			if method == "callback" {
				return nil, peer.Call(ctx, "held-source", nil, nil)
			}
			return nil, errors.New("unknown owner fixture operation")
		}, nil
	})
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func startReapingOwnerProcess(t *testing.T, id string, broker workerproc.Handler) *workerproc.Process {
	t.Helper()
	process, err := workerproc.Start(t.Context(), "reaping-owner-fixture", struct{}{}, broker, workerproc.Options{RunID: id, GracePeriod: 100 * time.Millisecond, KillWait: time.Second, Command: func(string) *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestReapingOwnerWorkerHelper$", "--", "reaping-owner-helper")
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Close() })
	return process
}

func reapingDownloadCount(s *Server, id string) int {
	s.resultDownloadMu.Lock()
	defer s.resultDownloadMu.Unlock()
	return s.resultDownloads[id]
}

func TestWorkerReapingRetainsSnapshotUntilActiveCallbackQuiesces(t *testing.T) {
	for _, download := range []bool{true, false} {
		t.Run(map[bool]string{true: "download-deletion-lease", false: "read-only-delete-allowed"}[download], func(t *testing.T) {
			testWorkerReapingSnapshotCallback(t, download)
		})
	}
}

func testWorkerReapingSnapshotCallback(t *testing.T, download bool) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "reaping-active-callback"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute))
	snapshot, err := s.captureResultFilesContext(t.Context(), id, download)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.close)
	file := snapshot.files[0].file
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(gate.Close)
	readDone := make(chan error, 1)
	process := startReapingOwnerProcess(t, id, func(_ context.Context, method string, _ json.RawMessage) (any, error) {
		if method != "held-source" {
			return nil, errors.New("unexpected source operation")
		}
		// An accepted parent callback owns a real HTTP operation independent
		// of RPC cancellation, then reads the captured source FD before exit.
		response, err := gate.Client().Get(gate.URL)
		if err == nil {
			_ = response.Body.Close()
			var probe [1]byte
			_, err = file.ReadAt(probe[:], 0)
		}
		readDone <- err
		return nil, err
	})
	callDone := make(chan error, 1)
	go func() { callDone <- process.Call(t.Context(), "callback", nil, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual child callback did not enter the HTTP source boundary")
	}
	closeCtx, closeCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	closeErr := process.CloseContext(closeCtx)
	closeCancel()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("fixture did not retain an active private callback: %v", closeErr)
	}
	select {
	case <-process.Done():
		t.Fatal("Process.Done closed while the actual callback still owned its source")
	default:
	}
	s.rememberUnreapedWorkerSnapshot(id, process, snapshot)
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	waitErr := s.waitControllerWorkers(waitCtx, []string{id})
	waitCancel()
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Fatalf("snapshot waiter did not preserve cancellation: %v", waitErr)
	}
	snapshot.close() // The original caller still runs its deferred close after handing off.
	wantDownloads := 0
	if download {
		wantDownloads = 1
	}
	if _, err := file.Stat(); err != nil || reapingDownloadCount(s, id) != wantDownloads {
		t.Fatalf("canceled waiter released an active callback's FD/lease: stat=%v leases=%d", err, reapingDownloadCount(s, id))
	}
	deleteResponse := resultRequest(s, http.MethodDelete, "/api/v1/results/"+id)
	wantDelete := http.StatusNoContent
	if download {
		wantDelete = http.StatusConflict
	}
	if deleteResponse.Code != wantDelete {
		t.Fatalf("active callback changed its deletion contract (download=%t): status=%d body=%s", download, deleteResponse.Code, deleteResponse.Body)
	}
	s.workerMu.Lock()
	owned := slices.Contains(s.unreapedWorkers[id], process) && s.workerSnapshots[process] != nil
	s.workerMu.Unlock()
	if !owned {
		t.Fatal("canceled wait discarded the active callback's snapshot owner")
	}
	unblock()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("callback lost its still-owned source FD: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("released callback did not finish its actual source read")
	}
	select {
	case <-process.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("OS/private RPC quiescence did not complete")
	}
	<-callDone
	if _, err := file.Stat(); err != nil || reapingDownloadCount(s, id) != wantDownloads {
		t.Fatalf("Done alone released a snapshot before the reap owner closed it: stat=%v lease=%d", err, reapingDownloadCount(s, id))
	}
	if download {
		if err := s.deleteSavedResult(id); !errors.Is(err, errResultBusy) {
			t.Fatalf("unclosed snapshot lost deletion protection after Done: %v", err)
		}
	}
	if err := s.waitControllerWorkers(t.Context(), []string{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) || reapingDownloadCount(s, id) != 0 {
		t.Fatalf("confirmed reap did not actually close FD/release lease: stat=%v lease=%d", err, reapingDownloadCount(s, id))
	}
	if download {
		if err := s.deleteSavedResult(id); err != nil {
			t.Fatalf("confirmed snapshot close did not restore deletion: %v", err)
		}
	} else {
		if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reaping a read-only callback restored its deleted result: %v", err)
		}
	}
}

func TestWorkerReapingCloseOwnerPreservesCanceledWaiterAndLateSnapshot(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	const id = "reaping-close-owner"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute))
	first, err := s.captureResultFilesContext(t.Context(), id, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.close)
	firstFD := first.files[0].file
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(gate.Close)
	previousRelease := first.release
	first.release = func() {
		// Delay an actual close action after the file descriptors close but
		// before the deletion counter releases. Another waiter may cancel.
		response, err := gate.Client().Get(gate.URL)
		if err != nil {
			t.Error(err)
		} else {
			_ = response.Body.Close()
		}
		previousRelease()
	}
	process := startReapingOwnerProcess(t, id, nil)
	s.rememberUnreapedWorkerSnapshot(id, process, first)
	if err := process.CloseContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- s.waitControllerWorkers(t.Context(), []string{id}) }()
	ownerJoined := false
	t.Cleanup(func() {
		unblock()
		if !ownerJoined {
			select {
			case <-ownerDone:
			case <-time.After(2 * time.Second):
				t.Error("reap close owner did not leave during test cleanup")
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.waitControllerWorkers(ctx, []string{id})
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reap owner did not enter the delayed actual snapshot close")
	}
	if _, err := firstFD.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("deletion counter release began before actual FD close: %v", err)
	}
	late, err := s.captureResultFilesContext(t.Context(), id, true)
	if err != nil {
		t.Fatal(err)
	}
	lateFD := late.files[0].file
	s.rememberUnreapedWorkerSnapshot(id, process, late)
	waitCtx, waitCancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	err = s.waitControllerWorkers(waitCtx, []string{id})
	waitCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("competing close waiter did not cancel: %v", err)
	}
	if _, err := lateFD.Stat(); err != nil || reapingDownloadCount(s, id) != 2 {
		t.Fatalf("competing waiter stole pending close ownership: stat=%v leases=%d", err, reapingDownloadCount(s, id))
	}
	if err := s.deleteSavedResult(id); !errors.Is(err, errResultBusy) {
		t.Fatalf("pending close/late snapshot failed to guard deletion: %v", err)
	}
	s.workerMu.Lock()
	owned := slices.Contains(s.unreapedWorkers[id], process) && s.workerSnapshots[process] != nil
	s.workerMu.Unlock()
	if !owned {
		t.Fatal("canceled competing waiter dropped the closing or late snapshot owner")
	}
	unblock()
	select {
	case err := <-ownerDone:
		ownerJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close owner did not consume its newly published snapshot")
	}
	if _, err := lateFD.Stat(); !errors.Is(err, os.ErrClosed) || reapingDownloadCount(s, id) != 0 {
		t.Fatalf("reap completion did not close every snapshot once: stat=%v leases=%d", err, reapingDownloadCount(s, id))
	}
	s.workerMu.Lock()
	remaining, holds := len(s.unreapedWorkers[id]), len(s.workerSnapshots)
	s.workerMu.Unlock()
	if remaining != 0 || holds != 0 {
		t.Fatalf("confirmed close retained worker ownership: workers=%d holds=%d", remaining, holds)
	}
	if err := s.deleteSavedResult(id); err != nil {
		t.Fatalf("confirmed multi-snapshot close did not restore deletion: %v", err)
	}
}

// This deliberately creates two real OS workers for one logical owner to
// exercise the helper's synchronization contract. It does not assert that an
// incident followed this interleaving or bypassed other production guards.
func TestWorkerReapingPreservesLaterSameRunOwner(t *testing.T) {
	// A wait-only legacy fixture excludes additional Agent/metric-summary
	// proofs, isolating the OS ownership guard shared by cleanup callers.
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: false}, nil)
	const id = "same-run-owner-reap"
	run, _ := resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute))
	run.CleanupState, run.DataState = "retained", "complete"
	s.state.experiments[id] = run
	a, b := startReapingOwnerProcess(t, id, nil), startReapingOwnerProcess(t, id, nil)
	s.rememberUnreapedWorker(id, a)
	firstCtx, firstCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer firstCancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- s.waitControllerWorkers(firstCtx, []string{id}) }()
	// Check the actual waiter's stack while A.Done is still open, before
	// allowing rememberUnreapedWorker to append B to the same logical Run.
	deadline := time.Now().Add(2 * time.Second)
	for {
		stack := make([]byte, 2<<20)
		n := runtime.Stack(stack, true)
		waiting := false
		for _, frame := range bytes.Split(stack[:n], []byte("\n\n")) {
			if bytes.Contains(frame, []byte("(*Server).waitControllerWorkers")) && bytes.Contains(frame, []byte("[select]")) {
				waiting = true
				break
			}
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker owner waiter did not reach its actual Done select")
		}
		time.Sleep(time.Millisecond)
	}
	s.rememberUnreapedWorker(id, b)
	s.workerMu.Lock()
	before := append([]*workerproc.Process(nil), s.unreapedWorkers[id]...)
	s.workerMu.Unlock()
	if len(before) != 2 || before[0] != a || before[1] != b {
		t.Fatal("fixture did not append the second same-Run owner after wait admission")
	}
	closeCtx, closeCancel := context.WithTimeout(t.Context(), 2*time.Second)
	if err := a.CloseContext(closeCtx); err != nil {
		closeCancel()
		t.Fatal(err)
	}
	closeCancel()
	firstReturned := false
	var firstErr error
	select {
	case firstErr = <-firstDone:
		firstReturned = true
	case <-time.After(40 * time.Millisecond):
	}
	s.workerMu.Lock()
	after := append([]*workerproc.Process(nil), s.unreapedWorkers[id]...)
	s.workerMu.Unlock()
	ownedB := slices.Contains(after, b)
	alive := func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var pid int
		if err := b.Call(ctx, "alive", nil, &pid); err != nil {
			t.Errorf("actual B worker could not answer: %v", err)
			return false
		}
		select {
		case <-b.Done():
			return false
		default:
			return pid == b.PID()
		}
	}
	if !alive() {
		t.Fatal("B was not an actual surviving OS worker")
	}
	secondCtx, secondCancel := context.WithTimeout(t.Context(), 35*time.Millisecond)
	secondErr := s.waitControllerWorkers(secondCtx, []string{id})
	secondCancel()
	queueCtx, queueCancel := context.WithTimeout(t.Context(), 65*time.Millisecond)
	queueErr := s.waitQueueCleanup(queueCtx, "", scenario.Scenario{JobShutdownTimeout: "1s"})
	queueCancel()
	s.cancelMu.Lock()
	queued := slices.Contains(s.queueCleanup, id)
	s.cancelMu.Unlock()
	_, markerErr := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id))
	bStillAlive := alive()
	t.Logf("A_reaped=true B_pid=%d B_alive=%t first_wait_returned=%t first_error=%v tracked_after_A=%d B_owned=%t second_wait_error=%v queue_wait_error=%v queue_marker=%t durable_marker=%t", b.PID(), bStillAlive, firstReturned, firstErr, len(after), ownedB, secondErr, queueErr, queued, markerErr == nil)
	if firstReturned && firstErr == nil {
		t.Error("copied worker list was reported fully reaped while a later same-Run OS worker was alive")
	}
	if !ownedB {
		t.Error("waiting for A deleted the subsequently registered B ownership")
	}
	if !errors.Is(secondErr, context.DeadlineExceeded) {
		t.Errorf("next worker wait did not preserve B's pending ownership: %v", secondErr)
	}
	if queueErr == nil || !queued || markerErr != nil {
		t.Errorf("next queue barrier forgot a real live B worker: queue_error=%v queued=%t marker_error=%v", queueErr, queued, markerErr)
	}
	closeCtx, closeCancel = context.WithTimeout(t.Context(), 2*time.Second)
	if err := b.CloseContext(closeCtx); err != nil {
		closeCancel()
		t.Fatal(err)
	}
	closeCancel()
	if !firstReturned {
		select {
		case firstErr = <-firstDone:
		case <-time.After(time.Second):
			t.Fatal("first wait did not finish after both actual children were reaped")
		}
	}
	if err := s.waitQueueCleanup(t.Context(), "", scenario.Scenario{JobShutdownTimeout: "1s"}); err != nil {
		t.Fatalf("physical reaping did not restore normal cleanup: %v", err)
	}
}
