//go:build linux

package workerproc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func helperCommand(kind string) *exec.Cmd {
	executable, _ := os.Executable()
	cmd := exec.Command(executable, "-test.run=^TestWorkerProcessHelper$", "--", "workerproc-helper", kind)
	cmd.Env = append(privateEnvironment(nil), "GORACE=atexit_sleep_ms=0")
	return cmd
}

func helperOptions() Options {
	return Options{Command: helperCommand, Stdout: io.Discard, Stderr: io.Discard, HandshakeTimeout: 3 * time.Second, GracePeriod: 100 * time.Millisecond, KillWait: 2 * time.Second}
}

func TestWorkerProcessHelper(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "workerproc-helper" {
		return
	}
	kind := os.Args[len(os.Args)-1]
	if kind == "idle-handshake" {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := Serve(ctx, kind, func(context.Context, string, json.RawMessage, *Peer) (Handler, error) {
			return nil, errors.New("idle handshake unexpectedly reached factory")
		})
		if errors.Is(err, context.DeadlineExceeded) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(7)
	}
	if kind == "ignore-close" {
		signal.Ignore(syscall.SIGTERM)
	}
	if kind == "parent-death" {
		path := os.Args[len(os.Args)-3]
		p, err := Start(context.Background(), "death-child", map[string]any{"runId": "parent-death", "proof": path}, nil, helperOptions())
		if err != nil {
			os.Exit(5)
		}
		if os.WriteFile(path, []byte(strconv.Itoa(p.PID())), 0600) != nil {
			os.Exit(6)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	var proof string
	err := Serve(context.Background(), kind, func(factoryCtx context.Context, _ string, bootstrap json.RawMessage, peer *Peer) (Handler, error) {
		var config struct {
			Proof string `json:"proof"`
		}
		json.Unmarshal(bootstrap, &config)
		proof = config.Proof
		if kind == "reject-bootstrap" {
			return nil, errors.New("fixture bootstrap rejected")
		}
		if kind == "bootstrap-callback" {
			if err := peer.Call(factoryCtx, "bootstrap-block", nil, nil); err != nil {
				return nil, err
			}
		}
		return func(ctx context.Context, method string, body json.RawMessage) (any, error) {
			switch method {
			case "echo":
				return body, nil
			case "callback":
				var result int
				err := peer.Call(ctx, "increment", 3, &result)
				return result, err
			case "environment":
				var keys []string
				for _, item := range os.Environ() {
					key, _, _ := strings.Cut(item, "=")
					keys = append(keys, key)
				}
				return keys, nil
			case "private-fd-flags":
				flags := make([]bool, 0, 4)
				for _, fd := range []uintptr{3, 4} {
					value, err := unix.FcntlInt(fd, unix.F_GETFD, 0)
					flags = append(flags, err == nil && value&unix.FD_CLOEXEC != 0)
					value, err = unix.FcntlInt(fd, unix.F_GETFL, 0)
					flags = append(flags, err == nil && value&unix.O_NONBLOCK != 0)
				}
				return flags, nil
			case "crash":
				os.Exit(17)
			case "wait":
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return nil, errors.New("unknown fixture method")
		}, nil
	})
	if kind == "exit-66" && err == nil {
		os.Exit(66)
	}
	if kind == "death-child" && err == nil && proof != "" {
		os.WriteFile(proof+".exited", []byte("closed"), 0600)
	}
	if kind == "ignore-close" {
		for {
			time.Sleep(time.Hour)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	os.Exit(0)
}

func TestSubprocessHandshakeNestedCallAndReaping(t *testing.T) {
	opts := helperOptions()
	opts.Command = func(kind string) *exec.Cmd {
		cmd := helperCommand(kind)
		cmd.Env = []string{"PATH=/fixture", "KPL_PASSWORD=fake-secret", "KPL_TOKEN=fake-token", "AWS_SECRET_ACCESS_KEY=fake-cloud-secret"}
		return cmd
	}
	p, err := Start(t.Context(), "fixture", map[string]any{"runId": "run-fixture"}, func(_ context.Context, method string, body json.RawMessage) (any, error) {
		var n int
		json.Unmarshal(body, &n)
		return n + 1, nil
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.PID() == os.Getpid() || p.RunID() != "run-fixture" || len(p.Epoch()) != 48 {
		t.Fatal("worker did not get a new identity")
	}
	var value int
	if err := p.Call(t.Context(), "callback", nil, &value); err != nil || value != 4 {
		t.Fatalf("nested child broker call: %d %v", value, err)
	}
	var keys []string
	if err := p.Call(t.Context(), "environment", nil, &keys); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if strings.HasPrefix(key, "KPL_") && key != privateMarker || strings.HasPrefix(key, "AWS_") {
			t.Fatalf("credential key inherited: %s", key)
		}
	}
	var privateFlags []bool
	if err := p.Call(t.Context(), "private-fd-flags", nil, &privateFlags); err != nil || len(privateFlags) != 4 || !privateFlags[0] || !privateFlags[1] || !privateFlags[2] || !privateFlags[3] {
		t.Fatalf("private IPC inherited by future exec: flags=%v err=%v", privateFlags, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.Call(ctx, "wait", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RPC deadline lost: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Close returned before Wait")
	}
}

func TestIdleInheritedHandshakeIsCancellableWhileParentKeepsPipesOpen(t *testing.T) {
	in, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	defer writer.Close()
	reader, out, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer out.Close()
	cmd := helperCommand("idle-handshake")
	cmd.ExtraFiles = []*os.File{in, out}
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("idle private handshake failed to observe context: %v", err)
		}
	case <-time.After(2 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("closing inherited pipe did not interrupt blocking handshake")
	}
}

func TestSubprocessCrashAndForcedTermination(t *testing.T) {
	for _, kind := range []string{"fixture", "ignore-close"} {
		t.Run(kind, func(t *testing.T) {
			p, err := Start(t.Context(), kind, map[string]any{"runId": "run"}, nil, helperOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if kind == "fixture" {
				if err := p.Call(t.Context(), "crash", nil, nil); err == nil {
					t.Fatal("worker crash acknowledged as success")
				}
				var exited *exec.ExitError
				if err := p.Wait(); !errors.As(err, &exited) || exited.ExitCode() != 17 {
					t.Fatalf("lost actual process exit: %v", err)
				}
			} else {
				started := time.Now()
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				if time.Since(started) < 80*time.Millisecond {
					t.Fatal("fixture did not exercise TERM grace before KILL")
				}
				if p.Wait() == nil {
					t.Fatal("unresponsive worker was not killed")
				}
			}
		})
	}
}

func TestSubprocessParentDeathKillsStoppedChild(t *testing.T) {
	// Adopt and reap the intentionally orphaned fixture instead of leaving its
	// zombie to an unrelated container PID 1 after proving pipe-EOF cleanup.
	var original int32
	if err := unix.Prctl(unix.PR_GET_CHILD_SUBREAPER, uintptr(unsafe.Pointer(&original)), 0, 0, 0); err != nil {
		t.Skipf("cannot inspect fixture subreaper: %v", err)
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		t.Skipf("cannot adopt orphan fixture: %v", err)
	}
	defer unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(original), 0, 0, 0)
	path := filepath.Join(t.TempDir(), "worker-pid")
	cmd := helperCommand("parent-death")
	cmd.Args = append(cmd.Args[:len(cmd.Args)-2], path, "workerproc-helper", "parent-death")
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			pid, _ = strconv.Atoi(string(data))
			if pid > 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("parent fixture failed to initialize worker")
	}
	reaped := false
	defer func() {
		if reaped {
			return
		}
		syscall.Kill(pid, syscall.SIGKILL)
		var status syscall.WaitStatus
		for {
			_, err := syscall.Wait4(pid, &status, 0, nil)
			if err != syscall.EINTR {
				break
			}
		}
	}()
	// Pipe EOF cannot execute Go cleanup in a SIGSTOPed process. The kernel
	// must reclaim this child without cooperation from its Go runtime.
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	for time.Now().Before(deadline) {
		var status syscall.WaitStatus
		got, err := syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
		if err != nil && err != syscall.EINTR {
			t.Fatalf("cannot reap adopted worker: %v", err)
		}
		if got == pid {
			reaped = true
			if !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("stopped child was not killed by parent death: %v", status)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("stopped worker survived its parent")
}

func TestSubprocessChurnDoesNotAccumulateDescriptors(t *testing.T) {
	count := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	before := count()
	for i := range 12 {
		p, err := Start(t.Context(), "fixture", map[string]any{"runId": fmt.Sprintf("run-%d", i)}, nil, helperOptions())
		if err != nil {
			t.Fatal(err)
		}
		var value int
		if err := p.Call(t.Context(), "echo", i, &value); err != nil || value != i {
			t.Fatalf("worker %d echo: %d %v", i, value, err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		assertReclaimedPeer(t, p.peer)
	}
	if after := count(); after > before+2 {
		t.Fatalf("worker churn retained descriptors: before=%d after=%d", before, after)
	}
}

func assertReclaimedPeer(t *testing.T, p *Peer) {
	t.Helper()
	select {
	case <-p.stopped:
	default:
		t.Fatal("Process completed before RPC workers")
	}
	p.mu.Lock()
	pending, active, handler := len(p.pending), len(p.active), p.handler != nil
	p.mu.Unlock()
	if pending != 0 || active != 0 || handler {
		t.Fatalf("retained RPC state: pending=%d active=%d handler=%v", pending, active, handler)
	}
	if len(p.data) != 0 || len(p.control) != 0 {
		t.Fatal("retained outbound buffers")
	}
	for depth := range p.requests {
		if len(p.requests[depth]) != 0 || len(p.credits[depth]) != 0 {
			t.Fatalf("retained depth %d request/credit", depth)
		}
	}
	if !p.bytes.TryAcquire(32 << 20) {
		t.Fatal("retained outbound byte budget")
	}
	p.bytes.Release(32 << 20)
}

func TestCloseRequiresParentHandlerQuiescence(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	opts := helperOptions()
	opts.GracePeriod, opts.KillWait = 10*time.Millisecond, 10*time.Millisecond
	p, err := Start(t.Context(), "fixture", map[string]any{"runId": "run"}, func(context.Context, string, json.RawMessage) (any, error) { close(entered); <-release; return 4, nil }, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	callDone := make(chan error, 1)
	go func() { callDone <- p.Call(t.Context(), "callback", nil, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("broker did not enter")
	}
	if err := p.Close(); !errors.Is(err, ErrNotQuiescent) {
		close(release)
		t.Fatalf("claimed reclamation with a live broker handler: %v", err)
	}
	select {
	case <-p.osDone:
	default:
		close(release)
		t.Fatal("fixture worker was not reaped")
	}
	select {
	case <-p.Done():
		close(release)
		t.Fatal("Done closed with a live broker handler")
	default:
	}
	close(release)
	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("finished broker did not release worker boundary")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	assertReclaimedPeer(t, p.peer)
	select {
	case <-callDone:
	case <-time.After(time.Second):
		t.Fatal("pending caller was retained")
	}
}

func TestCloseContextDoesNotClaimUnreapedWorker(t *testing.T) {
	p, err := Start(t.Context(), "ignore-close", map[string]any{"runId": "run"}, nil, helperOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := p.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short CloseContext claimed exit: %v", err)
	}
	select {
	case <-p.Done():
		t.Fatal("ignore-close fixture exited before TERM grace")
	default:
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosePreservesUnexpectedExitCodeAfterSuccessfulRPC(t *testing.T) {
	p, err := Start(t.Context(), "exit-66", map[string]any{"runId": "race-exit"}, nil, helperOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Call(t.Context(), "echo", true, nil); err != nil {
		t.Fatal(err)
	}
	var exited *exec.ExitError
	if err := p.Close(); !errors.As(err, &exited) || exited.ExitCode() != 66 {
		t.Fatalf("Close masked exit 66: %v", err)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("exit failure was confused with an unreaped worker")
	}
	assertReclaimedPeer(t, p.peer)
}

func TestExternalKillImmediatelyBeforeCloseKeepsFailure(t *testing.T) {
	for i := range 12 {
		p, err := Start(t.Context(), "fixture", map[string]any{"runId": fmt.Sprintf("external-kill-%d", i)}, nil, helperOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := p.cmd.Process.Kill(); err != nil {
			p.Close()
			t.Fatal(err)
		}
		var exited *exec.ExitError
		if err := p.Close(); !errors.As(err, &exited) {
			t.Fatalf("external kill was masked by Close at iteration %d: %v", i, err)
		}
		status, ok := exited.Sys().(syscall.WaitStatus)
		if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("lost external kill status: %v", exited)
		}
		if err := p.Wait(); !errors.As(err, &exited) {
			t.Fatalf("Wait lost external kill: %v", err)
		}
		select {
		case <-p.Done():
		default:
			t.Fatal("reclamation did not finish after external kill")
		}
		assertReclaimedPeer(t, p.peer)
	}
}

func TestFailedStartRetainsProcessUntilBootstrapBrokerQuiesces(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			opts := helperOptions()
			opts.GracePeriod, opts.KillWait = 10*time.Millisecond, 30*time.Millisecond
			opts.HandshakeTimeout = time.Second
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			type startResult struct {
				process *Process
				err     error
			}
			started := make(chan startResult, 1)
			go func() {
				process, err := Start(ctx, "bootstrap-callback", map[string]any{"runId": "failed-start"}, func(context.Context, string, json.RawMessage) (any, error) {
					close(entered)
					<-release // Deliberately uncooperative: cleanup must retain its owner.
					return nil, nil
				}, opts)
				started <- startResult{process, err}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("initialization did not enter parent broker")
			}
			want := context.DeadlineExceeded
			if canceled {
				cancel()
				want = context.Canceled
			}
			var result startResult
			select {
			case result = <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("failed initialization did not return its retained owner")
			}
			if result.process == nil || !errors.Is(result.err, want) || !errors.Is(result.err, ErrNotQuiescent) {
				t.Fatalf("failed initialization lost owner or error semantics: process=%v error=%v", result.process != nil, result.err)
			}
			select {
			case <-result.process.Done():
				t.Fatal("claimed quiescence with a retained bootstrap handler")
			default:
			}
			close(release)
			select {
			case <-result.process.Done():
			case <-time.After(time.Second):
				t.Fatal("finished bootstrap broker did not release its process")
			}
			if err := result.process.Close(); err != nil {
				t.Fatal(err)
			}
			assertReclaimedPeer(t, result.process.peer)
		})
	}
}

func TestFailedStartReturnsNoOwnerAfterCompleteCleanup(t *testing.T) {
	process, err := Start(t.Context(), "reject-bootstrap", map[string]any{"runId": "rejected"}, nil, helperOptions())
	if process != nil || err == nil || errors.Is(err, ErrNotQuiescent) || errors.Is(err, ErrNotReaped) {
		t.Fatalf("fully reaped initialization failure changed its caller contract: process=%v error=%v", process != nil, err)
	}
}
