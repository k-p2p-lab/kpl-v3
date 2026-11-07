package master

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestMasterProcessFixture(t *testing.T) {
	mode := os.Getenv("KPL_MASTER_PROCESS_FIXTURE")
	if mode == "" {
		return
	}
	switch mode {
	case "args":
		_ = json.NewEncoder(os.Stdout).Encode(os.Args[3:])
		os.Exit(0)
	case "exit":
		os.Exit(7)
	case "ignore-term":
		signal.Ignore(syscall.SIGTERM)
		for {
			time.Sleep(time.Second)
		}
	case "group":
		_ = json.NewEncoder(os.Stdout).Encode(syscall.Getpgrp())
		os.Exit(0)
	case "signal":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		os.Exit(1)
	case "ignore-term-child":
		signal.Ignore(syscall.SIGTERM)
		if err := os.WriteFile(os.Getenv("KPL_MASTER_CHILD_READY"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(98)
		}
		for {
			time.Sleep(time.Second)
		}
	case "spawn-ignore-term", "spawn-escaped-output", "exit-with-output-child", "exit-with-output-child-failed":
		child := exec.Command(os.Args[0], "-test.run=^TestMasterProcessFixture$")
		child.Env = append(os.Environ(), "KPL_MASTER_PROCESS_FIXTURE=ignore-term-child")
		if mode == "spawn-escaped-output" {
			child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			child.Stdout = os.Stdout
		}
		if strings.HasPrefix(mode, "exit-with-output-child") {
			child.Stdout = os.Stdout
		}
		if err := child.Start(); err != nil {
			os.Exit(97)
		}
		if strings.HasPrefix(mode, "exit-with-output-child") {
			for range 1000 {
				if _, err := os.Stat(os.Getenv("KPL_MASTER_CHILD_READY")); err == nil {
					if mode == "exit-with-output-child-failed" {
						os.Exit(3)
					}
					os.Exit(0)
				}
				time.Sleep(5 * time.Millisecond)
			}
			_ = child.Process.Kill()
			os.Exit(96)
		}
		// The parent retains ordinary SIGTERM handling. Its child ignores TERM
		// and has no inherited output pipes, so waiting for this parent finishes
		// before the remaining process group has stopped.
		for {
			time.Sleep(time.Second)
		}
	}
	os.Exit(1)
}

func fixtureCommand(t *testing.T, mode string, args ...string) Command {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return Command{Name: self, Args: append([]string{"-test.run=^TestMasterProcessFixture$", "--"}, args...), Env: append(os.Environ(), "KPL_MASTER_PROCESS_FIXTURE="+mode, "GORACE=atexit_sleep_ms=0")}
}

func TestExecRunnerPreservesLiteralArgvAndExitStatus(t *testing.T) {
	want := []string{"path with spaces", "$(do-not-execute)", "a'b", "--context=remote"}
	out, err := (ExecRunner{}).Output(t.Context(), fixtureCommand(t, "args", want...))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v", got)
	}
	err = (ExecRunner{}).Run(t.Context(), fixtureCommand(t, "exit"))
	if ExitCode(err) != 7 {
		t.Fatalf("exit code=%d, err=%v", ExitCode(err), err)
	}
}

func TestExecRunnerBoundsCancellationAndReapsProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	command := fixtureCommand(t, "ignore-term")
	command.CancelGrace = 100 * time.Millisecond
	start := time.Now()
	err := (ExecRunner{}).Run(ctx, command)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 3*time.Second {
		t.Fatalf("unbounded cancellation: %v after %v", err, time.Since(start))
	}
	if ExitCode(err) != 124 {
		t.Fatal("timeout status not retained")
	}
	cancelled, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	if err := (ExecRunner{}).Run(cancelled, Command{Name: "must-not-start"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExecRunnerCancelsDescendantAfterLeaderExits(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := filepath.Join(t.TempDir(), "child-ready")
	command := fixtureCommand(t, "spawn-ignore-term")
	command.Env = append(command.Env, "KPL_MASTER_CHILD_READY="+ready)
	command.CancelGrace = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- (ExecRunner{}).Run(ctx, command) }()
	deadline := time.Now().Add(5 * time.Second)
	pid := 0
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(ready)
		if err == nil {
			pid, _ = strconv.Atoi(string(data))
			if pid > 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid <= 0 {
		cancel()
		<-done
		t.Fatal("descendant did not initialize")
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wrong cancellation result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader was not reaped after cancellation")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !fixtureProcessRunning(pid) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("TERM-ignoring descendant survived leader cancellation")
}

func fixtureProcessRunning(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	index := strings.LastIndexByte(string(data), ')')
	if index < 0 {
		return true
	}
	fields := strings.Fields(string(data[index+1:]))
	return len(fields) == 0 || fields[0] != "Z"
}

func TestExecRunnerInteractiveKeepsForegroundProcessGroup(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		command := fixtureCommand(t, "group")
		command.Interactive = interactive
		out, err := (ExecRunner{}).Output(t.Context(), command)
		if err != nil {
			t.Fatal(err)
		}
		var group int
		if err := json.Unmarshal(out, &group); err != nil {
			t.Fatal(err)
		}
		if (group == syscall.Getpgrp()) != interactive {
			t.Fatalf("interactive=%v inherited foreground group=%v", interactive, group == syscall.Getpgrp())
		}
	}
}

func TestExecRunnerPreservesSignalExitStatus(t *testing.T) {
	err := (ExecRunner{}).Run(t.Context(), fixtureCommand(t, "signal"))
	if ExitCode(err) != 128+int(syscall.SIGKILL) {
		t.Fatalf("signal exit code=%d, want %d", ExitCode(err), 128+int(syscall.SIGKILL))
	}
}

func TestExecRunnerBoundsPipeWaitWithCustomGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := filepath.Join(t.TempDir(), "escaped-child-ready")
	command := fixtureCommand(t, "spawn-escaped-output")
	command.Env = append(command.Env, "KPL_MASTER_CHILD_READY="+ready)
	command.Stdout = io.Discard // os/exec uses a pipe for a non-file writer.
	command.CancelGrace = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- (ExecRunner{}).Run(ctx, command) }()
	deadline := time.Now().Add(5 * time.Second)
	pid := 0
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(ready)
		if err == nil {
			pid, _ = strconv.Atoi(string(data))
			if pid > 0 {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pid <= 0 {
		cancel()
		<-done
		t.Fatal("escaped descendant did not initialize")
	}
	// An intentionally detached process is outside the runner's group. Clean
	// up this test fixture explicitly while checking that its inherited pipe
	// cannot force the command to wait for the default five-second pipe delay.
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
			t.Fatalf("custom grace ignored: %v after %v", err, time.Since(start))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inherited pipe exceeded custom cancellation grace")
	}
}

func TestExecRunnerReapsOutputChildAfterLeaderExitsNormally(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "output-child-ready")
	command := fixtureCommand(t, "exit-with-output-child")
	command.Env = append(command.Env, "KPL_MASTER_CHILD_READY="+ready)
	command.CancelGrace = 100 * time.Millisecond
	_, err := (ExecRunner{}).Output(t.Context(), command)
	data, readErr := os.ReadFile(ready)
	if readErr != nil {
		t.Fatalf("child did not initialize: %v (command: %v)", readErr, err)
	}
	pid, parseErr := strconv.Atoi(string(data))
	if parseErr != nil || pid <= 0 {
		t.Fatalf("invalid child PID: %q", data)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("unacknowledged output pipe was treated as success: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !fixtureProcessRunning(pid) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("command returned while its output-holding child kept running")
}

func TestExecRunnerReapsOutputChildAfterLeaderFails(t *testing.T) {
	ready := filepath.Join(t.TempDir(), "failed-output-child-ready")
	command := fixtureCommand(t, "exit-with-output-child-failed")
	command.Env = append(command.Env, "KPL_MASTER_CHILD_READY="+ready)
	command.CancelGrace = 100 * time.Millisecond
	_, err := (ExecRunner{}).Output(t.Context(), command)
	data, readErr := os.ReadFile(ready)
	if readErr != nil {
		t.Fatalf("child did not initialize: %v (command: %v)", readErr, err)
	}
	pid, parseErr := strconv.Atoi(string(data))
	if parseErr != nil || pid <= 0 {
		t.Fatalf("invalid child PID: %q", data)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
	if ExitCode(err) != 3 {
		t.Fatalf("leader failure changed: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !fixtureProcessRunning(pid) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("failed command left its output-holding child running")
}
