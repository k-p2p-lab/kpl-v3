//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDockerDescendantHelper(t *testing.T) {
	mode := os.Getenv("KPL_DOCKER_DESCENDANT_MODE")
	if mode == "" {
		return
	}
	pidPath := os.Getenv("KPL_DOCKER_DESCENDANT_PID")
	if mode == "child" {
		if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestDockerDescendantHelper$")
	child.Env = append(os.Environ(), "KPL_DOCKER_DESCENDANT_MODE=child")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	for {
		if _, err := os.Stat(pidPath); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if mode == "exit" {
		os.Exit(0)
	}
	if mode == "fail" {
		os.Exit(3)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestDockerCommandReclaimsPipeHoldingDescendants(t *testing.T) {
	for _, mode := range []string{"exit", "fail", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			pidPath := t.TempDir() + "/child.pid"
			d := &dockerRuntime{binary: "docker-helper", commandContext: func(ctx context.Context, _ string, _ ...string) *exec.Cmd {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDockerDescendantHelper$")
				cmd.Env = append(os.Environ(), "KPL_DOCKER_DESCENDANT_MODE="+mode, "KPL_DOCKER_DESCENDANT_PID="+pidPath)
				cmd.WaitDelay = 50 * time.Millisecond
				return cmd
			}}
			done := make(chan error, 1)
			go func() { _, err := d.run(ctx, nil, "inspect"); done <- err }()
			var childPID int
			t.Cleanup(func() {
				cancel()
				if childPID > 0 {
					_ = syscall.Kill(childPID, syscall.SIGKILL)
				}
			})
			deadline := time.Now().Add(3 * time.Second)
			for childPID == 0 && time.Now().Before(deadline) {
				data, err := os.ReadFile(pidPath)
				if err == nil {
					childPID, _ = strconv.Atoi(string(data))
				}
				time.Sleep(time.Millisecond)
			}
			if childPID == 0 {
				t.Fatal("helper child did not start")
			}
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				want := error(exec.ErrWaitDelay)
				if mode == "cancel" {
					want = context.Canceled
				}
				var exited *exec.ExitError
				if mode == "fail" && (!errors.As(err, &exited) || exited.ExitCode() != 3) {
					t.Fatalf("lost Docker exit status: %v", err)
				}
				if mode != "fail" && !errors.Is(err, want) {
					t.Fatalf("lost process error: %v, want %v", err, want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Docker subprocess stayed blocked on inherited output")
			}
			deadline = time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", childPID))
				if os.IsNotExist(err) || err == nil && strings.Contains(string(data), ") Z ") {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatal("Docker command returned while its helper descendant remained running")
		})
	}
}
