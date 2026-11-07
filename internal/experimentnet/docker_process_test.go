//go:build linux

package experimentnet

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNetworkDockerProcessFixture(t *testing.T) {
	mode := os.Getenv("KPL_NETWORK_PROCESS_FIXTURE")
	if mode == "" {
		return
	}
	ready := os.Getenv("KPL_NETWORK_PROCESS_READY")
	if mode == "child" {
		signal.Ignore(syscall.SIGTERM)
		if err := os.WriteFile(ready, []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			os.Exit(98)
		}
		for {
			time.Sleep(time.Second)
		}
	}
	child := exec.Command(os.Args[0], "-test.run=^TestNetworkDockerProcessFixture$")
	child.Env = append(os.Environ(), "KPL_NETWORK_PROCESS_FIXTURE=child")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		os.Exit(97)
	}
	if mode == "parent-exits" || mode == "parent-fails" {
		for range 1000 {
			if _, err := os.Stat(ready); err == nil {
				if mode == "parent-fails" {
					os.Exit(3)
				}
				os.Exit(0)
			}
			time.Sleep(5 * time.Millisecond)
		}
		_ = child.Process.Kill()
		os.Exit(96)
	}
	for {
		time.Sleep(time.Second)
	}
}

func networkProcessRunning(pid int) bool {
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

func TestNetworkDockerCommandBoundsInheritedOutputAndStopsChildren(t *testing.T) {
	for _, mode := range []string{"cancel", "parent-exits", "parent-fails"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			// Use the test executable as a fake Docker CLI. No daemon is contacted.
			wrapper := "#!/bin/sh\nexec \"$KPL_NETWORK_TEST_BINARY\" -test.run=^TestNetworkDockerProcessFixture$ -- \"$@\"\n"
			if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(wrapper), 0700); err != nil {
				t.Fatal(err)
			}
			ready := filepath.Join(dir, "child-ready")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("KPL_NETWORK_TEST_BINARY", binary)
			t.Setenv("KPL_NETWORK_PROCESS_FIXTURE", mode)
			t.Setenv("KPL_NETWORK_PROCESS_READY", ready)
			t.Setenv("GORACE", "atexit_sleep_ms=0")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := dockerCommand(ctx, nil, "network", "ls"); done <- err }()
			var pid int
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if data, err := os.ReadFile(ready); err == nil {
					pid, _ = strconv.Atoi(string(data))
					if pid > 0 {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
			if pid <= 0 {
				cancel()
				t.Fatal("fake Docker child did not initialize")
			}
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("wrong subprocess failure: %v", err)
				}
			case <-time.After(2 * time.Second):
				_ = syscall.Kill(pid, syscall.SIGKILL)
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("fake Docker command remained blocked after fixture cleanup")
				}
				t.Fatal("Docker command remained blocked on inherited stdout/stderr")
			}
			deadline = time.Now().Add(time.Second)
			for time.Now().Before(deadline) && networkProcessRunning(pid) {
				time.Sleep(5 * time.Millisecond)
			}
			if networkProcessRunning(pid) {
				t.Fatal("fake Docker child survived command termination")
			}
		})
	}
}
