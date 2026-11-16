//go:build linux

package experimentnet

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestNetworkServerShutdownProcessFixture(t *testing.T) {
	if os.Getenv("KPL_TEST_NETWORK_SERVER_PROCESS") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	m, err := NewManager(ManagerConfig{Stack: "test", PeerNetwork: "test-peers", ControlNetwork: "test-control", ControllerURL: "http://controller:8080", Token: "fixture-only-token", SelfContainer: "manager", DataDir: os.Getenv("KPL_TEST_NETWORK_SERVER_DATA")})
	if err != nil {
		os.Exit(90)
	}
	records := make(chan slog.Record, 1)
	go func() {
		record := <-records
		record.Attrs(func(attribute slog.Attr) bool {
			if attribute.Key == "listen" {
				_ = os.WriteFile(os.Getenv("KPL_TEST_NETWORK_SERVER_READY"), []byte(attribute.Value.String()), 0600)
			}
			return true
		})
	}()
	err = serve(ctx, "127.0.0.1:0", m.Handler(), slog.New(listenLogHandler{records: records}))
	stop()
	if err != nil {
		os.Exit(91)
	}
	// Match cmd/kpl-master's immediate process exit after the role returns.
	os.Exit(0)
}

func waitNetworkFixtureFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return strings.TrimSpace(string(data))
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("network process fixture did not publish %s", filepath.Base(path))
	return ""
}

func TestNetworkServerShutdownWaitsForDockerProcessCleanup(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	ready, childReady, cliReady := filepath.Join(directory, "server-ready"), filepath.Join(directory, "child-ready"), filepath.Join(directory, "cli-ready")
	wrapper := "#!/bin/sh\nprintf '%s' \"$$\" > \"$KPL_NETWORK_CLI_READY\"\nexec \"$KPL_NETWORK_TEST_BINARY\" -test.run=^TestNetworkDockerProcessFixture$\n"
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestNetworkServerShutdownProcessFixture$")
	command.WaitDelay = time.Second
	command.Env = []string{"PATH=" + directory, "GOMAXPROCS=1", "GORACE=atexit_sleep_ms=0", "KPL_TEST_NETWORK_SERVER_PROCESS=1", "KPL_TEST_NETWORK_SERVER_DATA=" + filepath.Join(directory, "data"), "KPL_TEST_NETWORK_SERVER_READY=" + ready,
		"KPL_NETWORK_TEST_BINARY=" + binary, "KPL_NETWORK_PROCESS_FIXTURE=cancel", "KPL_NETWORK_PROCESS_READY=" + childReady, "KPL_NETWORK_CLI_READY=" + cliReady}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if data, err := os.ReadFile(cliReady); err == nil {
			if pid, err := strconv.Atoi(string(data)); err == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	address := waitNetworkFixtureFile(t, ready)
	payload, err := json.Marshal(model.ExperimentNetworkRequest{RunID: "run", Epoch: "attempt", RequestedAt: time.Now().UTC(), Agents: map[string]string{"agent": "http://agent:8090"}})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+"/api/v1/network/prepare", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer fixture-only-token")
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		if response, err := client.Do(request); err == nil {
			_ = response.Body.Close()
		}
	}()
	childPID, err := strconv.Atoi(waitNetworkFixtureFile(t, childReady))
	if err != nil {
		t.Fatal(err)
	}
	cliPID, err := strconv.Atoi(waitNetworkFixtureFile(t, cliReady))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Kill(-cliPID, syscall.SIGKILL); _ = syscall.Kill(childPID, syscall.SIGKILL) })
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("network role did not exit cleanly: %v: %s", err, output.String())
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("network request remained connected after role termination")
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) && (networkProcessRunning(cliPID) || networkProcessRunning(childPID)) {
		time.Sleep(5 * time.Millisecond)
	}
	if networkProcessRunning(cliPID) || networkProcessRunning(childPID) {
		t.Fatalf("network role acknowledged clean exit before Docker process cleanup: cliRunning=%t childRunning=%t", networkProcessRunning(cliPID), networkProcessRunning(childPID))
	}
}
