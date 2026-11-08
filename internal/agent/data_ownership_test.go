//go:build linux

package agent

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestDuplicateAgentCannotRecoverLiveTelemetrySpool(t *testing.T) {
	config := Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := New(config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := first.spool.append([]model.TraceEvent{{RunID: "run", EventID: "delivered"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.spool.acknowledge(1); err != nil {
		t.Fatal(err)
	}
	// An idle, acknowledged segment is deliberately reused by this live Agent.
	// A duplicate constructor must not unlink it during its own recovery, even
	// when its later Run would discover an occupied listen address.
	second, err := New(config, logger)
	if err == nil {
		defer second.Close()
	}
	if err := first.spool.append([]model.TraceEvent{{RunID: "run", EventID: "new-evidence"}}); err != nil {
		t.Fatalf("duplicate Agent recovery damaged the live spool: %v", err)
	}
}

func TestAgentDataOwnershipCoversAliasesAndSeparateDirectories(t *testing.T) {
	config := Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}
	first, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	alias := filepath.Join(t.TempDir(), "same-data")
	if err := os.Symlink(config.DataDir, alias); err != nil {
		t.Fatal(err)
	}
	aliasConfig := config
	aliasConfig.DataDir = alias
	if duplicate, err := New(aliasConfig, nil); err == nil {
		duplicate.Close()
		t.Fatal("directory alias bypassed live ownership")
	}
	otherConfig := config
	otherConfig.DataDir = t.TempDir()
	other, err := New(otherConfig, nil)
	if err != nil {
		t.Fatalf("independent Agent data directory was blocked: %v", err)
	}
	defer other.Close()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, err := New(aliasConfig, nil)
	if err != nil {
		t.Fatalf("closed Agent retained ownership: %v", err)
	}
	defer replacement.Close()
	if err := first.Close(); err != nil {
		t.Fatalf("repeated Close failed: %v", err)
	}
	if duplicate, err := New(config, nil); err == nil {
		duplicate.Close()
		t.Fatal("repeated old Close unlocked the replacement Agent")
	}
}

func TestAgentDataOwnershipReleasesFailedInitialization(t *testing.T) {
	config := Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}
	blocked := filepath.Join(config.DataDir, "telemetry-spool")
	if err := os.WriteFile(blocked, []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	if failed, err := New(config, nil); err == nil {
		failed.Close()
		t.Fatal("invalid spool initialization succeeded")
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	recovered, err := New(config, nil)
	if err != nil {
		t.Fatalf("failed initialization leaked data ownership: %v", err)
	}
	defer recovered.Close()
}

func TestAgentDataOwnerHelper(t *testing.T) {
	directory := os.Getenv("KPL_AGENT_DATA_OWNER_HELPER")
	if directory == "" {
		return
	}
	s, err := New(Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: directory}, nil)
	if err != nil {
		os.Exit(2)
	}
	if err := s.spool.append([]model.TraceEvent{{RunID: "run", EventID: "survives-crash"}}); err != nil {
		os.Exit(2)
	}
	if err := os.WriteFile(filepath.Join(directory, "owner.ready"), []byte("ready"), 0600); err != nil {
		os.Exit(2)
	}
	for {
		time.Sleep(time.Hour)
		// This helper does not Run the Server, so keep the lock's os.File
		// reachable instead of allowing its finalizer to unlock live ownership.
		runtime.KeepAlive(s)
	}
}

func TestAgentCrashReleasesDataOwnershipAndPreservesEvidence(t *testing.T) {
	directory := t.TempDir()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestAgentDataOwnerHelper$")
	command.Env = append(os.Environ(), "KPL_AGENT_DATA_OWNER_HELPER="+directory)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(directory, "owner.ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("data-owner helper did not start")
		}
		time.Sleep(time.Millisecond)
	}
	config := Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: directory}
	if duplicate, err := New(config, nil); err == nil {
		duplicate.Close()
		t.Fatal("another process bypassed Agent data ownership")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	recovered, err := New(config, nil)
	if err != nil {
		t.Fatalf("crashed process retained data ownership: %v", err)
	}
	defer recovered.Close()
	events, err := recovered.spool.loadPrefix()
	if err != nil || len(events) != 1 || events[0].EventID != "survives-crash" {
		t.Fatalf("crash recovery lost accepted evidence: %+v %v", events, err)
	}
}
