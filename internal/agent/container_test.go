package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAgentRequiresContainerReachableURLs(t *testing.T) {
	config := Config{DockerImage: "registry.example:5000/kpl-v3:test", DockerNetwork: "kpl-v3-peers", ID: "test", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080"}
	server, err := New(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	if server.docker == nil || server.config.SelfURL != config.AdvertiseURL {
		t.Fatalf("Docker configuration not applied: %+v", server.config)
	}
	for _, endpoint := range []string{"http://localhost:8090", "http://127.0.0.1:8090", "http://[::1]:8090", "http://0.0.0.0:8090"} {
		config.SelfURL = endpoint
		if _, err := New(config, nil); err == nil {
			t.Fatalf("accepted unreachable container callback %s", endpoint)
		}
	}
}

func newContainerTestServer(t *testing.T, settings map[string]string) (*Server, string) {
	t.Helper()
	server, err := New(Config{DockerImage: "registry.example:5000/kpl-v3:test", DockerNetwork: "kpl-v3-peers", ID: "agent", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	d, path := fakeDocker(t, settings)
	server.docker = d
	t.Cleanup(func() { server.stopAll(); server.waitStopped(5 * time.Second) })
	return server, path
}

func waitDockerCall(t *testing.T, path, command string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, call := range dockerCalls(t, path) {
			if call.Args[0] == command {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Docker %s did not start", command)
}

func TestContainerLifecycleStopsAndReportsCleanupFailures(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "cleanup-error"}[failCleanup], func(t *testing.T) {
			settings := map[string]string{"HANG": "wait"}
			if failCleanup {
				settings["FAIL"] = "rm"
			}
			server, path := newContainerTestServer(t, settings)
			request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", Generation: 2}
			node, err := server.createNode(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if node.Metadata["runtime"] != "docker" {
				t.Fatalf("metadata = %v", node.Metadata)
			}
			waitDockerCall(t, path, "wait")
			server.mu.RLock()
			proc := server.processes[node.ID]
			configPath, containerID := proc.configPath, proc.containerID
			server.mu.RUnlock()
			if containerID != fakeContainerID {
				t.Fatal("Docker peer must retain its container identity")
			}
			data, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			var config model.PeerProcessConfig
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			if config.APListen != "0.0.0.0:18000" || config.P2PListen != "/ip4/0.0.0.0/tcp/20000" || config.AgentURL != "http://agent:8090" {
				t.Fatalf("container configuration = %+v", config)
			}
			server.stopRunGeneration("run", 2)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = server.waitRunContainers(ctx, "run", 2)
			if (err != nil) != failCleanup {
				t.Fatalf("cleanup error = %v, want error %v", err, failCleanup)
			}
			wantState := model.NodeStopped
			if failCleanup {
				wantState = model.NodeFailed
			}
			waitForNodeState(t, server, "peer", wantState)
			if failCleanup {
				delete(settings, "FAIL")
				server.stopRunGeneration("run", 2)
				if err := server.waitRunContainers(ctx, "run", 2); err != nil {
					t.Fatalf("cleanup retry did not recover: %v", err)
				}
				waitForNodeState(t, server, "peer", model.NodeStopped)
			}
			if _, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "late", RunID: "run", Group: "workers", Generation: 2}); err == nil {
				t.Fatal("late container create bypassed generation fence")
			}
		})
	}
}

func TestAgentShutdownRejectsLateContainerCreate(t *testing.T) {
	server, path := newContainerTestServer(t, nil)
	server.beginShutdown()
	_, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "late", RunID: "run", Group: "workers"})
	if err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Fatalf("late create error = %v", err)
	}
	if len(dockerCalls(t, path)) != 0 {
		t.Fatal("late create reached Docker after shutdown")
	}
}

func TestDuplicateAgentDoesNotRemoveRunningPeers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, path := newContainerTestServer(t, nil)
	server.config.Listen = listener.Addr().String()
	if err := server.Run(context.Background()); err == nil {
		t.Fatal("duplicate Agent accepted occupied listen address")
	}
	if len(dockerCalls(t, path)) != 0 {
		t.Fatal("duplicate Agent touched Docker before acquiring its listen address")
	}
}

func TestContainerFenceCancelsAnInFlightCreate(t *testing.T) {
	server, path := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "300"})
	_, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"})
	if err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, path, "create")
	server.stopRunGeneration("run", 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.waitRunContainers(ctx, "run", 0); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, path, "rm")
	waitForNodeState(t, server, "peer", model.NodeStopped)
}

func TestDockerLifetimeBeginsAfterDaemonAdmission(t *testing.T) {
	// A zero lifetime is an immediate departure, but only once Docker has
	// admitted the container. It must not cancel a pending create command.
	server, path := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "300"})
	_, err := server.createNode(context.Background(), model.CreateNodeRequest{
		ID: "peer", RunID: "run", Group: "workers", Lifetime: "0s",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, path, "create")
	time.Sleep(50 * time.Millisecond)
	server.mu.RLock()
	proc := server.processes["peer"]
	state, admitted := proc.node.State, proc.node.Metadata["containerCreatedAt"]
	server.mu.RUnlock()
	if state != model.NodeStarting || admitted != "" {
		t.Fatalf("lifetime elapsed before daemon admission: state=%s admitted=%s", state, admitted)
	}
	server.stopAll()
	server.waitStopped(5 * time.Second)

	admittedServer, admittedPath := newContainerTestServer(t, map[string]string{"HANG": "cp"})
	_, err = admittedServer.createNode(context.Background(), model.CreateNodeRequest{
		ID: "admitted", RunID: "run", Group: "workers", Lifetime: "100ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, admittedPath, "cp")
	waitForNodeState(t, admittedServer, "admitted", model.NodeStopped)
	node := admittedServer.nodes()[0]
	if node.Metadata["containerCreatedAt"] == "" || node.Metadata["containerStartedAt"] != "" || node.Metadata["lifetimeBasis"] != "container-created" {
		t.Fatalf("incorrect lifecycle timeline: %v", node.Metadata)
	}
}

func TestDelayedDockerAdmissionDoesNotConsumeStartupBudget(t *testing.T) {
	const delay = 250 * time.Millisecond
	server, path := newContainerTestServer(t, map[string]string{"DELAY": "create", "DELAY_MS": "250", "HANG": "wait"})
	type observation struct {
		stage        string
		at, deadline time.Time
		hasDeadline  bool
	}
	observed := make(chan observation, 5)
	commandContext := server.docker.commandContext
	server.docker.commandContext = func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		switch args[0] {
		case "create", "cp", "start", "inspect", "wait":
			deadline, ok := ctx.Deadline()
			observed <- observation{stage: args[0], at: time.Now(), deadline: deadline, hasDeadline: ok}
		}
		return commandContext(ctx, binary, args...)
	}
	if _, err := server.createNode(context.Background(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, path, "wait")
	stages := make(map[string]observation)
	for i := 0; i < 5; i++ {
		item := <-observed
		stages[item.stage] = item
	}
	admission, copyStage := stages["create"], stages["cp"]
	if !admission.hasDeadline || !copyStage.hasDeadline || copyStage.deadline.Sub(admission.deadline) < delay {
		t.Fatalf("late admission consumed startup budget: admission=%+v startup=%+v", admission, copyStage)
	}
	if remaining := copyStage.deadline.Sub(copyStage.at); remaining < 44*time.Second || remaining > dockerStartupTimeout {
		t.Fatalf("startup did not receive a fresh 45-second budget: %v", remaining)
	}
	for _, stage := range []string{"start", "inspect"} {
		if !stages[stage].deadline.Equal(copyStage.deadline) {
			t.Fatalf("%s did not share the startup budget", stage)
		}
	}
	if stages["wait"].hasDeadline {
		t.Fatal("the startup deadline leaked into the peer's running lifetime")
	}
	if err := server.stopNode("peer"); err != nil {
		t.Fatal(err)
	}
	waitForNodeState(t, server, "peer", model.NodeStopped)
	waitDockerCall(t, path, "rm")
}

func TestAgentRequiresExplicitSwarmImageAndNetwork(t *testing.T) {
	base := Config{ID: "agent", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DockerImage: "registry.example:5000/kpl-v3:test", DockerNetwork: "kpl-v3-peers"}
	for _, field := range []string{"image", "network"} {
		t.Run(field, func(t *testing.T) {
			config := base
			if field == "image" {
				config.DockerImage = ""
			} else {
				config.DockerNetwork = ""
			}
			if _, err := New(config, nil); err == nil {
				t.Fatalf("accepted missing %s", field)
			}
		})
	}
}
