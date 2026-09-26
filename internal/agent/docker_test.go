package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const fakeContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type dockerInvocation struct {
	Args  []string `json:"args"`
	Input []byte   `json:"input,omitempty"`
}

// Each CLI command runs in a helper subprocess, exercising real stdin, command
// cancellation and exit handling without requiring a Docker daemon in unit CI.
func TestDockerCLIHelper(t *testing.T) {
	if os.Getenv("KPL_DOCKER_HELPER") != "1" {
		return
	}
	var args []string
	for i, arg := range os.Args {
		if arg == "--" {
			args = os.Args[i+1:]
			break
		}
	}
	if len(args) == 0 {
		os.Exit(2)
	}
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		os.Exit(2)
	}
	file, err := os.OpenFile(os.Getenv("KPL_DOCKER_LOG"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	_ = json.NewEncoder(file).Encode(dockerInvocation{Args: args, Input: input})
	_ = file.Close()
	if os.Getenv("KPL_DOCKER_DELAY") == args[0] {
		delayMS, _ := strconv.Atoi(os.Getenv("KPL_DOCKER_DELAY_MS"))
		time.Sleep(time.Duration(delayMS) * time.Millisecond)
	}
	if os.Getenv("KPL_DOCKER_HANG") == args[0] || (args[0] == "rm" && os.Getenv("KPL_DOCKER_HANG_ID") == args[len(args)-1]) {
		for {
			time.Sleep(time.Hour)
		}
	}
	for _, fail := range strings.Split(os.Getenv("KPL_DOCKER_FAIL"), ",") {
		if fail == args[0] {
			fmt.Fprint(os.Stderr, "daemon unavailable during "+args[0])
			os.Exit(1)
		}
	}
	switch args[0] {
	case "info":
		result := os.Getenv("KPL_DOCKER_INFO")
		if result == "" {
			result = `{"OSType":"linux","SwarmState":"active"}`
		}
		fmt.Print(result)
	case "image":
		fmt.Print("linux\n")
	case "network":
		result := os.Getenv("KPL_DOCKER_NETWORK")
		if result == "" {
			result = `[{"Name":"kpl-v3-peers","Driver":"overlay","Attachable":true,"Scope":"swarm"}]`
		}
		fmt.Print(result)
	case "create":
		fmt.Println(fakeContainerID)
	case "cp":
	case "start":
		fmt.Println(fakeContainerID)
	case "inspect":
		if len(args) > 1 && args[1] == "--type" {
			limit, _ := strconv.Atoi(os.Getenv("KPL_DOCKER_REMOVAL_PRESENT_POLLS"))
			data, _ := os.ReadFile(os.Getenv("KPL_DOCKER_LOG"))
			decoder := json.NewDecoder(bytes.NewReader(data))
			polls := 0
			for {
				var call dockerInvocation
				if err := decoder.Decode(&call); err != nil {
					break
				}
				if len(call.Args) > 1 && call.Args[0] == "inspect" && call.Args[1] == "--type" {
					polls++
				}
			}
			if limit >= 0 && polls > limit {
				fmt.Fprint(os.Stderr, "Error: No such object: "+args[len(args)-1])
				os.Exit(1)
			}
			fmt.Println(fakeContainerID)
		} else if os.Getenv("KPL_DOCKER_NO_IP") == "1" {
			fmt.Print(`{"kpl-v3-peers":{"IPAddress":""}}`)
		} else {
			fmt.Print(`{"kpl-v3-peers":{"IPAddress":"172.25.0.8"}}`)
		}
	case "wait":
		code := os.Getenv("KPL_DOCKER_EXIT")
		if code == "" {
			code = "0"
		}
		fmt.Println(code)
	case "logs":
		if count, _ := strconv.Atoi(os.Getenv("KPL_DOCKER_LOG_BYTES")); count > 0 {
			fmt.Print(strings.Repeat("x", count))
		} else {
			fmt.Fprint(os.Stderr, "tc: specified qdisc kind is unknown")
		}
	case "ps":
		fmt.Print(os.Getenv("KPL_DOCKER_PS"))
		count, _ := strconv.Atoi(os.Getenv("KPL_DOCKER_PS_COUNT"))
		for i := 0; i < count; i++ {
			fmt.Printf("%064x\n", i)
		}
	case "rm":
		if os.Getenv("KPL_DOCKER_MISSING") == "1" {
			fmt.Fprint(os.Stderr, "Error response from daemon: No such container: "+args[len(args)-1])
			os.Exit(1)
		}
		if os.Getenv("KPL_DOCKER_REMOVING") == "1" {
			fmt.Fprint(os.Stderr, "Error response from daemon: removal of container "+args[len(args)-1]+" is already in progress")
			os.Exit(1)
		}
		fmt.Println(args[len(args)-1])
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func fakeDocker(t *testing.T, settings map[string]string) (*dockerRuntime, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "docker.jsonl")
	runtime := &dockerRuntime{binary: "fake-docker", image: "registry.example:5000/kpl-v3:test", network: "kpl-v3-peers"}
	runtime.commandContext = func(ctx context.Context, binary string, args ...string) *exec.Cmd {
		if binary != "fake-docker" {
			t.Errorf("binary = %q", binary)
		}
		command := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestDockerCLIHelper$", "--"}, args...)...)
		command.Env = append(os.Environ(), "KPL_DOCKER_HELPER=1", "KPL_DOCKER_LOG="+logPath)
		for name, value := range settings {
			command.Env = append(command.Env, "KPL_DOCKER_"+name+"="+value)
		}
		return command
	}
	return runtime, logPath
}

func dockerCalls(t *testing.T, path string) []dockerInvocation {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var calls []dockerInvocation
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var call dockerInvocation
		if err := decoder.Decode(&call); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, call)
	}
	return calls
}

func dockerTestConfig(t *testing.T, impaired bool) (model.Node, []byte) {
	t.Helper()
	node := model.Node{AgentID: "agent-a", RunID: "run-a", ID: "node-1", Generation: 3}
	config := model.PeerProcessConfig{Node: node, APListen: "0.0.0.0:18000", P2PListen: "/ip4/0.0.0.0/tcp/20000", Token: "config-secret"}
	if impaired {
		config.NodeConfig.Network.Delay = "50ms"
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return node, data
}

func TestDockerCreateIsolatesPeerAndCopiesConfiguration(t *testing.T) {
	for _, impaired := range []bool{false, true} {
		t.Run(fmt.Sprint(impaired), func(t *testing.T) {
			d, logPath := fakeDocker(t, nil)
			node, config := dockerTestConfig(t, impaired)
			id, apiURL, err := d.create(context.Background(), node, config)
			if err != nil {
				t.Fatal(err)
			}
			if id != fakeContainerID || apiURL != "http://172.25.0.8:18000" {
				t.Fatalf("create = %q, %q", id, apiURL)
			}
			calls := dockerCalls(t, logPath)
			var steps []string
			for _, call := range calls {
				steps = append(steps, call.Args[0])
			}
			if !reflect.DeepEqual(steps, []string{"create", "cp", "start", "inspect"}) {
				t.Fatalf("steps = %v", steps)
			}
			args := calls[0].Args
			joined := strings.Join(args, " ")
			for _, expected := range []string{"--network kpl-v3-peers", "--cap-drop ALL", "--security-opt no-new-privileges", "--label io.kpl.agent=agent-a", "--label io.kpl.run=run-a", "--label io.kpl.generation=3", "registry.example:5000/kpl-v3:test peer --config /etc/kpl-peer.json"} {
				if !strings.Contains(joined, expected) {
					t.Errorf("missing %q in %s", expected, joined)
				}
			}
			for _, arg := range args {
				for _, forbidden := range []string{"--privileged", "--publish", "-p", "--volume", "-v", "--mount", "--pid", "--ipc"} {
					if arg == forbidden {
						t.Errorf("unexpected host isolation bypass %s", arg)
					}
				}
			}
			if strings.Contains(joined, "--cap-add NET_ADMIN") != impaired {
				t.Errorf("NET_ADMIN does not match impairment: %s", joined)
			}
			if strings.Contains(joined, "config-secret") {
				t.Error("configuration token leaked to command arguments")
			}
			reader := tar.NewReader(bytes.NewReader(calls[1].Input))
			header, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if header.Name != "etc/kpl-peer.json" || header.Mode != 0600 || !bytes.Equal(data, config) {
				t.Fatalf("incorrect configuration archive: %+v", header)
			}
			if _, err := reader.Next(); err != io.EOF {
				t.Fatalf("unexpected extra archive entry: %v", err)
			}
		})
	}
}

func TestDockerCreateCleansUpFailures(t *testing.T) {
	for _, stage := range []string{"create", "cp", "start", "inspect"} {
		t.Run(stage, func(t *testing.T) {
			d, logPath := fakeDocker(t, map[string]string{"FAIL": stage})
			node, config := dockerTestConfig(t, false)
			id, apiURL, err := d.create(context.Background(), node, config)
			if err == nil || id != "" || apiURL != "" {
				t.Fatalf("failed create = %q, %q, %v", id, apiURL, err)
			}
			calls := dockerCalls(t, logPath)
			last := calls[len(calls)-1].Args
			if len(last) != 3 || last[0] != "rm" || last[1] != "--force" {
				t.Fatalf("cleanup = %v", last)
			}
			if stage == "create" {
				if !dockerPeerName.MatchString(last[2]) {
					t.Fatalf("cleanup name = %q", last[2])
				}
			} else if last[2] != fakeContainerID {
				t.Fatalf("cleanup ID = %q", last[2])
			}
		})
	}
}

func TestDockerEarlyPeerExitPreservesStartupLogs(t *testing.T) {
	d, path := fakeDocker(t, map[string]string{"NO_IP": "1"})
	node, config := dockerTestConfig(t, false)
	_, _, err := d.create(context.Background(), node, config)
	if err == nil || !strings.Contains(err.Error(), "tc: specified qdisc kind is unknown") {
		t.Fatalf("early startup failure lost its cause: %v", err)
	}
	calls := dockerCalls(t, path)
	if len(calls) < 2 || calls[len(calls)-2].Args[0] != "logs" || calls[len(calls)-1].Args[0] != "rm" {
		t.Fatal("must capture startup logs before removing the exited peer")
	}
}

func TestDockerCreateCancellationCleansUpWithIndependentContext(t *testing.T) {
	for _, stage := range []string{"create", "cp", "start", "inspect"} {
		t.Run(stage, func(t *testing.T) {
			settings := map[string]string{"HANG": stage}
			if stage == "create" {
				settings = map[string]string{"DELAY": "create", "DELAY_MS": "200"}
			}
			d, logPath := fakeDocker(t, settings)
			node, config := dockerTestConfig(t, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := d.create(ctx, node, config); done <- err }()
			deadline := time.Now().Add(10 * time.Second)
			found := false
			for time.Now().Before(deadline) {
				for _, call := range dockerCalls(t, logPath) {
					if call.Args[0] == stage {
						found = true
					}
				}
				if found {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("create cancellation error = %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("create failed to finish after cancellation")
			}
			if !found {
				t.Fatal("helper did not reach intended cancellation point")
			}
			calls := dockerCalls(t, logPath)
			if calls[len(calls)-1].Args[0] != "rm" {
				t.Fatal("cleanup did not run after cancellation")
			}
			if stage == "create" {
				if len(calls) != 2 || calls[1].Args[2] != fakeContainerID {
					t.Fatalf("cancellation must await the late create ID and remove it without cp/start: %v", calls)
				}
			}
		})
	}
}

func TestDockerCreateAdmissionPreservesTheOriginalDeadline(t *testing.T) {
	d, _ := fakeDocker(t, map[string]string{"DELAY": "create", "DELAY_MS": "5000"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()
	commandContext := d.commandContext
	var observedDeadline time.Time
	d.commandContext = func(commandCtx context.Context, binary string, args ...string) *exec.Cmd {
		if args[0] == "create" {
			observedDeadline, _ = commandCtx.Deadline()
		}
		return commandContext(commandCtx, binary, args...)
	}
	node, config := dockerTestConfig(t, false)
	_, _, err := d.create(ctx, node, config)
	if !errors.Is(err, context.DeadlineExceeded) || !observedDeadline.Equal(wantDeadline) {
		t.Fatalf("create lost its original deadline: got=%v want=%v error=%v", observedDeadline, wantDeadline, err)
	}
}

func TestDockerCreationStageBudgetsRespectCallerDeadline(t *testing.T) {
	for _, callerTimeout := range []time.Duration{10 * time.Second, 2 * time.Minute} {
		t.Run(callerTimeout.String(), func(t *testing.T) {
			d, _ := fakeDocker(t, nil)
			ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
			defer cancel()
			callerDeadline, _ := ctx.Deadline()
			commandContext := d.commandContext
			d.commandContext = func(commandCtx context.Context, binary string, args ...string) *exec.Cmd {
				if args[0] == "create" || args[0] == "cp" {
					deadline, ok := commandCtx.Deadline()
					if !ok || deadline.After(callerDeadline) || time.Until(deadline) > 45*time.Second {
						t.Errorf("%s lost its own budget or the earlier caller deadline: %v", args[0], deadline)
					}
					if callerTimeout < 45*time.Second && !deadline.Equal(callerDeadline) {
						t.Errorf("%s did not preserve the earlier caller deadline", args[0])
					}
				}
				return commandContext(commandCtx, binary, args...)
			}
			node, config := dockerTestConfig(t, false)
			if _, _, err := d.create(ctx, node, config); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDockerCleanupReportsDaemonFailuresAndAcceptsAbsence(t *testing.T) {
	d, _ := fakeDocker(t, map[string]string{"FAIL": "cp,rm"})
	node, config := dockerTestConfig(t, false)
	id, apiURL, err := d.create(context.Background(), node, config)
	if err == nil || !strings.Contains(err.Error(), "copy peer container configuration") || !errors.Is(err, errDockerCleanup) {
		t.Fatalf("error = %v", err)
	}
	if id != fakeContainerID || apiURL != "" {
		t.Fatalf("cleanup failure must retain retry target, got ID %q and URL %q", id, apiURL)
	}
	d, _ = fakeDocker(t, map[string]string{"FAIL": "create,rm"})
	id, _, err = d.create(context.Background(), node, config)
	if !errors.Is(err, errDockerCleanup) || !dockerPeerName.MatchString(id) {
		t.Fatalf("failed create must retain attempt name for cleanup retry, got ID %q and error %v", id, err)
	}
	d, _ = fakeDocker(t, map[string]string{"MISSING": "1"})
	if err := d.remove(context.Background(), fakeContainerID); err != nil {
		t.Fatalf("remove missing container: %v", err)
	}
	if err := d.remove(context.Background(), "unrelated-container"); err == nil {
		t.Fatal("arbitrary container name accepted")
	}
}

func TestDockerRemoveWaitsForInProgressRemovalToActuallyFinish(t *testing.T) {
	for _, target := range []string{fakeContainerID, "kpl-peer-0123456789abcdef0123-0123456789ab"} {
		t.Run(target[:12], func(t *testing.T) {
			d, path := fakeDocker(t, map[string]string{"REMOVING": "1", "REMOVAL_PRESENT_POLLS": "2"})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := d.remove(ctx, target); err != nil {
				t.Fatal(err)
			}
			calls := dockerCalls(t, path)
			if len(calls) != 4 || calls[0].Args[0] != "rm" {
				t.Fatalf("expected rm conflict, two present inspections and one not-found inspection: %v", calls)
			}
			for _, call := range calls[1:] {
				want := []string{"inspect", "--type", "container", "--format", "{{.Id}}", target}
				if !reflect.DeepEqual(call.Args, want) {
					t.Fatalf("cleanup changed target or object type: %v", call.Args)
				}
			}
		})
	}
}

func TestDockerRemoveInProgressHonorsContextCancellation(t *testing.T) {
	d, path := fakeDocker(t, map[string]string{"REMOVING": "1", "REMOVAL_PRESENT_POLLS": "-1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.remove(ctx, fakeContainerID) }()
	waitDockerCall(t, path, "inspect")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("removal error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("removal polling ignored context cancellation")
	}
}

func TestDockerRemoveInProgressPreservesInspectDaemonFailure(t *testing.T) {
	d, _ := fakeDocker(t, map[string]string{"REMOVING": "1", "FAIL": "inspect"})
	err := d.remove(context.Background(), fakeContainerID)
	if err == nil || !strings.Contains(err.Error(), "daemon unavailable during inspect") {
		t.Fatalf("inspect failure must not be treated as deleted: %v", err)
	}
}

func TestDockerWaitReportsPeerFailureAndLogs(t *testing.T) {
	for _, code := range []string{"0", "7", "invalid"} {
		t.Run(code, func(t *testing.T) {
			d, _ := fakeDocker(t, map[string]string{"EXIT": code})
			err := d.wait(context.Background(), fakeContainerID)
			if code == "0" && err != nil {
				t.Fatal(err)
			}
			if code == "7" && (err == nil || !strings.Contains(err.Error(), "code 7") || !strings.Contains(err.Error(), "qdisc kind is unknown")) {
				t.Fatalf("error = %v", err)
			}
			if code == "invalid" && (err == nil || !strings.Contains(err.Error(), "invalid exit code")) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestDockerCheckRequiresSwarmAttachableOverlay(t *testing.T) {
	for _, test := range []struct {
		name, network, response string
		valid                   bool
	}{
		{"bridge", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"bridge","Scope":"local"}]`, false},
		{"overlay", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"overlay","Attachable":true,"Scope":"swarm"}]`, true},
		{"local-overlay", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"overlay","Attachable":true,"Scope":"local"}]`, false},
		{"missing-scope", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"overlay","Attachable":true}]`, false},
		{"host", "host", "", false},
		{"builtin", "bridge", "", false},
		{"builtin-id", "network-id", `[{"Name":"bridge","Driver":"bridge"}]`, false},
		{"missing-name", "network-id", `[{"Driver":"bridge"}]`, false},
		{"non-attachable", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"overlay","Attachable":false,"Scope":"swarm"}]`, false},
		{"host-driver", "kpl-v3-peers", `[{"Name":"kpl-v3-peers","Driver":"host"}]`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, _ := fakeDocker(t, map[string]string{"NETWORK": test.response})
			d.network = test.network
			err := d.check(context.Background())
			if (err == nil) != test.valid {
				t.Fatalf("check error = %v, valid = %v", err, test.valid)
			}
		})
	}
}

func TestDockerNetworkIDResolvesForCreationAndReconciliation(t *testing.T) {
	d, logPath := fakeDocker(t, nil)
	d.network = strings.Repeat("a", 64)
	if err := d.check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.network != "kpl-v3-peers" {
		t.Fatalf("network identity was not resolved: %q", d.network)
	}
	node, config := dockerTestConfig(t, false)
	_, apiURL, err := d.create(context.Background(), node, config)
	if err != nil || apiURL != "http://172.25.0.8:18000" {
		t.Fatalf("create using network ID = %q, %v", apiURL, err)
	}
	if err := d.removeAgentContainers(context.Background(), node.AgentID); err != nil {
		t.Fatal(err)
	}
	for _, call := range dockerCalls(t, logPath) {
		joined := strings.Join(call.Args, " ")
		switch call.Args[0] {
		case "network":
			if len(call.Args) != 5 || call.Args[2] != "--format" || strings.Contains(call.Args[3], ".Containers") {
				t.Fatalf("network check must select metadata without the growing container inventory: %v", call.Args)
			}
		case "create":
			if !strings.Contains(joined, "--network kpl-v3-peers") || !strings.Contains(joined, "--label io.kpl.network=kpl-v3-peers") {
				t.Fatalf("create did not use canonical network identity: %v", call.Args)
			}
		case "ps":
			if !strings.Contains(joined, "--filter label=io.kpl.network=kpl-v3-peers") {
				t.Fatalf("reconciliation did not use canonical network identity: %v", call.Args)
			}
		}
	}
}

func TestDockerReconcileOnlyListsManagedAgentContainers(t *testing.T) {
	d, logPath := fakeDocker(t, map[string]string{"PS": fakeContainerID + "\n"})
	if err := d.removeAgentContainers(context.Background(), "agent-a"); err != nil {
		t.Fatal(err)
	}
	calls := dockerCalls(t, logPath)
	if len(calls) != 2 {
		t.Fatalf("calls = %v", calls)
	}
	if want := []string{"ps", "--all", "--quiet", "--no-trunc", "--filter", "label=io.kpl.managed=true", "--filter", "label=io.kpl.agent=agent-a", "--filter", "label=io.kpl.network=kpl-v3-peers"}; !reflect.DeepEqual(calls[0].Args, want) {
		t.Fatalf("list args = %v", calls[0].Args)
	}
	if calls[1].Args[2] != fakeContainerID {
		t.Fatalf("remove args = %v", calls[1].Args)
	}
}

func TestDockerReconcileInventoryExceedsDiagnosticOutputLimit(t *testing.T) {
	const count = 600 // 39 KB of full IDs exceeds the 32 KiB diagnostic limit.
	d, _ := fakeDocker(t, map[string]string{"PS_COUNT": strconv.Itoa(count)})
	ids, err := d.agentContainerIDs(context.Background(), "agent-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != count {
		t.Fatalf("container inventory was truncated: got %d IDs, want %d", len(ids), count)
	}
	for i, id := range ids {
		if want := fmt.Sprintf("%064x", i); id != want {
			t.Fatalf("inventory ID %d = %q, want %q", i, id, want)
		}
	}
}

func TestDockerReconcileRejectsMalformedInventoryBeforeRemovingPeers(t *testing.T) {
	d, logPath := fakeDocker(t, map[string]string{"PS": fakeContainerID + "\ntruncated-id\n"})
	err := d.removeAgentContainers(context.Background(), "agent-a")
	if err == nil || !strings.Contains(err.Error(), "invalid container ID") {
		t.Fatalf("malformed inventory error = %v", err)
	}
	if calls := dockerCalls(t, logPath); len(calls) != 1 || calls[0].Args[0] != "ps" {
		t.Fatalf("malformed inventory caused partial deletion: %v", calls)
	}
}

func TestDockerPeerLogsRemainBounded(t *testing.T) {
	d, _ := fakeDocker(t, map[string]string{"LOG_BYTES": "65536"})
	output, err := d.run(context.Background(), nil, "logs", "--tail", "30", fakeContainerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 32*1024 {
		t.Fatalf("peer logs length = %d, want diagnostic limit 32768", len(output))
	}
}

func TestDockerCheckRejectsInactiveSwarmBeforeInspectingResources(t *testing.T) {
	for _, response := range []string{
		`{"OSType":"linux","SwarmState":"inactive"}`,
		`{"OSType":"linux","SwarmState":"pending"}`,
		`{"OSType":"linux","SwarmState":"locked"}`,
		`{"OSType":"linux"}`,
		`{"OSType":"windows","SwarmState":"active"}`,
		`invalid`,
	} {
		t.Run(response, func(t *testing.T) {
			d, path := fakeDocker(t, map[string]string{"INFO": response})
			if err := d.check(context.Background()); err == nil {
				t.Fatal("accepted an unsupported Docker daemon")
			}
			calls := dockerCalls(t, path)
			if len(calls) != 1 || calls[0].Args[0] != "info" {
				t.Fatalf("unsupported daemon reached image or network inspection: %v", calls)
			}
		})
	}
}

func TestDockerReconcileSlowContainerDoesNotBlockOtherCleanup(t *testing.T) {
	const count = 10
	d, path := fakeDocker(t, map[string]string{"PS_COUNT": strconv.Itoa(count), "HANG_ID": fmt.Sprintf("%064x", 0)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.removeAgentContainers(ctx, "agent-a") }()
	defer func() { cancel(); <-done }()
	waitDockerRemovals(t, path, count)
	select {
	case err := <-done:
		done <- err
		t.Fatalf("cleanup completed while a container was still blocked: %v", err)
	default:
	}
}

func TestDockerReconcileBoundsConcurrencyAndHonorsCancellation(t *testing.T) {
	d, path := fakeDocker(t, map[string]string{"PS_COUNT": "12", "HANG": "rm"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.removeAgentContainers(ctx, "agent-a") }()
	waitDockerRemovals(t, path, dockerReconcileWorkers)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled cleanup was acknowledged: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup cleanup did not honor cancellation")
	}
	removals := 0
	for _, call := range dockerCalls(t, path) {
		if call.Args[0] == "rm" {
			removals++
		}
	}
	if removals != dockerReconcileWorkers {
		t.Fatalf("started %d simultaneous removals, want %d", removals, dockerReconcileWorkers)
	}
}

func waitDockerRemovals(t *testing.T, path string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for _, call := range dockerCalls(t, path) {
			if call.Args[0] == "rm" {
				count++
			}
		}
		if count >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fewer than %d Peer removals started", want)
}
