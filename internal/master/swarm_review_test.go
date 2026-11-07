package master

import (
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSwarmDeployInsufficientAgentSelectionDoesNotChangePlacement(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "deploy")
	s.config["KPL_MIN_AGENTS"] = "2"
	s.a = s.a.WithConfig(s.config)
	model := &swarmLifecycleModel{paused: map[string]bool{}}
	placementAdded := false
	runner.handle = func(ctx context.Context, command Command) (string, error) {
		if command.Args[0] == "node" && command.Args[1] == "ls" {
			if placementAdded {
				return "control", nil
			}
			return "", nil
		}
		if command.Args[0] == "node" && command.Args[1] == "update" {
			placementAdded = true
		}
		return model.handle(ctx, command)
	}
	err := s.a.RunSwarm(t.Context(), "deploy", []string{"new-worker"})
	if err == nil || !strings.Contains(err.Error(), "at least 2") {
		t.Fatalf("insufficient placement was accepted or misreported: %v", err)
	}
	if swarmCallIndex(runner.calls, "node", "update") >= 0 || swarmCallIndex(runner.calls, "network", "create") >= 0 || swarmCallIndex(runner.calls, "stack", "deploy") >= 0 {
		t.Fatal("known-insufficient deployment modified placement before refusing admission")
	}
}

func TestSwarmDeployRechecksOwnershipAfterImagePull(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "deploy")
	model := &swarmLifecycleModel{paused: map[string]bool{}}
	pulled := false
	runner.handle = func(ctx context.Context, command Command) (string, error) {
		if command.Args[0] == "pull" {
			pulled = true
		}
		if pulled && command.Args[0] == "service" && command.Args[1] == "inspect" {
			return "kpl_controller|unrelated-application", nil
		}
		return model.handle(ctx, command)
	}
	if err := s.a.RunSwarm(t.Context(), "deploy", []string{"new-worker"}); err == nil {
		t.Fatal("deploy accepted a service replaced by another application while pulling")
	}
	for _, operation := range [][]string{{"service", "update"}, {"node", "update"}, {"network", "create"}, {"stack", "deploy"}} {
		if swarmCallIndex(runner.calls, operation...) >= 0 {
			t.Fatalf("deploy changed cluster state after service ownership changed: %v", operation)
		}
	}
}

func TestSwarmRemoveDoesNotDeleteReplacementServices(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "remove")
	s.serviceIDs = []string{"original"}
	removed := false
	runner.handle = func(_ context.Context, command Command) (string, error) {
		if command.Args[0] == "service" && command.Args[1] == "rm" {
			removed = true
			return "", nil
		}
		if command.Args[0] == "service" && command.Args[1] == "ls" && removed {
			return "replacement", nil
		}
		return "", nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := s.remove(ctx); err == nil {
		t.Fatal("remove accepted replacement services outside the checked ID set")
	}
	if swarmCallIndex(runner.calls, "stack", "rm") >= 0 {
		t.Fatal("stack removal targeted services whose ownership was never checked")
	}
}

func TestSwarmRemoveNodeAllowsDrainedAndPausedHostsOnlyWithCleanExit(t *testing.T) {
	for _, test := range []struct {
		name, availability, state string
		clean, wantSuccess        bool
	}{
		{"active", "active", "ready", true, true},
		{"pause", "pause", "ready", true, true},
		{"drain", "drain", "ready", true, true},
		{"unclean-drain", "drain", "ready", false, false},
		{"down-drain", "drain", "down", true, false},
		{"unknown-availability", "unknown", "ready", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "remove-node")
			s.present["agent"] = true
			s.options.nodes = []string{"selected-worker"}
			stopped := false
			runner.handle = func(_ context.Context, command Command) (string, error) {
				args := command.Args
				if args[0] == "node" && args[1] == "inspect" {
					switch args[3] {
					case "{{.ID}}":
						return "selectednode", nil
					case "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}":
						return "linux " + test.state + " " + test.availability, nil
					case "{{.Description.Platform.OS}} {{.Status.State}}":
						return "linux " + test.state, nil
					case "{{.Status.State}}":
						return test.state, nil
					}
				}
				if args[0] == "service" && args[1] == "ps" {
					if !slices.Contains(args, "node=selectednode") {
						t.Fatal("cleanup captured a host outside the selected node")
					}
					return "task1", nil
				}
				if args[0] == "inspect" {
					if stopped {
						if !test.clean {
							return "selectednode|shutdown|container|0|1|error", nil
						}
						return "selectednode|shutdown|container|0|0|ok", nil
					}
					return "selectednode|running|container|123|0|ok", nil
				}
				if args[0] == "service" && args[1] == "update" && slices.Contains(args, "--constraint-add") {
					stopped = true
				}
				return "", nil
			}
			err := s.removeNodes(t.Context())
			if (err == nil) != test.wantSuccess {
				t.Fatalf("availability %s, state %s, clean %t: %v", test.availability, test.state, test.clean, err)
			}
			if removed := swarmCallIndex(runner.calls, "node", "update", "--label-rm") >= 0; removed != test.wantSuccess {
				t.Fatal("placement label changed without a supported host and clean exit")
			}
		})
	}
}

func TestSwarmRemoveWithoutServicesIsIdempotentAndPreservesConfiguration(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "remove")
	before, err := os.ReadFile(s.a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	model := &swarmLifecycleModel{paused: map[string]bool{}, removed: true}
	runner.handle = model.handle
	for attempt := 0; attempt < 2; attempt++ {
		if err := s.a.RunSwarm(t.Context(), "remove", nil); err != nil {
			t.Fatalf("idempotent removal attempt %d: %v", attempt, err)
		}
	}
	after, err := os.ReadFile(s.a.EnvFile)
	if err != nil || string(after) != string(before) {
		t.Fatal("removing an absent stack modified the saved configuration")
	}
	for _, operation := range [][]string{{"service", "rm"}, {"volume", "rm"}, {"network", "rm"}} {
		if swarmCallIndex(runner.calls, operation...) >= 0 {
			t.Fatalf("idempotent remove attempted to remove retained state: %v", operation)
		}
	}
}

func TestSwarmNetworkCreateRetryRequiresAnExactDaemonRefusal(t *testing.T) {
	conflict := "Error response from daemon: network with name kpl-peers already exists\n"
	for _, test := range []struct {
		name, diagnostic string
		err              error
		wantRefused      bool
	}{
		{"daemon-refusal", conflict, fixtureNetworkCreateExitError{}, true},
		{"error-text-only", "", errors.New(strings.TrimSpace(conflict)), false},
		{"transport-after-text", conflict, errors.New("connection closed before acknowledgement"), false},
		{"wrong-network", strings.ReplaceAll(conflict, "kpl-peers", "unrelated-network"), fixtureNetworkCreateExitError{}, false},
		{"permission-denied", "permission denied\n", fixtureNetworkCreateExitError{}, false},
		{"mixed-diagnostics", conflict + "another error\n", fixtureNetworkCreateExitError{}, false},
		{"deadline", conflict, context.DeadlineExceeded, false},
		{"oversized", conflict + strings.Repeat("x", networkCreateDiagnosticLimit), fixtureNetworkCreateExitError{}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "reset-peer-network")
			runner.handle = func(_ context.Context, command Command) (string, error) {
				_, _ = io.WriteString(command.Stderr, test.diagnostic)
				return "", test.err
			}
			_, err, refused := s.tryCreateResetPeerNetwork(t.Context(), []string{"network", "create", s.network})
			if err == nil || refused != test.wantRefused {
				t.Fatalf("unsafe retry classification for %s: refused=%t err=%v", test.name, refused, err)
			}
			if test.name == "oversized" && !errors.Is(err, errNetworkCreateDiagnosticLimit) {
				t.Fatal("oversized diagnostic was accepted as complete refusal evidence")
			}
			if test.name == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("deadline error was not preserved")
			}
		})
	}
}
