package master

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func preflightFixture(t *testing.T) (*App, *configRunner, Config) {
	t.Helper()
	a, runner, _ := configApp(t)
	c := ConfigDefaults(Config{"KPL_STACK_NAME": "other-stack_2", "KPL_CONTROL_NODE_ID": "control1", "KPL_IMAGE": "registry/team:v3@sha256:" + strings.Repeat("a", 64), "KPL_USER": "operator", "KPL_PASSWORD": "fixture-private", "KPL_MIN_AGENTS": "2"})
	runner.respond = func(ctx context.Context, cmd Command) (string, error) {
		if !strings.Contains(strings.Join(cmd.Env, "\n"), "DOCKER_CONTEXT=fixture-manager") {
			t.Fatal("Docker context was lost")
		}
		switch strings.Join(cmd.Args, " ") {
		case "info --format {{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}":
			return "active true", nil
		case "node inspect --format {{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} {{.Spec.Role}} control1":
			return "control1 linux ready active manager", nil
		case "network inspect --format {{.Name}} {{.Driver}} {{.Attachable}} other-stack_2-peers":
			return "other-stack_2-peers overlay true", nil
		case "network inspect --format {{range .IPAM.Config}}{{println .Subnet}}{{end}} other-stack_2-peers":
			return "10.11.0.0/24", nil
		case "node ls --quiet --filter node.label=kpl.other-stack_2.agent=true":
			return "worker1\nworker2\nworker3", nil
		case "node inspect --format {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} worker1 worker2 worker3":
			return "linux ready active\nlinux ready active\nwindows down drain", nil
		default:
			return "", fmt.Errorf("unexpected Docker call: %v", cmd.Args)
		}
	}
	return a, runner, c
}

func TestPreflightNativeReadOnlyAndScopedLabels(t *testing.T) {
	a, runner, c := preflightFixture(t)
	if err := a.Preflight(t.Context(), c, false); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 5 {
		t.Fatalf("unexpected query count: %d", len(runner.calls))
	}
	for _, cmd := range runner.calls {
		joined := strings.Join(cmd.Args, " ")
		if strings.Contains(joined, "fixture-private") || strings.Contains(joined, "node.label=kpl.agent=") || strings.Contains(joined, " pull ") {
			t.Fatal("preflight exposed credentials or changed scope")
		}
	}
	c["KPL_PEER_SUBNET"] = "10.11.0.0/24"
	if err := a.Preflight(t.Context(), c, false); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 11 {
		t.Fatal("subnet check was not added")
	}
}

func TestPreflightRejectsBeforeDockerAndSupportsConfigOnly(t *testing.T) {
	for key, value := range map[string]string{"KPL_CONTROL_NODE_ID": "bad/id", "KPL_IMAGE": "repo", "KPL_USER": " bad", "KPL_PASSWORD": "", "KPL_MIN_AGENTS": "0", "KPL_IMAGE_PULL_TIMEOUT": "08", "KPL_AGENT_METRICS_PORT": "8080", "KPL_PEER_SUBNET": "10.11.0.1/24"} {
		a, runner, c := preflightFixture(t)
		c[key] = value
		if err := a.Preflight(t.Context(), c, false); err == nil {
			t.Errorf("invalid %s accepted", key)
		}
		if len(runner.calls) != 0 {
			t.Fatalf("invalid %s contacted Docker", key)
		}
	}
	a, runner, c := preflightFixture(t)
	if err := a.Preflight(t.Context(), c, true); err != nil || len(runner.calls) != 0 {
		t.Fatal("config-only check contacted Docker or failed")
	}
}

func TestPreflightRejectsUnhealthyOrMalformedInventory(t *testing.T) {
	cases := []struct{ match, response string }{
		{"info --format", "inactive false"},
		{"{{.ID}}", "control1 linux ready active worker"},
		{"{{.Name}}", "other-stack_2-peers bridge true"},
		{"node ls", ""},
		{"node ls", "worker1\nworker1"},
		{"node ls", "--all"},
		{"{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} worker1", "linux ready active\nlinux down active\nwindows ready active"},
		{"{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} worker1", "linux ready active"},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			a, runner, c := preflightFixture(t)
			old := runner.respond
			runner.respond = func(ctx context.Context, cmd Command) (string, error) {
				if strings.Contains(strings.Join(cmd.Args, " "), tc.match) {
					return tc.response, nil
				}
				return old(ctx, cmd)
			}
			if err := a.Preflight(t.Context(), c, false); err == nil {
				t.Fatal("unhealthy or malformed inventory accepted")
			}
		})
	}
	a, runner, c := preflightFixture(t)
	runner.respond = func(context.Context, Command) (string, error) { return "", errors.New("fixture Docker offline") }
	if err := a.Preflight(t.Context(), c, false); err == nil {
		t.Fatal("Docker error ignored")
	}
	if len(runner.calls) != 1 {
		t.Fatal("preflight continued after Docker failed")
	}
}
