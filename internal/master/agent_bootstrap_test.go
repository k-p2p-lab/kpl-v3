package master

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

type bootstrapRunner struct {
	calls []Command
	read  func(context.Context, Command) ([]byte, error)
}

func (r *bootstrapRunner) Run(context.Context, Command) error {
	return errors.New("bootstrap must not launch another kpl executable")
}
func (r *bootstrapRunner) Output(ctx context.Context, c Command) ([]byte, error) {
	r.calls = append(r.calls, c)
	return r.read(ctx, c)
}

func bootstrapFixture(t *testing.T) (*App, *bootstrapRunner) {
	t.Helper()
	a, _, _ := appFixture(t)
	a.Environ = []string{"KPL_SWARM_TASK_NAME=lab_agent.node.task", "KPL_SWARM_SERVICE_NAME=lab_agent", "KPL_SWARM_NODE_ID=node1", "KPL_SWARM_NODE_HOSTNAME=worker1", "KPL_PEER_NETWORK=lab-peers", "KPL_CONTROL_NETWORK=lab_control", "DOCKER_CONTEXT=local"}
	r := &bootstrapRunner{}
	r.read = func(ctx context.Context, c Command) ([]byte, error) {
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
			t.Fatal("Docker read has no bounded deadline")
		}
		if c.Name != "docker" || c.CancelGrace != time.Second {
			t.Fatalf("unexpected bootstrap process: %+v", c)
		}
		switch {
		case c.Args[0] == "info":
			return []byte("192.0.2.9"), nil
		case c.Args[4] == "{{.Image}}":
			return []byte("sha256:" + strings.Repeat("a", 64)), nil
		default:
			return []byte("lab_monitoring 10.1.0.8\nlab-peers 10.2.0.9\nlab_control 10.3.0.9\n"), nil
		}
	}
	a.Runner = r
	return a, r
}
func bootstrapValue(args []string, key string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == key {
			return args[i+1]
		}
	}
	return ""
}
func noBootstrapDelay(context.Context, time.Duration) error { return nil }

func TestSwarmAgentBootstrapUsesControlAddressAndLocalImage(t *testing.T) {
	a, r := bootstrapFixture(t)
	args, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"--id": "lab_agent-node1", "--name": "worker1", "--advertise-url": "http://10.3.0.9:8090", "--self-url": "http://10.3.0.9:8090", "--metrics-url": "http://192.0.2.9:9091/metrics", "--docker-image": "sha256:" + strings.Repeat("a", 64), "--docker-network": "lab-peers", "--control-network": "lab_control", "--capacity": "20"} {
		if got := bootstrapValue(args, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if !slices.Contains(args, "--auto-reset-network") || len(r.calls) != 3 {
		t.Fatal("automatic initialization or discovery changed")
	}
	a.Environ = append(a.Environ, "KPL_AGENT_METRICS_PORT=19091")
	original := r.read
	r.read = func(ctx context.Context, c Command) ([]byte, error) {
		if c.Args[0] == "info" {
			return []byte("2001:db8::9"), nil
		}
		return original(ctx, c)
	}
	args, err = a.swarmAgentArguments(t.Context(), noBootstrapDelay)
	if err != nil || bootstrapValue(args, "--metrics-url") != "http://[2001:db8::9]:19091/metrics" {
		t.Fatalf("IPv6 host metrics: %v %v", args, err)
	}
}

func TestSwarmAgentBootstrapRetriesOnlyIncompleteDiscovery(t *testing.T) {
	for _, stage := range []string{"address", "node", "image"} {
		t.Run(stage, func(t *testing.T) {
			a, r := bootstrapFixture(t)
			original := r.read
			attempts := 0
			pauses := 0
			r.read = func(ctx context.Context, c Command) ([]byte, error) {
				target := stage == "node" && c.Args[0] == "info" || stage == "image" && c.Args[0] == "inspect" && c.Args[4] == "{{.Image}}" || stage == "address" && c.Args[0] == "inspect" && c.Args[4] != "{{.Image}}"
				if target {
					attempts++
					if attempts < 3 {
						return nil, errors.New("metadata not ready")
					}
				}
				return original(ctx, c)
			}
			pause := func(_ context.Context, d time.Duration) error {
				if d != 2*time.Second {
					t.Fatal(d)
				}
				pauses++
				return nil
			}
			if _, err := a.swarmAgentArguments(t.Context(), pause); err != nil || attempts != 3 || pauses != 2 {
				t.Fatalf("bounded retry: %d %d %v", attempts, pauses, err)
			}
		})
	}
	a, r := bootstrapFixture(t)
	r.read = func(context.Context, Command) ([]byte, error) { return nil, errors.New("offline") }
	if _, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay); err == nil || len(r.calls) != 5 {
		t.Fatalf("unbounded discovery: %d %v", len(r.calls), err)
	}
}

func TestSwarmAgentBootstrapRejectsAmbiguousMetadataAndBadSettings(t *testing.T) {
	for _, setting := range []string{"KPL_AGENT_METRICS_PORT=0", "KPL_AGENT_METRICS_PORT=65536", "KPL_CONTROL_NETWORK=lab-peers", "KPL_SWARM_NODE_ID="} {
		a, r := bootstrapFixture(t)
		a.Environ = append(a.Environ, setting)
		if _, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay); err == nil || len(r.calls) != 0 {
			t.Fatalf("invalid setting reached Docker: %s %v", setting, err)
		}
	}
	for _, rows := range []string{"lab_control 10.3.0.9\nlab_control 10.3.0.10", "lab_control 999.1.1.1", "lab_control 2001:db8::1"} {
		a, r := bootstrapFixture(t)
		r.read = func(context.Context, Command) ([]byte, error) { return []byte(rows), nil }
		if _, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay); err == nil || len(r.calls) != 1 {
			t.Fatalf("ambiguous control address accepted/retried: %q %v", rows, err)
		}
	}
	a, r := bootstrapFixture(t)
	original := r.read
	r.read = func(ctx context.Context, c Command) ([]byte, error) {
		if len(c.Args) > 4 && c.Args[4] == "{{.Image}}" {
			return []byte("registry/image:tag"), nil
		}
		return original(ctx, c)
	}
	if _, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay); err == nil {
		t.Fatal("mutable image accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.swarmAgentArguments(ctx, noBootstrapDelay); !errors.Is(err, context.Canceled) {
		t.Fatal(fmt.Sprint(err))
	}
}
