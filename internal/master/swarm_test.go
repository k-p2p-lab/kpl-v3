package master

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

type swarmTestRunner struct {
	calls  []Command
	handle func(context.Context, Command) (string, error)
}

func (r *swarmTestRunner) record(ctx context.Context, c Command) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.calls = append(r.calls, c)
	if c.Name != "docker" {
		return "", fmt.Errorf("unexpected executable: %s", c.Name)
	}
	if r.handle != nil {
		return r.handle(ctx, c)
	}
	return "", nil
}
func (r *swarmTestRunner) Run(ctx context.Context, c Command) error {
	_, err := r.record(ctx, c)
	return err
}
func (r *swarmTestRunner) Output(ctx context.Context, c Command) ([]byte, error) {
	out, err := r.record(ctx, c)
	return []byte(out), err
}
func newSwarmFixture(t *testing.T, name string) (*swarm, *swarmTestRunner, *bytes.Buffer) {
	t.Helper()
	repo := t.TempDir()
	buffer := new(bytes.Buffer)
	runner := new(swarmTestRunner)
	for name, data := range map[string]string{"Dockerfile": "FROM scratch\n", ".dockerignore": ".env\n.env.*\n", ".env.swarm": "# test fixture\n", "stack.swarm.yaml": "services: {}\n"} {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{"KPL_IMAGE": "registry.example.test/lab:v3", "KPL_CONTROL_NODE_ID": "control", "KPL_PASSWORD": "test-only-password", "GRAFANA_ADMIN_PASSWORD": "test-only-grafana"}
	ConfigDefaults(cfg)
	a := &App{Repo: repo, EnvFile: filepath.Join(repo, ".env.swarm"), Runner: runner, Stdout: buffer, Stderr: buffer, Stdin: strings.NewReader(""), Environ: []string{"DOCKER_CONTEXT=manager", "DOCKER_DEFAULT_PLATFORM=linux/arm64"}}
	a = a.WithConfig(cfg)
	s := &swarm{a: a, config: cfg, options: swarmOptions{name: name, component: "controller", tail: "100"}, stack: "kpl", network: "kpl-peers", label: "kpl.kpl.agent", timeout: time.Minute, pullTimeout: 5 * time.Minute, buildTimeout: 30 * time.Minute, pushTimeout: 10 * time.Minute, agentTimeout: 4 * time.Minute, controllerTimeout: 11 * time.Minute, present: map[string]bool{}}
	return s, runner, buffer
}
func swarmCallIndex(calls []Command, parts ...string) int {
	for i, c := range calls {
		if len(c.Args) >= len(parts) && slices.Equal(c.Args[:len(parts)], parts) {
			return i
		}
	}
	return -1
}
func swarmCommandEnv(c Command, key string) string {
	for _, entry := range c.Env {
		if strings.HasPrefix(entry, key+"=") {
			return strings.TrimPrefix(entry, key+"=")
		}
	}
	return ""
}

func TestSwarmRejectsAmbiguousArgumentsBeforeConfigOrDocker(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"deploy", []string{"--workers", "node1"}}, {"restart", []string{"--all", "--workers"}}, {"publish", []string{"--platforms", "windows/amd64"}},
		{"publish", []string{"--platforms", "linux/amd64,"}}, {"logs", []string{"controller", "--context", "worker"}}, {"logs", []string{"auth", "--tail", "01"}},
		{"remove", []string{"--force"}}, {"add-node", nil}, {"remove-node", nil}, {"deploy", []string{"--bogus"}},
	} {
		t.Run(tc.name+strings.Join(tc.args, "_"), func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, tc.name)
			s.a.EnvFile = filepath.Join(t.TempDir(), "missing")
			s.a.EnvFileExplicit = true
			if err := s.a.RunSwarm(t.Context(), tc.name, tc.args); err == nil {
				t.Fatal("invalid input accepted")
			}
			if len(r.calls) > 0 {
				t.Fatal("invalid input contacted Docker")
			}
		})
	}
}

func TestSwarmPublishUsesNativeLiteralArgvAndProtectsSecrets(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "publish")
	s.a.Repo = filepath.Join(t.TempDir(), "repo $(literal) ' spaces")
	if err := os.MkdirAll(s.a.Repo, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.a.Repo, "Dockerfile"), []byte("FROM scratch\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// The configured file is outside the build context, and scripts/ is absent.
	if err := s.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 2 || !slices.Equal(r.calls[0].Args, []string{"build", "--tag", s.config["KPL_IMAGE"], s.a.Repo}) || r.calls[1].Args[0] != "push" {
		t.Fatalf("unexpected publish commands: %+v", r.calls)
	}
	for _, call := range r.calls {
		if swarmCommandEnv(call, "DOCKER_DEFAULT_PLATFORM") != "" {
			t.Fatal("build inherited default platform")
		}
	}
	t.Run("unsafe-config", func(t *testing.T) {
		s, r, _ := newSwarmFixture(t, "publish")
		s.a.EnvFile = filepath.Join(s.a.Repo, "secrets.conf")
		if err := os.WriteFile(s.a.EnvFile, []byte("secret\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := s.publish(t.Context()); err == nil {
			t.Fatal("unignored config entered build context")
		}
		if len(r.calls) > 0 {
			t.Fatal("unsafe build reached Docker")
		}
	})
	t.Run("symlink-target", func(t *testing.T) {
		s, r, _ := newSwarmFixture(t, "publish")
		target := filepath.Join(s.a.Repo, "secrets.conf")
		if err := os.WriteFile(target, []byte("secret\n"), 0600); err != nil {
			t.Fatal(err)
		}
		s.a.EnvFile = filepath.Join(t.TempDir(), "external.env")
		if err := os.Symlink(target, s.a.EnvFile); err != nil {
			t.Fatal(err)
		}
		if err := s.publish(t.Context()); err == nil {
			t.Fatal("external symlink bypassed config guard")
		}
		if len(r.calls) > 0 {
			t.Fatal("unsafe build reached Docker")
		}
	})
	t.Run("failed-build", func(t *testing.T) {
		s, r, _ := newSwarmFixture(t, "publish")
		r.handle = func(context.Context, Command) (string, error) { return "", errors.New("build failed") }
		if err := s.publish(t.Context()); err == nil {
			t.Fatal("build failure ignored")
		}
		if len(r.calls) != 1 {
			t.Fatal("push ran after failed build")
		}
	})
	t.Run("multi-platform", func(t *testing.T) {
		s, r, _ := newSwarmFixture(t, "publish")
		s.options.platforms = "linux/amd64,linux/arm64"
		if err := s.publish(t.Context()); err != nil {
			t.Fatal(err)
		}
		if len(r.calls) != 1 || !slices.Equal(r.calls[0].Args, []string{"buildx", "build", "--platform", s.options.platforms, "--tag", s.config["KPL_IMAGE"], "--push", s.a.Repo}) {
			t.Fatal("wrong multi-platform invocation")
		}
	})
}

func TestSwarmImageResolutionRejectsAmbiguousDigestsAndDoesNotSaveTag(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, output := range []string{"already up to date", "Digest: " + digest + "\nDigest: " + digest, "Digest: sha256:BAD", " Digest: " + digest + "\n", "Digest: " + digest + " \n"} {
		s, r, _ := newSwarmFixture(t, "deploy")
		before := s.config["KPL_IMAGE"]
		r.handle = func(context.Context, Command) (string, error) { return output, nil }
		if err := s.resolveImage(t.Context()); err == nil {
			t.Fatalf("accepted %q", output)
		}
		if s.config["KPL_IMAGE"] != before || len(r.calls) != 1 {
			t.Fatal("failed resolution changed configuration or continued")
		}
	}
	s, r, _ := newSwarmFixture(t, "deploy")
	before, err := os.ReadFile(s.a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	r.handle = func(context.Context, Command) (string, error) {
		return "Status: up to date\nDigest: " + digest + "\n", nil
	}
	if err := s.resolveImage(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s.config["KPL_IMAGE"] != "registry.example.test/lab@"+digest {
		t.Fatal("tag was not resolved")
	}
	after, _ := os.ReadFile(s.a.EnvFile)
	if !bytes.Equal(before, after) {
		t.Fatal("resolved digest overwrote saved mutable tag")
	}
}

func TestSwarmSelectorsDeduplicateAndPreserveRemovalReadiness(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "deploy")
	s.options.nodes = []string{"host", "node1", "other"}
	r.handle = func(_ context.Context, c Command) (string, error) {
		if c.Args[len(c.Args)-1] == "other" {
			return "node2", nil
		}
		return "node1", nil
	}
	nodes, err := s.commandNodes(t.Context())
	if err != nil || !slices.Equal(nodes, []string{"node1", "node2"}) {
		t.Fatalf("explicit selection: %v %v", nodes, err)
	}
	s.options = swarmOptions{name: "deploy", selector: "--workers"}
	r.handle = func(_ context.Context, c Command) (string, error) {
		if c.Args[0] == "node" && c.Args[1] == "ls" {
			return "node1 node2 node3", nil
		}
		switch c.Args[len(c.Args)-1] {
		case "node1":
			return "node1|worker|linux|ready|active", nil
		case "node2":
			return "node2|worker|linux|down|active", nil
		default:
			return "node3|manager|linux|ready|active", nil
		}
	}
	nodes, err = s.commandNodes(t.Context())
	if err != nil || !slices.Equal(nodes, []string{"node1"}) {
		t.Fatalf("eligible selection: %v %v", nodes, err)
	}
	s.options.name = "remove-node"
	nodes, err = s.commandNodes(t.Context())
	if err != nil || !slices.Equal(nodes, []string{"node1", "node2"}) {
		t.Fatalf("removal must retain unavailable owned nodes: %v %v", nodes, err)
	}
}

func TestSwarmWebLogsUseSeparateContextWithoutShell(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "logs")
	s.present["controller"] = true
	s.options.component = "auth"
	s.options.logContext = "worker context"
	s.options.tail = "200"
	r.handle = func(_ context.Context, c Command) (string, error) {
		switch {
		case slices.Equal(c.Args[:min(len(c.Args), 2)], []string{"service", "ps"}):
			return "task1", nil
		case c.Args[0] == "inspect":
			return "worker1|running|container1", nil
		case len(c.Args) > 2 && c.Args[0] == "--context" && c.Args[2] == "info":
			return "worker1", nil
		case len(c.Args) > 2 && c.Args[2] == "exec":
			return "", nil
		}
		return "", fmt.Errorf("unexpected call: %v", c.Args)
	}
	if err := s.logs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if r.calls[0].Args[0] != "service" || swarmCommandEnv(r.calls[0], "DOCKER_CONTEXT") != "manager" {
		t.Fatal("task discovery switched away from manager")
	}
	want := []string{"--context", "worker context", "exec", "container1", "tail", "-n", "200", "--", "/var/lib/kpl/data/logs/auth.jsonl"}
	if !slices.Equal(r.calls[len(r.calls)-1].Args, want) {
		t.Fatalf("wrong log read: %v", r.calls[len(r.calls)-1].Args)
	}
}

func TestSwarmTaskEvidenceRejectsFailuresAndKeepsHistoricalFailures(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "restart")
	r.handle = func(_ context.Context, c Command) (string, error) {
		if c.Args[0] == "service" {
			return "newest older failed", nil
		}
		if c.Args[0] == "node" {
			return "linux ready", nil
		}
		switch c.Args[len(c.Args)-1] {
		case "newest":
			return "node1|shutdown|newcontainer|0|0|ok", nil
		case "older":
			return "node1|shutdown|oldcontainer|0|0|ok", nil
		default:
			return "node1|failed|badcontainer|0|1|error", nil
		}
	}
	ids, err := s.captureTasks(t.Context(), "kpl_agent", "")
	if err != nil || !slices.Equal(ids, []string{"newest", "failed"}) {
		t.Fatalf("historical failure lost: %v %v", ids, err)
	}
	for _, record := range []string{"node1|shutdown|-|0|0|ok", "node1|shutdown|container|0|137|ok", "node1|failed|container|0|1|error", "node1|shutdown|container|0|0|error"} {
		r.handle = func(_ context.Context, c Command) (string, error) {
			if c.Args[0] == "node" {
				return "ready", nil
			}
			return record, nil
		}
		if err := s.waitTasks(t.Context(), []string{"task1"}, time.Second); err == nil {
			t.Fatalf("unclean task accepted: %s", record)
		}
	}
}

// Simulates task transitions only when the native program changes placement.
// This verifies ordering rather than duplicating the implementation's branches.
type swarmLifecycleModel struct {
	paused              map[string]bool
	removed             bool
	failStop, failBuild bool
}

func (m *swarmLifecycleModel) handle(_ context.Context, c Command) (string, error) {
	a := c.Args
	joined := strings.Join(a, " ")
	last := a[len(a)-1]
	if a[0] == "build" {
		if m.failBuild {
			return "", errors.New("build failed")
		}
		return "", nil
	}
	if a[0] == "push" {
		return "", nil
	}
	if a[0] == "pull" {
		return "Digest: sha256:" + strings.Repeat("b", 64), nil
	}
	if a[0] == "info" {
		if strings.Contains(joined, "NodeID") {
			return "control", nil
		}
		return "active true", nil
	}
	if a[0] == "node" {
		if a[1] == "ls" {
			return "control", nil
		}
		if a[1] == "update" {
			return "", nil
		}
		format := a[3]
		switch format {
		case "{{.Status.Addr}}":
			return "192.0.2.1", nil
		case "{{.Spec.Role}}":
			return "manager", nil
		case "{{.Status.State}}":
			return "ready", nil
		case "{{.Description.Platform.OS}} {{.Status.State}}":
			return "linux ready", nil
		case "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}":
			return "linux ready active", nil
		case "{{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} {{.Spec.Role}}":
			return "control linux ready active manager", nil
		case "{{.ID}}":
			return "control", nil
		}
	}
	if a[0] == "network" {
		if a[1] == "ls" {
			return "network1", nil
		}
		if a[1] == "inspect" {
			return "kpl-peers overlay true", nil
		}
	}
	if a[0] == "stack" {
		if a[1] == "rm" {
			m.removed = true
		}
		return "", nil
	}
	if a[0] == "service" {
		switch a[1] {
		case "ls":
			if m.removed {
				return "", nil
			}
			return "controller agent monitor", nil
		case "inspect":
			component := map[string]string{"controller": "controller", "agent": "agent", "monitor": "prometheus"}[last]
			return "kpl_" + component + "|" + swarmApplication, nil
		case "ps":
			return strings.ReplaceAll(last, "_", "") + "task", nil
		case "update":
			m.paused[last] = true
			return "", nil
		case "rm":
			m.removed = true
			return "", nil
		}
	}
	if a[0] == "inspect" {
		component := strings.TrimSuffix(strings.TrimPrefix(last, "kpl"), "task")
		service := "kpl_" + component
		if m.paused[service] {
			if m.failStop && component == "controller" {
				return "control|failed|container|0|1|error", nil
			}
			return "control|shutdown|container|0|0|ok", nil
		}
		return "control|running|container|123|0|ok", nil
	}
	return "", fmt.Errorf("unhandled fake Docker command: %v", a)
}

func TestSwarmRestartBuildsBeforeControllerThenAgentCleanup(t *testing.T) {
	for _, tc := range []struct {
		name        string
		build, stop bool
	}{{"success", false, false}, {"build-failure", true, false}, {"stop-failure", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "restart")
			model := &swarmLifecycleModel{paused: map[string]bool{}, failBuild: tc.build, failStop: tc.stop}
			r.handle = model.handle
			err := s.a.RunSwarm(t.Context(), "restart", nil)
			if (err != nil) != (tc.build || tc.stop) {
				t.Fatalf("unexpected result: %v", err)
			}
			build := swarmCallIndex(r.calls, "build")
			pause := swarmCallIndex(r.calls, "service", "update")
			deploy := swarmCallIndex(r.calls, "stack", "deploy")
			if build < 0 {
				t.Fatal("restart did not build")
			}
			if tc.build {
				if pause >= 0 || deploy >= 0 {
					t.Fatal("failed build stopped or deployed services")
				}
				return
			}
			if pause < build {
				t.Fatal("service stopped before image build")
			}
			var order []string
			for _, call := range r.calls {
				if len(call.Args) > 1 && call.Args[0] == "service" && call.Args[1] == "update" {
					order = append(order, call.Args[len(call.Args)-1])
				}
			}
			if tc.stop {
				if deploy >= 0 || !slices.Equal(order, []string{"kpl_controller"}) {
					t.Fatalf("unclean Controller advanced restart: %v", order)
				}
			} else {
				if deploy < 0 || !slices.Equal(order, []string{"kpl_controller", "kpl_agent", "kpl_prometheus"}) {
					t.Fatalf("wrong stop/deploy order: %v", order)
				}
			}
			for _, call := range r.calls {
				if slices.Equal(call.Args, []string{"stack", "rm", "kpl"}) {
					t.Fatal("restart removed persistent stack resources")
				}
			}
		})
	}
}

func TestSwarmRemoveDeletesUnhealthyServicesWithoutGatewayClaim(t *testing.T) {
	s, r, b := newSwarmFixture(t, "remove")
	model := &swarmLifecycleModel{paused: map[string]bool{}, failStop: true}
	r.handle = model.handle
	if err := s.a.RunSwarm(t.Context(), "remove", nil); err != nil {
		t.Fatal(err)
	}
	if swarmCallIndex(r.calls, "service", "rm") < 0 || swarmCallIndex(r.calls, "stack", "rm") < 0 {
		t.Fatal("remove did not delete services and stack resources")
	}
	removed := r.calls[swarmCallIndex(r.calls, "service", "rm")].Args
	if !slices.Equal(removed, []string{"service", "rm", "controller", "agent", "monitor"}) {
		t.Fatalf("remove used names instead of ownership-checked immutable service IDs: %v", removed)
	}
	if !strings.Contains(b.String(), "does not verify final experiment logs") {
		t.Fatal("removal overstated cleanup evidence")
	}
}

func TestSwarmNetworkResetPreservesAllocationAndStopsOnChangedNetwork(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			s, r, b := newSwarmFixture(t, "reset-peer-network")
			checks := 0
			removed := false
			expected := s.network + "|overlay|swarm|true|false|false|false|false||default|0|1|0|" + swarmApplication + "|kpl||"
			r.handle = func(_ context.Context, c Command) (string, error) {
				a := c.Args
				switch {
				case a[0] == "service":
					return "", nil
				case a[0] == "node" && a[1] == "ls":
					return "control worker", nil
				case a[0] == "node" && a[1] == "inspect":
					return "ready\nready", nil
				case a[0] == "network" && a[1] == "rm":
					removed = true
					return "", nil
				case a[0] == "network" && a[1] == "create":
					if !removed {
						t.Fatal("create preceded removal")
					}
					return "newnetwork", nil
				case a[0] == "network" && a[1] == "inspect":
					switch a[3] {
					case "{{.Id}}":
						return "oldnetwork", nil
					case swarmIPAMFormat:
						return `"10.42.0.0/24"|"10.42.0.1"|""|0`, nil
					case swarmResetFormat:
						checks++
						if changed && checks == 2 {
							return "changed", nil
						}
						return expected, nil
					}
				}
				return "", fmt.Errorf("unexpected command: %v", a)
			}
			err := s.resetPeerNetwork(t.Context())
			if (err != nil) != changed {
				t.Fatalf("result: %v", err)
			}
			if changed {
				if removed {
					t.Fatal("changed network was removed")
				}
				return
			}
			if !strings.Contains(b.String(), "Recovery command (only if the network is absent)") {
				t.Fatal("missing recovery command")
			}
			create := swarmCallIndex(r.calls, "network", "create")
			if create < 0 || !slices.Contains(r.calls[create].Args, "10.42.0.0/24") || !slices.Contains(r.calls[create].Args, "10.42.0.1") {
				t.Fatal("network allocation not preserved")
			}
		})
	}
}

func TestSwarmNetworkIPAMRejectsUnsafeGateway(t *testing.T) {
	for _, raw := range []string{`"10.0.0.0/24"|null|""|0`, `"10.0.0.0/24"|"10.0.0.0"|""|0`, `"10.0.0.0/24"|"10.0.0.255"|""|0`, `"10.0.0.0/24"|"10.0.1.1"|""|0`, `"10.0.0.1/24"|"10.0.0.2"|""|0`, `"10.0.0.0/24"|"10.0.0.1"|"10.0.0.0/25"|0`} {
		if _, _, err := parseSwarmIPAM(raw); err == nil {
			t.Fatalf("accepted unsafe IPAM %s", raw)
		}
	}
}

func TestSwarmGatewayRemovalRequiresOwnershipAndNoPeers(t *testing.T) {
	for _, unsafe := range []string{"none", "peer", "owner"} {
		t.Run(unsafe, func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "remove")
			r.handle = func(_ context.Context, c Command) (string, error) {
				a := c.Args
				switch {
				case a[0] == "service":
					return "", nil
				case a[0] == "ps" && slices.Contains(a, "label=io.kpl.managed=true"):
					if unsafe == "peer" {
						return "livepeer", nil
					}
					return "", nil
				case a[0] == "ps":
					return "gateway1", nil
				case a[0] == "inspect":
					if unsafe == "owner" {
						return "other|other|true", nil
					}
					return "kpl|kpl|", nil
				case a[0] == "rm":
					return "", nil
				}
				return "", errors.New("unexpected command")
			}
			err := s.removeGateway(t.Context())
			if (err != nil) != (unsafe != "none") {
				t.Fatalf("result: %v", err)
			}
			removed := swarmCallIndex(r.calls, "rm", "--force") >= 0
			if removed != (unsafe == "none") {
				t.Fatal("unsafe gateway deletion")
			}
		})
	}
}

func TestSwarmLoginUsesInteractiveDockerContext(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "login")
	if err := s.login(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 1 || !r.calls[0].Interactive || !slices.Equal(r.calls[0].Args, []string{"login", "registry.example.test"}) {
		t.Fatal("login lost terminal or registry selection")
	}
}

func TestSwarmRestartRechecksOwnershipAfterPublish(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "restart")
	model := &swarmLifecycleModel{paused: map[string]bool{}}
	published := false
	r.handle = func(ctx context.Context, c Command) (string, error) {
		if c.Args[0] == "push" {
			published = true
		}
		if published && len(c.Args) > 1 && c.Args[0] == "service" && c.Args[1] == "inspect" {
			return "kpl_controller|foreign-application", nil
		}
		return model.handle(ctx, c)
	}
	if err := s.a.RunSwarm(t.Context(), "restart", nil); err == nil {
		t.Fatal("foreign replacement service accepted after publish")
	}
	if !published || swarmCallIndex(r.calls, "service", "update") >= 0 || swarmCallIndex(r.calls, "stack", "deploy") >= 0 {
		t.Fatal("ownership change was not detected before stopping")
	}
}

func TestSwarmRemoveNodeRetainsLabelsUntilCleanExit(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(fmt.Sprint(clean), func(t *testing.T) {
			s, r, _ := newSwarmFixture(t, "remove-node")
			s.present["agent"] = true
			s.options.nodes = []string{"node1"}
			paused := false
			r.handle = func(_ context.Context, c Command) (string, error) {
				a := c.Args
				if a[0] == "node" && a[1] == "inspect" {
					switch a[3] {
					case "{{.ID}}":
						return "node1", nil
					case "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}":
						return "linux ready active", nil
					case "{{.Description.Platform.OS}} {{.Status.State}}":
						return "linux ready", nil
					case "{{.Status.State}}":
						return "ready", nil
					}
				}
				if a[0] == "service" && a[1] == "ps" {
					return "task1", nil
				}
				if a[0] == "service" && a[1] == "update" {
					paused = true
					return "", nil
				}
				if a[0] == "inspect" {
					if paused {
						if clean {
							return "node1|shutdown|container1|0|0|ok", nil
						}
						return "node1|failed|container1|0|1|error", nil
					}
					return "node1|running|container1|123|0|ok", nil
				}
				if a[0] == "node" && a[1] == "update" {
					if !paused {
						t.Fatal("node label removed before exclusion")
					}
					return "", nil
				}
				return "", fmt.Errorf("unexpected Docker call: %v", a)
			}
			err := s.removeNodes(t.Context())
			if (err == nil) != clean {
				t.Fatalf("cleanup result %v", err)
			}
			labelRemoved := swarmCallIndex(r.calls, "node", "update", "--label-rm") >= 0
			if labelRemoved != clean {
				t.Fatal("label changed without verified clean exit")
			}
		})
	}
}

func TestSwarmCleanupUsesRemainingBudgetAndCancellation(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "restart")
	r.handle = func(ctx context.Context, c Command) (string, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 100*time.Millisecond {
			t.Fatal("Docker check did not inherit remaining cleanup deadline")
		}
		if c.Args[0] == "node" {
			return "ready", nil
		}
		return "node1|running|container1|123|0|ok", nil
	}
	started := time.Now()
	err := s.waitTasks(t.Context(), []string{"task1"}, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("unbounded cleanup: %v", err)
	}
}

func TestSwarmScenarioRejectsFIFOWithoutOpeningWriter(t *testing.T) {
	s, r, _ := newSwarmFixture(t, "scenario")
	path := filepath.Join(t.TempDir(), "scenario.pipe")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.a.RunSwarm(t.Context(), "scenario", []string{path}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("scenario FIFO accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("scenario read blocked on FIFO")
	}
	if len(r.calls) > 0 {
		t.Fatal("scenario contacted Docker")
	}
}
