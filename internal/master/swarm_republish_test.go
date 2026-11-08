package master

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Unlike the general lifecycle model, this model removes placement labels when
// remove runs. A redeploy cannot accidentally pass using the previous labels.
type republishLifecycleModel struct {
	base            swarmLifecycleModel
	labels          map[string]bool
	failure         string
	unready         string
	newDigest       string
	duplicateLabels bool
}

func newRepublishLifecycleModel() *republishLifecycleModel {
	return &republishLifecycleModel{
		base:      swarmLifecycleModel{paused: map[string]bool{}},
		labels:    map[string]bool{"workerone": true},
		newDigest: "sha256:" + strings.Repeat("c", 64),
	}
}

func (m *republishLifecycleModel) handle(ctx context.Context, c Command) (string, error) {
	a := c.Args
	last := a[len(a)-1]
	if a[0] == "build" || a[0] == "buildx" {
		if m.failure == "build" {
			return "", errors.New("fixture image build failed")
		}
		return "", nil
	}
	if a[0] == "push" {
		if m.failure == "push" {
			return "", errors.New("fixture image push failed")
		}
		return "", nil
	}
	if a[0] == "pull" {
		if m.failure == "pull" {
			return "", errors.New("fixture registry pull failed")
		}
		return "Digest: " + m.newDigest, nil
	}
	if slices.Equal(a[:min(len(a), 2)], []string{"service", "rm"}) && m.failure == "remove" {
		return "", errors.New("fixture service removal failed")
	}
	if slices.Equal(a[:min(len(a), 2)], []string{"stack", "config"}) && m.failure == "compose" {
		return "", errors.New("fixture invalid Compose configuration")
	}
	if a[0] == "node" {
		switch a[1] {
		case "ls":
			if slices.Contains(a, "--filter") {
				if m.duplicateLabels {
					return "workerone\nworkerone", nil
				}
				var nodes []string
				for _, node := range []string{"control", "workerone", "workertwo"} {
					if m.labels[node] {
						nodes = append(nodes, node)
					}
				}
				return strings.Join(nodes, "\n"), nil
			}
			return "control\nworkerone\nworkertwo", nil
		case "update":
			if slices.Contains(a, "--label-rm") {
				delete(m.labels, last)
			}
			if slices.Contains(a, "--label-add") {
				m.labels[last] = true
			}
			return "", nil
		case "inspect":
			format := a[3]
			var records []string
			for _, node := range a[4:] {
				role := "worker"
				if node == "control" {
					role = "manager"
				}
				state := "ready"
				if node == m.unready {
					state = "down"
				}
				switch format {
				case "{{.ID}}":
					records = append(records, node)
				case "{{.ID}}|{{.Spec.Role}}|{{.Description.Platform.OS}}|{{.Status.State}}|{{.Spec.Availability}}":
					records = append(records, node+"|"+role+"|linux|"+state+"|active")
				case "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}":
					records = append(records, "linux "+state+" active")
				case "{{.Description.Platform.OS}} {{.Status.State}}":
					records = append(records, "linux "+state)
				case "{{.Spec.Role}}":
					records = append(records, role)
				default:
					return m.base.handle(ctx, c)
				}
			}
			return strings.Join(records, "\n"), nil
		}
	}
	return m.base.handle(ctx, c)
}

func assertNoRepublishChanges(t *testing.T, runner *swarmTestRunner) {
	t.Helper()
	for _, command := range [][]string{{"service", "rm"}, {"stack", "rm"}, {"node", "update"}, {"build"}, {"buildx"}, {"push"}, {"pull"}, {"stack", "deploy"}} {
		if swarmCallIndex(runner.calls, command...) >= 0 {
			t.Fatalf("invalid republish changed deployment or contacted registry: %v", command)
		}
	}
}

func TestSwarmRepublishRemovesPublishesAndDeploysFreshDigest(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "restart")
	model := newRepublishLifecycleModel()
	runner.handle = model.handle
	before, err := os.ReadFile(s.a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.a.RunSwarm(t.Context(), "restart", []string{"--republish"}); err != nil {
		t.Fatal(err)
	}
	compose := swarmCallIndex(runner.calls, "stack", "config")
	if compose < 0 || compose >= swarmCallIndex(runner.calls, "service", "rm") {
		t.Fatal("deployment assets were not validated before removal")
	}
	if runner.calls[compose].Stdout != io.Discard || swarmCommandEnv(runner.calls[compose], "KPL_CONTROLLER_METRICS_URL") != "http://192.0.2.1:8080/metrics" || swarmCommandEnv(runner.calls[compose], "KPL_PROMETHEUS_EXTERNAL_URL") != "http://192.0.2.1:9090/" {
		t.Fatal("Compose preflight exposed interpolated output or omitted the deployment URLs")
	}
	steps := [][]string{{"service", "rm"}, {"stack", "rm"}, {"build"}, {"push"}, {"pull", "registry.example.test/lab:v3"}, {"stack", "deploy"}}
	previous := -1
	for _, step := range steps {
		index := swarmCallIndex(runner.calls, step...)
		if index <= previous {
			t.Fatalf("republish step %v missing or out of order: %v", step, runner.calls)
		}
		previous = index
	}
	deploy := runner.calls[swarmCallIndex(runner.calls, "stack", "deploy")]
	if got := swarmCommandEnv(deploy, "KPL_IMAGE"); got != "registry.example.test/lab@"+model.newDigest {
		t.Fatalf("deployed a stale image: %q", got)
	}
	if !model.labels["workerone"] || len(model.labels) != 1 {
		t.Fatalf("bare republish lost or expanded the previous placement: %v", model.labels)
	}
	for _, command := range runner.calls {
		if command.Args[0] == "build" || command.Args[0] == "push" || command.Args[0] == "pull" {
			if swarmCommandEnv(command, "DOCKER_DEFAULT_PLATFORM") != "" {
				t.Fatal("republish inherited an unrelated default build/pull platform")
			}
		}
	}
	after, err := os.ReadFile(s.a.EnvFile)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("republish changed persistent configuration")
	}
}

func TestSwarmRepublishUsesExplicitOrSelectedPlacement(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want map[string]bool
	}{
		{"explicit", []string{"workertwo", "--republish"}, map[string]bool{"workertwo": true}},
		{"workers", []string{"--republish", "--workers"}, map[string]bool{"workerone": true, "workertwo": true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			model := newRepublishLifecycleModel()
			runner.handle = model.handle
			if err := s.a.RunSwarm(t.Context(), "restart", test.args); err != nil {
				t.Fatal(err)
			}
			if len(model.labels) != len(test.want) {
				t.Fatalf("placement changed: got %v want %v", model.labels, test.want)
			}
			for node := range test.want {
				if !model.labels[node] {
					t.Fatalf("missing requested Agent node %s: %v", node, model.labels)
				}
			}
		})
	}
}

func TestSwarmRepublishStopsAfterEachFailedStage(t *testing.T) {
	for _, stage := range []string{"remove", "build", "push", "pull"} {
		t.Run(stage, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			model := newRepublishLifecycleModel()
			model.failure = stage
			runner.handle = model.handle
			err := s.a.RunSwarm(t.Context(), "restart", []string{"--republish"})
			stageWord := stage
			if stage == "remove" {
				stageWord = "remov"
			}
			if err == nil || !strings.Contains(err.Error(), stageWord) {
				t.Fatalf("failed %s stage was not reported: %v", stage, err)
			}
			for _, command := range [][]string{{"stack", "deploy"}} {
				if swarmCallIndex(runner.calls, command...) >= 0 {
					t.Fatalf("republish deployed after failed %s", stage)
				}
			}
			if stage == "remove" && swarmCallIndex(runner.calls, "build") >= 0 || (stage == "remove" || stage == "build") && swarmCallIndex(runner.calls, "push") >= 0 || stage != "pull" && swarmCallIndex(runner.calls, "pull") >= 0 {
				t.Fatalf("republish continued beyond failed %s", stage)
			}
			if stage != "remove" && (!model.base.removed || !strings.Contains(err.Error(), "stack removal") || !strings.Contains(err.Error(), "not restored")) {
				t.Fatalf("failure omitted the already removed stack state: %v", err)
			}
		})
	}
}

func TestSwarmRepublishRejectsInvalidInputsBeforeDocker(t *testing.T) {
	digest := "sha256:" + strings.Repeat("b", 64)
	for _, args := range [][]string{
		{"--republish", "--image", digest}, {"--image=" + digest, "--republish"},
		{"--republish", "--republish"}, {"--republish=true"},
		{"--republish", "--workers", "workerone"},
		{"--republish", "--platforms", "windows/amd64"},
	} {
		s, runner, _ := newSwarmFixture(t, "restart")
		if err := s.a.RunSwarm(t.Context(), "restart", args); ExitCode(err) != 2 || len(runner.calls) != 0 {
			t.Fatalf("invalid republish arguments reached Docker: %q: %v", args, err)
		}
	}
	for _, name := range []string{"deploy", "publish", "add-node", "remove-node"} {
		s, runner, _ := newSwarmFixture(t, name)
		if err := s.a.RunSwarm(t.Context(), name, []string{"--republish"}); ExitCode(err) != 2 || len(runner.calls) != 0 {
			t.Fatalf("accepted --republish for %s: %v", name, err)
		}
	}
	s, runner, _ := newSwarmFixture(t, "restart")
	s.config["KPL_IMAGE"] = "registry.example.test/lab@" + digest
	s.a = s.a.WithConfig(s.config)
	if err := s.a.RunSwarm(t.Context(), "restart", []string{"--republish"}); err == nil || len(runner.calls) != 0 {
		t.Fatalf("pinned republish reached Docker: %v", err)
	}
}

func TestSwarmRepublishPlatformsAreOrderIndependent(t *testing.T) {
	for _, args := range [][]string{
		{"--republish", "--platforms", "linux/amd64,linux/arm64"},
		{"--platforms=linux/amd64,linux/arm64", "--republish"},
	} {
		s, runner, _ := newSwarmFixture(t, "restart")
		model := newRepublishLifecycleModel()
		runner.handle = model.handle
		if err := s.a.RunSwarm(t.Context(), "restart", args); err != nil {
			t.Fatal(err)
		}
		build := swarmCallIndex(runner.calls, "buildx", "build", "--platform", "linux/amd64,linux/arm64", "--tag", "registry.example.test/lab:v3", "--push")
		if build <= swarmCallIndex(runner.calls, "stack", "rm") || swarmCallIndex(runner.calls, "pull") <= build {
			t.Fatal("multi-platform republish was not performed after removal and before tag resolution")
		}
		if swarmCallIndex(runner.calls, "build") >= 0 || swarmCallIndex(runner.calls, "push") >= 0 {
			t.Fatal("multi-platform republish also invoked native build/push")
		}
	}
}

func TestSwarmRepublishPreflightDoesNotRemoveKnownInvalidDeployments(t *testing.T) {
	for _, mode := range []string{"source", "config", "node", "placement", "duplicate-placement", "compose"} {
		t.Run(mode, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			model := newRepublishLifecycleModel()
			switch mode {
			case "source":
				if err := os.Remove(filepath.Join(s.a.Repo, "Dockerfile")); err != nil {
					t.Fatal(err)
				}
			case "config":
				s.config["KPL_PASSWORD"] = ""
				s.a = s.a.WithConfig(s.config)
			case "node":
				model.unready = "workerone"
			case "placement":
				s.config["KPL_MIN_AGENTS"] = "2"
				s.a = s.a.WithConfig(s.config)
			case "duplicate-placement":
				model.duplicateLabels = true
				s.config["KPL_MIN_AGENTS"] = "2"
				s.a = s.a.WithConfig(s.config)
			case "compose":
				model.failure = "compose"
			}
			runner.handle = model.handle
			if err := s.a.RunSwarm(t.Context(), "restart", []string{"--republish"}); err == nil {
				t.Fatalf("known-invalid %s accepted", mode)
			}
			assertNoRepublishChanges(t, runner)
		})
	}
}
