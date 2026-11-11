package master

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDedicatedAnalysisStackIsolation(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI is not installed")
	}
	s, _, _ := newSwarmFixture(t, "deploy")
	s.a.Repo = repositoryForTest(t)
	base, closeBase, err := s.deploymentComposeFiles()
	defer closeBase()
	if err != nil || len(base) != 2 {
		t.Fatalf("analysis service should be absent by default: %v %v", base, err)
	}
	s.config["KPL_ANALYSIS_NODE_IDS"] = "analysisnode otheranalysis"
	args, cleanup, err := s.deploymentComposeFiles()
	defer cleanup()
	if err != nil || len(args) != 4 {
		t.Fatalf("configured analysis service must be part of deployment: %v %v", args, err)
	}
	info, err := os.Stat(args[3])
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("analysis Compose override must be a private temporary file: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, docker, append([]string{"stack", "config"}, args...)...)
	command.Dir = s.a.Repo
	// This parses real Compose input with synthetic values and no Docker daemon.
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + t.TempDir(), "DOCKER_HOST=unix:///nonexistent-kpl-offline-config.sock",
		"KPL_IMAGE=fixture.invalid/kpl@sha256:" + strings.Repeat("c", 64), "KPL_CONTROL_NODE_ID=control", "KPL_USER=fixture-user", "KPL_PASSWORD=fixture-password",
		"GRAFANA_ADMIN_PASSWORD=fixture-grafana", "KPL_CONTROLLER_METRICS_URL=http://fixture:8080/metrics", "KPL_PROMETHEUS_EXTERNAL_URL=http://fixture:9090/",
		"KPL_ANALYSIS_SECRET=fixture-analysis", "KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND=11",
		"KPL_ANALYSIS_CPUS=3", "KPL_ANALYSIS_MEMORY_MIB=2048",
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("offline Compose validation failed: %v: %s", err, output)
	}
	type service struct {
		Command, Ports, Volumes []any
		User, Image             string
		Environment             map[string]string
		Networks                map[string]any
		Secrets                 []struct {
			Source, Target, UID, GID string
			Mode                     int
		}
		Deploy struct {
			Replicas  int
			Placement struct{ Constraints []string }
			Resources struct{ Limits map[string]any }
		}
	}
	var stack struct {
		Services map[string]service
		Networks map[string]struct {
			Driver     string
			Attachable bool
			DriverOpts map[string]string `yaml:"driver_opts"`
		}
		Secrets map[string]struct {
			Name     string
			External bool
		}
	}
	if err := yaml.Unmarshal(output, &stack); err != nil {
		t.Fatal(err)
	}
	controller, agent := stack.Services["controller"], stack.Services["agent"]
	workers := []service{stack.Services["analysis-worker-1"], stack.Services["analysis-worker-2"]}
	for i, node := range []string{"analysisnode", "otheranalysis"} {
		worker := workers[i]
		if len(worker.Ports) != 0 || len(worker.Volumes) != 0 || worker.User != "100:101" || worker.Image != controller.Image {
			t.Fatalf("analysis worker must use the Controller image and unprivileged UID without host ports or volumes: %+v", worker)
		}
		if worker.Deploy.Replicas != 1 || !slices.Contains(worker.Deploy.Placement.Constraints, "node.id == "+node) {
			t.Fatalf("worker must have one replica on %s: %+v", node, worker.Deploy.Placement)
		}
		if worker.Deploy.Resources.Limits["cpus"] != "3" || fmt.Sprint(worker.Deploy.Resources.Limits["memory"]) != "2147483648" {
			t.Fatalf("resource limits not applied: %+v", worker.Deploy.Resources.Limits)
		}
		if len(worker.Networks) != 1 || len(worker.Environment) != 2 || worker.Environment["KPL_ANALYSIS_WORKER_TOKEN_FILE"] != "/run/secrets/kpl-analysis" || worker.Environment["KPL_RUNTIME_IMAGE"] != worker.Image || worker.Environment["KPL_RUNTIME_IMAGE"] != controller.Environment["KPL_RUNTIME_IMAGE"] {
			t.Fatalf("unexpected analysis worker environment or networks: %+v", worker)
		}
	}
	if !slices.Equal(agent.Deploy.Placement.Constraints, []string{"node.platform.os == linux", "node.labels.kpl.kpl.agent == true"}) {
		t.Fatalf("analysis workers changed Agent eligibility: %+v", agent.Deploy.Placement)
	}
	network := stack.Networks["analysis"]
	if _, encrypted := network.DriverOpts["encrypted"]; !encrypted || network.Driver != "overlay" || network.Attachable {
		t.Fatalf("analysis overlay must be encrypted and available only to its services: %+v", network)
	}
	for name, service := range stack.Services {
		_, joined := service.Networks["analysis"]
		if joined != (name == "controller" || name == "analysis-worker-1" || name == "analysis-worker-2") {
			t.Fatalf("unexpected analysis overlay membership for %s: %+v", name, service.Networks)
		}
	}
	if len(controller.Networks) != 3 || controller.Environment["KPL_ANALYSIS_WORKER_URLS"] != "http://analysis-worker-1:8093 http://analysis-worker-2:8093" || controller.Environment["KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND"] != "11" {
		t.Fatalf("wrong worker or Controller environment or network configuration")
	}
	for _, service := range append(workers, controller) {
		if len(service.Secrets) != 1 {
			t.Fatal("both services need the same analysis credential")
		}
		secret := service.Secrets[0]
		if secret.Source != "analysis-auth" || secret.Target != "kpl-analysis" || secret.UID != "100" || secret.GID != "101" || secret.Mode != 0400 {
			t.Fatalf("credential was not mounted as a private file: %+v", secret)
		}
	}
	if secret := stack.Secrets["analysis-auth"]; secret.Name != "fixture-analysis" || !secret.External {
		t.Fatalf("credential must be provided as an external Docker secret: %+v", secret)
	}
}

func TestAnalysisWorkerConfigRequiresDistinctNonControllerNodes(t *testing.T) {
	s, _, _ := newSwarmFixture(t, "deploy")
	s.config["KPL_ANALYSIS_SECRET"] = "fixture-analysis"
	for _, invalid := range []string{"analysis analysis", "analysis control", "analysis /bad", strings.Repeat("node ", maxAnalysisWorkers+1)} {
		s.config["KPL_ANALYSIS_NODE_IDS"] = invalid
		if err := validateConfig(s.config); err == nil {
			t.Fatalf("accepted invalid analysis node list %q", invalid)
		}
	}
	s.config["KPL_ANALYSIS_NODE_IDS"] = "analysisnode otheranalysis"
	if err := validateConfig(s.config); err != nil {
		t.Fatal(err)
	}
}

func TestAnalysisWorkerMovesAndDisablementRequireCleanRestart(t *testing.T) {
	for _, node := range []string{"", "otheranalysis"} {
		t.Run("node="+node, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "deploy")
			s.config["KPL_ANALYSIS_NODE_IDS"] = node
			s.config["KPL_ANALYSIS_SECRET"] = "fixture-analysis"
			s.services, s.serviceIDs = []string{"kpl_analysis-worker-1"}, []string{"ownedanalysis"}
			runner.handle = func(_ context.Context, c Command) (string, error) {
				if c.Args[0] == "service" && c.Args[1] == "inspect" && c.Args[len(c.Args)-1] == "ownedanalysis" {
					return "node.platform.os == linux\nnode.id == analysisnode", nil
				}
				return "", fmt.Errorf("unexpected Docker command: %v", c.Args)
			}
			if err := s.deploy(t.Context()); err == nil || !strings.Contains(err.Error(), "kpl swarm restart") {
				t.Fatalf("ordinary deploy must reject the transition before any mutation: %v", err)
			}
			for _, c := range runner.calls {
				if c.Args[0] != "service" || c.Args[1] != "inspect" {
					t.Fatalf("deployment did more than inspect placement: %v", c.Args)
				}
			}
		})
	}
}

func TestSwarmRestartRemovesDisabledAnalysisWorkerAfterVerifiedStop(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "restart")
	model := &swarmLifecycleModel{paused: map[string]bool{}}
	const workerID = "ownedanalysis"
	const worker = "kpl_analysis-worker-1"
	const workerTask = "analysisworkertask"
	stopped := false
	runner.handle = func(ctx context.Context, c Command) (string, error) {
		args := c.Args
		last := args[len(args)-1]
		if args[0] == "service" {
			switch args[1] {
			case "ls":
				return "controller agent monitor " + workerID, nil
			case "inspect":
				if last == workerID {
					return worker + "|" + swarmApplication, nil
				}
			case "ps":
				if last == worker {
					return workerTask, nil
				}
			case "rm":
				if last == workerID {
					if !stopped {
						t.Fatal("worker removal preceded shutdown verification")
					}
					return "", nil
				}
			}
		}
		if args[0] == "inspect" && last == workerTask {
			if model.paused[worker] {
				stopped = true
				return "analysisnode|shutdown|analysiscontainer|0|0|ok", nil
			}
			return "analysisnode|running|analysiscontainer|123|0|ok", nil
		}
		return model.handle(ctx, c)
	}
	if err := s.a.RunSwarm(t.Context(), "restart", nil); err != nil {
		t.Fatal(err)
	}
	removed, deployed := swarmCallIndex(runner.calls, "service", "rm", workerID), swarmCallIndex(runner.calls, "stack", "deploy")
	if removed < 0 || deployed <= removed || len(archiveComposePaths(runner.calls[deployed].Args)) != 1 {
		t.Fatalf("restart must remove only the disabled worker before applying the base stack: removal=%d deployment=%d", removed, deployed)
	}
}

func TestSwarmRepublishVerifiesAnalysisWorkerShutdownBeforeReusingEndpoint(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "restart")
	s.config["KPL_ANALYSIS_NODE_IDS"] = "analysisnode otheranalysis"
	s.config["KPL_ANALYSIS_SECRET"] = "fixture-analysis"
	s.a = s.a.WithConfig(s.config)
	model := newRepublishLifecycleModel()
	const workerID = "ownedanalysis"
	const workerTask = "analysisworkertask"
	workerStopped := false
	runner.handle = func(ctx context.Context, c Command) (string, error) {
		args := c.Args
		last := args[len(args)-1]
		switch args[0] {
		case "node":
			if args[1] == "inspect" && last == "otheranalysis" {
				return "analysisnode linux ready active\notheranalysis linux ready active", nil
			}
		case "secret":
			if args[1] == "inspect" && last == "fixture-analysis" {
				return last, nil
			}
		case "service":
			switch args[1] {
			case "ls":
				if !model.base.removed {
					return "controller agent monitor " + workerID, nil
				}
			case "inspect":
				if last == workerID {
					return "kpl_analysis-worker-1|" + swarmApplication, nil
				}
			case "ps":
				if last == workerID {
					return workerTask, nil
				}
			case "rm":
				if !workerStopped || !slices.Contains(args, workerID) {
					t.Fatal("stack removal occurred before analysis worker shutdown was verified")
				}
			}
		case "inspect":
			if last == workerTask {
				if model.base.paused[workerID] {
					if !model.base.paused["kpl_controller"] {
						t.Fatal("worker stopped before Controller stopped admitting jobs")
					}
					workerStopped = true
					return "analysisnode|shutdown|analysiscontainer|0|0|ok", nil
				}
				return "analysisnode|running|analysiscontainer|123|0|ok", nil
			}
		}
		return model.handle(ctx, c)
	}
	if err := s.a.RunSwarm(t.Context(), "restart", []string{"--republish"}); err != nil {
		t.Fatal(err)
	}
	removed, deployed := swarmCallIndex(runner.calls, "service", "rm"), swarmCallIndex(runner.calls, "stack", "deploy")
	if removed < 0 || deployed <= removed || len(archiveComposePaths(runner.calls[deployed].Args)) != 2 {
		t.Fatalf("republish must shut down the old worker then deploy the configured worker pool: removal=%d deployment=%d", removed, deployed)
	}
}
