package master

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestAgentAnalysisStackUsesCommonConfigurationWithoutDedicatedServices(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI is not installed")
	}
	s, _, _ := newSwarmFixture(t, "deploy")
	s.a.Repo = repositoryForTest(t)
	// Old in-memory configuration cannot recreate the removed service path.
	s.config["KPL_ANALYSIS_NODE_IDS"] = "analysisnode otheranalysis"
	s.config["KPL_ANALYSIS_SECRET"] = "fixture-analysis"
	args, cleanup, err := s.deploymentComposeFiles()
	defer cleanup()
	if err != nil || len(args) != 2 {
		t.Fatalf("unexpected deployment assets: %v %v", args, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, docker, append([]string{"stack", "config"}, args...)...)
	command.Dir = s.a.Repo
	command.Env = []string{
		"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + t.TempDir(), "DOCKER_HOST=unix:///nonexistent-kpl-offline-config.sock",
		"KPL_IMAGE=fixture.invalid/kpl@sha256:" + strings.Repeat("c", 64), "KPL_CONTROL_NODE_ID=control", "KPL_USER=fixture-user", "KPL_PASSWORD=fixture-password",
		"GRAFANA_ADMIN_PASSWORD=fixture-grafana", "KPL_CONTROLLER_METRICS_URL=http://fixture:8080/metrics", "KPL_PROMETHEUS_EXTERNAL_URL=http://fixture:9090/",
		"KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND=11", "KPL_ANALYSIS_PREFETCH_MIB=128", "KPL_ANALYSIS_CPUS=64", "KPL_ANALYSIS_MEMORY_MIB=2048",
	}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("offline Compose validation failed: %v: %s", err, output)
	}
	var stack struct {
		Services map[string]struct {
			Environment map[string]string
			Deploy      struct {
				Placement struct{ Constraints []string }
			}
		}
		Networks map[string]any
		Secrets  map[string]any
	}
	if err := yaml.Unmarshal(output, &stack); err != nil {
		t.Fatal(err)
	}
	env := stack.Services["controller"].Environment
	for key, want := range map[string]string{"KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND": "11", "KPL_ANALYSIS_PREFETCH_MIB": "128", "KPL_ANALYSIS_CPUS": "64", "KPL_ANALYSIS_MEMORY_MIB": "2048"} {
		if env[key] != want {
			t.Fatalf("common analysis setting %s=%s, want %s", key, env[key], want)
		}
	}
	for _, retired := range []string{"KPL_ANALYSIS_WORKER_URLS", "KPL_ANALYSIS_WORKER_TOKEN_FILE"} {
		if _, ok := env[retired]; ok {
			t.Fatalf("Controller still depends on dedicated analysis setting %s", retired)
		}
	}
	for name := range stack.Services {
		if legacyAnalysisWorker(name) {
			t.Fatalf("deployment recreated retired service %s", name)
		}
	}
	if _, ok := stack.Networks["analysis"]; ok || len(stack.Secrets) != 0 {
		t.Fatal("dedicated analysis network/secret remains in deployment")
	}
	if !slices.Equal(stack.Services["agent"].Deploy.Placement.Constraints, []string{"node.platform.os == linux", "node.labels.kpl.kpl.agent == true"}) {
		t.Fatal("analysis changed ordinary Agent eligibility")
	}
}

func TestRetiredAnalysisConfigMigratesWithoutRecreatingPublicEntrypoints(t *testing.T) {
	a, _, _ := configApp(t)
	if err := a.ConfigCommand(t.Context(), "init", []string{"KPL_IMAGE=fixture.invalid/kpl:v3"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	old := append(data, []byte("KPL_ANALYSIS_NODE_IDS=oldnode\nKPL_ANALYSIS_SECRET=fixture-secret\nKPL_ANALYSIS_WORKER_URLS=http://analysis-worker-1:8093\nKPL_ANALYSIS_WORKER_TOKEN=fixture-retired-token\n")...)
	if err := os.WriteFile(a.EnvFile, old, 0600); err != nil {
		t.Fatal(err)
	}
	c, err := a.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c["KPL_ANALYSIS_NODE_IDS"] != "" || c["KPL_ANALYSIS_SECRET"] != "" {
		t.Fatal("retired settings remain effective")
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_ANALYSIS_CPUS=64"}); err != nil {
		t.Fatal(err)
	}
	cleaned, err := os.ReadFile(a.EnvFile)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(cleaned, []byte("KPL_ANALYSIS_NODE_IDS")) || bytes.Contains(cleaned, []byte("KPL_ANALYSIS_SECRET")) || bytes.Contains(cleaned, []byte("KPL_ANALYSIS_WORKER_")) {
		t.Fatal("configuration save retained retired keys")
	}
	for _, key := range []string{"KPL_ANALYSIS_NODE_IDS", "KPL_ANALYSIS_SECRET"} {
		if err := a.ConfigCommand(t.Context(), "configure", []string{key + "=value"}); err == nil {
			t.Fatalf("retired public setting %s accepted", key)
		}
	}
	if IsRuntimeCommand("analysis-worker") {
		t.Fatal("retired analysis role remains executable")
	}
	if _, err := parseSwarmOptions("logs", []string{"analysis-worker-1"}); err == nil {
		t.Fatal("retired log component remains public")
	}
	after, err := os.ReadFile(a.EnvFile)
	if err != nil || !bytes.Equal(after, cleaned) {
		t.Fatal("rejected retired setting changed configuration")
	}
}

func TestSwarmRestartRemovesRetiredAnalysisWorkerAfterVerifiedStop(t *testing.T) {
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
				if last == worker || last == workerID {
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

func TestSwarmRepublishVerifiesRetiredAnalysisWorkerShutdown(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "restart")
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
	if removed < 0 || deployed <= removed || len(archiveComposePaths(runner.calls[deployed].Args)) != 1 {
		t.Fatalf("republish must shut down the old worker then deploy the ordinary Agent stack: removal=%d deployment=%d", removed, deployed)
	}
	if got := swarmCommandEnv(runner.calls[deployed], retiredAnalysisEnvironment); got != "http://analysis-worker-1:8093" {
		t.Fatalf("republish lost retirement proof: %q", got)
	}
}

func TestSwarmDeployRetiresOwnedAnalysisServicesBeforePublishingProof(t *testing.T) {
	for _, failStop := range []bool{false, true} {
		name := "clean"
		if failStop {
			name = "unclean"
		}
		t.Run(name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "deploy")
			model := &swarmLifecycleModel{paused: map[string]bool{}}
			removed := false
			verified := false
			const workerID = "ownedanalysis"
			const worker = "kpl_analysis-worker-1"
			const task = "legacyanalysisworkertask"
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				args := c.Args
				last := args[len(args)-1]
				if args[0] == "service" {
					switch args[1] {
					case "ls":
						if !removed {
							return "controller agent monitor " + workerID, nil
						}
					case "inspect":
						if last == workerID {
							return worker + "|" + swarmApplication, nil
						}
					case "ps":
						if last == workerID || last == worker {
							return task, nil
						}
					case "rm":
						if last == workerID {
							if !verified || !model.paused["kpl_controller"] {
								t.Fatal("retired service removed before Controller and worker cleanup were verified")
							}
							removed = true
							return "", nil
						}
					}
				}
				if args[0] == "inspect" && last == task {
					if model.paused[workerID] || model.paused[worker] {
						if failStop {
							return "analysisnode|failed|legacycontainer|0|1|error", nil
						}
						verified = true
						return "analysisnode|shutdown|legacycontainer|0|0|ok", nil
					}
					return "analysisnode|running|legacycontainer|123|0|ok", nil
				}
				return model.handle(ctx, c)
			}
			err := s.a.RunSwarm(t.Context(), "deploy", nil)
			deployed := swarmCallIndex(runner.calls, "stack", "deploy")
			if failStop {
				if err == nil || removed || deployed >= 0 {
					t.Fatalf("unclean retirement was published: error=%v removed=%t deploy=%d", err, removed, deployed)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !removed || deployed < 0 {
				t.Fatal("owned retired worker was not removed before deployment")
			}
			if got := swarmCommandEnv(runner.calls[deployed], retiredAnalysisEnvironment); got != "http://analysis-worker-1:8093" {
				t.Fatalf("verified retirement proof was not passed to Controller: %q", got)
			}
		})
	}
}

func TestSwarmDeployCarriesOnlyRecordedRetirementProof(t *testing.T) {
	for _, proof := range []string{"", "http://analysis-worker-2:8093"} {
		t.Run("proof="+proof, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "deploy")
			model := &swarmLifecycleModel{paused: map[string]bool{}}
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				if len(c.Args) > 3 && c.Args[0] == "service" && c.Args[1] == "inspect" && c.Args[3] == retiredAnalysisEnvironmentFormat {
					if proof == "" {
						return "", nil
					}
					return retiredAnalysisEnvironment + "=" + proof, nil
				}
				return model.handle(ctx, c)
			}
			if err := s.a.RunSwarm(t.Context(), "deploy", nil); err != nil {
				t.Fatal(err)
			}
			deployed := swarmCallIndex(runner.calls, "stack", "deploy")
			if deployed < 0 {
				t.Fatal("deployment not submitted")
			}
			if got := swarmCommandEnv(runner.calls[deployed], retiredAnalysisEnvironment); got != proof {
				t.Fatalf("proof lost or inferred from absent services: %q want %q", got, proof)
			}
			if swarmCallIndex(runner.calls, "service", "rm") >= 0 || swarmCallIndex(runner.calls, "service", "update") >= 0 {
				t.Fatal("proof carry restarted unrelated services")
			}
		})
	}
}

func TestSwarmDeployRejectsMalformedRetirementProofBeforeServiceChanges(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "deploy")
	model := &swarmLifecycleModel{paused: map[string]bool{}}
	runner.handle = func(ctx context.Context, c Command) (string, error) {
		if len(c.Args) > 3 && c.Args[0] == "service" && c.Args[1] == "inspect" && c.Args[3] == retiredAnalysisEnvironmentFormat {
			return retiredAnalysisEnvironment + "=http://arbitrary-host:8093", nil
		}
		return model.handle(ctx, c)
	}
	if err := s.a.RunSwarm(t.Context(), "deploy", nil); err == nil {
		t.Fatal("unverified arbitrary endpoint accepted as retirement proof")
	}
	for _, prefix := range [][]string{{"service", "update"}, {"service", "rm"}, {"stack", "deploy"}, {"node", "update"}} {
		if swarmCallIndex(runner.calls, prefix...) >= 0 {
			t.Fatalf("malformed proof changed deployment: %v", prefix)
		}
	}
}
