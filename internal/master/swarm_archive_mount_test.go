package master

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestArchiveHostPathConfigurationPersistsLiterallyAndCanReturnToLocal(t *testing.T) {
	a, runner, _ := configApp(t)
	if err := os.WriteFile(a.EnvFile, encodeConfig(ConfigDefaults(Config{"KPL_PASSWORD": "fixture-only"})), 0600); err != nil {
		t.Fatal(err)
	}
	source := "/remote-only/archive space/$literal/${VALUE}/quote\"/한글"
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_ARCHIVE_HOST_PATH=" + source}); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.LoadConfig()
	if err != nil || loaded["KPL_ARCHIVE_HOST_PATH"] != source {
		t.Fatalf("archive setting did not round trip literally: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatal("configuring an archive path inspected Docker or the remote directory")
	}
	if err := a.ConfigCommand(t.Context(), "configure", []string{"KPL_ARCHIVE_HOST_PATH="}); err != nil {
		t.Fatal(err)
	}
	loaded, err = a.LoadConfig()
	if err != nil || loaded["KPL_ARCHIVE_HOST_PATH"] != "" {
		t.Fatalf("could not return to the explicit local-storage setting: %v", err)
	}
	app, completionRunner, output := completionFixture(t)
	if got := completionCandidates(t, app, output, "config", "set", "KPL_ARCHIVE_HOST"); !slices.Equal(got, []string{"KPL_ARCHIVE_HOST_PATH="}) {
		t.Fatalf("new setting missing from completion: %v", got)
	}
	if len(completionRunner.calls) != 0 {
		t.Fatal("archive setting completion contacted Docker")
	}
}

func archiveComposePaths(args []string) []string {
	var paths []string
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--compose-file" {
			paths = append(paths, args[i+1])
		}
	}
	return paths
}

type archiveComposeVolume struct {
	Type   string `yaml:"type"`
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type archiveComposeFile struct {
	Services map[string]struct {
		Volumes []archiveComposeVolume `yaml:"volumes"`
	} `yaml:"services"`
}

func inspectArchiveOverride(t *testing.T, name, source string) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("archive override must be a private regular file: %v", err)
	}
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var compose archiveComposeFile
	if err := yaml.Unmarshal(data, &compose); err != nil {
		t.Fatal(err)
	}
	if len(compose.Services) != 1 || !reflect.DeepEqual(compose.Services["controller"].Volumes, []archiveComposeVolume{{Type: "bind", Source: strings.ReplaceAll(source, "$", "$$"), Target: "/var/lib/kpl/data/runs"}}) {
		t.Fatalf("override changed services or used the wrong literal archive mount: %s", data)
	}
}

func TestSwarmArchiveMountUsesSamePrivateComposeForValidationAndDeployment(t *testing.T) {
	for _, action := range []string{"deploy", "restart"} {
		for _, source := range []string{"", "/remote-only/archive space/$literal/${VALUE}/quote\"/한글"} {
			t.Run(action+source, func(t *testing.T) {
				s, runner, _ := newSwarmFixture(t, action)
				s.config["KPL_ARCHIVE_HOST_PATH"] = source
				s.a = s.a.WithConfig(s.config)
				base := filepath.Join(s.a.Repo, "stack.swarm.yaml")
				before, err := os.ReadFile(base)
				if err != nil {
					t.Fatal(err)
				}
				var captured [][]string
				model := &swarmLifecycleModel{paused: map[string]bool{}}
				runner.handle = func(ctx context.Context, cmd Command) (string, error) {
					if len(cmd.Args) >= 2 && cmd.Args[0] == "stack" && (cmd.Args[1] == "config" || cmd.Args[1] == "deploy") {
						paths := archiveComposePaths(cmd.Args)
						captured = append(captured, paths)
						if len(paths) == 0 || paths[0] != base || source == "" && len(paths) != 1 || source != "" && len(paths) != 2 {
							t.Fatalf("wrong Compose file list: %v", paths)
						}
						if cmd.Args[1] == "config" && cmd.Stdout != io.Discard {
							t.Fatal("Compose validation could disclose interpolated credentials")
						}
						if source != "" {
							inspectArchiveOverride(t, paths[1], source)
						}
					}
					return model.handle(ctx, cmd)
				}
				if err := s.a.RunSwarm(t.Context(), action, nil); err != nil {
					t.Fatal(err)
				}
				if len(captured) != 2 || !slices.Equal(captured[0], captured[1]) {
					t.Fatalf("deployment differed from the validated archive configuration: %v", captured)
				}
				if source != "" {
					if _, err := os.Stat(captured[0][1]); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("archive override was not removed after deployment")
					}
				}
				after, err := os.ReadFile(base)
				if err != nil || !slices.Equal(before, after) {
					t.Fatal("archive mounting rewrote the checkout's deployment asset")
				}
			})
		}
	}
}

func TestSwarmArchiveOverrideIsRemovedOnEveryDeploymentFailure(t *testing.T) {
	for _, failure := range []string{"config", "pull", "stop", "deploy", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			s.config["KPL_ARCHIVE_HOST_PATH"] = "/remote-only/archive"
			s.a = s.a.WithConfig(s.config)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			model := &swarmLifecycleModel{paused: map[string]bool{}, failPull: failure == "pull", failStop: failure == "stop"}
			var temporary string
			runner.handle = func(ctx context.Context, cmd Command) (string, error) {
				if len(cmd.Args) >= 2 && cmd.Args[0] == "stack" {
					if cmd.Args[1] == "config" {
						temporary = archiveComposePaths(cmd.Args)[1]
						if failure == "config" {
							return "", errors.New("invalid deployment")
						}
						if failure == "cancel" {
							cancel()
							return "", context.Canceled
						}
					}
					if cmd.Args[1] == "deploy" && failure == "deploy" {
						return "", errors.New("deployment failed")
					}
				}
				return model.handle(ctx, cmd)
			}
			if err := s.a.RunSwarm(ctx, "restart", nil); err == nil {
				t.Fatal("deployment failure was ignored")
			}
			if temporary == "" {
				t.Fatal("failure did not exercise archive configuration")
			}
			if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed deployment retained its private archive override")
			}
			if failure == "config" || failure == "pull" || failure == "cancel" {
				for _, operation := range [][]string{{"service", "update"}, {"node", "update"}, {"network", "create"}, {"stack", "deploy"}} {
					if swarmCallIndex(runner.calls, operation...) >= 0 {
						t.Fatal("failed deployment preparation changed the cluster")
					}
				}
			}
		})
	}
}

func TestArchiveComposeOverridePreservesLiteralSourceWithOfflineDockerCLI(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI is not installed")
	}
	s, _, _ := newSwarmFixture(t, "deploy")
	source := "/remote archive/$VALUE/${VALUE}/$$literal/quote\"/single'/한글"
	s.config["KPL_ARCHIVE_HOST_PATH"] = source
	base := filepath.Join(s.a.Repo, "stack.swarm.yaml")
	if err := os.WriteFile(base, []byte("version: \"3.8\"\nservices:\n  controller:\n    image: fixture.invalid/kpl:latest\n    volumes: [controller-data:/var/lib/kpl/data]\nvolumes:\n  controller-data: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args, cleanup, err := s.deploymentComposeFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, docker, append([]string{"stack", "config"}, args...)...)
	command.Dir = s.a.Repo
	// No real Docker config, credentials, context, environment or daemon.
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + t.TempDir(), "DOCKER_HOST=unix:///nonexistent-kpl-offline-config.sock", "VALUE=must-not-expand"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("offline Compose validation failed: %v: %s", err, output)
	}
	var compose archiveComposeFile
	if err := yaml.Unmarshal(output, &compose); err != nil {
		t.Fatal(err)
	}
	volumes := compose.Services["controller"].Volumes
	if len(volumes) != 2 || !slices.Contains(volumes, archiveComposeVolume{Type: "volume", Source: "controller-data", Target: "/var/lib/kpl/data"}) || !slices.Contains(volumes, archiveComposeVolume{Type: "bind", Source: source, Target: "/var/lib/kpl/data/runs"}) {
		t.Fatalf("Compose changed the literal path or replaced the local parent volume: %+v", volumes)
	}
}
