package master

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type appRecorder struct {
	commands []Command
	failAt   int
	err      error
	output   string
}

func (r *appRecorder) Run(_ context.Context, c Command) error {
	r.commands = append(r.commands, c)
	if r.failAt == len(r.commands) {
		return r.err
	}
	return nil
}
func (r *appRecorder) Output(ctx context.Context, c Command) ([]byte, error) {
	return []byte(r.output), r.Run(ctx, c)
}

func sourceFixture(t *testing.T, module string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module "+module+"\n\ngo 1.25.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return root
}
func appFixture(t *testing.T) (*App, *appRecorder, *bytes.Buffer) {
	t.Helper()
	recorder := &appRecorder{}
	out := &bytes.Buffer{}
	app := &App{Repo: sourceFixture(t, runtimeModule), Environ: []string{"PATH=/test", "DOCKER_CONTEXT=manager", "KEEP=unchanged"}, Stdin: strings.NewReader(""), Stdout: out, Stderr: io.Discard, Runner: recorder}
	return app, recorder, out
}

func TestHelpVersionAndCompletionDoNotNeedCheckoutOrCredentials(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"swarm"}, {"swarm", "help"}, {"swarm", "restart", "--help"}, {"version"}, {"completion", "bash"}} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			a, r, _ := appFixture(t)
			a.EnvFile = filepath.Join(t.TempDir(), "missing")
			a.EnvFileExplicit = true
			if err := a.Execute(t.Context(), args, "test-version"); err != nil {
				t.Fatal(err)
			}
			if len(r.commands) > 0 {
				t.Fatalf("offline help made subprocess calls: %+v", r.commands)
			}
		})
	}
}

func TestRepositoryDiscoveryWorksWithoutLegacyEntrypoints(t *testing.T) {
	workspace := t.TempDir()
	repo := filepath.Join(workspace, "v3")
	master := filepath.Join(workspace, "unrelated")
	for path, module := range map[string]string{repo: runtimeModule, master: "example.com/unrelated"} {
		if err := os.MkdirAll(filepath.Join(path, "nested"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "go.mod"), []byte("module "+module+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, cwd := range []string{workspace, filepath.Join(master, "nested"), filepath.Join(repo, "nested")} {
		found, err := findModule("", runtimeModule, cwd, "/not-installed/kpl")
		if err != nil || found != repo {
			t.Fatalf("discovery from %s: %s %v", cwd, found, err)
		}
	}
	if _, err := findModule(master, runtimeModule, workspace, ""); err == nil {
		t.Fatal("accepted operator checkout as runtime")
	}
}

func TestDevelopmentCommandsAreNativeAndPreserveArguments(t *testing.T) {
	a, r, _ := appFixture(t)
	for _, path := range []string{"internal/webui/a_test.cjs", "internal/webui/path with spaces_test.cjs"} {
		p := filepath.Join(a.Repo, path)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("// fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.RunDevelopment(t.Context(), "check", nil); err != nil {
		t.Fatal(err)
	}
	if len(r.commands) != 6 {
		t.Fatalf("check commands=%d: %+v", len(r.commands), r.commands)
	}
	for _, c := range r.commands {
		if c.Name != "go" && c.Name != "node" {
			t.Fatalf("legacy dependency: %+v", c)
		}
	}
	if r.commands[0].Args[3] != "./cmd/kpl-master" || r.commands[1].Args[3] != "./cmd/kpl" {
		t.Fatal("operator and runtime build targets were mixed")
	}
	if len(r.commands[5].Args) != 3 || !strings.Contains(r.commands[5].Args[2], "path with spaces") {
		t.Fatalf("UI filenames were shell-split: %+v", r.commands[5].Args)
	}
	r.commands = nil
	input := filepath.Join(t.TempDir(), "scenario $(touch marker) ' spaced.yaml")
	if err := a.RunDevelopment(t.Context(), "validate", []string{"--scenario", input}); err != nil {
		t.Fatal(err)
	}
	want := []string{"run", "./cmd/kpl", "validate", "--scenario", input}
	if !reflect.DeepEqual(r.commands[0].Args, want) {
		t.Fatalf("literal path changed: %#v", r.commands[0].Args)
	}
}

func TestDevelopmentFailureStopsLaterCommands(t *testing.T) {
	a, r, _ := appFixture(t)
	failure := errors.New("build failed")
	r.failAt = 1
	r.err = failure
	if err := a.RunDevelopment(t.Context(), "check", nil); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if len(r.commands) != 1 {
		t.Fatal("ran checks after build failed")
	}
}

func TestTestScopesAndEnvironmentAreExplicit(t *testing.T) {
	for _, scope := range []string{"go", "swarm", "race", "linux"} {
		t.Run(scope, func(t *testing.T) {
			a, r, _ := appFixture(t)
			if err := a.RunDevelopment(t.Context(), "test", []string{scope}); err != nil {
				t.Fatal(err)
			}
			for _, c := range r.commands {
				if c.Name == "make" || c.Name == "sh" || c.Name == "bash" {
					t.Fatal("native test depends on legacy entrypoints")
				}
			}
			if scope == "race" {
				joined := strings.Join(r.commands[0].Env, "\n")
				if !strings.Contains(joined, "GORACE=atexit_sleep_ms=0") || !strings.Contains(joined, "KEEP=unchanged") {
					t.Fatal(joined)
				}
				if a.Env("GORACE") != "" {
					t.Fatal("mutated parent environment")
				}
			}
			if scope == "linux" && !reflect.DeepEqual(r.commands[0].Args, []string{"build", "--target", "test", "-t", "kpl-v3:test", a.Repo}) {
				t.Fatalf("wrong container check: %+v", r.commands[0])
			}
		})
	}
}

func TestCLIExplicitPathsUseInvocationDirectory(t *testing.T) {
	a, r, _ := appFixture(t)
	cwd := t.TempDir()
	t.Chdir(cwd)
	args := []string{"--repo", a.Repo, "--env-file", "nested/config with spaces", "--", "validate", "--scenario", "scenario.yaml"}
	if err := a.Execute(t.Context(), args, "test"); err != nil {
		t.Fatal(err)
	}
	if a.EnvFile != filepath.Join(cwd, "nested/config with spaces") {
		t.Fatal(a.EnvFile)
	}
	if got := r.commands[0].Args[4]; got != filepath.Join(cwd, "scenario.yaml") {
		t.Fatal(got)
	}
	if r.commands[0].Dir != a.Repo {
		t.Fatal("runtime validation used operator checkout")
	}
}

func TestUnknownArgumentsNeverInvokeDocker(t *testing.T) {
	for _, tail := range [][]string{{"swarm", "deploy", "--workers", "node1"}, {"swarm", "restart", "--unknown"}, {"test", "unknown"}, {"build", "extra"}, {"swarm", "remove", "extra"}, {"swarm", "logs", "controller", "--context", "remote"}} {
		a, r, _ := appFixture(t)
		args := append([]string{"--repo", a.Repo}, tail...)
		if err := a.Execute(t.Context(), args, "test"); err == nil {
			t.Fatalf("accepted invalid args %q", tail)
		}
		if len(r.commands) != 0 {
			t.Fatalf("invalid args contacted subprocess: %q", tail)
		}
	}
}

func TestWithEnvDoesNotChangeOriginalOrDropDockerSettings(t *testing.T) {
	a, _, _ := appFixture(t)
	a.Environ = append(a.Environ, "DOCKER_DEFAULT_PLATFORM=linux/arm64")
	value := "literal $(never run)"
	copy := a.WithEnv(map[string]*string{"DOCKER_DEFAULT_PLATFORM": nil, "VALUE": &value})
	if copy.Env("DOCKER_DEFAULT_PLATFORM") != "" || copy.Env("DOCKER_CONTEXT") != "manager" || copy.Env("VALUE") != value {
		t.Fatal(copy.Environ)
	}
	if a.Env("DOCKER_DEFAULT_PLATFORM") == "" || a.Env("VALUE") != "" {
		t.Fatal("parent environment changed")
	}
}

func TestRuntimeBuildCannotOverwriteOperator(t *testing.T) {
	a, r, _ := appFixture(t)
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(a.Repo, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(self, filepath.Join(a.Repo, "bin", "kpl-runtime")); err != nil {
		t.Skipf("hardlink unavailable: %v", err)
	}
	if err := a.build(t.Context()); err == nil {
		t.Fatal("would overwrite operator")
	}
	if len(r.commands) != 0 {
		t.Fatal("started unsafe build")
	}
}
