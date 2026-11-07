package master

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCLICommandNormalizationCannotActivateOptionsAfterSeparator(t *testing.T) {
	for _, command := range [][]string{
		{"swarm", "--", "node", "add", "--workers"},
		{"swarm", "--", "node", "remove", "--all"},
		{"swarm", "node", "--", "add", "--workers"},
		{"swarm", "node", "--", "remove", "--all"},
		{"swarm", "--", "deploy", "--workers"},
		{"config", "--", "set", "--help"},
		{"version", "--", "--json"},
		{"config", "version", "--", "--json"},
	} {
		a, recorder, _ := appFixture(t)
		args := append([]string{"--repo", a.Repo}, command...)
		err := a.Execute(t.Context(), args, "fixture")
		if ExitCode(err) != 2 || len(recorder.commands) != 0 {
			t.Errorf("post-separator option became active: %q: error=%v, subprocesses=%d", command, err, len(recorder.commands))
		}
	}
}

func TestCompletionRejectsSeparatorInsideCommandPathWithoutDocker(t *testing.T) {
	a, recorder, output := completionFixture(t)
	for _, args := range [][]string{
		{"swarm", "--", "node", "add", ""},
		{"swarm", "node", "--", "remove", ""},
		{"swarm", "--", "deploy", ""},
		{"config", "--", "set", "KPL_"},
	} {
		if got := completionCandidates(t, a, output, args...); len(got) != 0 || len(recorder.calls) != 0 {
			t.Fatalf("invalid partial command path offered completions or queried Docker: %q => %q", args, got)
		}
	}
}

func TestCLIGlobalSeparatorRetainsSubcommandHelp(t *testing.T) {
	for _, command := range [][]string{
		{"config"}, {"config", "version"}, {"dev", "build"},
		{"image", "publish"}, {"swarm", "deploy"}, {"swarm", "node", "remove"},
		{"scenario", "show"}, {"scenario", "validate"},
	} {
		a, recorder, output := appFixture(t)
		args := append([]string{"--"}, command...)
		args = append(args, "--help")
		if err := a.Execute(t.Context(), args, "fixture"); err != nil {
			t.Fatalf("global separator disabled subcommand help: %q: %v", command, err)
		}
		if !strings.HasPrefix(output.String(), "Usage: kpl ") || len(recorder.commands) != 0 {
			t.Fatalf("help after global separator performed I/O: %q", command)
		}
	}
}

func TestCLIGlobalSeparatorDoesNotEraseLeafSeparator(t *testing.T) {
	for _, command := range [][]string{
		{"swarm", "deploy", "--", "--help"},
		{"swarm", "node", "remove", "--", "--all"},
		{"version", "--", "--json"},
	} {
		a, recorder, output := appFixture(t)
		args := append([]string{"--"}, command...)
		if err := a.Execute(t.Context(), args, "fixture"); ExitCode(err) != 2 || len(recorder.commands) != 0 || output.Len() != 0 {
			t.Fatalf("global separator erased the leaf boundary: %q: %v", command, err)
		}
	}
	cwd := t.TempDir()
	t.Chdir(cwd)
	if err := os.WriteFile("--help", []byte(validScenarioFixture), 0600); err != nil {
		t.Fatal(err)
	}
	a, recorder, output := appFixture(t)
	if err := a.Execute(t.Context(), []string{"--", "scenario", "show", "--", "--help"}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if output.String() != validScenarioFixture || len(recorder.commands) != 0 {
		t.Fatal("global separator changed a literal scenario filename to help")
	}
}

func TestCompletionGlobalSeparatorRetainsOnlySubcommandOptions(t *testing.T) {
	a, recorder, output := completionFixture(t)
	got := completionCandidates(t, a, output, "--", "swarm", "deploy", "--")
	if !reflect.DeepEqual(got, []string{"--all", "--all-excluding-self", "--image", "--workers"}) || len(recorder.calls) != 0 {
		t.Fatalf("global separator suppressed leaf options or suggested global ones: %q", got)
	}
}

func TestCompletionDoesNotActivateLiteralValueOptions(t *testing.T) {
	for _, args := range [][]string{
		{"swarm", "logs", "--", "--context", ""},
		{"swarm", "logs", "auth", "--", "--context", ""},
		{"swarm", "logs", "--", "--tail", ""},
		{"image", "publish", "--", "--platforms", "linux/"},
		{"--", "swarm", "logs", "--", "--context", ""},
		{"--", "image", "publish", "--", "--platforms", "linux/"},
	} {
		a, recorder, output := completionFixture(t)
		if got := completionCandidates(t, a, output, args...); len(got) != 0 || len(recorder.calls) != 0 {
			t.Errorf("literal value option activated completion: %q => %q, subprocesses=%d", args, got, len(recorder.calls))
		}
	}
	a, recorder, output := completionFixture(t)
	if got := completionCandidates(t, a, output, "image", "publish", "--platforms", "--", "linux/"); len(got) != 4 || len(recorder.calls) != 0 {
		t.Fatalf("separator between an active option and its value lost completion: %q", got)
	}
}

func TestCompletionNodeMetadataCannotSuggestSelectorsAsHostnames(t *testing.T) {
	a, _, output := completionFixture(t)
	runner := &appRecorder{output: "node1 --all\nnode2 worker-normal\nnode3 --workers\nbad/id misleading-host\nnode4 invalid/host\n"}
	a.Runner = runner
	got := completionCandidates(t, a, output, "swarm", "node", "remove", "worker-normal", "")
	want := []string{"node1", "node3", "node4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("node metadata suggested a selector or invalid node argument: got %q want %q", got, want)
	}
}

func TestCLIExplicitConfigurationWorksWithoutCheckout(t *testing.T) {
	t.Chdir(t.TempDir())
	a, runner, output := configApp(t)
	a.Repo = filepath.Join(t.TempDir(), "missing-checkout")
	for _, command := range [][]string{
		{"config", "init", "KPL_USER=fixture", "KPL_PASSWORD=fake-dashboard", "GRAFANA_ADMIN_PASSWORD=fake-grafana"},
		{"config", "set", "KPL_AGENT_CAPACITY=37"},
		{"config", "show"},
		{"config", "version", "--json"},
		{"config", "credentials"},
	} {
		output.Reset()
		before := len(runner.calls)
		if err := a.Execute(t.Context(), append([]string{"--env-file", a.EnvFile}, command...), "fixture"); err != nil {
			t.Fatalf("explicit configuration required a checkout: %q: %v", command[:2], err)
		}
		if command[1] != "init" && len(runner.calls) != before {
			t.Fatalf("configuration unexpectedly contacted Docker: %q", command[:2])
		}
		if command[1] != "credentials" && (strings.Contains(output.String(), "fake-dashboard") || strings.Contains(output.String(), "fake-grafana")) {
			t.Fatalf("configuration output disclosed a fake credential: %q", command[:2])
		}
	}
}

func TestCLIExplicitScenarioShowWorksWithoutCheckout(t *testing.T) {
	t.Chdir(t.TempDir())
	a, recorder, output := appFixture(t)
	path := filepath.Join(t.TempDir(), "standalone.yaml")
	if err := os.WriteFile(path, []byte(validScenarioFixture), 0600); err != nil {
		t.Fatal(err)
	}
	a.Repo = filepath.Join(t.TempDir(), "missing-checkout")
	if err := a.Execute(t.Context(), []string{"scenario", "show", path}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if output.String() != validScenarioFixture || len(recorder.commands) != 0 {
		t.Fatal("explicit scenario output depended on a source checkout or subprocess")
	}
}

func TestCLIExplicitFileOperationsHonorRequestedCheckout(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, command := range [][]string{{"config", "show"}, {"config", "version"}, {"scenario", "show", "fixture.yaml"}} {
		for _, environment := range []bool{false, true} {
			a, recorder, _ := appFixture(t)
			missing := filepath.Join(t.TempDir(), "missing-checkout")
			args := append([]string{"--env-file", filepath.Join(t.TempDir(), "explicit.env")}, command...)
			if environment {
				a.Environ = append(a.Environ, "KPL_REPO="+missing)
			} else {
				args = append([]string{"--repo", missing}, args...)
			}
			err := a.Execute(t.Context(), args, "fixture")
			if err == nil || !strings.Contains(err.Error(), "source checkout") || len(recorder.commands) != 0 {
				t.Fatalf("explicit checkout was ignored: %q: %v", command, err)
			}
		}
	}
}

func FuzzCLICommandBoundaries(f *testing.F) {
	repo := f.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module "+runtimeModule+"\n"), 0600); err != nil {
		f.Fatal(err)
	}
	// Inputs are indices into an intentionally credential-free vocabulary.
	// No runtime role can start and all subprocesses use an in-memory recorder.
	tokens := []string{"dev", "build", "check", "test", "go", "swarm", "node", "add", "remove", "network", "reset", "config", "set", "show", "version", "image", "publish", "inspect", "scenario", "validate", "help", "--", "--help", "-h", "--json", "--workers", "--all", "--repo", "--env-file", "--scenario", "--image", "--platforms", "--tail", "--context", "", "fixture", "KPL_USER=fixture", "linux/amd64"}
	for _, seed := range [][]byte{{5, 21, 6, 8, 26}, {5, 6, 21, 8, 25}, {11, 21, 12, 22}, {18, 19, 21, 22}, {20, 20, 0, 1, 22}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			return
		}
		args := []string{"--repo", repo}
		for _, value := range data {
			args = append(args, tokens[int(value)%len(tokens)])
		}
		runner := &appRecorder{}
		a := &App{Repo: repo, EnvFile: filepath.Join(repo, "fixture.env"), EnvFileExplicit: true, Runner: runner, Stdout: new(bytes.Buffer), Stderr: io.Discard}
		_ = a.Execute(t.Context(), args, "fixture")
		for _, call := range runner.commands {
			if call.Name != "go" && call.Name != "node" && call.Name != "docker" {
				t.Fatalf("unexpected executable from malformed input: %q", call.Name)
			}
		}
	})
}

func FuzzCLIRemovedPathsNeverExecuteOrDiscloseArguments(f *testing.F) {
	repo := f.TempDir()
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module "+runtimeModule+"\n"), 0600); err != nil {
		f.Fatal(err)
	}
	removed := [][]string{{"build"}, {"test"}, {"validate"}, {"swarm-restart"}, {"test-swarm"}, {"check-swarm"}, {"config-version"}, {"controller"}, {"agent"}, {"peer"}, {"swarm-agent"}, {"swarm", "config"}, {"swarm", "credentials"}, {"swarm", "publish"}, {"swarm", "nodes"}, {"swarm", "image-inspect"}}
	tokens := []string{"--help", "-h", "--", "help", "version", "--json", "--repo", "--env-file", "--image", "--scenario", "--context", "--platforms", "", "fixture", "KPL_PASSWORD=must-not-echo-review-secret", "GRAFANA_ADMIN_PASSWORD=must-not-echo-review-secret"}
	f.Add([]byte{0, 0})
	f.Add([]byte{3, 2, 14, 0})
	f.Add([]byte{13, 6, 8, 14})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 64 {
			return
		}
		path := append([]string{}, removed[int(data[0])%len(removed)]...)
		for _, value := range data[1:] {
			path = append(path, tokens[int(value)%len(tokens)])
		}
		runner := &appRecorder{}
		output := new(bytes.Buffer)
		a := &App{Repo: repo, EnvFile: filepath.Join(repo, "fixture.env"), EnvFileExplicit: true, Runner: runner, Stdout: output, Stderr: io.Discard}
		args := append([]string{"--repo", repo}, path...)
		err := a.Execute(t.Context(), args, "fixture")
		if ExitCode(err) != 2 || len(runner.commands) != 0 || output.Len() != 0 || strings.Contains(err.Error(), "must-not-echo-review-secret") {
			t.Fatalf("removed path bypassed rejection or disclosed arguments: error=%v subprocesses=%d", err, len(runner.commands))
		}
		output.Reset()
		if err := a.Complete(t.Context(), append(path, "")); err != nil || output.Len() != 0 || len(runner.commands) != 0 {
			t.Fatal("removed path produced completions or subprocesses")
		}
	})
}
