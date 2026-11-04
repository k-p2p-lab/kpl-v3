package master

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIAliasHelpUsesCanonicalCommandWithoutIO(t *testing.T) {
	for _, args := range [][]string{
		{"swarm-restart", "--help"}, {"swarm-status", "-h"},
		{"check-swarm", "--help"}, {"swarm-reset-network", "--help"},
		{"help", "swarm-restart"}, {"help", "check-swarm"},
	} {
		t.Run(strings.Join(args, "_"), func(t *testing.T) {
			a, recorder, output := appFixture(t)
			a.EnvFile = filepath.Join(t.TempDir(), "missing-config")
			a.EnvFileExplicit = true
			if err := a.Execute(t.Context(), args, "fixture"); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(output.String(), "Usage: kpl [--repo DIR] [--env-file PATH] swarm COMMAND") {
				t.Fatalf("alias did not select Swarm help: %s", output.String())
			}
			if len(recorder.commands) != 0 {
				t.Fatalf("help executed a subprocess: %+v", recorder.commands)
			}
		})
	}
}

func TestCLISeparatorPreservesOptionLikeScenarioNames(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, name := range []string{"--help", "-h", "--repo", "--env-file"} {
		if err := os.WriteFile(name, []byte("name: literal-path-fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
		for _, command := range [][]string{{"validate"}, {"swarm", "scenario"}, {"swarm-scenario"}} {
			a, recorder, output := appFixture(t)
			repo := a.Repo
			args := append([]string{"--repo", repo}, command...)
			args = append(args, "--", name)
			if err := a.Execute(t.Context(), args, "fixture"); err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			if a.Repo != repo {
				t.Fatalf("literal filename changed the selected repository: %v", args)
			}
			if command[0] == "validate" {
				if len(recorder.commands) != 1 || recorder.commands[0].Name != "go" || recorder.commands[0].Args[4] != filepath.Join(cwd, name) {
					t.Fatalf("validation did not preserve one absolute path: %+v", recorder.commands)
				}
			} else if output.String() != "name: literal-path-fixture\n" || len(recorder.commands) != 0 {
				t.Fatalf("literal scenario was treated as an option: %q %+v", output.String(), recorder.commands)
			}
		}
	}
}

func TestCLIHelpLikeOptionValuesAndSettingsStayLiteral(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	a, recorder, output := appFixture(t)
	if err := a.Execute(t.Context(), []string{"--repo", a.Repo, "validate", "--scenario", "--help"}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(recorder.commands) != 1 || recorder.commands[0].Args[4] != filepath.Join(cwd, "--help") {
		t.Fatalf("scenario option value was parsed as help: %+v", recorder.commands)
	}
	for _, value := range []string{"--help", "-h", "--repo"} {
		a, recorder, output = appFixture(t)
		envPath := filepath.Join(a.Repo, "fixture.env")
		fixture := "KPL_USER=fixture-user\nKPL_PASSWORD=fixture-only-password\nGRAFANA_ADMIN_PASSWORD=fixture-only-grafana\n"
		if err := os.WriteFile(envPath, []byte(fixture), 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"--repo", a.Repo, "--env-file", envPath, "swarm-configure", "KPL_USER=" + value}
		if err := a.Execute(t.Context(), args, "fixture"); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(envPath)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "KPL_USER="+value+"\n") || strings.Contains(output.String(), "Usage:") || len(recorder.commands) != 0 {
			t.Fatalf("literal setting was interpreted as a CLI option: %q", value)
		}
	}
}

func TestCLISeparatorDoesNotActivateOpaqueSwarmOptions(t *testing.T) {
	for _, command := range [][]string{
		{"swarm", "deploy", "--", "--workers"},
		{"swarm-deploy", "--", "--workers"},
		{"swarm", "restart", "--", "--platforms", "linux/arm64"},
		{"swarm", "logs", "--", "--tail", "all"},
		{"swarm", "configure", "--", "--help"},
		{"build", "--", "--help"},
	} {
		a, recorder, _ := appFixture(t)
		args := append([]string{"--repo", a.Repo}, command...)
		if err := a.Execute(t.Context(), args, "fixture"); ExitCode(err) != 2 {
			t.Fatalf("opaque option was accepted or treated as help: %v => %v", command, err)
		}
		if len(recorder.commands) != 0 {
			t.Fatalf("opaque option triggered a subprocess: %v => %+v", command, recorder.commands)
		}
	}
}

func TestCLIGlobalExtractionPreservesSubcommandOptionValues(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, value := range []string{"--repo", "--env-file", "--help", "-h"} {
		a, recorder, _ := appFixture(t)
		repo := a.Repo
		envPath := filepath.Join(repo, "selected.env")
		args := []string{"--repo", repo, "validate", "--scenario", value, "--env-file", envPath}
		if err := a.Execute(t.Context(), args, "fixture"); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if a.Repo != repo || a.EnvFile != envPath || len(recorder.commands) != 1 || recorder.commands[0].Args[4] != filepath.Join(cwd, value) {
			t.Fatalf("subcommand value changed global parsing: %v => %+v", args, recorder.commands)
		}
	}
	for _, args := range [][]string{
		{"validate", "--scenario", "--", "--repo"},
		{"validate", "--scenario", "--", "--help"},
	} {
		a, recorder, _ := appFixture(t)
		if err := a.Execute(t.Context(), append([]string{"--repo", a.Repo}, args...), "fixture"); err != nil {
			t.Fatalf("separator after value option: %v: %v", args, err)
		}
		if len(recorder.commands) != 1 || recorder.commands[0].Args[4] != filepath.Join(cwd, args[len(args)-1]) {
			t.Fatalf("separator did not preserve option value: %+v", recorder.commands)
		}
	}
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"swarm", "publish", "--platforms", "--repo"}, "--platforms requires"},
		{[]string{"swarm-restart", "--platforms", "--env-file"}, "--platforms requires"},
		{[]string{"swarm", "logs", "--tail", "--repo"}, "--tail needs"},
		{[]string{"swarm-logs", "access", "--context", "--env-file"}, "invalid Docker context"},
	} {
		a, recorder, _ := appFixture(t)
		repo := a.Repo
		args := append([]string{"--repo", repo}, test.args...)
		err := a.Execute(t.Context(), args, "fixture")
		if ExitCode(err) != 2 || !strings.Contains(err.Error(), test.want) || a.Repo != repo || a.EnvFile != filepath.Join(repo, ".env.swarm") || len(recorder.commands) != 0 {
			t.Fatalf("literal value was consumed as a global: %v => %v", args, err)
		}
	}
}
