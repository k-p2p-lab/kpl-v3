package master

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCLIModernTreeResolvesNativeCommands(t *testing.T) {
	for _, test := range []struct {
		args []string
		want []string
	}{
		{[]string{"dev", "build"}, []string{"build"}},
		{[]string{"dev", "test", "race"}, []string{"test", "race"}},
		{[]string{"config", "init", "KPL_USER=fixture"}, []string{"swarm", "init", "KPL_USER=fixture"}},
		{[]string{"config", "set", "KPL_USER=fixture"}, []string{"swarm", "configure", "KPL_USER=fixture"}},
		{[]string{"config", "show"}, []string{"swarm", "config"}},
		{[]string{"config", "credentials"}, []string{"swarm", "credentials"}},
		{[]string{"config", "version", "--json"}, []string{"config-version", "--json"}},
		{[]string{"image", "login"}, []string{"swarm", "login"}},
		{[]string{"image", "publish", "--platforms", "linux/arm64"}, []string{"swarm", "publish", "--platforms", "linux/arm64"}},
		{[]string{"image", "inspect"}, []string{"swarm", "image-inspect"}},
		{[]string{"swarm", "node", "list"}, []string{"swarm", "nodes"}},
		{[]string{"swarm", "node", "add", "worker-a"}, []string{"swarm", "add-node", "worker-a"}},
		{[]string{"swarm", "node", "remove", "--workers"}, []string{"swarm", "remove-node", "--workers"}},
		{[]string{"swarm", "network", "reset"}, []string{"swarm", "reset-peer-network"}},
		{[]string{"scenario", "validate", "--scenario", "with space.yaml"}, []string{"validate", "--scenario", "with space.yaml"}},
		{[]string{"scenario", "show", "with space.yaml"}, []string{"swarm", "scenario", "with space.yaml"}},
		{[]string{"run", "peer", "--id", "fixture"}, []string{"peer", "--id", "fixture"}},
	} {
		command, args := canonicalCLICommand(test.args[0], test.args[1:])
		if got := append([]string{command}, args...); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("%q => %q; want %q", test.args, got, test.want)
		}
	}
}

func TestCLIModernGroupHelpRequiresNoConfigOrSubprocess(t *testing.T) {
	for _, path := range [][]string{
		{"dev"}, {"dev", "test"}, {"config"}, {"config", "version"},
		{"image"}, {"image", "inspect"}, {"scenario"}, {"scenario", "show"},
		{"run"}, {"swarm", "node"}, {"swarm", "network"},
		{"version"}, {"completion"},
	} {
		for _, args := range [][]string{append(append([]string{}, path...), "--help"), append([]string{"help"}, path...)} {
			a, recorder, output := appFixture(t)
			a.Repo = "/unavailable/no-checkout"
			a.EnvFile = "/unavailable/no-credentials"
			a.EnvFileExplicit = true
			if err := a.Execute(t.Context(), args, "fixture"); err != nil {
				t.Fatalf("%q: %v", args, err)
			}
			if !strings.HasPrefix(output.String(), "Usage: kpl ") || len(recorder.commands) != 0 {
				t.Fatalf("%q performed I/O or lost help: %q %+v", args, output.String(), recorder.commands)
			}
		}
	}
}

func TestCLIModernTreeExecutesDevelopmentAndLiteralScenarios(t *testing.T) {
	a, recorder, _ := appFixture(t)
	if err := a.Execute(t.Context(), []string{"--repo", a.Repo, "dev", "build"}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(recorder.commands) != 1 || recorder.commands[0].Name != "go" || recorder.commands[0].Args[3] != "./cmd/kpl-master" {
		t.Fatalf("modern build did not use the native handler: %+v", recorder.commands)
	}
	cwd := t.TempDir()
	t.Chdir(cwd)
	if err := os.WriteFile("--help", []byte(validScenarioFixture), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{{"scenario", "validate"}, {"scenario", "show"}} {
		a, recorder, output := appFixture(t)
		args := append([]string{"--repo", a.Repo}, command...)
		args = append(args, "--", "--help")
		if err := a.Execute(t.Context(), args, "fixture"); err != nil {
			t.Fatalf("%q: %v", args, err)
		}
		if len(recorder.commands) != 0 || strings.Contains(output.String(), "Usage:") {
			t.Fatalf("literal file became help: %q %+v", output.String(), recorder.commands)
		}
	}
}

func TestCLIImageOptionValuesNeverBecomeGlobalFlagsOrHelp(t *testing.T) {
	for _, value := range []string{"--repo", "--env-file", "--help", "-h"} {
		a, recorder, output := appFixture(t)
		repo := a.Repo
		err := a.Execute(t.Context(), []string{"--repo", repo, "swarm", "deploy", "--image", value}, "fixture")
		if ExitCode(err) != 2 || a.Repo != repo || strings.Contains(output.String(), "Usage:") || len(recorder.commands) != 0 {
			t.Fatalf("image value changed dispatch: %q => %v, %q %+v", value, err, output.String(), recorder.commands)
		}
	}
}

func TestCLIRuntimeAfterGlobalOptionsPreservesRoleFlags(t *testing.T) {
	for _, args := range [][]string{
		{"run", "controller", "--repo", "literal-role-value"},
		{"--repo", "/missing-checkout", "run", "controller", "--repo", "literal-role-value"},
		{"--env-file=/missing-config", "run", "controller", "--env-file", "literal-role-value"},
	} {
		a, recorder, _ := appFixture(t)
		err := a.Execute(t.Context(), args, "fixture")
		if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") || len(recorder.commands) != 0 {
			t.Fatalf("operator parser consumed role flags or resolved an irrelevant path: %q => %v", args, err)
		}
		if !IsRuntimeInvocation(args) {
			t.Fatalf("runtime error routing disagreed with execution: %q", args)
		}
	}
	for _, args := range [][]string{{"run"}, {"config", "show"}, {"controller"}, {"--repo", "checkout", "agent"}, {"--repo"}, {"--repo=", "run", "peer"}, {"--env-file", "", "run", "agent"}} {
		if IsRuntimeInvocation(args) {
			t.Fatalf("non-runtime invocation identified as a role: %q", args)
		}
	}
}

func TestCLIConfiguredVersionRejectsOptionsBeforeRepositoryLookup(t *testing.T) {
	a, recorder, _ := appFixture(t)
	a.Repo = "/unavailable/no-checkout"
	a.EnvFile = "/unavailable/no-config"
	err := a.Execute(t.Context(), []string{"--repo", a.Repo, "config", "version", "--invalid"}, "fixture")
	if ExitCode(err) != 2 || !strings.Contains(err.Error(), "version accepts only --json") || len(recorder.commands) != 0 {
		t.Fatalf("invalid version options read repository/config: %v", err)
	}
}

func TestCLIRemovedEntrypointsRejectBeforeRepositoryConfigOrDocker(t *testing.T) {
	paths := [][]string{
		{"build"}, {"check"}, {"test", "go"}, {"vet"}, {"validate"},
		{"swarm-restart"}, {"swarm-status"}, {"check-swarm"}, {"swarm-reset-network"}, {"test-race"},
		{"config-version"}, {"--version"},
	}
	for _, name := range []string{"init", "configure", "config", "credentials", "login", "publish", "nodes", "add-node", "remove-node", "scenario", "reset-peer-network", "reset-network", "image-inspect", "help"} {
		paths = append(paths, []string{"swarm", name})
	}
	for _, role := range roleCommands {
		paths = append(paths, []string{role})
	}
	for _, path := range paths {
		for _, args := range [][]string{path, append(append([]string{}, path...), "--help"), append([]string{"help"}, path...)} {
			a, recorder, output := appFixture(t)
			a.Repo = "/unavailable/no-checkout"
			a.EnvFile = "/unavailable/no-config"
			a.EnvFileExplicit = true
			if err := a.Execute(t.Context(), args, "fixture"); ExitCode(err) != 2 {
				t.Fatalf("removed entrypoint accepted or read an unavailable path: %q => %v", args, err)
			}
			if output.Len() != 0 || len(recorder.commands) != 0 {
				t.Fatalf("removed entrypoint produced help or executed a subprocess: %q => %q %+v", args, output.String(), recorder.commands)
			}
		}
	}
}

func TestCLIInvalidCommandNeverEchoesCredentialArguments(t *testing.T) {
	a, _, output := appFixture(t)
	secret := "fixture-secret-never-print"
	err := a.Execute(t.Context(), []string{"config", "sett", "KPL_PASSWORD=" + secret}, "fixture")
	if ExitCode(err) != 2 || strings.Contains(err.Error(), secret) || strings.Contains(output.String(), secret) {
		t.Fatalf("command rejection leaked a credential argument: %v", err)
	}
}

func TestCLISeparatorPreservesOptionLikeScenarioNames(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	for _, name := range []string{"--help", "-h", "--repo", "--env-file"} {
		if err := os.WriteFile(name, []byte(validScenarioFixture), 0600); err != nil {
			t.Fatal(err)
		}
		for _, command := range [][]string{{"scenario", "validate"}, {"scenario", "show"}} {
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
			if command[1] == "validate" {
				if len(recorder.commands) != 0 || !strings.Contains(output.String(), "valid scenario: literal-path-fixture") {
					t.Fatalf("validation was not performed inside kpl: %s", output.String())
				}

			} else if output.String() != validScenarioFixture || len(recorder.commands) != 0 {
				t.Fatalf("literal scenario was treated as an option: %q %+v", output.String(), recorder.commands)
			}
		}
	}
}

func TestCLIHelpLikeOptionValuesAndSettingsStayLiteral(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	a, recorder, output := appFixture(t)
	if err := os.WriteFile("--help", []byte(validScenarioFixture), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.Execute(t.Context(), []string{"--repo", a.Repo, "scenario", "validate", "--scenario", "--help"}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(recorder.commands) != 0 || !strings.Contains(output.String(), "valid scenario: literal-path-fixture") {
		t.Fatal("scenario option value was parsed as help")
	}

	for _, value := range []string{"--help", "-h", "--repo"} {
		a, recorder, output = appFixture(t)
		envPath := filepath.Join(a.Repo, "fixture.env")
		fixture := "KPL_USER=fixture-user\nKPL_PASSWORD=fixture-only-password\nGRAFANA_ADMIN_PASSWORD=fixture-only-grafana\n"
		if err := os.WriteFile(envPath, []byte(fixture), 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"--repo", a.Repo, "--env-file", envPath, "config", "set", "KPL_USER=" + value}
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
		{"swarm", "node", "add", "--", "--workers"},
		{"swarm", "restart", "--", "--platforms", "linux/arm64"},
		{"swarm", "logs", "--", "--tail", "all"},
		{"config", "set", "--", "--help"},
		{"dev", "build", "--", "--help"},
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
		a, recorder, output := appFixture(t)
		if err := os.WriteFile(value, []byte(validScenarioFixture), 0600); err != nil {
			t.Fatal(err)
		}
		repo := a.Repo
		envPath := filepath.Join(repo, "selected.env")
		args := []string{"--repo", repo, "scenario", "validate", "--scenario", value, "--env-file", envPath}
		if err := a.Execute(t.Context(), args, "fixture"); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if a.Repo != repo || a.EnvFile != envPath || len(recorder.commands) != 0 || !strings.Contains(output.String(), "valid scenario: literal-path-fixture") {
			t.Fatalf("subcommand value changed global parsing: %v => %+v", args, recorder.commands)
		}
	}
	for _, args := range [][]string{
		{"scenario", "validate", "--scenario", "--", "--repo"},
		{"scenario", "validate", "--scenario", "--", "--help"},
	} {
		a, recorder, output := appFixture(t)
		if err := a.Execute(t.Context(), append([]string{"--repo", a.Repo}, args...), "fixture"); err != nil {
			t.Fatalf("separator after value option: %v: %v", args, err)
		}
		if len(recorder.commands) != 0 || !strings.Contains(output.String(), "valid scenario: literal-path-fixture") {
			t.Fatalf("separator did not preserve option value: %+v", recorder.commands)
		}
	}
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"image", "publish", "--platforms", "--repo"}, "--platforms requires"},
		{[]string{"swarm", "restart", "--image", "--env-file"}, "--image requires"},
		{[]string{"swarm", "logs", "--tail", "--repo"}, "--tail needs"},
		{[]string{"swarm", "logs", "access", "--context", "--env-file"}, "invalid Docker context"},
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
