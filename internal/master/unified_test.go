package master

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

func TestContainerRolesNeedNoCheckoutOrOperatorConfiguration(t *testing.T) {
	t.Setenv("KPL_RUN_MIN_FREE_BYTES", "")
	t.Setenv("KPL_ARCHIVE_MIB_PER_SECOND", "")
	for _, role := range roleCommands {
		t.Run(role, func(t *testing.T) {
			if command, _ := canonicalCLICommand("run", []string{role}); command != role {
				t.Fatal("runtime role did not resolve to the native role handler")
			}
			a, recorder, _ := appFixture(t)
			a.Repo = ""
			a.EnvFile = filepath.Join(t.TempDir(), "missing.env")
			a.EnvFileExplicit = true
			a.Environ = []string{"KPL_REPO=/missing-source"}
			if err := a.Execute(t.Context(), []string{"run", role, "--help"}, "fixture"); err != nil {
				t.Fatal(err)
			}
			if a.Repo != "" || len(recorder.commands) != 0 {
				t.Fatal("container role used operator repository discovery or a subprocess")
			}
		})
	}
}

func TestValidationUsesSameExecutableWithoutGoOrCheckout(t *testing.T) {
	t.Chdir(t.TempDir())
	a, recorder, out := appFixture(t)
	a.Repo = ""
	a.Environ = nil
	if err := os.WriteFile("input.yaml", []byte(validScenarioFixture), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.Execute(t.Context(), []string{"scenario", "validate", "--scenario=input.yaml"}, "fixture"); err != nil {
		t.Fatal(err)
	}
	if len(recorder.commands) != 0 || a.Repo != "" || !strings.Contains(out.String(), "valid scenario: literal-path-fixture") {
		t.Fatal("validation required an external executable or checkout")
	}
	if err := os.WriteFile("invalid.yaml", []byte("version: 99\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.Execute(t.Context(), []string{"scenario", "validate", "--scenario", "invalid.yaml"}, "fixture"); err == nil {
		t.Fatal("invalid scenario was accepted")
	}
	if err := syscall.Mkfifo("input.pipe", 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.Execute(t.Context(), []string{"scenario", "validate", "--scenario", "input.pipe"}, "fixture"); err == nil {
		t.Fatal("FIFO accepted as scenario input")
	}
}

func repositoryForTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func TestMakeOnlyBootstrapsSingleBinary(t *testing.T) {
	makePath, err := exec.LookPath("make")
	if err != nil {
		t.Skip("Make is optional outside bootstrap checks")
	}
	data, err := os.ReadFile(filepath.Join(repositoryForTest(t), "Makefile"))
	if os.IsNotExist(err) {
		t.Skip("bootstrap Makefile is absent")
	}
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Makefile"), data, 0600); err != nil {
		t.Fatal(err)
	}
	toolDir := t.TempDir()
	logPath := filepath.Join(dir, "go-args")
	if err := os.WriteFile(filepath.Join(toolDir, "go"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"$KPL_BOOTSTRAP_TEST_LOG\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{nil, {"build"}, {"check"}, {"test"}, {"swarm-deploy"}} {
		if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), makePath, append([]string{"--no-print-directory", "-rR"}, args...)...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "PATH="+toolDir, "KPL_BOOTSTRAP_TEST_LOG="+logPath, "MAKEFLAGS=", "MAKEOVERRIDES=")
		output, err := cmd.CombinedOutput()
		if len(args) == 0 || args[0] == "build" {
			if err != nil {
				t.Fatalf("bootstrap failed: %v %s", err, output)
			}
			got, err := os.ReadFile(logPath)
			if err != nil || string(got) != "build\n-o\nbin/kpl\n./cmd/kpl-master\n" {
				t.Fatalf("unexpected bootstrap commands: %q %v", got, err)
			}
		} else {
			if err == nil {
				t.Fatalf("removed Make entrypoint %q accepted", args)
			}
			if _, err := os.Stat(logPath); !os.IsNotExist(err) {
				t.Fatalf("removed entrypoint ran a build: %v", err)
			}
		}
	}
}
