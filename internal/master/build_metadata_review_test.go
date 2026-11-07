package master

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildSourceMetadataDoesNotFollowForeignGitDirectory(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("Git is unavailable")
	}
	baseEnv := []string{"PATH=" + os.Getenv("PATH"), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1"}
	git := func(dir string, args ...string) string {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir, command.Env = dir, baseEnv
		out, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	createRepo := func(value string) (string, string) {
		t.Helper()
		dir := t.TempDir()
		git(dir, "init", "--quiet")
		if err := os.WriteFile(filepath.Join(dir, "fixture.txt"), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		git(dir, "add", "fixture.txt")
		git(dir, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "-m", "fixture")
		return dir, git(dir, "rev-parse", "HEAD")
	}
	source, sourceRevision := createRepo("source being built")
	foreign, foreignRevision := createRepo("unrelated inherited Git context")
	if sourceRevision == foreignRevision {
		t.Fatal("fixtures unexpectedly share a revision")
	}
	s, _, _ := newSwarmFixture(t, "publish")
	s.a.Repo = source
	s.a.Environ = append(baseEnv,
		"GIT_DIR="+filepath.Join(foreign, ".git"), "GIT_WORK_TREE="+foreign,
		"GIT_COMMON_DIR="+filepath.Join(foreign, ".git"), "GIT_INDEX_FILE="+filepath.Join(foreign, ".git", "index"),
		"GIT_OBJECT_DIRECTORY="+filepath.Join(foreign, ".git", "objects"), "GIT_ALTERNATE_OBJECT_DIRECTORIES="+filepath.Join(foreign, ".git", "objects"))
	s.a.Runner = ExecRunner{}
	args, err := s.sourceBuildArguments(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(args) < 4 || args[1] != "KPL_SOURCE_REVISION="+sourceRevision || args[3] != "KPL_SOURCE_MODIFIED=false" {
		t.Fatalf("stamped foreign Git context instead of actual source checkout: %q", args)
	}
	t.Run("injected worktree config cannot hide source modifications", func(t *testing.T) {
		if err := os.WriteFile(filepath.Join(source, "fixture.txt"), []byte("actual modified source"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(foreign, "fixture.txt"), []byte("source being built"), 0600); err != nil {
			t.Fatal(err)
		}
		s.a.Environ = append(baseEnv, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=core.worktree", "GIT_CONFIG_VALUE_0="+foreign)
		args, err := s.sourceBuildArguments(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(args) < 4 || args[1] != "KPL_SOURCE_REVISION="+sourceRevision || args[3] != "KPL_SOURCE_MODIFIED=true" {
			t.Fatalf("different worktree hid actual source modifications: %q", args)
		}
	})
}
