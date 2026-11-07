package master

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type buildSourceRunner struct {
	revision string
	status   string
	fail     bool
	calls    int
}

func (r *buildSourceRunner) Run(context.Context, Command) error {
	return errors.New("source metadata must not mutate the checkout")
}

func (r *buildSourceRunner) Output(_ context.Context, c Command) ([]byte, error) {
	r.calls++
	if c.Name != "git" || r.fail {
		return nil, errors.New("source query failed")
	}
	if c.Args[0] == "rev-parse" {
		return []byte(r.revision), nil
	}
	return []byte(r.status), nil
}

func TestImageBuildMetadataUsesSourceCheckoutAndMarksModified(t *testing.T) {
	s, _, _ := newSwarmFixture(t, "publish")
	if err := os.WriteFile(filepath.Join(s.a.Repo, ".git"), []byte("gitdir: fixture-worktree"), 0600); err != nil {
		t.Fatal(err)
	}
	runner := &buildSourceRunner{revision: strings.Repeat("b", 40), status: " M implementation.go\n?? another.go\n"}
	s.a.Runner = runner
	args, err := s.sourceBuildArguments(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 6 || args[1] != "KPL_SOURCE_REVISION="+runner.revision || args[3] != "KPL_SOURCE_MODIFIED=true" || runner.calls != 2 {
		t.Fatalf("incorrect source build arguments: %q", args)
	}
	if _, err := time.Parse(time.RFC3339, strings.TrimPrefix(args[5], "KPL_BUILD_TIME=")); err != nil {
		t.Fatal("build timestamp is not UTC RFC3339")
	}
	runner.revision = "not-a-revision"
	if _, err := s.sourceBuildArguments(t.Context()); err == nil {
		t.Fatal("invalid source revision accepted")
	}
	runner.fail = true
	if _, err := s.sourceBuildArguments(t.Context()); err == nil {
		t.Fatal("failed source query silently stamped unrelated metadata")
	}
}

func TestImageBuildSourceArchiveDoesNotInventRevision(t *testing.T) {
	s, runner, _ := newSwarmFixture(t, "publish")
	args, err := s.sourceBuildArguments(t.Context())
	if err != nil || len(args) != 0 || len(runner.calls) != 0 {
		t.Fatal("archive without Git invented source metadata or invoked external commands")
	}
}
