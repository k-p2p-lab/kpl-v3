package master

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (a *App) RunDevelopment(ctx context.Context, name string, args []string) error {
	switch name {
	case "validate":
		path := filepath.Join(a.Repo, "examples", "smoke.yaml")
		if len(args) == 1 && !strings.HasPrefix(args[0], "-") {
			var err error
			path, err = filepath.Abs(args[0])
			if err != nil {
				return err
			}
		} else if len(args) == 2 && args[0] == "--scenario" {
			var err error
			path, err = filepath.Abs(args[1])
			if err != nil {
				return err
			}
		} else if len(args) != 0 {
			return usageError("usage: kpl validate [--scenario FILE]")
		}
		return a.Run(ctx, "go", "run", "./cmd/kpl", "validate", "--scenario", path)
	case "test":
		if len(args) > 1 {
			return usageError("usage: kpl test [go|swarm|ui|race|linux|compat]")
		}
		scope := "go"
		if len(args) == 1 {
			scope = args[0]
		}
		return a.test(ctx, scope)
	case "build", "check", "vet":
		if len(args) != 0 {
			return usageError(name + " takes no arguments")
		}
	default:
		return usageError("unknown development command " + name)
	}
	if name == "vet" {
		return a.Run(ctx, "go", "vet", "./...")
	}
	if err := a.build(ctx); err != nil {
		return err
	}
	if name == "build" {
		return nil
	}
	if err := a.Run(ctx, "go", "vet", "./..."); err != nil {
		return err
	}
	// The Go package suite already includes the operator's native regressions.
	for _, scope := range []string{"go", "ui"} {
		if err := a.test(ctx, scope); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) build(ctx context.Context) error {
	out := filepath.Join(a.Repo, "bin", "kpl-runtime")
	if executable, err := os.Executable(); err == nil {
		self, e1 := os.Stat(executable)
		target, e2 := os.Stat(out)
		if e1 == nil && e2 == nil && os.SameFile(self, target) {
			return fmt.Errorf("runtime build output would replace this operator executable; keep the operator at <repo>/bin/kpl")
		}
	}
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err := a.Run(ctx, "go", "build", "-o", filepath.Join(a.Repo, "bin", "kpl"), "./cmd/kpl-master"); err != nil {
		return err
	}
	return a.Run(ctx, "go", "build", "-o", out, "./cmd/kpl")
}

func (a *App) test(ctx context.Context, scope string) error {
	switch scope {
	case "go":
		if err := a.Run(ctx, "go", "test", "-buildvcs=false", "./...", "-timeout", "120s"); err != nil {
			return err
		}
		return a.Run(ctx, "go", "test", "-buildvcs=false", "github.com/libp2p/go-libp2p-pubsub", "-run", "^Test(KPL|Score)", "-timeout", "120s")
	case "swarm":
		return a.Run(ctx, "go", "test", "-buildvcs=false", "./cmd/kpl-master", "./internal/master", "-timeout", "120s")
	case "ui":
		files, err := filepath.Glob(filepath.Join(a.Repo, "internal", "webui", "*_test.cjs"))
		if err != nil {
			return err
		}
		if len(files) == 0 {
			return fmt.Errorf("no UI test files found under the selected repository")
		}
		sort.Strings(files)
		return a.Run(ctx, "node", append([]string{"--test"}, files...)...)
	case "race":
		setting := "atexit_sleep_ms=0"
		return a.WithEnv(map[string]*string{"GORACE": &setting}).Run(ctx, "go", "test", "-race", "-buildvcs=false", "./...", "-count=1", "-timeout", "120s")
	case "linux":
		return a.Run(ctx, "docker", "build", "--target", "test", "-t", "kpl-v3:test", a.Repo)
	case "compat":
		for _, script := range []string{"test-swarm-agent.sh", "test-check-swarm.sh", "test-swarm-config.sh", "test-swarm.sh", "test-swarm-completion.sh", "test-make.sh"} {
			path := filepath.Join(a.Repo, "scripts", script)
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("compatibility checks require the retained legacy scripts: %w", err)
			}
			interpreter := "sh"
			if script == "test-swarm-completion.sh" {
				interpreter = "bash"
			}
			if err := a.Run(ctx, interpreter, path); err != nil {
				return err
			}
		}
		return nil
	default:
		return usageError("unknown test scope " + scope + "; use go, swarm, ui, race, linux or compat")
	}
}
