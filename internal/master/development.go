package master

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func (a *App) RunDevelopment(ctx context.Context, name string, args []string) error {
	switch name {
	case "validate":
		return a.validateScenario(args)
	case "test":
		if len(args) > 1 {
			return usageError("usage: kpl test [go|swarm|ui|race|linux]")
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
	out := filepath.Join(a.Repo, "bin", "kpl")
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	return a.Run(ctx, "go", "build", "-o", out, "./cmd/kpl-master")
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
	default:
		return usageError("unknown test scope " + scope + "; use go, swarm, ui, race or linux")
	}
}
