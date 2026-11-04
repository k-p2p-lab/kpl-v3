package master

import (
	"context"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// App owns an invocation. Repository assets are separate from this operator
// program; runtime binaries are never looked up by the ambiguous name "kpl".
type App struct {
	Repo            string
	EnvFile         string
	EnvFileExplicit bool
	Environ         []string
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	Runner          Runner
}

type Command struct {
	Interactive bool
	CancelGrace time.Duration
	Name        string
	Args        []string
	Dir         string
	Env         []string
	Stdin       io.Reader
	Stdout      io.Writer
	Stderr      io.Writer
}

type Runner interface {
	Run(context.Context, Command) error
	Output(context.Context, Command) ([]byte, error)
}

func NewApp() *App {
	return &App{Environ: os.Environ(), Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Runner: ExecRunner{}}
}

func (a *App) command(name string, args []string) Command {
	return Command{Name: name, Args: args, Dir: a.Repo, Env: a.Environ, Stdin: a.Stdin, Stdout: a.Stdout, Stderr: a.Stderr}
}

func (a *App) Run(ctx context.Context, name string, args ...string) error {
	return a.Runner.Run(ctx, a.command(name, args))
}

func (a *App) RunInteractive(ctx context.Context, name string, args ...string) error {
	command := a.command(name, args)
	command.Interactive = true
	return a.Runner.Run(ctx, command)
}

func (a *App) Output(ctx context.Context, name string, args ...string) (string, error) {
	out, err := a.Runner.Output(ctx, a.command(name, args))
	return strings.TrimSpace(string(out)), err
}

// WithEnv returns a copy and never mutates the caller's process environment.
// A nil value removes a variable, including DOCKER_DEFAULT_PLATFORM for image work.
func (a *App) WithEnv(values map[string]*string) *App {
	copy := *a
	env := make(map[string]string)
	for _, entry := range a.Environ {
		if k, v, ok := strings.Cut(entry, "="); ok {
			env[k] = v
		}
	}
	for key, value := range values {
		if value == nil {
			delete(env, key)
		} else {
			env[key] = *value
		}
	}
	copy.Environ = nil
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		copy.Environ = append(copy.Environ, k+"="+env[k])
	}
	return &copy
}

func (a *App) WithConfig(config Config) *App {
	values := make(map[string]*string, len(config))
	for key, value := range config {
		v := value
		values[key] = &v
	}
	return a.WithEnv(values)
}

func (a *App) Env(key string) string {
	for i := len(a.Environ) - 1; i >= 0; i-- {
		if k, v, ok := strings.Cut(a.Environ[i], "="); ok && k == key {
			return v
		}
	}
	return ""
}
