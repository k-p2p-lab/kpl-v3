package master

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func (a *App) validateScenario(args []string) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(a.Stderr)
	path := flags.String("scenario", filepath.Join(a.Repo, "examples", "smoke.yaml"), "scenario YAML file")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return usageError(err.Error())
	}
	if flags.NArg() > 1 {
		return usageError("usage: kpl scenario validate [--scenario FILE]")
	}
	if flags.NArg() == 1 {
		if flags.NFlag() != 0 {
			return usageError("supply one scenario path")
		}
		*path = flags.Arg(0)
	}
	file, err := os.OpenFile(*path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("scenario must be a regular file")
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	parsed, err := scenario.Parse(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(a.Stdout, "valid scenario: %s (%d phases, seed %d)\n", parsed.Name, len(parsed.Phases), parsed.Seed)
	return err
}
