package master

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func (a *App) validateScenario(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("kpl scenario validate", flag.ContinueOnError)
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
	if err := ctx.Err(); err != nil {
		return err
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
	tooLarge := func() error {
		return fmt.Errorf("scenario YAML cannot exceed %d bytes", scenario.MaxSubmissionBytes)
	}
	if info.Size() > scenario.MaxSubmissionBytes {
		return tooLarge()
	}
	data, err := io.ReadAll(io.LimitReader(contextReader{ctx: ctx, reader: file}, scenario.MaxSubmissionBytes+1))
	if err != nil {
		return err
	}
	if len(data) > scenario.MaxSubmissionBytes {
		return tooLarge()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	parsed, err := scenario.Parse(data)
	if cancelErr := ctx.Err(); cancelErr != nil {
		return cancelErr
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(contextWriter{ctx: ctx, writer: a.Stdout}, "valid scenario: %s (%d phases, seed %d)\n", parsed.Name, len(parsed.Phases), parsed.Seed)
	return err
}

// Context checks are cooperative between file/output operations and do not
// spawn background I/O goroutines. An already-blocked filesystem syscall or
// output write must finish before its cancellation can be observed.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if cancelErr := r.ctx.Err(); cancelErr != nil {
		return n, cancelErr
	}
	return n, err
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := w.writer.Write(p)
	if cancelErr := w.ctx.Err(); cancelErr != nil {
		return n, cancelErr
	}
	return n, err
}
