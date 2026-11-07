package master

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type ExecRunner struct{}

const maxCapturedCommandOutput = 8 << 20

var errCapturedCommandOutputLimit = errors.New("captured command output exceeds 8 MiB")

type capturedCommandOutput struct {
	buffer   bytes.Buffer
	cancel   context.CancelFunc
	exceeded bool
}

func (out *capturedCommandOutput) Write(data []byte) (int, error) {
	if len(data) > maxCapturedCommandOutput-out.buffer.Len() {
		out.exceeded = true
		out.cancel()
		return 0, errCapturedCommandOutputLimit
	}
	return out.buffer.Write(data)
}

func (ExecRunner) Run(ctx context.Context, command Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cmd := exec.Command(command.Name, command.Args...)
	cmd.Dir, cmd.Env = command.Dir, command.Env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = command.Stdin, command.Stdout, command.Stderr
	if !command.Interactive {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	}
	cmd.WaitDelay = 5 * time.Second
	if command.CancelGrace > 0 {
		cmd.WaitDelay = command.CancelGrace
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", command.Name, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if !command.Interactive && err != nil {
			// The leader may exit normally while a child holds its output pipe.
			// A nonzero leader exit masks ErrWaitDelay. Both failure results must
			// stop the remaining group after the inherited pipe wait is bounded.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		if ctx.Err() != nil {
			if !command.Interactive {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		pid := cmd.Process.Pid
		if !command.Interactive {
			pid = -pid
		}
		_ = syscall.Kill(pid, syscall.SIGTERM)
		grace := command.CancelGrace
		if grace <= 0 {
			grace = 5 * time.Second
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-done:
			// A leader can honor TERM while its plugin/test children ignore it.
			// Its exit must not leave the rest of this command's group running.
			if !command.Interactive {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		case <-timer.C:
			_ = syscall.Kill(pid, syscall.SIGKILL)
			<-done
		}
		return ctx.Err()
	}
}

func (r ExecRunner) Output(ctx context.Context, command Command) ([]byte, error) {
	// These calls collect metadata for parsing, unlike Run's streamed logs.
	// Stop malformed/oversized producers instead of retaining unlimited output
	// or passing truncated metadata to deployment decisions.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	out := capturedCommandOutput{cancel: cancel}
	command.Stdout = &out
	err := r.Run(ctx, command)
	if out.exceeded {
		return nil, fmt.Errorf("%s: %w", command.Name, errCapturedCommandOutputLimit)
	}
	return out.buffer.Bytes(), err
}

type UsageError struct{ Message string }

func (e *UsageError) Error() string { return e.Message }

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var usage *UsageError
	if errors.As(err, &usage) {
		return 2
	}
	if errors.Is(err, context.Canceled) {
		return 130
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 124
	}
	var exited *exec.ExitError
	if errors.As(err, &exited) {
		if exited.ExitCode() > 0 {
			return exited.ExitCode()
		}
		if status, ok := exited.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return 128 + int(status.Signal())
		}
	}
	return 1
}
