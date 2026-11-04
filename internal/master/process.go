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
	var out bytes.Buffer
	command.Stdout = &out
	err := r.Run(ctx, command)
	return out.Bytes(), err
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
