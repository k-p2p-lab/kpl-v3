//go:build linux

package agent

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Docker daemon containers have their own processes and are removed separately.
// This group covers only the CLI and its transport/wrapper helper descendants.
func configureDockerCommand(command *exec.Cmd) {
	if command.SysProcAttr == nil {
		command.SysProcAttr = &syscall.SysProcAttr{}
	}
	command.SysProcAttr.Setpgid = true
	if command.Cancel != nil {
		command.Cancel = func() error {
			if command.Process == nil {
				return os.ErrProcessDone
			}
			err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			if errors.Is(err, syscall.ESRCH) {
				return os.ErrProcessDone
			}
			return err
		}
	}
}

func reclaimDockerCommand(command *exec.Cmd) {
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
}
