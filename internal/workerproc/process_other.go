//go:build !linux

package workerproc

import (
	"os"
	"os/exec"
)

func currentExecutable() (string, error)                    { return os.Executable() }
func configureProcess(cmd *exec.Cmd)                        {}
func terminateProcess(cmd *exec.Cmd) error                  { return cmd.Process.Signal(os.Interrupt) }
func killProcess(cmd *exec.Cmd) error                       { return cmd.Process.Kill() }
func cleanupProcessGroup(cmd *exec.Cmd)                     { _ = cmd.Process.Kill() }
func preparePrivatePipes() error                            { return nil }
func privateFilesCloseOnExec(files ...*os.File) error       { return nil }
func expectedTermination(err error, term, killed bool) bool { return false }
