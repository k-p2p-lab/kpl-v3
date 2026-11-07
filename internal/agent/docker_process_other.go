//go:build !linux

package agent

import "os/exec"

// Agent container execution is supported on Linux. Keep other platforms able
// to compile tooling; the common WaitDelay still bounds output pipe waits.
func configureDockerCommand(*exec.Cmd) {}
func reclaimDockerCommand(*exec.Cmd)   {}
