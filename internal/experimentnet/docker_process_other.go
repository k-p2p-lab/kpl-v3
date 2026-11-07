//go:build !linux

package experimentnet

import "os/exec"

// Swarm network management runs on Linux; keep tooling builds portable.
func configureDockerCommand(*exec.Cmd) {}
func reclaimDockerCommand(*exec.Cmd)   {}
