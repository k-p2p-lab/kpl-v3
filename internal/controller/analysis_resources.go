package controller

import (
	"errors"
	"math"
	"os/exec"
	"runtime"

	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

const (
	defaultAnalysisCPUs      uint64 = 4
	defaultAnalysisMemoryMiB uint64 = 4096
	maxAnalysisCPUs          uint64 = 4096
)

// Both local and Agent calculations use the same resource policy, applied only
// to their short-lived child. A zero bootstrap/config setting uses the default.
func (s *Server) analysisProcessResources(input analysisProcessInput) analysisProcessInput {
	if input.CPUs == 0 {
		input.CPUs = s.config.AnalysisCPUs
	}
	if input.MemoryMiB == 0 {
		input.MemoryMiB = s.config.AnalysisMemoryMiB
	}
	return input
}

func analysisProcessOptions(input analysisProcessInput, command func(string) *exec.Cmd) (workerproc.Options, error) {
	cpus, memory := input.CPUs, input.MemoryMiB
	if cpus == 0 {
		cpus = defaultAnalysisCPUs
	}
	if memory == 0 {
		memory = defaultAnalysisMemoryMiB
	}
	if cpus > maxAnalysisCPUs {
		return workerproc.Options{}, errors.New("analysis CPUs must be between 1 and 4096")
	}
	if memory > math.MaxInt64/(1<<20) {
		return workerproc.Options{}, errors.New("analysis memory target exceeds the supported range")
	}
	lanes := min(int(cpus), max(1, runtime.NumCPU()), max(1, runtime.GOMAXPROCS(0)))
	return workerproc.Options{Command: command, MaxProcs: lanes, MemoryLimitBytes: int64(memory) << 20}, nil
}
