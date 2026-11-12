package controller

import (
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"testing"
)

func TestAnalysisResourcesReachLocalAndAgentChildrenWithoutChangingSupervisors(t *testing.T) {
	s, localCommands := isolatedAnalysisServer(t)
	s.config.AnalysisMemoryMiB = 96
	s.config.AnalysisAgentToken = remoteTestToken
	batchFixture(t, s, "resource-batch", "resource-run", "completed", 1, 1, 1)
	snapshot, err := s.captureResultFilesContext(t.Context(), "resource-run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	remote, remoteCommands := isolatedAnalysisServer(t)
	worker := newRemoteAnalysisWorker(remoteTestToken, remote.logger, remote.workerCommand)
	endpoint := httptest.NewServer(worker)
	defer endpoint.Close()
	agent := analysisPoolAgent("resource-agent", "resource-host", endpoint.URL)
	registerAnalysisPoolAgent(t, s, agent)
	beforeCPUs, beforeMemory := runtime.GOMAXPROCS(0), debug.SetMemoryLimit(-1)
	input := analysisProcessInput{RunID: snapshot.result.ID, AnalysisID: "resource-analysis", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}
	for _, configuredCPUs := range []uint64{8, 64} {
		s.config.AnalysisCPUs = configuredCPUs
		expectedCPUs := min(int(configuredCPUs), beforeCPUs, runtime.NumCPU())
		for _, target := range []struct {
			name string
			slot *workSlot
		}{
			{"Controller", &workSlot{}},
			{"Agent", &workSlot{analysisWorkerURL: endpoint.URL, analysisAgent: &agent}},
		} {
			t.Run(fmt.Sprintf("cpus_%d/%s", configuredCPUs, target.name), func(t *testing.T) {
				artifacts, result, err := s.computeAssignedAnalysis(t.Context(), target.slot, input, []*resultSnapshot{snapshot}, root, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer artifacts.discard()
				if result.CPUs != expectedCPUs || result.MemoryLimitBytes != 96<<20 {
					t.Fatalf("configured analysis resources did not reach %s child: CPUs=%d memory=%d, want CPUs=%d memory=%d", target.name, result.CPUs, result.MemoryLimitBytes, expectedCPUs, 96<<20)
				}
				if runtime.GOMAXPROCS(0) != beforeCPUs || debug.SetMemoryLimit(-1) != beforeMemory {
					t.Fatal("analysis child resources changed a long-lived supervisor's runtime")
				}
			})
		}
	}
	for _, commands := range []func() []*exec.Cmd{localCommands, remoteCommands} {
		processes := commands()
		if len(processes) != 2 {
			t.Fatalf("configured analyses launched %d calculation children, want 2", len(processes))
		}
		for _, process := range processes {
			if process.ProcessState == nil {
				t.Fatal("configured analysis did not reclaim its calculation child")
			}
		}
	}
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || len(pending) != 0 {
		t.Fatalf("configured Agent analysis retained its cleanup ownership: %v %v", pending, err)
	}
}
