package controller

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The v9 baseline predates binary child transport and parallel projection:
// SHA256 0b884334810a0335337825a949e5376fd0d3b4c65e78a23f882be9a327c296a9.
// An independent complete JSON comparison verified that v10 changes only the
// analysisVersion field in this fixture. Keep the full artifact regression.
func TestAnalysisPerformancePreservesVersion10Reference(t *testing.T) {
	const referenceVersion = 10
	const referenceSHA256 = "bdbe30bfe387a7c67b603838349e7a6507b0b18ae330a9e1c4b1ee4ec627b1a4"
	if currentAnalysisVersion != referenceVersion {
		t.Fatalf("analysis definition changed to v%d; evaluate and replace the v%d scientific reference", currentAnalysisVersion, referenceVersion)
	}
	_, snapshot := analysisPipelineBenchmarkFixture(t)
	for _, cpus := range []uint64{1, 16} {
		t.Run(fmt.Sprintf("cpus_%d", cpus), func(t *testing.T) {
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true, AnalysisCPUs: cpus}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			s.workerCommand = func(kind string) *exec.Cmd {
				command := exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
				command.Stdout, command.Stderr = io.Discard, io.Discard
				return command
			}
			input := analysisProcessInput{RunID: snapshot.result.ID, AnalysisID: "pipeline-benchmark", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}
			artifacts, _, err := s.computeAnalysisProcess(t.Context(), input, []*resultSnapshot{snapshot}, root, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer artifacts.discard()
			artifact := artifacts.sinks["full"]
			if artifact == nil {
				t.Fatal("full analysis artifact was not produced")
			}
			data, err := root.ReadFile(artifact.staged.temp)
			if err != nil {
				t.Fatal(err)
			}
			actual := fmt.Sprintf("%x", sha256.Sum256(data))
			if actual != referenceSHA256 {
				path := filepath.Join(t.TempDir(), "actual-analysis-v10.json")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				t.Fatalf("scientific artifact changed: SHA256 %s, want %s; actual artifact: %s", actual, referenceSHA256, path)
			}
		})
	}
}

// The original v9 research SHA256 was
// 4952d22a7b35053f0b5d0ae2bdf1fbc2ae7b6f1a22e293f22b4159ff7b42a5d1.
// Compensated v10 statistics change seven mean/deviation scalars by at most
// seven ULP in this fixture; every other JSON value and order was compared.
// Serial and parallel calculations must preserve the full v10 artifact.
func TestResearchPerformancePreservesVersion10Reference(t *testing.T) {
	const referenceVersion = 10
	const referenceSHA256 = "0c99dc848609b1bcf4208d385a5775d6eba798eb20cb3b2274b70c2cecf6bba6"
	if currentAnalysisVersion != referenceVersion {
		t.Fatalf("analysis definition changed to v%d; evaluate and replace the v%d scientific reference", currentAnalysisVersion, referenceVersion)
	}
	for _, workers := range []int{1, 16} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			metrics, observations, _ := researchParallelFixture(4096, 48, 32)
			result, err := metrics.research.finishWithWorkers(t.Context(), metrics, observations, workers)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			actual := fmt.Sprintf("%x", sha256.Sum256(data))
			if actual != referenceSHA256 {
				path := filepath.Join(t.TempDir(), "actual-research-v10.json")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				t.Fatalf("scientific research changed: SHA256 %s, want %s; actual artifact: %s", actual, referenceSHA256, path)
			}
		})
	}
}
