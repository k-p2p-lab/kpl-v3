package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// The same captured in-memory input compares decode-only throughput with the
// complete ordered metrics/research pipeline at larger execution budgets.
func analysisPipelineBenchmarkFixture(b testing.TB) (resultFile, *resultSnapshot) {
	b.Helper()
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for index := range 16384 {
		event := model.TraceEvent{RunID: "pipeline", AgentID: "agent", NodeID: fmt.Sprintf("worker-%04d", index%500),
			PeerID: "peer", Group: "worker", SessionID: "session", Sequence: uint64(index + 1),
			Timestamp: at.Add(time.Duration(index) * time.Millisecond), Topic: "topic", MessageID: fmt.Sprintf("message-%08d", index/4)}
		switch index % 4 {
		case 0:
			event.Type = "publish"
			event.Fields = map[string]any{"payloadBytes": 1024, "wireBytes": 1160, "payloadEncoding": "envelope", "clockBasis": "controller-offset-v1", "targetNodeIds": []string{"worker-0101", "worker-0102", "worker-0103"}}
		case 1, 2:
			event.Type, event.LatencyMS = "deliver", 12.75
			event.Fields = map[string]any{"payloadBytes": 1024, "wireBytes": 1160, "payloadEncoding": "envelope", "latencyAvailable": true, "latencyClockSynchronized": true, "localDelivery": false, "pubsubMessageId": "wire-id", "clockBasis": "controller-offset-v1"}
		case 3:
			event.Type, event.RemotePeerID = "recv_ihave", "remote-peer"
			event.Fields = map[string]any{"messageIdCount": 8, "controlEntries": 1, "messageIds": []string{"message-1", "message-2", "message-3", "message-4", "message-5", "message-6", "message-7", "message-8"}}
		}
		if err := encoder.Encode(event); err != nil {
			b.Fatal(err)
		}
	}
	file := resultFile{name: "events.jsonl", size: int64(data.Len()), readAt: bytes.NewReader(data.Bytes())}
	snapshot := &resultSnapshot{result: savedResult{ID: "pipeline", State: "completed", StartedAt: at, FinishedAt: at.Add(time.Minute)}, files: []resultFile{file}, exportedAt: at.Add(time.Hour)}
	return file, snapshot
}

func BenchmarkAnalysisPipelineCPU(b *testing.B) {
	file, snapshot := analysisPipelineBenchmarkFixture(b)
	for _, workers := range []int{4, 16} {
		b.Run(fmt.Sprintf("decode_%d", workers), func(b *testing.B) {
			b.SetBytes(file.size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				count := 0
				if err := scanDecodedAnalysisFileWithWorkers(b.Context(), file, workers, func(event model.TraceEvent) error { count++; return nil }); err != nil {
					b.Fatal(err)
				}
				if count != 16384 {
					b.Fatalf("decoded %d events", count)
				}
			}
		})
		b.Run(fmt.Sprintf("full_%d", workers), func(b *testing.B) {
			b.SetBytes(file.size)
			b.ReportAllocs()
			b.ResetTimer()
			var scanning, summarizing, research time.Duration
			for range b.N {
				started := time.Now()
				var aggregatingAt, researchAt time.Time
				ctx := context.WithValue(b.Context(), analysisProgressKey{}, analysisProgress(func(phase string, _ int64) {
					if phase == "aggregating" {
						aggregatingAt = time.Now()
					}
					if researchAt.IsZero() && strings.HasPrefix(phase, "propagation ") {
						researchAt = time.Now()
					}
				}))
				result, err := analyzeResultWithWorkers(ctx, snapshot, workers)
				if err != nil {
					b.Fatal(err)
				}
				if result.EventCount != 16384 {
					b.Fatalf("analyzed %d events", result.EventCount)
				}
				finished := time.Now()
				if aggregatingAt.IsZero() || researchAt.IsZero() {
					b.Fatal("analysis phase boundaries were not reported")
				}
				scanning += aggregatingAt.Sub(started)
				summarizing += researchAt.Sub(aggregatingAt)
				research += finished.Sub(researchAt)
			}
			b.ReportMetric(float64(scanning)/float64(time.Millisecond)/float64(b.N), "scan_ms/op")
			b.ReportMetric(float64(summarizing)/float64(time.Millisecond)/float64(b.N), "summarize_ms/op")
			b.ReportMetric(float64(research)/float64(time.Millisecond)/float64(b.N), "research_ms/op")
		})
	}
}

// Exercise the production child/RPC path as well as the calculation engine.
// The profile of the benchmark process measures supervisor IPC work; the
// child process CPU time is reported separately from its wall-clock duration.
func BenchmarkAnalysisPipelineChildCPU(b *testing.B) {
	file, snapshot := analysisPipelineBenchmarkFixture(b)
	root, err := os.OpenRoot(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer root.Close()
	s := New(ServerConfig{DataDir: b.TempDir(), RunWorkers: true}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var command *exec.Cmd
	s.workerCommand = func(kind string) *exec.Cmd {
		command = exec.Command(os.Args[0], "-test.run=^TestControllerOSWorkerHelper$", "--", "controller-worker-helper", kind)
		command.Stdout, command.Stderr = io.Discard, io.Discard
		return command
	}
	input := analysisProcessInput{RunID: snapshot.result.ID, AnalysisID: "pipeline-benchmark", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}
	for _, cpus := range []uint64{4, 16} {
		b.Run(fmt.Sprintf("cpus_%d", cpus), func(b *testing.B) {
			s.config.AnalysisCPUs = cpus
			expectedCPUs := min(int(cpus), runtime.GOMAXPROCS(0), runtime.NumCPU())
			b.SetBytes(file.size)
			b.ReportAllocs()
			b.ResetTimer()
			var childCPU, elapsed time.Duration
			for range b.N {
				started := time.Now()
				artifacts, result, err := s.computeAnalysisProcess(b.Context(), input, []*resultSnapshot{snapshot}, root, nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				if result.CPUs != expectedCPUs || command.ProcessState == nil {
					artifacts.discard()
					b.Fatalf("child execution budget/reaping: CPUs=%d command=%v", result.CPUs, command.ProcessState)
				}
				childCPU += command.ProcessState.UserTime() + command.ProcessState.SystemTime()
				artifacts.discard()
				elapsed += time.Since(started)
			}
			b.ReportMetric(float64(childCPU)/float64(elapsed), "child_cores")
		})
	}
}
