package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Keep source construction outside the measured loop. An in-memory captured
// source isolates JSON decoding from NAS/network speed and filesystem caches.
func BenchmarkAnalysisEventDecode(b *testing.B) {
	var source bytes.Buffer
	encoder := json.NewEncoder(&source)
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	for index := range 16384 {
		event := model.TraceEvent{
			RunID: "run-benchmark", AgentID: "agent-1", NodeID: fmt.Sprintf("worker-%04d", index%500),
			PeerID:    "12D3KooWQvYwMJrUkYBm6HDeK7hSCkPA3W7iBzaDzWkVDJxTXhFB",
			SessionID: "measurement-session-37", Sequence: uint64(index + 1), Group: "workers",
			Timestamp: start.Add(time.Duration(index) * time.Millisecond), Topic: "benchmark-topic",
			MessageID: fmt.Sprintf("message-%08d", index/4),
		}
		switch index % 4 {
		case 0:
			event.Type = "publish"
			event.Fields = map[string]any{"payloadBytes": 1024, "wireBytes": 1160, "payloadEncoding": "envelope", "clockBasis": "controller-offset-v1", "targetNodeIds": []string{"worker-0101", "worker-0102", "worker-0103"}}
		case 1, 2:
			event.Type = "deliver"
			event.LatencyMS = 12.75
			event.Fields = map[string]any{"publisher": "publisher-0001", "payloadBytes": 1024, "wireBytes": 1160, "payloadEncoding": "envelope", "latencyAvailable": true, "latencyClockSynchronized": true, "localDelivery": false, "pubsubMessageId": "6eab03f6d976d609074b201c1ac91188", "clockBasis": "controller-offset-v1"}
		case 3:
			event.Type = "recv_ihave"
			event.RemotePeerID = "12D3KooWL2ezkXa8vLkzF2hQH3WSqAF6qDgC6xjy8Ra3WrHcBBxL"
			event.Fields = map[string]any{"messageIdCount": 8, "controlEntries": 1, "messageIds": []string{"message-1", "message-2", "message-3", "message-4", "message-5", "message-6", "message-7", "message-8"}, "timestampSource": "peer-clock"}
		}
		if err := encoder.Encode(event); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkAnalysisDecode(b, "events.jsonl", source.Bytes(), func(event model.TraceEvent) int { return len(event.Type) })
	file := resultFile{name: "events.jsonl", size: int64(source.Len()), readAt: bytes.NewReader(source.Bytes())}
	snapshot := &resultSnapshot{
		result: savedResult{ID: "run-benchmark", State: "completed", StartedAt: start, FinishedAt: start.Add(time.Minute)},
		files:  []resultFile{file}, exportedAt: start.Add(time.Hour),
	}
	for _, workers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("analysis_workers_%d", workers), func(b *testing.B) {
			b.SetBytes(int64(source.Len()))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				analysis, err := analyzeResultWithWorkers(b.Context(), snapshot, workers)
				if err != nil {
					b.Fatal(err)
				}
				if analysis.EventCount != 16384 {
					b.Fatalf("analysis lost events: %d", analysis.EventCount)
				}
				analysisDecodeBenchmarkSink = analysis.EventCount + analysis.ObservationCount
			}
		})
	}
}

func BenchmarkAnalysisObservationDecode(b *testing.B) {
	var source bytes.Buffer
	encoder := json.NewEncoder(&source)
	start := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	degree, clustering := 6., .15
	graph := analysisGraph{Protocol: "gossipsub", Nodes: make([]string, 500), Groups: make([]string, 500), Edges: make([][2]int, 0, 1500)}
	for index := range graph.Nodes {
		graph.Nodes[index] = fmt.Sprintf("worker-%04d", index)
		graph.Groups[index] = "workers"
		for neighbor := 1; neighbor <= 3; neighbor++ {
			graph.Edges = append(graph.Edges, [2]int{index, (index + neighbor) % len(graph.Nodes)})
		}
	}
	for index := range 128 {
		observation := analysisObservation{
			RunID: "run-benchmark", At: start.Add(time.Duration(index) * 5 * time.Second), Graphs: []analysisGraph{graph},
			Groups: []analysisGroup{{Group: "workers", Ready: 500, Reporting: 500, Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: 500, AverageDegree: &degree, Clustering: &clustering, Degrees: []analysisPoint{{X: 6, Y: 500}}}}}},
		}
		if err := encoder.Encode(observation); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkAnalysisDecode(b, "observations.jsonl", source.Bytes(), func(observation analysisObservation) int { return len(observation.Graphs) })
}

var analysisDecodeBenchmarkSink int

func benchmarkAnalysisDecode[T any](b *testing.B, name string, source []byte, value func(T) int) {
	b.Helper()
	file := resultFile{name: name, size: int64(len(source)), readAt: bytes.NewReader(source)}
	b.Run("baseline_serial", func(b *testing.B) {
		b.SetBytes(int64(len(source)))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			count := 0
			if err := scanAnalysisFile(b.Context(), file, func(line []byte) error {
				var item T
				if err := json.Unmarshal(line, &item); err != nil {
					return err
				}
				count += value(item)
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			analysisDecodeBenchmarkSink = count
		}
	})
	for _, workers := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			b.SetBytes(int64(len(source)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				count := 0
				if err := scanDecodedAnalysisFileWithWorkers(b.Context(), file, workers, func(item T) error {
					count += value(item)
					return nil
				}); err != nil {
					b.Fatal(err)
				}
				analysisDecodeBenchmarkSink = count
			}
		})
	}
}
