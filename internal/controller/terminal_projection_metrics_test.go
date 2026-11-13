package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func retiredHistoryMetricFixture(t *testing.T, count int) (*Server, string) {
	t.Helper()
	s, id, summary := summaryFreshnessFixture(t, time.Time{})
	summary.Retired = false
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, workerSummaryFile, summary)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	for i := range count {
		node := historicalFixtureNode(fmt.Sprintf("%s-peer-%04d", id, i))
		node.RunID, node.AgentID = id, "agent"
		s.state.setNodeLocked(node)
		if i == 0 {
			if _, err := s.state.terminalHistory.reconcile([]model.Node{node}, map[string]bool{node.ID: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.retireRunRuntime(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	return s, id
}

func TestRetiredNodeMetricsRejectMissingOrCorruptIdentityEvidence(t *testing.T) {
	for _, damage := range []string{"missing", "json", "run", "node-owner", "duplicate-overlap", "duplicate-non-overlap", "incomplete"} {
		t.Run(damage, func(t *testing.T) {
			s, id := retiredHistoryMetricFixture(t, 3)
			path := filepath.Join(s.config.DataDir, retiredRuntimeDirectory, id+".json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var proof retiredRunRuntime
			if err := json.Unmarshal(data, &proof); err != nil {
				t.Fatal(err)
			}
			switch damage {
			case "missing":
				err = os.Remove(path)
			case "json":
				err = os.WriteFile(path, []byte(`{"runId":`), 0600)
			default:
				switch damage {
				case "run":
					proof.RunID = "foreign-run"
				case "node-owner":
					proof.Nodes[0].AgentID = "foreign-agent"
				case "duplicate-overlap":
					proof.Nodes[1] = proof.Nodes[0]
				case "duplicate-non-overlap":
					// Only peer 0000 is in history; duplicate another identity.
					var first int
					for index, node := range proof.Nodes {
						if !strings.HasSuffix(node.ID, "0000") {
							first = index
							break
						}
					}
					proof.Nodes[(first+1)%len(proof.Nodes)] = proof.Nodes[first]
				case "incomplete":
					proof.Nodes = proof.Nodes[:2]
				}
				data, err = json.Marshal(proof)
				if err == nil {
					err = os.WriteFile(path, data, 0600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.state.metrics.registry.Gather(); err == nil {
				t.Fatal("unverified Node union became a successful count")
			}
		})
	}
}

func TestRetiredNodeMetricUnionRejectsChangedHistorySnapshot(t *testing.T) {
	for _, change := range []string{"transaction", "replacement-with-same-transaction"} {
		t.Run(change, func(t *testing.T) {
			s, id := retiredHistoryMetricFixture(t, 3)
			collector := newControllerStateCollector(s.state)
			snapshot, err := collector.collectStateSnapshot(make(chan prometheus.Metric, 1024))
			if err != nil {
				t.Fatal(err)
			}
			if change == "transaction" {
				node := historicalFixtureNode("history-changed-after-snapshot")
				if _, err := s.state.terminalHistory.reconcile([]model.Node{node}, map[string]bool{node.ID: true}); err != nil {
					t.Fatal(err)
				}
			} else {
				path := filepath.Join(s.config.DataDir, terminalHistoryFile)
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path+".replacement", data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path+".replacement", path); err != nil {
					t.Fatal(err)
				}
			}
			summary, err := s.state.readWorkerSummary(id)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.mergeRetiredNodeMetricCounts(snapshot, summary); err == nil || !strings.Contains(err.Error(), "changed") {
				t.Fatalf("mixed history revisions produced a count: %v", err)
			}
		})
	}
}

func TestRetiredNodeMetricsStreamIdentityFileLargerThanOneRecord(t *testing.T) {
	s, id := retiredHistoryMetricFixture(t, 1024)
	path := filepath.Join(s.config.DataDir, retiredRuntimeDirectory, id+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var proof retiredRunRuntime
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(file, `{"runId":%q,"at":%q,"nodes":[`, id, proof.At.Format(time.RFC3339Nano))
	encoder := json.NewEncoder(file)
	padding := strings.Repeat("x", 17<<10)
	for index, node := range proof.Nodes {
		if index > 0 {
			_, _ = io.WriteString(file, ",")
		}
		node.Metadata["legacyDiagnostic"] = padding
		if err := encoder.Encode(node); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = io.WriteString(file, "]}")
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() <= terminalHistoryRecordLimit {
		t.Fatalf("fixture did not exceed a single-record limit: %v %v", info, err)
	}
	metrics := gatherTestMetrics(t, s.state)
	requireMetricValue(t, metrics, "kpl_nodes", map[string]string{"run_id": id, "agent_id": "agent", "group": "workers", "role": "worker", "node_type": "full", "state": "stopped"}, 1024)
}

type metricPaddingReader byte

func (value metricPaddingReader) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = byte(value)
	}
	return len(buffer), nil
}

func TestRetiredMetricJSONReadBudgetIncludesIgnoredFieldsAndWhitespace(t *testing.T) {
	for _, whitespace := range []bool{false, true} {
		t.Run(fmt.Sprintf("whitespace=%t", whitespace), func(t *testing.T) {
			prefix, suffix, padding := `{"ignored":"`, `"}`, metricPaddingReader('x')
			if whitespace {
				prefix, suffix, padding = "[", "]", metricPaddingReader(' ')
			}
			reader := &terminalMetricProofReader{source: io.MultiReader(strings.NewReader(prefix), io.LimitReader(padding, 2*terminalHistoryRecordLimit), strings.NewReader(suffix))}
			decoder := json.NewDecoder(reader)
			var err error
			if whitespace {
				_, err = terminalMetricProofToken(decoder, reader)
				if err == nil {
					// More's bool does not expose peek errors. The next decoder
					// operation must preserve that error without reading farther.
					_ = terminalMetricProofMore(decoder, reader)
				}
				_, err = terminalMetricProofToken(decoder, reader)
			} else {
				var node terminalMetricNode
				err = decodeTerminalMetricProofValue(decoder, reader, &node)
			}
			if err == nil || errors.Is(err, io.EOF) || reader.read > terminalHistoryRecordLimit+1024 {
				t.Fatalf("oversized value was read without its budget: read=%d err=%v", reader.read, err)
			}
			t.Logf("read=%d limit=%d err=%v", reader.read, terminalHistoryRecordLimit, err)
		})
	}
}

type projectionMetricInterleave struct {
	prometheus.Collector
	once sync.Once
	step func()
}

func (collector *projectionMetricInterleave) Collect(out chan<- prometheus.Metric) {
	metrics := make(chan prometheus.Metric)
	go func() {
		defer close(metrics)
		collector.Collector.Collect(metrics)
	}()
	for metric := range metrics {
		if strings.Contains(metric.Desc().String(), `fqName: "kpl_experiment_info"`) {
			collector.once.Do(collector.step)
		}
		out <- metric
	}
}

func TestProjectionScrapeKeepsOneCounterAndNodeOwnerAcrossRetirement(t *testing.T) {
	s, id, summary := summaryFreshnessFixture(t, time.Time{})
	summary.Retired = false
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	err = writeAnalysisJSON(root, workerSummaryFile, summary)
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		node := historicalFixtureNode(fmt.Sprintf("%s-peer-%d", id, i))
		node.RunID, node.AgentID = id, "agent"
		s.state.setNodeLocked(node)
	}
	for range 3 {
		s.state.metrics.observeEvent(model.TraceEvent{RunID: id, AgentID: "agent", Type: "publish", Topic: "topic"})
	}
	collector := &controllerProjectionCollector{events: &controllerEventCollector{metrics: s.state.metrics}, runs: newRunMetricsCollector(s.state), state: newControllerStateCollector(s.state)}
	registry := prometheus.NewRegistry()
	registry.MustRegister(&projectionMetricInterleave{Collector: collector, step: func() {
		// The collector has captured current counters and Nodes and closed
		// history storage. Commit actual retirement before it reads summaries.
		if err := s.retireRunRuntime(t.Context(), id); err != nil {
			t.Error(err)
		}
	}})
	s.state.metrics.registry = registry
	for index, want := range []float64{3, 7} {
		metrics := gatherTestMetrics(t, s.state)
		requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": id, "agent_id": "agent", "event_type": "publish", "topic": "topic"}, want)
		requireMetricValue(t, metrics, "kpl_nodes", map[string]string{"run_id": id, "agent_id": "agent", "group": "workers", "role": "worker", "node_type": "full", "state": "stopped"}, 2)
		if index == 0 && len(s.state.nodes) != 0 {
			t.Fatal("collection gate did not commit actual retirement")
		}
	}
}

func TestRetiredNodeMetricsConcurrentScrapesKeepIndependentUnion(t *testing.T) {
	s, id := retiredHistoryMetricFixture(t, 140)
	var group sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 3 {
				families, err := s.state.metrics.registry.Gather()
				if err != nil {
					errors <- err
					return
				}
				var count float64
				for _, family := range families {
					if family.GetName() == "kpl_nodes" {
						for _, metric := range family.GetMetric() {
							for _, label := range metric.GetLabel() {
								if label.GetName() == "run_id" && label.GetValue() == id {
									count += metric.GetGauge().GetValue()
								}
							}
						}
					}
				}
				if count != 140 {
					errors <- fmt.Errorf("concurrent union count=%v, want 140", count)
					return
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
