package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func projectionNodeLabels(id string, row workerNodeCount) map[string]string {
	return map[string]string{"run_id": id, "agent_id": row.AgentID, "group": row.Group, "role": row.Role, "node_type": row.Type, "state": row.State}
}

func TestRetiredNodeMetricsSurviveRetirementAndRestart(t *testing.T) {
	f := newControllerWorkerFixture(t)
	run := summaryBoundaryCompleteRun(t, f)
	summary, err := f.server.state.readWorkerSummary(run.ID)
	if err != nil || !summary.Retired || len(summary.Nodes) == 0 {
		t.Fatalf("Run did not retain terminal Node evidence: summary=%+v err=%v", summary, err)
	}
	for name, server := range map[string]*Server{"after-retirement": f.server, "after-restart": New(f.server.config, f.server.logger)} {
		t.Run(name, func(t *testing.T) {
			metrics := gatherTestMetrics(t, server.state)
			for _, row := range summary.Nodes {
				requireMetricValue(t, metrics, "kpl_nodes", projectionNodeLabels(run.ID, row), float64(row.Count))
			}
			if len(server.state.nodes) != 0 || len(server.state.workerSummaries) != 0 || len(server.state.metrics.sealed) != 0 {
				t.Fatal("scrape warmed retired Nodes, summaries or counters back into the persistent service")
			}
		})
	}
}

func TestRetiredNodeMetricsMergeHistoricalPeersWithoutDuplication(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprintf("overlap=%t", overlap), func(t *testing.T) {
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
			agent := model.Agent{ID: "agent", URL: "http://agent.invalid", StartedAt: time.Now().Add(-time.Hour).UTC()}
			if _, err := s.state.registerAgent(agent); err != nil {
				t.Fatal(err)
			}
			node := historicalFixtureNode(id + "-peer")
			node.RunID, node.AgentID = id, agent.ID
			s.state.setNodeLocked(node)
			reported := node
			want := 1
			if !overlap {
				reported.ID += "-historical"
				want = 2
			} else {
				// The immutable history already knew a failed terminal status.
				// A stale stopped hot record must not rewrite it during retirement.
				reported.State = model.NodeFailed
			}
			h := model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{reported}, Partial: true}
			retired, _, err := s.state.heartbeatRetirements(h.Nodes)
			if err != nil {
				t.Fatal(err)
			}
			// Pause at the real heartbeat's persisted-history boundary before
			// its live-map application. Retirement can commit in this window.
			stored, err := s.state.prepareHistoricalHeartbeat(h, retired)
			if err != nil || len(stored) != 1 {
				t.Fatalf("fixture did not persist historical evidence: %v %v", stored, err)
			}
			if err := s.retireRunRuntime(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			for _, view := range []*Server{s, New(s.config, s.logger)} {
				metrics := gatherTestMetrics(t, view.state)
				if overlap {
					requireMetricValue(t, metrics, "kpl_nodes", projectionNodeLabels(id, workerNodeCount{AgentID: agent.ID, Group: node.Group, Role: node.Role, Type: node.Type, State: reported.State}), 1)
					key := metricTestKey("kpl_nodes", projectionNodeLabels(id, workerNodeCount{AgentID: agent.ID, Group: node.Group, Role: node.Role, Type: node.Type, State: node.State}))
					if metric, exists := metrics[key]; exists && metric.value != 0 {
						t.Fatalf("retired proof reclassified frozen failed history: %s=%v", key, metric.value)
					}
				} else {
					requireMetricValue(t, metrics, "kpl_nodes", projectionNodeLabels(id, workerNodeCount{AgentID: agent.ID, Group: node.Group, Role: node.Role, Type: node.Type, State: node.State}), float64(want))
				}
			}
		})
	}
}

func projectionReadBytes(t *testing.T) uint64 {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("process read accounting is Linux-specific")
	}
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Skipf("process read accounting is unavailable: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if value, ok := strings.CutPrefix(line, "rchar: "); ok {
			n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
	}
	t.Fatal("missing process read accounting")
	return 0
}

func projectionDiskFixture(tb testing.TB, runs, counters int) (*Server, int64) {
	tb.Helper()
	s := New(ServerConfig{DataDir: tb.TempDir(), RunWorkers: true}, nil)
	var total int64
	for i := range runs {
		id := fmt.Sprintf("projection-%03d", i)
		directory := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
		if err := os.MkdirAll(directory, 0700); err != nil {
			tb.Fatal(err)
		}
		root, err := os.OpenRoot(directory)
		if err != nil {
			tb.Fatal(err)
		}
		summary := runWorkerSummary{RunID: id, Metrics: model.Metrics{RunID: id}, Retired: true, At: time.Unix(1700000000, 0).UTC(), Nodes: []workerNodeCount{{AgentID: "agent", Group: "workers", Role: "worker", Type: "full", State: model.NodeStopped, Count: 2}}}
		for row := range counters {
			summary.Counters = append(summary.Counters, workerCounterSeries{Family: "events", Labels: []string{id, "agent", "publish", fmt.Sprintf("topic-%04d", row)}, Value: float64(row)})
		}
		if err := writeAnalysisJSON(root, workerSummaryFile, summary); err != nil {
			tb.Fatal(err)
		}
		info, err := root.Stat(workerSummaryFile)
		root.Close()
		if err != nil {
			tb.Fatal(err)
		}
		total += info.Size()
	}
	return s, total
}

func TestScrapeReadsHistoricalProjectionOnce(t *testing.T) {
	s, total := projectionDiskFixture(t, 32, 1024)
	if _, err := s.state.metrics.registry.Gather(); err != nil {
		t.Fatal(err)
	}
	before := projectionReadBytes(t)
	families, err := s.state.metrics.registry.Gather()
	read := projectionReadBytes(t) - before
	if err != nil {
		t.Fatal(err)
	}
	// Allow process/Go collector reads and a generous fixed margin, while
	// rejecting a second complete historical JSON read on every scrape.
	t.Logf("historicalBytes=%d processReadBytes=%d families=%d", total, read, len(families))
	if read > uint64(total+total/4+(64<<10)) {
		t.Fatalf("scrape reread historical projections: source=%d processReads=%d", total, read)
	}
	if len(s.state.workerSummaries) != 0 || len(s.state.metrics.sealed) != 0 {
		t.Fatal("scrape retained a process-lifetime historical projection cache")
	}
}

func TestProjectionMetricsKeepCounterOwnersAndDeletionFences(t *testing.T) {
	s, _ := projectionDiskFixture(t, 4, 2)
	const topic = "topic-0000"
	s.state.metrics.observeEvent(model.TraceEvent{RunID: "projection-000", AgentID: "agent", Type: "publish", Topic: topic})
	s.state.metrics.restoreSealedCounters("projection-001", []workerCounterSeries{{Family: "events", Labels: []string{"projection-001", "agent", "publish", topic}, Value: 5}})
	cached, err := s.state.readWorkerSummary("projection-002")
	if err != nil {
		t.Fatal(err)
	}
	cached.Counters[0].Value = 17
	cached.Metrics.Published = 17
	s.state.workerSummaries = map[string]runWorkerSummary{cached.RunID: cached}
	s.state.persistMu.Lock()
	err = s.markResultDeletedLocked("projection-003")
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	metrics := gatherTestMetrics(t, s.state)
	for id, want := range map[string]float64{"projection-000": 1, "projection-001": 5, "projection-002": 17} {
		requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": id, "agent_id": "agent", "event_type": "publish", "topic": topic}, want)
	}
	requireMetricValue(t, metrics, "kpl_window_legacy_publications", map[string]string{"run_id": cached.RunID}, 17)
	for key := range metrics {
		if strings.Contains(key, `run_id="projection-003"`) {
			t.Fatalf("deleted disk projection reappeared: %s", key)
		}
	}
	if len(s.state.workerSummaries) != 1 || len(s.state.metrics.sealed) != 1 {
		t.Fatal("scrape changed active/cached projection ownership")
	}
}

func TestProjectionMetricsRejectDamagedSummaryAndNodeCounts(t *testing.T) {
	for _, damage := range []string{"json", "identity", "negative-node-count", "overflow-node-count"} {
		t.Run(damage, func(t *testing.T) {
			s, _ := projectionDiskFixture(t, 1, 1)
			const id = "projection-000"
			root, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			if damage == "json" {
				err = os.WriteFile(filepath.Join(root.Name(), workerSummaryFile), []byte("invalid"), 0600)
			} else {
				summary, readErr := s.state.readWorkerSummary(id)
				if readErr != nil {
					t.Fatal(readErr)
				}
				switch damage {
				case "identity":
					summary.RunID = "wrong-run"
				case "negative-node-count":
					summary.Nodes[0].Count = -1
				case "overflow-node-count":
					summary.Nodes[0].Count = int(^uint(0) >> 1)
					summary.Nodes = append(summary.Nodes, summary.Nodes[0])
				}
				err = writeAnalysisJSON(root, workerSummaryFile, summary)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.state.metrics.registry.Gather(); err == nil {
				t.Fatal("damaged projection produced a successful monitoring snapshot")
			}
		})
	}
}

func BenchmarkHistoricalProjectionScrape(b *testing.B) {
	s, total := projectionDiskFixture(b, 32, 1024)
	b.ReportAllocs()
	b.ReportMetric(float64(total), "source-bytes")
	b.ResetTimer()
	for range b.N {
		if _, err := s.state.metrics.registry.Gather(); err != nil {
			b.Fatal(err)
		}
	}
}
