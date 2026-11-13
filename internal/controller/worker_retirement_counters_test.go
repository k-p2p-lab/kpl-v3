package controller

import (
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRetirementCounterSourcesPreservePublishedMetrics(t *testing.T) {
	for _, mode := range []struct {
		name             string
		retired          bool
		complete         bool
		parentPublishes  int
		savedPublishes   int
		wantPublishes    int
		preserveBaseline bool
	}{
		{"complete-replay-over-partial-parent", false, true, 2, 7, 7, true},
		{"already-retired-over-partial-parent", true, true, 2, 7, 7, false},
		{"live-summary-with-parent-counters", false, false, 3, 0, 3, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			original, id, before := summaryFreshnessFixture(t, time.Time{})
			before.Retired, before.CountersComplete = mode.retired, mode.complete
			before.Metrics.Published = mode.wantPublishes
			before.Counters = nil
			if mode.savedPublishes > 0 {
				before.Counters = []workerCounterSeries{
					{Family: "events", Labels: []string{id, "agent", "publish", "topic"}, Value: float64(mode.savedPublishes)},
					{Family: "messageBytes", Labels: []string{id, "agent", "topic", "publish", "raw"}, Value: float64(mode.savedPublishes * 32)},
				}
			}
			if mode.retired {
				before.Nodes = []workerNodeCount{{AgentID: "agent", Group: "workers", Role: "worker", Type: "full", State: model.NodeStopped, Count: 3}}
			}
			root, err := os.OpenRoot(filepath.Join(original.config.DataDir, currentRunsDirectory, id))
			if err != nil {
				t.Fatal(err)
			}
			err = writeAnalysisJSON(root, workerSummaryFile, before)
			root.Close()
			if err != nil {
				t.Fatal(err)
			}

			parent := New(original.config, original.logger)
			parent.state.experiments[id] = original.state.experiments[id]
			if err := parent.loadWorkerSummaries(); err != nil {
				t.Fatal(err)
			}
			// Normal heartbeat initialization supplies zero-valued baselines.
			// Only a suffix of the Run's publications reached this parent.
			parent.state.metrics.initNode(model.Node{ID: "peer", RunID: id, AgentID: "agent", State: model.NodeReady, Metadata: map[string]string{"topics": "topic"}})
			for range mode.parentPublishes {
				parent.state.metrics.observeEvent(model.TraceEvent{RunID: id, AgentID: "agent", NodeID: "peer", Type: "publish", Topic: "topic", Fields: map[string]any{"wireBytes": 32, "payloadEncoding": "raw"}})
			}
			if err := retireColdRunForTest(t, parent, id); err != nil {
				t.Fatal(err)
			}
			after, err := parent.state.readWorkerSummary(id)
			if err != nil {
				t.Fatal(err)
			}
			if !after.Retired || !reflect.DeepEqual(before.Metrics, after.Metrics) || !before.At.Equal(after.At) {
				t.Fatal("retirement changed the saved measurement projection")
			}
			if mode.retired && (!reflect.DeepEqual(before.Counters, after.Counters) || !reflect.DeepEqual(before.Nodes, after.Nodes)) {
				t.Fatalf("repeated retirement replaced durable counts: before=%+v after=%+v", before, after)
			}

			// Check consumer-visible counters both immediately and after a
			// further restart, when no parent projection can mask disk loss.
			for _, view := range []*Server{parent, New(original.config, original.logger)} {
				metrics := gatherTestMetrics(t, view.state)
				requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": id, "agent_id": "agent", "event_type": "publish", "topic": "topic"}, float64(mode.wantPublishes))
				requireMetricValue(t, metrics, "kpl_message_bytes_total", map[string]string{"run_id": id, "agent_id": "agent", "topic": "topic", "direction": "publish", "encoding": "raw"}, float64(mode.wantPublishes*32))
				if mode.preserveBaseline {
					requireMetricValue(t, metrics, "kpl_events_total", map[string]string{"run_id": id, "agent_id": "agent", "event_type": "add_peer", "topic": ""}, 0)
					requireMetricValue(t, metrics, "kpl_gossipsub_control_rpcs_total", map[string]string{"run_id": id, "agent_id": "agent", "direction": "send", "control_type": "graft"}, 0)
				}
				response := resultRequest(view, http.MethodGet, "/api/v1/prometheus/agent-targets")
				if response.Code != http.StatusOK {
					t.Fatalf("shared-state API unavailable after retirement: status=%d body=%s", response.Code, response.Body)
				}
			}
		})
	}
}
