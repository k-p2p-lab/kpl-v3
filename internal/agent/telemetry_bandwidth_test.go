package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

// Exercise the typed Peer -> Agent -> Controller boundary, including admission,
// failed HTTP retry, event dedup, archive persistence and fresh-Controller replay.
func TestBandwidthTelemetrySurvivesAgentRetryAndControllerRestart(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "runs", "run")
	if err := os.MkdirAll(runDir, 0755); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	manifest, _ := json.Marshal(model.Experiment{ID: "run", State: "completed", StartedAt: now.Add(-time.Minute), FinishedAt: now})
	for name, data := range map[string][]byte{"experiment.json": manifest, "scenario.yaml": []byte("version: 1\nname: bandwidth-test\n")} {
		if err := os.WriteFile(filepath.Join(runDir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	core := authenticatedTestController(dir)
	handler := core.Handler(context.Background())
	var mu sync.Mutex
	var bodies [][]byte
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/events/batch" {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			mu.Lock()
			bodies = append(bodies, data)
			first := len(bodies) == 1
			mu.Unlock()
			if first {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	agent := &Server{config: Config{ID: "agent", ControllerURL: endpoint.URL, Token: controllerTestToken}, client: endpoint.Client(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	original := model.TraceEvent{RunID: "run", NodeID: "peer", AgentID: "untrusted-agent", SessionID: "session", Sequence: 1, Type: "bandwidth", Timestamp: now, Bandwidth: &model.BandwidthSample{ElapsedNS: int64(5 * time.Second), ReceivedBytes: 1024, SentBytes: 2048, Final: true, Protocols: []model.BandwidthProtocol{{Protocol: "/meshsub/1.2.0", ReceivedBytes: 1024, SentBytes: 2048}}}}
	body, _ := json.Marshal(model.EventBatch{Events: []model.TraceEvent{original}})
	admit := func() {
		t.Helper()
		response := httptest.NewRecorder()
		agent.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewReader(body)))
		if response.Code != http.StatusNoContent {
			t.Fatalf("admission: %d %s", response.Code, response.Body)
		}
	}
	admit()
	agent.flushEvents(context.Background())
	if len(agent.events) != 1 || agent.events[0].Bandwidth == nil {
		t.Fatal("failed HTTP attempt lost the sample")
	}
	agent.flushEvents(context.Background())
	admit()
	agent.flushEvents(context.Background())
	if len(agent.events) != 0 {
		t.Fatal("retry was not acknowledged")
	}
	mu.Lock()
	same := len(bodies) == 3 && bytes.Equal(bodies[0], bodies[1])
	mu.Unlock()
	if !same {
		t.Fatal("retry changed serialized event identity/counters")
	}
	raw, err := os.ReadFile(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(raw, []byte("\n")) != 1 {
		t.Fatalf("source retry duplicated the archive: %s", raw)
	}
	var stored model.TraceEvent
	if err := json.Unmarshal(bytes.TrimSpace(raw), &stored); err != nil {
		t.Fatal(err)
	}
	if stored.AgentID != "agent" || stored.SessionID != "session" || stored.Sequence != 1 || !reflect.DeepEqual(stored.Bandwidth, original.Bandwidth) {
		t.Fatalf("relay changed typed evidence: %+v", stored)
	}
	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !bytes.Contains(metrics.Body.Bytes(), []byte("kpl_p2p_stream_bytes_total{")) {
		t.Fatal("HTTP collector omitted accepted bandwidth")
	}
	restarted := authenticatedTestController(dir)
	response := httptest.NewRecorder()
	restartedHandler := restarted.Handler(context.Background())
	restartedHandler.ServeHTTP(response, controllerReadRequest(t, restartedHandler, "/api/v1/experiments/run/analysis"))
	if response.Code != http.StatusOK {
		t.Fatalf("restart analysis: %d %s", response.Code, response.Body)
	}
	var analysis struct {
		Metrics    model.Metrics `json:"metrics"`
		EventCount int           `json:"eventCount"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &analysis); err != nil {
		t.Fatal(err)
	}
	if b := analysis.Metrics.Bandwidth; b == nil || b.SentBytes != 2048 || b.ReceivedBytes != 1024 || b.Sessions != 1 || b.FinalizedSessions != 1 || analysis.EventCount != 1 {
		t.Fatalf("replay lost counters: %+v", analysis)
	}
}
