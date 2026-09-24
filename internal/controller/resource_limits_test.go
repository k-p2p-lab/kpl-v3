package controller

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func TestSequencedDeduplicationCompactsLongHistoryAndKeepsGaps(t *testing.T) {
	a := newRunMetricAccumulator()
	e := model.TraceEvent{NodeID: "peer", SessionID: "session", Type: "rpc_metadata"}
	for i := uint64(1); i <= 100000; i++ {
		if i == 50000 {
			continue
		}
		e.Sequence = i
		if !a.observe(e) {
			t.Fatalf("first sequence %d rejected", i)
		}
	}
	key := measurementSessionKey{e.NodeID, e.SessionID}
	if len(a.seen) != 0 || !reflect.DeepEqual(a.sequences[key], sequenceRanges{{1, 49999}, {50001, 100000}}) {
		t.Fatalf("dedup storage grew per event: legacy=%d, ranges=%v", len(a.seen), a.sequences[key])
	}
	e.Sequence = 50000
	if !a.observe(e) || a.observe(e) || !reflect.DeepEqual(a.sequences[key], sequenceRanges{{1, 100000}}) {
		t.Fatal("late gap fill or retry was counted incorrectly")
	}
	e.Sequence = 100000
	if a.observe(e) {
		t.Fatal("old retry admitted")
	}
	e.SessionID = "restart"
	if !a.observe(e) {
		t.Fatal("restart confused with previous session")
	}
	e.SessionID = ""
	e.EventID = "legacy"
	if !a.observe(e) || a.observe(e) {
		t.Fatal("legacy retry handling changed")
	}
}

func TestBandwidthSeriesBoundedAcrossChurnAndConserveBytes(t *testing.T) {
	s := newState(t.TempDir())
	a := newRunMetricAccumulator()
	s.runMetrics["run"] = a
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newBandwidthCollector(s))
	const sessions = 5000
	for i := 0; i < sessions; i++ {
		a.observe(bandwidthEvent(fmt.Sprint(i), 1, 10, 100, 200, true))
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	series := 0
	for _, family := range families {
		for _, metric := range family.Metric {
			series++
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["node_id"] != "" || labels["session_id"] != "" {
				t.Fatal("unbounded session labels returned")
			}
			switch family.GetName() {
			case "kpl_p2p_stream_bytes_total", "kpl_p2p_protocol_stream_bytes_total":
				want := float64(sessions * 100)
				if labels["direction"] == "send" {
					want *= 2
				}
				if metric.Counter.GetValue() != want {
					t.Fatalf("lost departed peer bytes: %v", metric)
				}
			case "kpl_p2p_bandwidth_sessions":
				want := 0.
				if labels["state"] == "final" {
					want = sessions
				}
				if metric.Gauge.GetValue() != want {
					t.Fatalf("lost session quality: %v", metric)
				}
			}
		}
	}
	if series != 11 {
		t.Fatalf("5000 sessions should yield 11 aggregate series, got %d", series)
	}
	if len(a.bandwidth.sessions) != sessions {
		t.Fatal("analysis session detail discarded")
	}
}

func TestLiveSummaryRefreshesPendingDeadlinesAndLateEvidence(t *testing.T) {
	a := newRunMetricAccumulator()
	for _, e := range windowEvents(windowStart("receiver", "r", 0, "topic")) {
		a.observe(e)
	}
	now := windowTestEpoch.Add(15 * time.Second)
	first, _ := a.liveSummary("run", now)
	if first.PendingPublications != 1 {
		t.Fatalf("pending=%d", first.PendingPublications)
	}
	settled, _ := a.liveSummary("run", now.Add(10*time.Second))
	if settled.PendingPublications != 0 {
		t.Fatal("cache froze pending publication")
	}
	a.observe(windowEvent("receiver", "r", 2, "measurement_checkpoint", 22))
	updated, _ := a.liveSummary("run", now.Add(11*time.Second))
	exact, _ := a.summarize("run", now.Add(11*time.Second))
	if !reflect.DeepEqual(updated, exact) {
		t.Fatal("late evidence did not invalidate cached summary")
	}
	revision := a.cachedRevision
	at := a.cachedAt
	a.liveSummary("run", now.Add(time.Hour))
	if a.cachedRevision != revision || a.cachedAt != at {
		t.Fatal("unchanged settled history recomputed")
	}
}

func TestStreamCoalescesFrequentTelemetryAndSharesEncoding(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	first, err := s.streamSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.streamSnapshot()
	if err != nil || &first[0] != &second[0] {
		t.Fatal("tabs did not share encoded snapshot")
	}
	server := httptest.NewServer(s.apiTestHandler(context.Background()))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 1250*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/stream", nil)
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.state.notify()
			}
		}
	}()
	count := 0
	scanner := bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if scanner.Text() == "event: snapshot" {
			count++
		}
	}
	<-done
	if count < 1 || count > 2 {
		t.Fatalf("frequent telemetry produced %d snapshots in 1.25 seconds", count)
	}
}

func TestShutdownAcceptsLastEventsBeforeAgentDrainAcknowledgment(t *testing.T) {
	s := newLifecycleTestController(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + listener.Addr().String()
	var drained atomic.Bool
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/nodes" {
			http.NotFound(w, r)
			return
		}
		body := `{"agentId":"agent","events":[{"eventId":"last","runId":"finished","nodeId":"peer","type":"measurement_terminated","timestamp":"2026-09-10T00:00:00Z"}]}`
		request, _ := http.NewRequest(http.MethodPost, base+"/api/v1/events/batch", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+s.config.Token)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			http.Error(w, err.Error(), 503)
			return
		}
		defer response.Body.Close()
		if response.StatusCode >= 300 {
			http.Error(w, response.Status, 503)
			return
		}
		drained.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer agent.Close()
	if _, err = s.state.registerAgent(model.Agent{ID: "agent", URL: agent.URL}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, listener) }()
	response, err := http.Get(base + "/api/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if !drained.Load() {
		t.Fatal("Agent drain was skipped")
	}
	if a := s.state.runMetrics["finished"]; a == nil || !a.hasEvent(model.TraceEvent{NodeID: "peer", EventID: "last"}) {
		t.Fatal("last event was not persisted/accounted")
	}
}

func BenchmarkSequencedEventHistory(b *testing.B) {
	a := newRunMetricAccumulator()
	e := model.TraceEvent{NodeID: "peer", SessionID: "session", Type: "rpc_metadata"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.Sequence = uint64(i + 1)
		a.observe(e)
	}
	if len(a.seen) > 0 {
		b.Fatal("unbounded event IDs")
	}
}
