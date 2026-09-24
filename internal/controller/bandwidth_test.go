package controller

import (
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func bandwidthEvent(session string, sequence uint64, seconds int, received, sent int64, final bool) model.TraceEvent {
	return model.TraceEvent{RunID: "run", NodeID: "node", AgentID: "agent", SessionID: session, Sequence: sequence, Type: "bandwidth", Timestamp: time.Unix(1000+int64(seconds), 0).UTC(), Bandwidth: &model.BandwidthSample{ElapsedNS: int64(time.Duration(seconds) * time.Second), ReceivedBytes: received, SentBytes: sent, Final: final, Protocols: []model.BandwidthProtocol{{Protocol: "/meshsub/1.2.0", ReceivedBytes: received, SentBytes: sent}}}}
}
func TestBandwidthCumulativeLossRetryRestartAndValidation(t *testing.T) {
	a := newRunMetricAccumulator()
	var timeline bandwidthTimeline
	a.onBandwidth = timeline.add
	first := bandwidthEvent("s1", 1, 10, 100, 200, false)
	final := bandwidthEvent("s1", 3, 40, 400, 800, true) // Missing middle sample still recovers all bytes.
	restart := bandwidthEvent("s2", 1, 5, 50, 100, false)
	restart.Timestamp = final.Timestamp.Add(5 * time.Second)
	for _, e := range []model.TraceEvent{first, final, first, restart, bandwidthEvent("s1", 2, 20, 200, 400, false)} {
		a.observe(e)
	}
	bad := bandwidthEvent("s2", 2, 10, 1, 2, false)
	a.observe(bad) // Same-session counter reset is invalid.
	missing := bandwidthEvent("s3", 1, 5, 1, 1, false)
	missing.Bandwidth = nil
	a.observe(missing)
	negative := bandwidthEvent("s4", 1, 5, -1, 0, false)
	a.observe(negative)
	summary, _ := a.summarize("run")
	s := summary.Bandwidth
	if s == nil || s.Sessions != 2 || s.FinalizedSessions != 1 || s.ReceivedBytes != 450 || s.SentBytes != 900 || s.Samples != 3 || s.RejectedSamples != 3 || s.SupersededSamples != 1 {
		t.Fatalf("incorrect cumulative counters: %+v", s)
	}
	bins, width := timeline.result()
	var total float64
	for _, b := range bins {
		total += b.SentBytes
	}
	if math.Abs(total-900) > 1e-8 || width != 5 {
		t.Fatalf("timeline lost bytes: %v width=%d", total, width)
	}
	if empty := newRunMetricAccumulator(); empty.bandwidth.summarize() != nil {
		t.Fatal("legacy must remain unavailable")
	}
	zero := newRunMetricAccumulator()
	zero.observe(bandwidthEvent("zero", 1, 5, 0, 0, true))
	if s := zero.bandwidth.summarize(); s == nil || s.Sessions != 1 || s.SentBytes != 0 {
		t.Fatal("valid zero was lost")
	}
	for _, mutate := range []func(*model.TraceEvent){
		func(e *model.TraceEvent) { e.SessionID = "" }, func(e *model.TraceEvent) { e.Timestamp = time.Time{} }, func(e *model.TraceEvent) { e.Bandwidth.ElapsedNS = 0 },
		func(e *model.TraceEvent) { e.Bandwidth.Protocols[0].SentBytes++ }, func(e *model.TraceEvent) {
			e.Bandwidth.Protocols = append(e.Bandwidth.Protocols, e.Bandwidth.Protocols[0])
		},
	} {
		e := bandwidthEvent("s", 1, 5, 1, 2, false)
		mutate(&e)
		if validBandwidthSample(e) {
			t.Fatalf("accepted malformed sample: %+v", e)
		}
	}
}
func TestBandwidthTimelineBoundedAndConservesProtocolBytes(t *testing.T) {
	var timeline bandwidthTimeline
	for i := 0; i < 1000; i++ {
		timeline.add(bandwidthInterval{at: time.Unix(1000+int64(i)*5, 0), durationNS: int64(5 * time.Second), receivedBytes: 50, sentBytes: 100, protocols: []model.BandwidthProtocol{{Protocol: "/test", ReceivedBytes: 50, SentBytes: 100}}})
	}
	// A year-long reporting interval must not enumerate seconds before coarsening.
	timeline.add(bandwidthInterval{at: time.Unix(1000+365*86400, 0), durationNS: int64(365 * 24 * time.Hour), sentBytes: 300, protocols: []model.BandwidthProtocol{{Protocol: "/test", SentBytes: 300}}})
	bins, width := timeline.result()
	var total, protocol float64
	for _, b := range bins {
		total += b.SentBytes
		for _, p := range b.Protocols {
			protocol += p.SentBytes
		}
	}
	if len(bins) > 360 || width <= 5 || math.Abs(total-100300) > 1e-6 || math.Abs(protocol-total) > 1e-6 {
		t.Fatalf("binning mismatch: bins=%d width=%d total=%g protocol=%g", len(bins), width, total, protocol)
	}
}
func TestBandwidthArchiveAnalysisZIPAndRestart(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run", "completed", time.Unix(1000, 0))
	events := []model.TraceEvent{bandwidthEvent("s", 1, 5, 100, 200, false), bandwidthEvent("s", 3, 15, 300, 600, true)}
	analysisWriteLines(t, server, "run", "events.jsonl", append(events, events[1]))
	restarted := New(server.config, nil)
	a := analysisRequest(t, restarted, "run")
	if a.Metrics.Bandwidth == nil || a.Metrics.Bandwidth.SentBytes != 600 || a.Metrics.Bandwidth.Samples != 2 || len(a.BandwidthTimeline) != 3 || a.BandwidthBinSeconds != 5 {
		t.Fatalf("archive analysis: %+v", a)
	}
	response := resultRequest(restarted, http.MethodGet, "/api/v1/experiments/run/download")
	if response.Code != 200 {
		t.Fatalf("download: %s", response.Body)
	}
	files := decodeResultZIP(t, response.Body.Bytes())
	var m model.Metrics
	if err := json.Unmarshal(files["metrics.json"], &m); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.Bandwidth, a.Metrics.Bandwidth) {
		t.Fatalf("ZIP counters differ: %+v vs %+v", m.Bandwidth, a.Metrics.Bandwidth)
	}
	var timeline bandwidthTimeline
	accumulator := newRunMetricAccumulator()
	accumulator.onBandwidth = timeline.add
	accumulator.observe(events[1])
	accumulator.observe(events[0])
	if s := accumulator.bandwidth.summarize(); s.SentBytes != 600 || s.FinalizedSessions != 1 {
		t.Fatalf("out-of-order final counter lost: %+v", s)
	}
}
func TestBandwidthPrometheusCountersFreshnessAndDeletion(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	a := newRunMetricAccumulator()
	e := bandwidthEvent("s", 1, 5, 100, 200, false)
	e.Timestamp = time.Now().UTC()
	a.observe(e)
	server.state.runMetrics["run"] = a
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(newBandwidthCollector(server.state))
	read := func(name string) []float64 {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		out := []float64{}
		for _, f := range families {
			if f.GetName() == name {
				for _, m := range f.Metric {
					if m.Counter != nil {
						out = append(out, m.Counter.GetValue())
					} else {
						out = append(out, m.Gauge.GetValue())
					}
				}
			}
		}
		return out
	}
	if got := read("kpl_p2p_stream_bits_per_second"); !reflect.DeepEqual(got, []float64{160, 320}) {
		t.Fatalf("rate did not use monotonic 5 seconds: %v", got)
	}
	e = bandwidthEvent("stale", 1, 5, 10, 20, false)
	e.Timestamp = time.Now().Add(-time.Minute)
	a.observe(e)
	if got := read("kpl_p2p_stream_bits_per_second"); len(got) != 2 {
		t.Fatalf("stale session fabricated current rate: %v", got)
	}
	if got := read("kpl_p2p_stream_bytes_total"); !reflect.DeepEqual(got, []float64{110, 220}) {
		t.Fatalf("stale counters disappeared: %v", got)
	}
	e = bandwidthEvent("s", 2, 10, 300, 500, true)
	e.Timestamp = time.Now().UTC()
	a.observe(e)
	if got := read("kpl_p2p_stream_bits_per_second"); !reflect.DeepEqual(got, []float64{0, 0}) {
		t.Fatalf("closed session has throughput: %v", got)
	}
	delete(server.state.runMetrics, "run")
	if got := read("kpl_p2p_stream_bytes_total"); len(got) != 0 {
		t.Fatal("deleted run still exported")
	}
}

func TestBandwidthDoesNotChangePublicationMeasurementDefinition(t *testing.T) {
	a := newRunMetricAccumulator()
	a.observe(cohortPublish("message", "topic", []string{"receiver"}))
	a.observe(cohortDelivery("message", "topic", "receiver", 20))
	before, _ := a.summarize("run")
	a.observe(bandwidthEvent("s", 1, 5, 100, 200, false))
	after, _ := a.summarize("run")
	after.Bandwidth = nil
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("bandwidth changed publication metrics: before=%+v after=%+v", before, after)
	}
}

func TestBandwidthReviewWideTimestampAndSubsecondPrecision(t *testing.T) {
	for _, year := range []int{1900, 2026, 2500, 9998} {
		var timeline bandwidthTimeline
		at := time.Date(year, 1, 1, 0, 0, 1, 0, time.UTC)
		timeline.add(bandwidthInterval{at: at, durationNS: 1, sentBytes: 37})
		bins, width := timeline.result()
		if len(bins) != 1 || width != 5 || bins[0].At.Year() != year || bins[0].SentBytes != 37 {
			t.Fatalf("timestamp %v lost/misplaced bytes: %+v width=%d", at, bins, width)
		}
	}
}
func TestBandwidthReviewCounterOverflowIsRejected(t *testing.T) {
	a := newRunMetricAccumulator()
	a.observe(bandwidthEvent("large", 1, 5, 0, math.MaxInt64, false))
	a.observe(bandwidthEvent("overflow", 1, 5, 0, 1, false))
	summary := a.bandwidth.summarize()
	if summary.SentBytes != math.MaxInt64 || summary.Sessions != 1 || summary.RejectedSamples != 1 {
		t.Fatalf("aggregate overflow: %+v", summary)
	}
}

func TestBandwidthMissingTimestampRemainsInvalidAfterAdmission(t *testing.T) {
	s := newState(t.TempDir())
	event := bandwidthEvent("missing-time", 1, 5, 100, 200, false)
	event.Timestamp = time.Time{}
	if err := s.appendEvents(model.EventBatch{AgentID: "agent", Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	if len(s.events) != 1 || !s.events[0].Timestamp.IsZero() {
		t.Fatal("admission fabricated a source timestamp")
	}
	summary := s.runMetrics["run"].bandwidth.summarize()
	if summary.Sessions != 0 || summary.RejectedSamples != 1 {
		t.Fatalf("untimed bandwidth admitted: %+v", summary)
	}
}
