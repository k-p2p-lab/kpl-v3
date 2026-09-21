package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestBandwidthLiveRatesUseSourceIntervalsAndExcludeStaleSessions(t *testing.T) {
	now := time.Now().UTC()
	a := newRunMetricAccumulator()
	first := bandwidthEvent("fresh", 1, 5, 100, 200, false)
	first.Timestamp = now.Add(-9 * time.Second)
	next := bandwidthEvent("fresh", 2, 10, 300, 500, false)
	next.Timestamp = now // Nine wall-clock seconds, five source monotonic seconds.
	a.observe(first)
	a.observe(next)
	a.observe(next) // Retry cannot inflate the rate.
	for _, spec := range []struct {
		id    string
		at    time.Time
		final bool
	}{
		{"other", now, false}, {"stale", now.Add(-time.Minute), false}, {"future", now.Add(time.Minute), false}, {"closed", now.Add(-time.Hour), true},
	} {
		event := bandwidthEvent(spec.id, 1, 10, 100, 200, spec.final)
		event.Timestamp = spec.at
		a.observe(event)
	}
	got := a.bandwidth.currentRates(now)
	if !got.Available || got.SentBitsPerSecond != 640 || got.ReceivedBitsPerSecond != 400 || got.ReportingSessions != 2 || got.StaleSessions != 2 {
		t.Fatalf("wrong live rate: %+v", got)
	}
	summary := a.bandwidth.summarize()
	if summary.SentBytes != 1300 || summary.ReceivedBytes != 700 || summary.FinalizedSessions != 1 || summary.CurrentRates != nil {
		t.Fatalf("live rates changed archive totals: %+v", summary)
	}
}

func TestBandwidthLiveAvailabilityDistinguishesMissingStaleAndZero(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name             string
		age              time.Duration
		final, available bool
		reporting, stale int
	}{
		{"fresh zero", 0, false, true, 1, 0},
		{"past boundary", 15 * time.Second, false, true, 1, 0},
		{"future boundary", -15 * time.Second, false, true, 1, 0},
		{"expired", 15*time.Second + time.Nanosecond, false, false, 0, 1},
		{"clock skew", -15*time.Second - time.Nanosecond, false, false, 0, 1},
		{"finalized", time.Hour, true, true, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newRunMetricAccumulator()
			event := bandwidthEvent("s", 1, 5, 0, 0, tc.final)
			event.Timestamp = now.Add(-tc.age)
			a.observe(event)
			got := a.bandwidth.currentRates(now)
			if got.Available != tc.available || got.ReportingSessions != tc.reporting || got.StaleSessions != tc.stale || got.SentBitsPerSecond != 0 || got.ReceivedBitsPerSecond != 0 {
				t.Fatalf("availability: %+v", got)
			}
		})
	}
	if got := newRunMetricAccumulator().bandwidth.currentRates(now); got.Available {
		t.Fatal("missing samples fabricated measured zero")
	}
}

func TestSnapshotBandwidthUsesTheSameRunAsMessageMetrics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	for _, run := range []model.Experiment{{ID: "active", State: "running", StartedAt: now.Add(-time.Minute)}, {ID: "queued", State: "queued", StartedAt: now}, {ID: "previous", State: "completed", StartedAt: now.Add(-time.Hour)}} {
		s.state.experiments[run.ID] = run
		a := newRunMetricAccumulator()
		event := bandwidthEvent("s", 1, 5, 100, 200, false)
		event.Timestamp = now
		event.RunID = run.ID
		if run.ID != "active" {
			event.Bandwidth.SentBytes = 10000
		}
		a.observe(event)
		s.state.runMetrics[run.ID] = a
	}
	response := resultRequest(s, http.MethodGet, "/api/v1/snapshot")
	var snapshot model.Snapshot
	if response.Code != 200 {
		t.Fatalf("snapshot: %d", response.Code)
	}
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	bw := snapshot.Metrics.Bandwidth
	if snapshot.Metrics.RunID != "active" || bw == nil || bw.SentBytes != 200 || bw.CurrentRates == nil || !bw.CurrentRates.Available || bw.CurrentRates.SentBitsPerSecond != 320 {
		t.Fatalf("wrong run bandwidth: %+v", snapshot.Metrics)
	}
	// Current rates are a live view, not a new interpretation of saved results.
	saved, _ := s.state.runMetrics["active"].summarize("active")
	if saved.Bandwidth.CurrentRates != nil {
		t.Fatal("wall-clock rates leaked into archive metrics")
	}
	s.state.experiments["active"] = model.Experiment{ID: "active", State: "completed", StartedAt: now.Add(-time.Minute)}
	s.state.experiments["new"] = model.Experiment{ID: "new", State: "running", StartedAt: now}
	if snapshot := s.state.snapshot(); snapshot.Metrics.RunID != "new" || snapshot.Metrics.Bandwidth != nil {
		t.Fatal("previous run bandwidth leaked into a new run without samples")
	}
}

func TestStreamRefreshesExpiredBandwidthWithoutTelemetry(t *testing.T) {
	for _, query := range []string{"", "?view=dashboard"} {
		t.Run("stream"+query, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir()}, nil)
				now := time.Now().UTC()
				s.state.experiments["run"] = model.Experiment{ID: "run", State: "running", StartedAt: now}
				a := newRunMetricAccumulator()
				event := bandwidthEvent("s", 1, 5, 100, 200, false)
				event.Timestamp = now.Add(-time.Second)
				a.observe(event)
				s.state.runMetrics["run"] = a
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				response := httptest.NewRecorder()
				done := make(chan struct{})
				go func() {
					defer close(done)
					s.handleStream(response, httptest.NewRequest(http.MethodGet, "/api/v1/stream"+query, nil).WithContext(ctx))
				}()
				synctest.Wait()
				time.Sleep(15 * time.Second)
				synctest.Wait()
				cancel()
				<-done // Stop writes before reading the non-concurrent response recorder.
				snapshots := []model.Snapshot{}
				for _, line := range strings.Split(response.Body.String(), "\n") {
					if !strings.HasPrefix(line, "data: ") {
						continue
					}
					var snapshot model.Snapshot
					if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snapshot); err != nil {
						t.Fatal(err)
					}
					snapshots = append(snapshots, snapshot)
				}
				if len(snapshots) != 2 {
					t.Fatalf("idle stream did not refresh metrics: snapshots=%d", len(snapshots))
				}
				if bw := snapshots[0].Metrics.Bandwidth; bw == nil || bw.CurrentRates == nil || !bw.CurrentRates.Available {
					t.Fatal("initial live rate missing")
				}
				bw := snapshots[1].Metrics.Bandwidth
				if bw.CurrentRates.Available || bw.CurrentRates.StaleSessions != 1 || bw.SentBytes != 200 {
					t.Fatalf("expired rate or retained total incorrect: %+v", bw)
				}
			})
		})
	}
}
