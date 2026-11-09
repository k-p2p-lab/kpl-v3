package runtimehealth

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("runtime health worker did not make progress")
		var zero T
		return zero
	}
}

func TestRunEmitsStartupAndTicksThenStops(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, nil)).With("image", "test-image")
	ticks := make(chan time.Time)
	sampled := make(chan int, 2)
	done := make(chan struct{})
	calls := 0
	go func() {
		defer close(done)
		run(ctx, logger, "controller", func(now time.Time) observation {
			calls++
			sampled <- calls
			return observation{attrs: []slog.Attr{slog.Time("sampledAt", now.UTC()), slog.Uint64("rssFileBytes", 0)}, unavailable: []string{"openFDs", "cpuUserTicks", "openFDs"}}
		}, ticks, func() time.Time { return time.Date(2026, 11, 9, 4, 0, 0, 0, time.FixedZone("test", 9*60*60)) })
	}()
	if got := await(t, sampled); got != 1 {
		t.Fatalf("startup sample = %d", got)
	}
	ticks <- time.Time{}
	if got := await(t, sampled); got != 2 {
		t.Fatalf("tick sample = %d", got)
	}
	cancel()
	await(t, done)
	lines := strings.Split(strings.TrimSpace(buffer.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("records = %d, want startup and one tick", len(lines))
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] != "runtime health" || record["role"] != "controller" || record["image"] != "test-image" || record["rssFileBytes"] != float64(0) || record["sampledAt"] != "2026-11-08T19:00:00Z" {
			t.Fatalf("unexpected record: %v", record)
		}
		if _, exists := record["openFDs"]; exists || !reflect.DeepEqual(record["unavailable"], []any{"cpuUserTicks", "openFDs"}) {
			t.Fatalf("unknown measurement was fabricated or not identified: %v", record)
		}
	}
}

func TestCanceledRunDoesNotObserve(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var buffer bytes.Buffer
	run(ctx, slog.New(slog.NewJSONHandler(&buffer, nil)), "agent", func(time.Time) observation {
		t.Fatal("canceled worker sampled")
		return observation{}
	}, nil, time.Now)
	if buffer.Len() != 0 {
		t.Fatal("canceled worker emitted a record")
	}
}

type blockedWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockedWriter) Write(data []byte) (int, error) {
	w.entered <- struct{}{}
	<-w.release
	return len(data), nil
}

func TestSlowLoggerDoesNotSpawnTickWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &blockedWriter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	logger := slog.New(slog.NewJSONHandler(writer, nil))
	ticks := make(chan time.Time, 100)
	done := make(chan struct{})
	var calls atomic.Int32
	go func() {
		defer close(done)
		run(ctx, logger, "agent", func(time.Time) observation { calls.Add(1); return observation{} }, ticks, time.Now)
	}()
	await(t, writer.entered)
	for i := 0; i < cap(ticks); i++ {
		ticks <- time.Now()
	}
	if calls.Load() != 1 {
		t.Fatalf("slow output accumulated concurrent samples: %d", calls.Load())
	}
	cancel()
	close(writer.release)
	await(t, done)
	if calls.Load() != 1 {
		t.Fatalf("worker consumed queued ticks after cancellation: %d", calls.Load())
	}
}

func TestSamplerUsesRuntimeMetricsAndInjectedBuild(t *testing.T) {
	start := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	s := newSampler(start, 123, model.BuildMetadata{Version: "test-version", Revision: "test-revision", GoVersion: "test-go", BinaryHash: "sha256:test", Modified: true})
	s.readProcess = func(time.Time) observation {
		return observation{attrs: []slog.Attr{slog.Uint64("rssFileBytes", 0)}, unavailable: []string{"openFDs"}}
	}
	o := s.observe(start.Add(2 * time.Minute))
	attrs := attrMap(o)
	if attrs["pid"] != int64(123) || attrs["version"] != "test-version" || attrs["sourceRevision"] != "test-revision" || attrs["binaryHash"] != "sha256:test" || attrs["sourceModified"] != true || attrs["observerUptimeSeconds"] != float64(120) {
		t.Fatalf("identity/clock metadata missing: %v", attrs)
	}
	for _, field := range runtimeFields {
		if _, exists := attrs[field.field]; !exists {
			t.Errorf("supported Go runtime metric %s missing", field.field)
		}
	}
	if attrs["rssFileBytes"] != uint64(0) || !reflect.DeepEqual(o.unavailable, []string{"buildTime", "openFDs"}) {
		t.Fatalf("known zero or missing metadata changed: %+v", o)
	}
	if _, exists := attrs["openFDs"]; exists {
		t.Fatal("unsupported OS measurement became zero")
	}
	if _, exists := attrs["buildTime"]; exists {
		t.Fatal("missing build time became a fabricated timestamp")
	}
}

func TestUnsupportedRuntimeMetricIsOmitted(t *testing.T) {
	samples := []metrics.Sample{{Name: "/not/a/runtime/metric:bytes"}}
	metrics.Read(samples)
	o := observation{}
	appendRuntimeMetrics(&o, []metricField{{samples[0].Name, "unsupported"}, {"/missing:bytes", "absent"}}, samples)
	if len(o.attrs) != 0 || !reflect.DeepEqual(o.unavailable, []string{"unsupported", "absent"}) {
		t.Fatalf("unsupported values were fabricated: %+v", o)
	}
}

func attrMap(o observation) map[string]any {
	result := make(map[string]any, len(o.attrs))
	for _, attr := range o.attrs {
		result[attr.Key] = attr.Value.Any()
	}
	return result
}
