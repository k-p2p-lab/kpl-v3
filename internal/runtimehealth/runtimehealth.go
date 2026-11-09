// Package runtimehealth records small process observations independently of
// application locks, Prometheus collectors and experiment telemetry.
package runtimehealth

import (
	"context"
	"log/slog"
	"math"
	"os"
	"runtime/metrics"
	"slices"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const interval = time.Minute

type metricField struct{ name, field string }

var runtimeFields = []metricField{
	{"/memory/classes/heap/objects:bytes", "goHeapObjectsBytes"},
	{"/gc/heap/live:bytes", "goHeapLiveBytes"},
	{"/gc/heap/goal:bytes", "goHeapGoalBytes"},
	{"/gc/heap/objects:objects", "goHeapObjects"},
	{"/gc/heap/allocs:bytes", "goAllocBytesTotal"},
	{"/memory/classes/total:bytes", "goMemoryTotalBytes"},
	{"/memory/classes/heap/released:bytes", "goHeapReleasedBytes"},
	{"/sched/goroutines:goroutines", "goGoroutines"},
	{"/gc/cycles/total:gc-cycles", "goGCCycles"},
	{"/cpu/classes/gc/total:cpu-seconds", "goGCCPUSeconds"},
}

type observation struct {
	attrs       []slog.Attr
	unavailable []string
}

func (o *observation) missing(fields ...string) { o.unavailable = append(o.unavailable, fields...) }

type sampler struct {
	startedAt   time.Time
	pid         int
	build       model.BuildMetadata
	samples     []metrics.Sample
	readRuntime func([]metrics.Sample)
	readProcess func(time.Time) observation
}

func newSampler(startedAt time.Time, pid int, build model.BuildMetadata) *sampler {
	samples := make([]metrics.Sample, len(runtimeFields))
	for i, field := range runtimeFields {
		samples[i].Name = field.name
	}
	return &sampler{startedAt: startedAt, pid: pid, build: build, samples: samples, readRuntime: metrics.Read, readProcess: processObservation}
}

func (s *sampler) observe(now time.Time) observation {
	o := observation{attrs: []slog.Attr{
		slog.Int("pid", s.pid),
		slog.Time("sampledAt", now.UTC()),
		slog.Time("observerStartedAt", s.startedAt.UTC()),
		slog.Float64("observerUptimeSeconds", max(0, now.Sub(s.startedAt).Seconds())),
		slog.Bool("sourceModified", s.build.Modified),
	}}
	for _, field := range []struct{ name, value string }{
		{"version", s.build.Version}, {"sourceRevision", s.build.Revision},
		{"goVersion", s.build.GoVersion}, {"binaryHash", s.build.BinaryHash}, {"buildTime", s.build.BuildTime},
	} {
		if field.value == "" {
			o.missing(field.name)
		} else {
			o.attrs = append(o.attrs, slog.String(field.name, field.value))
		}
	}
	s.readRuntime(s.samples)
	appendRuntimeMetrics(&o, runtimeFields, s.samples)
	process := s.readProcess(now)
	o.attrs = append(o.attrs, process.attrs...)
	o.unavailable = append(o.unavailable, process.unavailable...)
	return o
}

func appendRuntimeMetrics(o *observation, fields []metricField, samples []metrics.Sample) {
	for i, field := range fields {
		if i >= len(samples) {
			o.missing(field.field)
			continue
		}
		value := samples[i].Value
		switch value.Kind() {
		case metrics.KindUint64:
			o.attrs = append(o.attrs, slog.Uint64(field.field, value.Uint64()))
		case metrics.KindFloat64:
			n := value.Float64()
			if math.IsNaN(n) || math.IsInf(n, 0) {
				o.missing(field.field)
			} else {
				o.attrs = append(o.attrs, slog.Float64(field.field, n))
			}
		default:
			o.missing(field.field)
		}
	}
}

// Run emits an initial observation, then at most one per minute. The caller
// owns its goroutine and cancels ctx when the role has finished shutting down.
// A blocked logger occupies this goroutine; no replacement worker is spawned.
// Cancellation takes effect between writes because slog handlers may block.
func Run(ctx context.Context, logger *slog.Logger, role string) {
	if ctx.Err() != nil {
		return
	}
	if logger == nil {
		logger = slog.Default()
	}
	started := time.Now()
	s := newSampler(started, os.Getpid(), model.BuildDetails())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	run(ctx, logger, role, s.observe, ticker.C, time.Now)
}

func run(ctx context.Context, logger *slog.Logger, role string, sample func(time.Time) observation, ticks <-chan time.Time, now func() time.Time) {
	emit := func() {
		o := sample(now())
		o.attrs = append(o.attrs, slog.String("role", role))
		if len(o.unavailable) != 0 {
			slices.Sort(o.unavailable)
			o.attrs = append(o.attrs, slog.Any("unavailable", slices.Compact(o.unavailable)))
		}
		logger.LogAttrs(ctx, slog.LevelInfo, "runtime health", o.attrs...)
	}
	if ctx.Err() != nil {
		return
	}
	emit()
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ticks:
			if !ok || ctx.Err() != nil {
				return
			}
			emit()
		}
	}
}
