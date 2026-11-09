package controller

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

// Direct per-bound counting is the pre-optimization definition. Keep it
// independent of the disjoint-bucket/prefix implementation under test.
func referencePropagationHistograms(samples []propagationSample) map[propagationSeriesKey]*propagationHistogram {
	result := make(map[propagationSeriesKey]*propagationHistogram)
	for _, sample := range samples {
		histogram := result[sample.key]
		if histogram == nil {
			histogram = &propagationHistogram{buckets: make(map[float64]uint64, len(propagationBounds))}
			for _, bound := range propagationBounds {
				histogram.buckets[bound] = 0
			}
			result[sample.key] = histogram
		}
		histogram.count++
		histogram.sum += sample.seconds
		for _, bound := range propagationBounds {
			if sample.seconds <= bound {
				histogram.buckets[bound]++
			}
		}
	}
	return result
}

func assertPropagationHistogramsMatch(t *testing.T, samples []propagationSample) {
	t.Helper()
	got, want := propagationHistograms(samples), referencePropagationHistograms(samples)
	if len(got) != len(want) {
		t.Fatalf("series count got=%d want=%d", len(got), len(want))
	}
	for key, expected := range want {
		actual := got[key]
		if actual == nil || actual.count != expected.count || !reflect.DeepEqual(actual.buckets, expected.buckets) {
			t.Fatalf("histogram %v changed: got=%+v want=%+v", key, actual, expected)
		}
		if !(math.IsNaN(actual.sum) && math.IsNaN(expected.sum)) && math.Float64bits(actual.sum) != math.Float64bits(expected.sum) {
			t.Fatalf("histogram %v sum changed: got=%v want=%v", key, actual.sum, expected.sum)
		}
	}
}

func TestPropagationHistogramsPreserveBoundariesAndSpecialValues(t *testing.T) {
	assertPropagationHistogramsMatch(t, nil)
	var samples []propagationSample
	values := []float64{math.Inf(-1), -1, math.Copysign(0, -1), 0, 0.00001, 61, math.Inf(1), math.NaN()}
	for _, bound := range propagationBounds {
		values = append(values, math.Nextafter(bound, math.Inf(-1)), bound, math.Nextafter(bound, math.Inf(1)))
	}
	for _, key := range []propagationSeriesKey{{}, {"agent-a", "topic-a"}, {"agent-a", "topic-b"}, {"agent-b", "topic-a"}} {
		for _, value := range values {
			samples = append(samples, propagationSample{key: key, seconds: value, nodeID: "receiver"})
		}
	}
	assertPropagationHistogramsMatch(t, samples)
	for seed := int64(0); seed < 8; seed++ {
		random := rand.New(rand.NewSource(seed))
		random.Shuffle(len(samples), func(i, j int) { samples[i], samples[j] = samples[j], samples[i] })
		assertPropagationHistogramsMatch(t, samples)
	}
}

func TestPropagationHistogramsPreserveSeriesAndWindowEvidence(t *testing.T) {
	random := rand.New(rand.NewSource(20261109))
	samples := make([]propagationSample, 20000)
	for index := range samples {
		samples[index] = propagationSample{
			key:     propagationSeriesKey{fmt.Sprintf("agent-%d", index%7), fmt.Sprintf("topic-%d", index%3)},
			seconds: random.ExpFloat64(), nodeID: fmt.Sprintf("receiver-%d", index%13),
		}
	}
	assertPropagationHistogramsMatch(t, samples)
	a := newLiveRunMetricAccumulator()
	events := windowEvents(windowStart("receiver", "receiver-session", 0, "topic"), windowDeliveredEvent("receiver", "receiver-session", 3, 12), windowEvent("receiver", "receiver-session", 4, "measurement_checkpoint", 22))
	for _, event := range events {
		a.observe(event)
	}
	for _, boundary := range []int{15, 30} {
		_, accepted, err := a.summarizeContext(t.Context(), "run", windowTestEpoch.Add(time.Duration(boundary)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		assertPropagationHistogramsMatch(t, accepted)
	}
	// Filling the missing source sequence changes receipt evidence; the cached
	// Prometheus histogram must still use the newly accepted exact sample set.
	a.observe(windowEvent("receiver", "receiver-session", 2, "rpc_metadata", 1))
	now := windowTestEpoch.Add(30 * time.Second)
	_, accepted, err := a.summarizeContext(t.Context(), "run", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 1 {
		t.Fatalf("late continuity recovery lost its receipt: %d samples", len(accepted))
	}
	_, cached := a.livePrometheusSummary("run", now)
	if !reflect.DeepEqual(cached, referencePropagationHistograms(accepted)) {
		t.Fatal("cached monitoring histogram changed after late evidence")
	}
}

var propagationHistogramBenchmarkResult map[propagationSeriesKey]*propagationHistogram

func BenchmarkPropagationHistograms(b *testing.B) {
	samples := make([]propagationSample, 50000)
	values := []float64{0.0001, 0.002, 0.009, 0.04, 0.08, 0.5, 3, 9, 40, 90}
	for index := range samples {
		samples[index] = propagationSample{key: propagationSeriesKey{fmt.Sprintf("agent-%d", index%4), fmt.Sprintf("topic-%d", index%3)}, seconds: values[index%len(values)]}
	}
	for _, implementation := range []struct {
		name  string
		build func([]propagationSample) map[propagationSeriesKey]*propagationHistogram
	}{{"per_bound_reference", referencePropagationHistograms}, {"prefix", propagationHistograms}} {
		b.Run(implementation.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				propagationHistogramBenchmarkResult = implementation.build(samples)
			}
		})
	}
}
