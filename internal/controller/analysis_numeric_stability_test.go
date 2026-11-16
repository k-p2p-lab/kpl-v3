package controller

import (
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func exactFiniteMean(values []float64) float64 {
	total := new(big.Rat)
	count := int64(0)
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		total.Add(total, new(big.Rat).SetFloat64(value))
		count++
	}
	total.Quo(total, new(big.Rat).SetInt64(count))
	value, _ := total.Float64()
	return value
}

func TestAnalysisStatsCompensatesSignedScoreCancellation(t *testing.T) {
	cases := [][]float64{
		{-1e16, 1, 1e16},
		{-1e16, -1e16, 1, 1, 1e16, 1e16},
		{math.NaN(), math.Inf(1), -1e16, 1, 1e16},
	}
	for _, values := range cases {
		want := exactFiniteMean(values)
		got := analysisStats(values)
		if got.Average == nil || math.Abs(*got.Average-want) > 1e-14 {
			t.Fatalf("finite signed scores lost their mean: values=%v got=%v want=%.17g", values, got.Average, want)
		}
	}
	for exponent := -1000; exponent <= 1000; exponent += 100 {
		factor := math.Ldexp(1, exponent)
		values := []float64{-8 * factor, factor, 8 * factor}
		want := exactFiniteMean(values)
		got := analysisStats(values)
		if got.Average == nil || math.Abs((*got.Average-want)/want) > 1e-14 {
			t.Fatalf("score units changed the mean at exponent %d: got=%v want=%g", exponent, got.Average, want)
		}
	}
}

func TestAnalysisStatsMedianPreservesRepresentableTinyValues(t *testing.T) {
	for _, value := range []float64{math.SmallestNonzeroFloat64, 3 * math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64} {
		got := analysisStats([]float64{value, value})
		if got.Median == nil || *got.Median != value || got.Average == nil || *got.Average != value || got.Deviation == nil || *got.Deviation != 0 {
			t.Fatalf("constant finite sample changed: value=%g result=%+v", value, got)
		}
	}
	got := analysisStats([]float64{-math.MaxFloat64, math.MaxFloat64})
	if got.Median == nil || *got.Median != 0 || got.Average == nil || *got.Average != 0 {
		t.Fatalf("opposite finite extrema lost their center: %+v", got)
	}
}

func TestAnalysisStatsMatchesExactMeanAcrossInputPermutations(t *testing.T) {
	rng := rand.New(rand.NewSource(20261116))
	for index := 0; index < 200; index++ {
		magnitude := math.Ldexp(1, 40+rng.Intn(20))
		residual := float64(1 + rng.Intn(1000))
		values := []float64{-magnitude, residual, magnitude}
		original := append([]float64(nil), values...)
		want := exactFiniteMean(values)
		for permutation := 0; permutation < 6; permutation++ {
			rng.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
			before := append([]float64(nil), values...)
			got := batchStats(values)
			if got.Average == nil || math.Abs((*got.Average-want)/want) > 1e-14 || got.Count != len(original) || !reflect.DeepEqual(values, before) {
				t.Fatalf("permutation changed the mean or source: index=%d input=%v got=%+v want=%g", index, before, got, want)
			}
		}
	}
}

func TestAnalysisStatsPopulationDeviationStaysWithinFiniteSampleBound(t *testing.T) {
	// For a balanced pair of +/-MaxFloat64, the exact population SD is
	// MaxFloat64. Accumulated normalized variance can round just above one.
	for count := 2; count <= 1024; count += 2 {
		values := make([]float64, count)
		for i := range values {
			values[i] = math.MaxFloat64
			if i%2 == 0 {
				values[i] = -values[i]
			}
		}
		got := analysisStats(values)
		if got.Count != count || got.Average == nil || *got.Average != 0 || got.Deviation == nil || *got.Deviation > math.MaxFloat64 || *got.Deviation < math.MaxFloat64*(1-1e-13) {
			t.Fatalf("finite population deviation lost its sample bound: count=%d result=%+v", count, got)
		}
	}
	// The population bound does not cap the separate sample SD used between
	// Runs: sqrt(2) for [-1, 1] remains larger than the sample magnitude.
	sample := batchStats([]float64{-1, 1})
	if sample.Deviation == nil || *sample.Deviation != math.Sqrt2 {
		t.Fatalf("population bound changed sample SD: %+v", sample)
	}
}

func TestAnalysisObservationScoreMeanPreservesSignedResidual(t *testing.T) {
	now := time.Now().UTC()
	agents := map[string]model.Agent{"agent": {ID: "agent", State: model.AgentOnline, LastSeen: now}}
	rng := rand.New(rand.NewSource(20261116))
	for permutation := 0; permutation < 100; permutation++ {
		values := []float64{-1e16, 1, 1e16}
		rng.Shuffle(len(values), func(i, j int) { values[i], values[j] = values[j], values[i] })
		for _, oneNode := range []bool{false, true} {
			var nodes []model.Node
			for i, score := range values {
				id := string(rune('a' + i))
				if oneNode && len(nodes) > 0 {
					nodes[0].PeerScores[id] = score
				} else {
					nodes = append(nodes, model.Node{ID: id, PeerID: id, AgentID: "agent", Group: "scored", State: model.NodeReady, LastSeen: now, PeerScores: map[string]float64{id: score}})
				}
			}
			observation := makeAnalysisObservation("run", now, nodes, agents)
			for _, group := range observation.Groups {
				if group.ScoreCount != 3 || group.ScoreMean == nil || math.Abs(*group.ScoreMean-1.0/3) > 1e-14 {
					t.Fatalf("observation order changed the score mean: permutation=%d oneNode=%t group=%q mean=%v count=%d", permutation, oneNode, group.Group, group.ScoreMean, group.ScoreCount)
				}
			}
		}
	}
}
