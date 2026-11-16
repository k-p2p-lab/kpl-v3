package controller

import (
	"encoding/binary"
	"math"
	"math/big"
	"sort"
	"testing"
)

// Use a common binary unit and balanced, larger sentinel values. The exact
// rational oracle covers cancellation without claiming arbitrary exponent-span
// precision, and the sentinels ensure no sample is omitted from the mean.
func FuzzAnalysisStatsBinaryUnitInvariants(f *testing.F) {
	f.Add([]byte{0, 0})
	f.Add([]byte{1, 0, 1, 0, 255, 255})
	f.Add([]byte{194, 1, 255, 127, 0, 128, 1, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) < 2 {
			return
		}
		if len(input) > 258 {
			input = input[:258]
		}
		exponent := int(int16(binary.LittleEndian.Uint16(input))) % 451
		unit := math.Ldexp(1, exponent)
		values := []float64{-65536 * unit, 65536 * unit}
		for at := 2; at+1 < len(input); at += 2 {
			values = append(values, float64(int16(binary.LittleEndian.Uint16(input[at:])))*unit)
		}
		before := append([]float64(nil), values...)
		stat := analysisStats(values)
		want := exactFiniteMean(values)
		if stat.Count != len(values) || stat.Average == nil || *stat.Average != want {
			t.Fatalf("binary-unit mean disagrees with exact rational result: unit=%g values=%v mean=%v want=%g count=%d", unit, values, stat.Average, want, stat.Count)
		}
		for i, value := range values {
			if math.Float64bits(value) != math.Float64bits(before[i]) {
				t.Fatal("statistics mutated source samples")
			}
		}
		sort.Float64s(before)
		middle := new(big.Rat).SetFloat64(before[len(before)/2])
		if len(before)%2 == 0 {
			middle.Add(middle, new(big.Rat).SetFloat64(before[len(before)/2-1]))
			middle.Quo(middle, big.NewRat(2, 1))
		}
		median, _ := middle.Float64()
		if stat.Median == nil || *stat.Median != median || stat.Deviation == nil || *stat.Deviation < 0 || math.IsInf(*stat.Deviation, 0) || math.IsNaN(*stat.Deviation) {
			t.Fatalf("invalid finite median/deviation: result=%+v median=%g", stat, median)
		}
	})
}
