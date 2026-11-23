package model

import (
	"math"
	"testing"
)

func TestHopwaveConfigurationRequiresGossipSubAndPreservesFalseOverride(t *testing.T) {
	on, off := true, false
	base := NodeConfig{GossipSub: GossipSubConfig{Hopwave: &on}}.WithDefaults()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := base.Merge(NodeConfig{GossipSub: GossipSubConfig{Hopwave: &off}}).WithDefaults(); got.GossipSub.Hopwave == nil || *got.GossipSub.Hopwave {
		t.Fatal("explicit false did not override inherited Hopwave")
	}
	if got := (NodeConfig{}).WithDefaults(); got.GossipSub.Hopwave != nil && *got.GossipSub.Hopwave {
		t.Fatal("Hopwave enabled without opt-in")
	}
	for _, router := range []string{"floodsub", "randomsub"} {
		config := base
		config.GossipSub.Router = router
		if err := config.Validate(); err == nil {
			t.Fatalf("Hopwave accepted by %s", router)
		}
	}
}

func TestHopwaveRejectsUnsafeParametersAndPreservesZeroFactor(t *testing.T) {
	on, zero, interval := true, float64(0), 3
	config := NodeConfig{GossipSub: GossipSubConfig{Hopwave: &on, Params: GossipSubParamsConfig{HopwaveFactor: &zero, HopwaveInterval: &interval}}}.WithDefaults()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if *config.GossipSub.Params.HopwaveFactor != 0 {
		t.Fatal("explicit zero factor was replaced by defaults")
	}
	for _, factor := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		invalid := config
		invalid.GossipSub.Params.HopwaveFactor = &factor
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid factor %v accepted", factor)
		}
	}
	for _, interval := range []int{0, -1, math.MaxInt32 + 1} {
		invalid := config
		invalid.GossipSub.Params.HopwaveInterval = &interval
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid interval %d accepted", interval)
		}
	}
}

func TestHopwaveAdaptiveRangesAndWindow(t *testing.T) {
	factor := .7
	band := func(low int, high *int) HopwavePayloadRange {
		return HopwavePayloadRange{MinBytes: low, MaxBytes: high, Factor: &factor}
	}
	for _, tc := range []struct {
		name   string
		ranges []HopwavePayloadRange
		window string
		ok     bool
	}{
		{"size only", []HopwavePayloadRange{band(10240, nil), band(1024, intPointer(10240))}, "0s", true},
		{"retention only", nil, "60s", true},
		{"empty policy", nil, "", true},
		{"overlap", []HopwavePayloadRange{band(1024, nil), band(0, intPointer(1025))}, "", false},
		{"open overlaps next", []HopwavePayloadRange{band(0, nil), band(30, intPointer(40))}, "", false},
		{"empty range", []HopwavePayloadRange{band(10, intPointer(10))}, "", false},
		{"negative bytes", []HopwavePayloadRange{band(-1, intPointer(10))}, "", false},
		{"missing factor", []HopwavePayloadRange{{}}, "", false},
		{"malformed window", nil, "minute", false},
		{"other duration", nil, "10s", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := HopwaveAdaptiveConfig{PayloadRanges: tc.ranges, RetentionWindow: tc.window}
			var first int
			if len(tc.ranges) > 0 {
				first = tc.ranges[0].MinBytes
			}
			if err := c.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v", err)
			}
			if len(tc.ranges) > 0 && tc.ranges[0].MinBytes != first {
				t.Fatal("validation reordered the profile's ranges")
			}
		})
	}
	for _, factor := range []float64{-.1, 1.1, math.NaN(), math.Inf(1)} {
		config := HopwaveAdaptiveConfig{PayloadRanges: []HopwavePayloadRange{{Factor: &factor}}}
		if err := config.Validate(); err == nil {
			t.Fatalf("accepted invalid factor %v", factor)
		}
	}
}
