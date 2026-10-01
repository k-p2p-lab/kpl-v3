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
