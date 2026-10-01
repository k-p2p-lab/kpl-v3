package model

import (
	"math"
	"testing"
)

func TestHopWaveConfigurationRequiresGossipSubAndPreservesFalseOverride(t *testing.T) {
	on, off := true, false
	base := NodeConfig{GossipSub: GossipSubConfig{HopWave: &on}}.WithDefaults()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := base.Merge(NodeConfig{GossipSub: GossipSubConfig{HopWave: &off}}).WithDefaults(); got.GossipSub.HopWave == nil || *got.GossipSub.HopWave {
		t.Fatal("explicit false did not override inherited HopWave")
	}
	if got := (NodeConfig{}).WithDefaults(); got.GossipSub.HopWave != nil && *got.GossipSub.HopWave {
		t.Fatal("HopWave enabled without opt-in")
	}
	for _, router := range []string{"floodsub", "randomsub"} {
		config := base
		config.GossipSub.Router = router
		if err := config.Validate(); err == nil {
			t.Fatalf("HopWave accepted by %s", router)
		}
	}
}

func TestHopWaveRejectsUnsafeParametersAndPreservesZeroFactor(t *testing.T) {
	on, zero, interval := true, float64(0), 3
	config := NodeConfig{GossipSub: GossipSubConfig{HopWave: &on, Params: GossipSubParamsConfig{HopWaveFactor: &zero, HopWaveInterval: &interval}}}.WithDefaults()
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if *config.GossipSub.Params.HopWaveFactor != 0 {
		t.Fatal("explicit zero factor was replaced by defaults")
	}
	for _, factor := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		invalid := config
		invalid.GossipSub.Params.HopWaveFactor = &factor
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid factor %v accepted", factor)
		}
	}
	for _, interval := range []int{0, -1, math.MaxInt32 + 1} {
		invalid := config
		invalid.GossipSub.Params.HopWaveInterval = &interval
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid interval %d accepted", interval)
		}
	}
}
