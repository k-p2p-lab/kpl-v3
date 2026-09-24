package model

import (
	"encoding/json"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/distribution"
)

func TestNetworkConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    NetworkConfig
		wantError string
	}{
		{"empty", NetworkConfig{}, ""},
		{"all impairments", NetworkConfig{Delay: "100ms", Jitter: "10ms", LossPercent: float64Ptr(100), DuplicatePercent: float64Ptr(1), CorruptPercent: float64Ptr(0.1), ReorderPercent: float64Ptr(2), RateMbps: float64Ptr(10), QueueLimit: intPtr(1000)}, ""},
		{"explicit zeros", NetworkConfig{Delay: "0s", Jitter: "0s", LossPercent: float64Ptr(0), ReorderPercent: float64Ptr(0), RateMbps: float64Ptr(0)}, ""},
		{"invalid delay", NetworkConfig{Delay: "1s; rm -rf /"}, "delay"},
		{"negative delay", NetworkConfig{Delay: "-1ms"}, "delay"},
		{"invalid jitter", NetworkConfig{Delay: "1s", Jitter: "not-time"}, "jitter"},
		{"negative jitter", NetworkConfig{Delay: "1s", Jitter: "-1ns"}, "jitter"},
		{"maximum kernel jitter", NetworkConfig{Delay: "3s", Jitter: "2147483647ns"}, ""},
		{"jitter silently clamped by kernel", NetworkConfig{Delay: "3s", Jitter: "2147483648ns"}, "jitter must not exceed"},
		{"distributed delay with excessive jitter", NetworkConfig{Jitter: "3s", DelayDistribution: &distribution.Distribution{Model: "fixed", Value: "5s", Min: "1ms"}}, "jitter must not exceed"},
		{"jitter requires delay", NetworkConfig{Jitter: "1ms"}, "jitter requires"},
		{"reorder requires delay", NetworkConfig{ReorderPercent: float64Ptr(1)}, "reorderPercent requires"},
		{"loss out of range", NetworkConfig{LossPercent: float64Ptr(101)}, "lossPercent"},
		{"negative loss", NetworkConfig{LossPercent: float64Ptr(-1)}, "lossPercent"},
		{"duplicate nan", NetworkConfig{DuplicatePercent: float64Ptr(math.NaN())}, "duplicatePercent"},
		{"corrupt inf", NetworkConfig{CorruptPercent: float64Ptr(math.Inf(1))}, "corruptPercent"},
		{"reorder out of range", NetworkConfig{ReorderPercent: float64Ptr(101)}, "reorderPercent"},
		{"negative rate", NetworkConfig{RateMbps: float64Ptr(-1)}, "rateMbps"},
		{"infinite rate", NetworkConfig{RateMbps: float64Ptr(math.Inf(1))}, "rateMbps"},
		{"nan rate", NetworkConfig{RateMbps: float64Ptr(math.NaN())}, "rateMbps"},
		{"zero queue", NetworkConfig{QueueLimit: intPtr(0)}, "queueLimit"},
		{"negative queue", NetworkConfig{QueueLimit: intPtr(-1)}, "queueLimit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Validate() = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestNetworkConfigEnabled(t *testing.T) {
	for _, test := range []struct {
		config  NetworkConfig
		enabled bool
	}{
		{NetworkConfig{}, false},
		{NetworkConfig{Delay: "0s", Jitter: "0s", LossPercent: float64Ptr(0), DuplicatePercent: float64Ptr(0), CorruptPercent: float64Ptr(0), ReorderPercent: float64Ptr(0), RateMbps: float64Ptr(0)}, false},
		{NetworkConfig{Delay: "1ns"}, true},
		{NetworkConfig{LossPercent: float64Ptr(100)}, true},
		{NetworkConfig{DuplicatePercent: float64Ptr(1)}, true},
		{NetworkConfig{CorruptPercent: float64Ptr(1)}, true},
		{NetworkConfig{Delay: "1ms", ReorderPercent: float64Ptr(1)}, true},
		{NetworkConfig{RateMbps: float64Ptr(1)}, true},
		{NetworkConfig{QueueLimit: intPtr(1)}, true},
	} {
		if actual := test.config.Enabled(); actual != test.enabled {
			t.Errorf("Enabled(%+v) = %v, want %v", test.config, actual, test.enabled)
		}
	}
}

func TestNetworkOverridesPreserveExplicitZeroAndJSON(t *testing.T) {
	base := NodeConfig{Network: NetworkConfig{Delay: "100ms", Jitter: "10ms", LossPercent: float64Ptr(5), DuplicatePercent: float64Ptr(2), RateMbps: float64Ptr(10)}}
	merged := base.Merge(NodeConfig{Network: NetworkConfig{Delay: "0s", Jitter: "0s", LossPercent: float64Ptr(0), RateMbps: float64Ptr(0)}}).WithDefaults()
	if merged.Network.Delay != "0s" || merged.Network.Jitter != "0s" || *merged.Network.LossPercent != 0 || *merged.Network.RateMbps != 0 || *merged.Network.DuplicatePercent != 2 {
		t.Fatalf("incorrect network overlay: %+v", merged.Network)
	}
	if base.Network.Delay != "100ms" || *base.Network.LossPercent != 5 {
		t.Fatal("merge modified the base profile")
	}
	data, err := json.Marshal(merged)
	if err != nil {
		t.Fatal(err)
	}
	var restored NodeConfig
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Network.LossPercent == nil || *restored.Network.LossPercent != 0 || restored.Network.RateMbps == nil || *restored.Network.RateMbps != 0 {
		t.Fatalf("network zero overrides were lost over the agent JSON API: %s", data)
	}
}

func TestNodeValidationIncludesNetwork(t *testing.T) {
	if err := (NodeConfig{Network: NetworkConfig{LossPercent: float64Ptr(101)}}).Validate(); err == nil || !strings.Contains(err.Error(), "network: lossPercent") {
		t.Fatalf("NodeConfig.Validate() = %v", err)
	}
}

func TestNetworkReproductionValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		config NetworkConfig
		valid  bool
	}{
		{"whole interface", NetworkConfig{Scope: "all", LossPercent: float64Ptr(1)}, true},
		{"bad scope", NetworkConfig{Scope: "host"}, false},
		{"uniform jitter", NetworkConfig{Delay: "50ms", Jitter: "5ms", JitterDistribution: "uniform"}, true},
		{"unknown jitter model", NetworkConfig{JitterDistribution: "exponential"}, false},
		{"correlated reordering", NetworkConfig{Delay: "10ms", ReorderPercent: float64Ptr(2), ReorderCorrelationPercent: float64Ptr(50)}, true},
		{"orphan correlation", NetworkConfig{ReorderCorrelationPercent: float64Ptr(1)}, false},
		{"invalid correlation", NetworkConfig{Delay: "10ms", ReorderPercent: float64Ptr(2), ReorderCorrelationPercent: float64Ptr(101)}, false},
		{"TBF", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "50ms"}}, true},
		{"two rate modes", NetworkConfig{RateMbps: float64Ptr(10), TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "50ms"}}, false},
		{"TBF missing burst", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, Latency: "50ms"}}, false},
		{"TBF infinite burst", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: math.Inf(1), Latency: "50ms"}}, false},
		{"TBF missing latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128}}, false},
		{"TBF sub-microsecond latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "999ns"}}, false},
		{"TBF minimum latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "1us"}}, true},
		{"TBF fractional microsecond latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "1.5us"}}, true},
		{"TBF maximum latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "4294967295us"}}, true},
		{"TBF overflowing latency", NetworkConfig{TBF: &TBFConfig{RateMbps: 10, BurstKbit: 128, Latency: "4294967295001ns"}}, false},
		{"zero delay sample", NetworkConfig{DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "-1s", Sigma: "0s"}}, true},
		{"two delay modes", NetworkConfig{Delay: "0s", DelayDistribution: &distribution.Distribution{Model: "fixed", Value: "10ms"}}, false},
		{"distribution jitter without minimum", NetworkConfig{Jitter: "1ms", DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "0s", Sigma: "5ms"}}, false},
		{"distribution reorder without minimum", NetworkConfig{ReorderPercent: float64Ptr(1), DelayDistribution: &distribution.Distribution{Model: "exponential", Mean: "10ms"}}, false},
		{"bounded distribution jitter", NetworkConfig{Jitter: "1ms", DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "0s", Sigma: "5ms", Min: "1ms"}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, valid=%v", err, test.valid)
			}
		})
	}
}

func TestNetworkResolveProducesReproduciblePerPeerDelays(t *testing.T) {
	profile := NetworkConfig{Jitter: "1ms", ReorderPercent: float64Ptr(2), DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "0s", Sigma: "10ms", Min: "1ms"}}
	left, right := rand.New(rand.NewSource(99)), rand.New(rand.NewSource(99))
	seen := make(map[string]bool)
	for i := 0; i < 40; i++ {
		a, err := profile.Resolve(left)
		if err != nil {
			t.Fatal(err)
		}
		b, err := profile.Resolve(right)
		if err != nil {
			t.Fatal(err)
		}
		if a.Delay != b.Delay || a.DelayDistribution != nil {
			t.Fatalf("resolved configurations differ or remain unresolved: %+v / %+v", a, b)
		}
		if err := a.Validate(); err != nil {
			t.Fatalf("resolved sample must remain valid: %v", err)
		}
		seen[a.Delay] = true
	}
	if len(seen) < 2 {
		t.Fatal("per-peer delays are identical")
	}
	if profile.Delay != "" || profile.DelayDistribution == nil {
		t.Fatal("Resolve changed source profile")
	}
	zero, err := (NetworkConfig{DelayDistribution: &distribution.Distribution{Model: "normal", Mean: "-1s", Sigma: "0s"}}).Resolve(rand.New(rand.NewSource(1)))
	if err != nil || zero.Delay != "0s" || zero.Enabled() {
		t.Fatalf("zero sample = %+v, %v", zero, err)
	}
}

func TestNetworkMergeReplacesDelayAndRateAlternatives(t *testing.T) {
	base := NodeConfig{Network: NetworkConfig{Delay: "10ms", RateMbps: float64Ptr(5)}}
	dist := &distribution.Distribution{Model: "fixed", Value: "30ms"}
	tbf := &TBFConfig{RateMbps: 20, BurstKbit: 256, Latency: "10ms"}
	merged := base.Merge(NodeConfig{Network: NetworkConfig{DelayDistribution: dist, TBF: tbf}})
	if merged.Network.Delay != "" || merged.Network.RateMbps != nil || merged.Network.DelayDistribution != dist || merged.Network.TBF != tbf {
		t.Fatalf("alternatives were not replaced: %+v", merged.Network)
	}
	if err := merged.Network.Validate(); err != nil {
		t.Fatal(err)
	}
	disabled := merged.Merge(NodeConfig{Network: NetworkConfig{Delay: "0s", RateMbps: float64Ptr(0)}})
	if disabled.Network.DelayDistribution != nil || disabled.Network.TBF != nil || disabled.Network.Enabled() {
		t.Fatalf("explicit zero did not replace and disable inherited modes: %+v", disabled.Network)
	}
	if base.Network.Delay != "10ms" || *base.Network.RateMbps != 5 {
		t.Fatal("merge changed base")
	}
	invalid := base.Merge(NodeConfig{Network: NetworkConfig{Delay: "1s", DelayDistribution: dist}})
	if err := invalid.Network.Validate(); err == nil {
		t.Fatal("explicit conflicting overlay was silently normalized")
	}
}
