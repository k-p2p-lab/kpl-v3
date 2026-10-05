package model

import (
	"math"
	"testing"
)

func topologyPtr[T any](v T) *T { return &v }

func TestTopologyValidationStrictParameters(t *testing.T) {
	cases := []struct {
		name string
		c    TopologyConfig
		n    int
	}{
		{"unknown model", TopologyConfig{Model: "ER", P: topologyPtr(0.1)}, 10},
		{"missing probability", TopologyConfig{Model: "er"}, 10},
		{"irrelevant explicit zero", TopologyConfig{Model: "er", P: topologyPtr(0.1), Radius: topologyPtr(0.0)}, 10},
		{"probability negative", TopologyConfig{Model: "er", P: topologyPtr(-0.1)}, 10},
		{"probability too high", TopologyConfig{Model: "er", P: topologyPtr(1.1)}, 10},
		{"probability nan", TopologyConfig{Model: "er", P: topologyPtr(math.NaN())}, 10},
		{"ws missing k", TopologyConfig{Model: "ws", P: topologyPtr(0.1)}, 10},
		{"ws odd k", TopologyConfig{Model: "ws", P: topologyPtr(0.1), K: topologyPtr(3)}, 10},
		{"ws zero k", TopologyConfig{Model: "ws", P: topologyPtr(0.1), K: topologyPtr(0)}, 10},
		{"ws k reaches count", TopologyConfig{Model: "ws", P: topologyPtr(0.1), K: topologyPtr(10)}, 10},
		{"ws excessive edges", TopologyConfig{Model: "ws", P: topologyPtr(0.1), K: topologyPtr(202)}, 10_000},
		{"ba missing m", TopologyConfig{Model: "ba"}, 10},
		{"ba zero m", TopologyConfig{Model: "ba", M: topologyPtr(0)}, 10},
		{"ba m reaches count", TopologyConfig{Model: "ba", M: topologyPtr(10)}, 10},
		{"ba excessive edges", TopologyConfig{Model: "ba", M: topologyPtr(5000)}, 10_000},
		{"rgg missing radius", TopologyConfig{Model: "rgg"}, 10},
		{"rgg negative radius", TopologyConfig{Model: "rgg", Radius: topologyPtr(-1.0)}, 10},
		{"rgg infinite radius", TopologyConfig{Model: "rgg", Radius: topologyPtr(math.Inf(1))}, 10},
		{"rgg excessive complete graph", TopologyConfig{Model: "rgg", Radius: topologyPtr(math.Sqrt2)}, 10_000},
		{"waxman missing beta", TopologyConfig{Model: "waxman", Alpha: topologyPtr(0.5)}, 10},
		{"waxman zero alpha", TopologyConfig{Model: "waxman", Alpha: topologyPtr(0.0), Beta: topologyPtr(1.0)}, 10},
		{"waxman invalid beta", TopologyConfig{Model: "waxman", Alpha: topologyPtr(1.0), Beta: topologyPtr(1.1)}, 10},
		{"waxman nan alpha", TopologyConfig{Model: "waxman", Alpha: topologyPtr(math.NaN()), Beta: topologyPtr(1.0)}, 10},
		{"grid missing dimension", TopologyConfig{Model: "grid", Rows: topologyPtr(2)}, 10},
		{"grid zero dimension", TopologyConfig{Model: "grid", Rows: topologyPtr(2), Columns: topologyPtr(0)}, 10},
		{"grid wrong product", TopologyConfig{Model: "grid", Rows: topologyPtr(2), Columns: topologyPtr(4)}, 10},
		{"grid large product", TopologyConfig{Model: "grid", Rows: topologyPtr(10_000), Columns: topologyPtr(10_000)}, 0},
		{"grid overflow dimensions", TopologyConfig{Model: "grid", Rows: topologyPtr(math.MaxInt), Columns: topologyPtr(math.MaxInt)}, 0},
		{"triangle wrong product", TopologyConfig{Model: "triangular", Rows: topologyPtr(2), Columns: topologyPtr(4)}, 10},
		{"er excessive complete graph", TopologyConfig{Model: "er", P: topologyPtr(1.0)}, 10_000},
		{"negative count", TopologyConfig{Model: "er", P: topologyPtr(0.0)}, -1},
		{"excessive count", TopologyConfig{Model: "er", P: topologyPtr(0.0)}, 10_001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.c.Validate(tc.n); err == nil {
				t.Fatalf("invalid topology accepted: %+v, n=%d", tc.c, tc.n)
			}
		})
	}
}

func TestTopologyValidationDeferredCohort(t *testing.T) {
	for _, config := range []TopologyConfig{
		{Model: "er", P: topologyPtr(0.0)},
		{Model: "ws", P: topologyPtr(0.0), K: topologyPtr(4)},
		{Model: "ba", M: topologyPtr(2)},
		{Model: "rgg", Radius: topologyPtr(0.0)},
		{Model: "waxman", Alpha: topologyPtr(1.0), Beta: topologyPtr(1.0)},
		{Model: "grid", Rows: topologyPtr(2), Columns: topologyPtr(3)},
		{Model: "triangular", Rows: topologyPtr(2), Columns: topologyPtr(3)},
	} {
		if err := config.Validate(0); err != nil {
			t.Fatalf("%s requires an unnecessary static cohort count: %v", config.Model, err)
		}
		if err := config.Validate(6); err != nil {
			t.Fatalf("%s rejected valid live cohort: %v", config.Model, err)
		}
	}
}
