package model

import (
	"fmt"
	"math"
)

const (
	MaxTopologyNodes = 10_000
	MaxTopologyEdges = 1_000_000
)

// TopologyConfig describes a simple undirected graph over an ordered cohort.
// Pointers distinguish omitted parameters from explicit zero values.
type TopologyConfig struct {
	Model   string   `json:"model" yaml:"model"`
	Seed    *int64   `json:"seed,omitempty" yaml:"seed,omitempty"`
	P       *float64 `json:"p,omitempty" yaml:"p,omitempty"`
	K       *int     `json:"k,omitempty" yaml:"k,omitempty"`
	M       *int     `json:"m,omitempty" yaml:"m,omitempty"`
	Radius  *float64 `json:"radius,omitempty" yaml:"radius,omitempty"`
	Alpha   *float64 `json:"alpha,omitempty" yaml:"alpha,omitempty"`
	Beta    *float64 `json:"beta,omitempty" yaml:"beta,omitempty"`
	Rows    *int     `json:"rows,omitempty" yaml:"rows,omitempty"`
	Columns *int     `json:"columns,omitempty" yaml:"columns,omitempty"`
}

// Validate checks parameters and, when n is positive, cohort-dependent bounds.
// n == 0 is used during scenario validation before the live cohort is known.
func (c TopologyConfig) Validate(n int) error {
	if n < 0 || n > MaxTopologyNodes {
		return fmt.Errorf("topology node count must be between 1 and %d when specified", MaxTopologyNodes)
	}
	allowed := map[string]bool{}
	switch c.Model {
	case "er":
		allowed["p"] = true
	case "ws":
		allowed["p"], allowed["k"] = true, true
	case "ba":
		allowed["m"] = true
	case "rgg":
		allowed["radius"] = true
	case "waxman":
		allowed["alpha"], allowed["beta"] = true, true
	case "grid", "triangular":
		allowed["rows"], allowed["columns"] = true, true
	default:
		return fmt.Errorf("topology model must be er, ws, ba, rgg, waxman, grid, or triangular")
	}
	for _, field := range []struct {
		name    string
		present bool
	}{
		{"p", c.P != nil}, {"k", c.K != nil}, {"m", c.M != nil},
		{"radius", c.Radius != nil}, {"alpha", c.Alpha != nil}, {"beta", c.Beta != nil},
		{"rows", c.Rows != nil}, {"columns", c.Columns != nil},
	} {
		if field.present && !allowed[field.name] {
			return fmt.Errorf("topology model %s does not support %s", c.Model, field.name)
		}
		if !field.present && allowed[field.name] {
			return fmt.Errorf("topology model %s requires %s", c.Model, field.name)
		}
	}
	if c.P != nil && (!FiniteScore(*c.P) || *c.P < 0 || *c.P > 1) {
		return fmt.Errorf("topology p must be finite and in [0, 1]")
	}
	if c.K != nil && (*c.K < 2 || *c.K%2 != 0 || *c.K >= MaxTopologyNodes || n > 0 && *c.K >= n) {
		return fmt.Errorf("topology ws k must be even, at least 2, and smaller than the node count")
	}
	if c.M != nil && (*c.M < 1 || *c.M >= MaxTopologyNodes || n > 0 && *c.M >= n) {
		return fmt.Errorf("topology ba m must be at least 1 and smaller than the node count")
	}
	if c.Radius != nil && (!FiniteScore(*c.Radius) || *c.Radius < 0) {
		return fmt.Errorf("topology radius must be finite and non-negative")
	}
	for _, field := range []struct {
		name  string
		value *float64
	}{{"alpha", c.Alpha}, {"beta", c.Beta}} {
		if field.value != nil && (!FiniteScore(*field.value) || *field.value <= 0 || *field.value > 1) {
			return fmt.Errorf("topology %s must be finite and in (0, 1]", field.name)
		}
	}
	if c.Rows != nil {
		rows, columns := *c.Rows, *c.Columns
		if rows < 1 || columns < 1 || rows > MaxTopologyNodes || columns > MaxTopologyNodes || rows > MaxTopologyNodes/columns {
			return fmt.Errorf("topology rows and columns must be positive with product at most %d", MaxTopologyNodes)
		}
		if n > 0 && rows*columns != n {
			return fmt.Errorf("topology rows * columns must equal selected node count %d", n)
		}
	}
	var edges int64
	if n > 0 {
		switch c.Model {
		case "ws":
			edges = int64(n) * int64(*c.K) / 2
		case "ba":
			edges = int64(*c.M) * int64(n-*c.M)
		case "er":
			if *c.P == 1 {
				edges = int64(n) * int64(n-1) / 2
			}
		case "rgg":
			if *c.Radius >= math.Sqrt2 {
				edges = int64(n) * int64(n-1) / 2
			}
		}
	}
	if edges > MaxTopologyEdges {
		return fmt.Errorf("topology exceeds the limit of %d undirected edges", MaxTopologyEdges)
	}
	return nil
}
