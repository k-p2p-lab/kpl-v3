package model

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/distribution"
)

// NetworkConfig configures Linux netem on a peer container's outbound traffic.
// Scope defaults to p2p; all also shapes control and telemetry traffic. Delay
// and jitter are Go durations; jitter defaults to a normal distribution.
// Pointer fields preserve explicit zero overrides when merging node profiles.
// A delay of "0s" and percentages/rate of zero disable the respective setting.
type NetworkConfig struct {
	Scope                     string                     `json:"scope,omitempty" yaml:"scope,omitempty"`
	Delay                     string                     `json:"delay,omitempty" yaml:"delay,omitempty"`
	DelayDistribution         *distribution.Distribution `json:"delayDistribution,omitempty" yaml:"delayDistribution,omitempty"`
	Jitter                    string                     `json:"jitter,omitempty" yaml:"jitter,omitempty"`
	JitterDistribution        string                     `json:"jitterDistribution,omitempty" yaml:"jitterDistribution,omitempty"`
	LossPercent               *float64                   `json:"lossPercent,omitempty" yaml:"lossPercent,omitempty"`
	DuplicatePercent          *float64                   `json:"duplicatePercent,omitempty" yaml:"duplicatePercent,omitempty"`
	CorruptPercent            *float64                   `json:"corruptPercent,omitempty" yaml:"corruptPercent,omitempty"`
	ReorderPercent            *float64                   `json:"reorderPercent,omitempty" yaml:"reorderPercent,omitempty"`
	ReorderCorrelationPercent *float64                   `json:"reorderCorrelationPercent,omitempty" yaml:"reorderCorrelationPercent,omitempty"`
	RateMbps                  *float64                   `json:"rateMbps,omitempty" yaml:"rateMbps,omitempty"`
	TBF                       *TBFConfig                 `json:"tbf,omitempty" yaml:"tbf,omitempty"`
	QueueLimit                *int                       `json:"queueLimit,omitempty" yaml:"queueLimit,omitempty"`
}

// TBFConfig is a token bucket shaper attached beneath netem. BurstKbit is a
// token bucket size in kilobits; Latency bounds time waiting for tokens. TBF
// accepts 1..4294967295 microseconds; fractional microseconds round up. It is
// distinct from netem's per-packet serialization delay configured by RateMbps.
type TBFConfig struct {
	RateMbps  float64 `json:"rateMbps" yaml:"rateMbps"`
	BurstKbit float64 `json:"burstKbit" yaml:"burstKbit"`
	Latency   string  `json:"latency" yaml:"latency"`
}

// Validate rejects unsupported values before a container or qdisc is created.
func (c NetworkConfig) Validate() error {
	if c.Scope != "" && c.Scope != "p2p" && c.Scope != "all" {
		return fmt.Errorf("scope must be p2p or all")
	}
	if c.JitterDistribution != "" && c.JitterDistribution != "uniform" && c.JitterDistribution != "normal" && c.JitterDistribution != "pareto" && c.JitterDistribution != "paretonormal" {
		return fmt.Errorf("jitterDistribution must be uniform, normal, pareto, or paretonormal")
	}
	delay, err := networkDuration("delay", c.Delay)
	if err != nil {
		return err
	}
	jitter, err := networkDuration("jitter", c.Jitter)
	if err != nil {
		return err
	}
	// Linux netem's tabledist() uses a signed 32-bit nanosecond jitter.
	// netem_change() silently clamps larger values even when tc accepts them.
	// Reject them here so the effective impairment matches experiment metadata.
	if jitter > time.Duration(math.MaxInt32) {
		return fmt.Errorf("jitter must not exceed %dns (Linux netem limit)", int64(math.MaxInt32))
	}
	if c.DelayDistribution != nil {
		if c.Delay != "" {
			return fmt.Errorf("delay and delayDistribution are mutually exclusive")
		}
		if err := c.DelayDistribution.Validate(false); err != nil {
			return fmt.Errorf("delayDistribution: %w", err)
		}
		if jitter > 0 || c.ReorderPercent != nil && *c.ReorderPercent > 0 {
			minimum, _ := time.ParseDuration(c.DelayDistribution.Min)
			if minimum <= 0 {
				return fmt.Errorf("delayDistribution requires a positive min when jitter or reorderPercent is enabled")
			}
		}
	}
	if jitter > 0 && delay == 0 && c.DelayDistribution == nil {
		return fmt.Errorf("jitter requires a positive delay")
	}
	for _, field := range []struct {
		name  string
		value *float64
	}{
		{"lossPercent", c.LossPercent},
		{"duplicatePercent", c.DuplicatePercent},
		{"corruptPercent", c.CorruptPercent},
		{"reorderPercent", c.ReorderPercent},
		{"reorderCorrelationPercent", c.ReorderCorrelationPercent},
	} {
		if field.value != nil && (math.IsNaN(*field.value) || math.IsInf(*field.value, 0) || *field.value < 0 || *field.value > 100) {
			return fmt.Errorf("%s must be finite and between 0 and 100", field.name)
		}
	}
	if c.ReorderPercent != nil && *c.ReorderPercent > 0 && delay == 0 && c.DelayDistribution == nil {
		return fmt.Errorf("reorderPercent requires a positive delay")
	}
	if c.ReorderCorrelationPercent != nil && *c.ReorderCorrelationPercent > 0 && (c.ReorderPercent == nil || *c.ReorderPercent <= 0) {
		return fmt.Errorf("reorderCorrelationPercent requires a positive reorderPercent")
	}
	if c.RateMbps != nil && (math.IsNaN(*c.RateMbps) || math.IsInf(*c.RateMbps, 0) || *c.RateMbps < 0) {
		return fmt.Errorf("rateMbps must be finite and non-negative (0 disables the limit)")
	}
	if c.TBF != nil {
		if c.RateMbps != nil && *c.RateMbps > 0 {
			return fmt.Errorf("tbf and positive rateMbps are mutually exclusive")
		}
		if !networkPositiveFinite(c.TBF.RateMbps) || !networkPositiveFinite(c.TBF.BurstKbit) {
			return fmt.Errorf("tbf rateMbps and burstKbit must be positive and finite")
		}
		latency, err := networkDuration("tbf latency", c.TBF.Latency)
		if err != nil {
			return err
		}
		if latency < time.Microsecond || latency > time.Duration(math.MaxUint32)*time.Microsecond {
			return fmt.Errorf("tbf latency must be between 1us and %dus", uint64(math.MaxUint32))
		}
	}
	if c.QueueLimit != nil && (*c.QueueLimit <= 0 || uint64(*c.QueueLimit) > math.MaxUint32) {
		return fmt.Errorf("queueLimit must be between 1 and %d packets", uint64(math.MaxUint32))
	}
	return nil
}

func networkPositiveFinite(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

// Resolve samples a per-peer base delay and returns the concrete configuration
// sent to the Agent. Sampling consumes only the supplied RNG. The original
// profile remains unchanged; callers can keep it alongside the effective config
// for experiment provenance. Zero samples are valid when jitter/reorder are off.
func (c NetworkConfig) Resolve(rng *rand.Rand) (NetworkConfig, error) {
	if err := c.Validate(); err != nil {
		return c, err
	}
	if c.DelayDistribution == nil {
		return c, nil
	}
	if rng == nil {
		return c, fmt.Errorf("delayDistribution requires a random source")
	}
	c.Delay = c.DelayDistribution.Sample(rng).String()
	c.DelayDistribution = nil
	return c, c.Validate()
}

// Enabled reports whether at least one impairment is configured. Call Validate
// first when accepting user input: this method does not report invalid values.
func (c NetworkConfig) Enabled() bool {
	delay, _ := time.ParseDuration(c.Delay)
	jitter, _ := time.ParseDuration(c.Jitter)
	if delay != 0 || jitter != 0 || c.QueueLimit != nil || c.DelayDistribution != nil || c.TBF != nil {
		return true
	}
	for _, value := range []*float64{c.LossPercent, c.DuplicatePercent, c.CorruptPercent, c.ReorderPercent, c.RateMbps} {
		if value != nil && *value != 0 {
			return true
		}
	}
	return false
}

func networkDuration(name, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("%s must be a non-negative Go duration, got %q", name, value)
	}
	return duration, nil
}
