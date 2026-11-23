package model

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

type HopwaveAdaptiveConfig struct {
	PayloadRanges   []HopwavePayloadRange `json:"payloadRanges,omitzero" yaml:"payloadRanges,omitempty"`
	RetentionWindow string                `json:"retentionWindow,omitempty" yaml:"retentionWindow,omitempty"`
}

// MinBytes is inclusive; a nil MaxBytes extends the range indefinitely.
type HopwavePayloadRange struct {
	MinBytes int      `json:"minBytes" yaml:"minBytes"`
	MaxBytes *int     `json:"maxBytes,omitempty" yaml:"maxBytes,omitempty"`
	Factor   *float64 `json:"factor" yaml:"factor"`
}

func (c HopwaveAdaptiveConfig) WindowDuration() (time.Duration, error) {
	if c.RetentionWindow == "" {
		return 0, nil
	}
	window, err := time.ParseDuration(c.RetentionWindow)
	if err != nil || window != 0 && window != time.Minute {
		return 0, fmt.Errorf("hopwaveAdaptive.retentionWindow must be 0s or 60s")
	}
	return window, nil
}

func (c HopwaveAdaptiveConfig) Validate() error {
	if _, err := c.WindowDuration(); err != nil {
		return err
	}
	for i, r := range c.PayloadRanges {
		if r.MinBytes < 0 || r.MaxBytes != nil && *r.MaxBytes <= r.MinBytes {
			return fmt.Errorf("hopwaveAdaptive.payloadRanges[%d] requires nonnegative minBytes and a larger maxBytes when supplied", i)
		}
		if r.Factor == nil || !validNumber(*r.Factor) || *r.Factor < 0 || *r.Factor > 1 {
			return fmt.Errorf("hopwaveAdaptive.payloadRanges[%d].factor must be supplied, finite and in [0, 1]", i)
		}
	}
	ranges := slices.Clone(c.PayloadRanges)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].MinBytes < ranges[j].MinBytes })
	for i := 1; i < len(ranges); i++ {
		previousEnd := ranges[i-1].MaxBytes
		if previousEnd == nil || *previousEnd > ranges[i].MinBytes {
			return fmt.Errorf("hopwaveAdaptive.payloadRanges overlap at %d bytes", ranges[i].MinBytes)
		}
	}
	return nil
}
