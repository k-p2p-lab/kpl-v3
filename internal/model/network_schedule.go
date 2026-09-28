package model

import (
	"fmt"
	"math/rand"
	"time"
)

// NetworkSchedule changes the initial network configuration on one clock.
// Changes are cumulative patches; explicit zeros disable inherited settings.
type NetworkSchedule struct {
	Reference string          `json:"reference,omitempty" yaml:"reference,omitempty"`
	Changes   []NetworkChange `json:"changes" yaml:"changes"`
}

type NetworkChange struct {
	After string        `json:"after" yaml:"after"`
	Set   NetworkConfig `json:"set" yaml:"set"`
}

func (s *NetworkSchedule) Clock() string {
	if s == nil || s.Reference == "" {
		return "peer-join"
	}
	return s.Reference
}

func (c NetworkConfig) Initial() NetworkConfig { c.Schedule = nil; return c }
func (c NetworkConfig) Scheduled() bool        { return c.Schedule != nil && len(c.Schedule.Changes) > 0 }

// Merge uses the same explicit-zero and mutually exclusive mode semantics as
// profile overlays. Schedule pointers replace, rather than append to, a plan.
func (c NetworkConfig) Merge(overlay NetworkConfig) NetworkConfig {
	return (NodeConfig{Network: c}).Merge(NodeConfig{Network: overlay}).Network
}

func (c NetworkConfig) validateSchedule() error {
	if c.Schedule == nil {
		return nil
	}
	if ref := c.Schedule.Clock(); ref != "peer-join" && ref != "experiment-start" {
		return fmt.Errorf("schedule.reference must be peer-join or experiment-start")
	}
	if len(c.Schedule.Changes) > 128 {
		return fmt.Errorf("schedule supports at most 128 changes")
	}
	current := c.Initial()
	var previous time.Duration
	for i, change := range c.Schedule.Changes {
		after, err := time.ParseDuration(change.After)
		if err != nil || after < 0 {
			return fmt.Errorf("schedule.changes[%d].after must be a non-negative Go duration", i)
		}
		if i > 0 && after <= previous {
			return fmt.Errorf("schedule.changes after times must be strictly increasing")
		}
		if change.Set.Schedule != nil {
			return fmt.Errorf("schedule.changes[%d].set cannot contain another schedule", i)
		}
		current = current.Merge(change.Set)
		if err := current.Validate(); err != nil {
			return fmt.Errorf("schedule.changes[%d].set: %w", i, err)
		}
		previous = after
	}
	return nil
}

func (c NetworkConfig) resolveSchedule(rng *rand.Rand) (NetworkConfig, error) {
	initial, err := c.Initial().Resolve(rng)
	if err != nil {
		return c, err
	}
	plan := &NetworkSchedule{Reference: c.Schedule.Clock(), Changes: make([]NetworkChange, len(c.Schedule.Changes))}
	current := initial
	for i, change := range c.Schedule.Changes {
		current, err = current.Merge(change.Set).Resolve(rng)
		if err != nil {
			return c, fmt.Errorf("schedule.changes[%d]: %w", i, err)
		}
		// Store complete concrete states so later patches never resample an inherited
		// distribution, and the Peer needs no RNG or shared Controller scheduling.
		plan.Changes[i] = NetworkChange{After: change.After, Set: current}
	}
	initial.Schedule = plan
	return initial, nil
}
