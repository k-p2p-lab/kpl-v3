package model

import (
	"fmt"
	"reflect"
	"time"
)

// RuntimeProfilePatch contains only settings which can change without replacing
// a Peer. Omitted fields retain their current value, including explicit zeros.
type RuntimeProfilePatch struct {
	Sink         *ProbabilityAttack      `json:"sink,omitempty" yaml:"sink,omitempty"`
	Censorship   *ProbabilityAttack      `json:"censorship,omitempty" yaml:"censorship,omitempty"`
	MessageDelay *ProbabilityAttack      `json:"messageDelay,omitempty" yaml:"messageDelay,omitempty"`
	GossipSub    *RuntimeGossipSubConfig `json:"gossipsub,omitempty" yaml:"gossipsub,omitempty"`
	Network      *NetworkConfig          `json:"network,omitempty" yaml:"network,omitempty"`
}

type RuntimeGossipSubConfig struct {
	Params RuntimeGossipSubParams `json:"params" yaml:"params"`
}

type RuntimeGossipSubParams struct {
	D               *int     `json:"d,omitempty" yaml:"d,omitempty"`
	DLow            *int     `json:"dLow,omitempty" yaml:"dLow,omitempty"`
	DHigh           *int     `json:"dHigh,omitempty" yaml:"dHigh,omitempty"`
	DScore          *int     `json:"dScore,omitempty" yaml:"dScore,omitempty"`
	DOut            *int     `json:"dOut,omitempty" yaml:"dOut,omitempty"`
	DLazy           *int     `json:"dLazy,omitempty" yaml:"dLazy,omitempty"`
	GossipFactor    *float64 `json:"gossipFactor,omitempty" yaml:"gossipFactor,omitempty"`
	HopwaveFactor   *float64 `json:"hopwaveFactor,omitempty" yaml:"hopwaveFactor,omitempty"`
	HopwaveInterval *int     `json:"hopwaveInterval,omitempty" yaml:"hopwaveInterval,omitempty"`
}

type ProfileSchedule struct {
	Reference string          `json:"reference,omitempty" yaml:"reference,omitempty"`
	Changes   []ProfileChange `json:"changes" yaml:"changes"`
}

type ProfileChange struct {
	After string              `json:"after" yaml:"after"`
	Set   RuntimeProfilePatch `json:"set" yaml:"set"`
}

func (p RuntimeProfilePatch) HasAttacks() bool {
	return p.Sink != nil || p.Censorship != nil || p.MessageDelay != nil
}

func (p RuntimeProfilePatch) Validate() error {
	for name, attack := range map[string]*ProbabilityAttack{"sink": p.Sink, "censorship": p.Censorship, "messageDelay": p.MessageDelay} {
		if attack != nil {
			if attack.Enabled == nil && attack.Probability == nil {
				return fmt.Errorf("%s patch must not be empty", name)
			}
			if err := attack.Validate(); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if p.GossipSub == nil && p.Network == nil && !p.HasAttacks() {
		return fmt.Errorf("set requires gossipsub.params, network, or attack options")
	}
	if p.GossipSub != nil {
		g := p.GossipSub.Params
		if g == (RuntimeGossipSubParams{}) {
			return fmt.Errorf("gossipsub.params must not be empty")
		}
		for name, value := range map[string]*int{"d": g.D, "dLow": g.DLow, "dHigh": g.DHigh, "dScore": g.DScore, "dOut": g.DOut, "dLazy": g.DLazy, "hopwaveInterval": g.HopwaveInterval} {
			if value != nil && (*value < 0 || (name == "d" || name == "dHigh" || name == "hopwaveInterval") && *value == 0) {
				return fmt.Errorf("invalid gossipsub.params.%s", name)
			}
		}
		if g.HopwaveInterval != nil && *g.HopwaveInterval > 2147483647 {
			return fmt.Errorf("hopwaveInterval must not exceed 2147483647")
		}
		for name, value := range map[string]*float64{"gossipFactor": g.GossipFactor, "hopwaveFactor": g.HopwaveFactor} {
			if value != nil && (!validNumber(*value) || *value < 0 || *value > 1) {
				return fmt.Errorf("%s must be finite and in [0, 1]", name)
			}
		}
	}
	if p.Network != nil {
		if reflect.DeepEqual(*p.Network, NetworkConfig{}) {
			return fmt.Errorf("network must not be empty")
		}
		if p.Network.Schedule != nil || p.Network.DelayDistribution != nil {
			return fmt.Errorf("schedule action network patches cannot contain schedule or delayDistribution; use concrete conditions")
		}
		// Cross-field checks use the inherited configuration in Apply.
		base := NetworkConfig{Delay: "1ms"}.Merge(*p.Network)
		if p.Network.ReorderPercent == nil && p.Network.ReorderCorrelationPercent != nil {
			value := 1.0
			base.ReorderPercent = &value
		}
		if err := base.Validate(); err != nil {
			return fmt.Errorf("network: %w", err)
		}
	}
	return nil
}

func (p RuntimeProfilePatch) Apply(base NodeConfig) (NodeConfig, error) {
	if err := p.Validate(); err != nil {
		return base, err
	}
	base = base.WithDefaults()
	if p.HasAttacks() && (!*base.GossipSub.Enabled || base.GossipSub.Router != "gossipsub") {
		return base, fmt.Errorf("attack runtime changes require enabled GossipSub")
	}
	var overlay NodeConfig
	overlay.Sink = p.Sink
	if p.Censorship != nil {
		overlay.Censorship = &CensorshipConfig{ProbabilityAttack: *p.Censorship}
	}
	if p.MessageDelay != nil {
		overlay.MessageDelay = &MessageDelayConfig{ProbabilityAttack: *p.MessageDelay}
	}
	if p.GossipSub != nil {
		if !*base.GossipSub.Enabled || base.GossipSub.Router != "gossipsub" {
			return base, fmt.Errorf("gossipsub runtime changes require enabled GossipSub")
		}
		g := p.GossipSub.Params
		overlay.GossipSub.Params = GossipSubParamsConfig{D: g.D, DLow: g.DLow, DHigh: g.DHigh, DScore: g.DScore, DOut: g.DOut, DLazy: g.DLazy, GossipFactor: g.GossipFactor, HopwaveFactor: g.HopwaveFactor, HopwaveInterval: g.HopwaveInterval}
	}
	if p.Network != nil {
		if base.Network.Scheduled() {
			return base, fmt.Errorf("schedule action network changes cannot overlap network.schedule on the same Peer")
		}
		overlay.Network = *p.Network
	}
	updated := base.Merge(overlay).WithDefaults()
	return updated, updated.Validate()
}

func RuntimeProfileSnapshot(config NodeConfig) RuntimeProfilePatch {
	config = config.WithDefaults()
	g := config.GossipSub.Params
	network := config.Network.Initial()
	result := RuntimeProfilePatch{GossipSub: &RuntimeGossipSubConfig{Params: RuntimeGossipSubParams{D: g.D, DLow: g.DLow, DHigh: g.DHigh, DScore: g.DScore, DOut: g.DOut, DLazy: g.DLazy, GossipFactor: g.GossipFactor, HopwaveFactor: g.HopwaveFactor, HopwaveInterval: g.HopwaveInterval}}, Network: &network}
	if config.Sink != nil {
		value := *config.Sink
		result.Sink = &value
	}
	if config.Censorship != nil {
		value := config.Censorship.ProbabilityAttack
		result.Censorship = &value
	}
	if config.MessageDelay != nil {
		value := config.MessageDelay.ProbabilityAttack
		result.MessageDelay = &value
	}
	return result
}

func (p RuntimeProfilePatch) Snapshot(config NodeConfig) RuntimeProfilePatch {
	result := RuntimeProfileSnapshot(config)
	if p.Sink == nil {
		result.Sink = nil
	}
	if p.Censorship == nil {
		result.Censorship = nil
	}
	if p.MessageDelay == nil {
		result.MessageDelay = nil
	}
	if p.GossipSub == nil {
		result.GossipSub = nil
	}
	if p.Network == nil {
		result.Network = nil
	}
	return result
}

func (g RuntimeGossipSubParams) nodeParams() GossipSubParamsConfig {
	return GossipSubParamsConfig{D: g.D, DLow: g.DLow, DHigh: g.DHigh, DScore: g.DScore, DOut: g.DOut, DLazy: g.DLazy, GossipFactor: g.GossipFactor, HopwaveFactor: g.HopwaveFactor, HopwaveInterval: g.HopwaveInterval}
}

func ValidateRuntimeProfileSnapshot(p RuntimeProfilePatch) error {
	for name, attack := range map[string]*ProbabilityAttack{"sink": p.Sink, "censorship": p.Censorship, "messageDelay": p.MessageDelay} {
		if attack != nil {
			if attack.Enabled == nil || attack.Probability == nil {
				return fmt.Errorf("incomplete runtime %s snapshot", name)
			}
			if err := attack.Validate(); err != nil {
				return err
			}
		}
	}
	if p.GossipSub == nil && p.Network == nil && !p.HasAttacks() {
		return fmt.Errorf("empty runtime profile snapshot")
	}
	if p.GossipSub != nil {
		g := p.GossipSub.Params
		if g.D == nil || g.DLow == nil || g.DHigh == nil || g.DScore == nil || g.DOut == nil || g.DLazy == nil || g.GossipFactor == nil || g.HopwaveFactor == nil || g.HopwaveInterval == nil {
			return fmt.Errorf("incomplete runtime GossipSub snapshot")
		}
		if err := (NodeConfig{GossipSub: GossipSubConfig{Params: g.nodeParams()}}).Validate(); err != nil {
			return err
		}
	}
	if p.Network != nil {
		if p.Network.Schedule != nil || p.Network.DelayDistribution != nil {
			return fmt.Errorf("runtime network snapshot must be concrete")
		}
		if err := p.Network.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func ValidateProfileAcknowledgement(request ProfileUpdateRequest, response ProfileUpdateResponse) error {
	actual := response.Effective
	if (request.Set.GossipSub == nil) != (actual.GossipSub == nil) || (request.Set.Network == nil) != (actual.Network == nil) || (request.Set.Sink == nil) != (actual.Sink == nil) || (request.Set.Censorship == nil) != (actual.Censorship == nil) || (request.Set.MessageDelay == nil) != (actual.MessageDelay == nil) {
		return fmt.Errorf("profile acknowledgement fields do not match the request")
	}
	if err := ValidateRuntimeProfileSnapshot(actual); err != nil {
		return err
	}
	for _, pair := range [][2]*ProbabilityAttack{{request.Set.Sink, actual.Sink}, {request.Set.Censorship, actual.Censorship}, {request.Set.MessageDelay, actual.MessageDelay}} {
		if pair[0] == nil {
			continue
		}
		wanted, got := pair[0], pair[1]
		if wanted.Enabled != nil && *wanted.Enabled != *got.Enabled || wanted.Probability != nil && *wanted.Probability != *got.Probability {
			return fmt.Errorf("profile acknowledgement does not contain requested attack values")
		}
	}
	// Reconstruct only the ordinary runtime fields. Attack selector/queue settings
	// are construction-time values and cannot be reconstructed from this ACK.
	ordinary := request.Set
	ordinary.Sink = nil
	ordinary.Censorship = nil
	ordinary.MessageDelay = nil
	if ordinary.GossipSub == nil && ordinary.Network == nil {
		return nil
	}
	var base NodeConfig
	if actual.GossipSub != nil {
		base.GossipSub.Params = actual.GossipSub.Params.nodeParams()
	}
	if actual.Network != nil {
		base.Network = *actual.Network
	}
	updated, err := ordinary.Apply(base)
	if err != nil {
		return err
	}
	expected := ordinary.Snapshot(updated)
	if !reflect.DeepEqual(expected.GossipSub, actual.GossipSub) || !reflect.DeepEqual(expected.Network, actual.Network) {
		return fmt.Errorf("profile acknowledgement does not contain the requested values")
	}
	return nil
}

func (s ProfileSchedule) Clock() string {
	if s.Reference == "" {
		return "phase-start"
	}
	return s.Reference
}

func (s ProfileSchedule) Validate() error {
	if s.Clock() != "phase-start" && s.Clock() != "experiment-start" {
		return fmt.Errorf("schedule.reference must be phase-start or experiment-start")
	}
	if len(s.Changes) == 0 || len(s.Changes) > 128 {
		return fmt.Errorf("schedule requires 1..128 changes")
	}
	var previous time.Duration
	for i, change := range s.Changes {
		after, err := time.ParseDuration(change.After)
		if err != nil || after < 0 || i > 0 && after <= previous {
			return fmt.Errorf("schedule.changes[%d].after must be non-negative and strictly increasing", i)
		}
		if err := change.Set.Validate(); err != nil {
			return fmt.Errorf("schedule.changes[%d].set: %w", i, err)
		}
		previous = after
	}
	return nil
}

const MaxProfileUpdateBytes = 32 << 10

type ProfileUpdateRequest struct {
	RunID      string              `json:"runId"`
	Generation uint64              `json:"generation"`
	Revision   uint64              `json:"revision"`
	Stage      string              `json:"stage"`
	Set        RuntimeProfilePatch `json:"set"`
}

func (r ProfileUpdateRequest) Validate() error {
	if r.RunID == "" || r.Revision == 0 || r.Revision > 2147483647 || r.Stage != "prepare" && r.Stage != "apply" {
		return fmt.Errorf("profile update requires runId, positive revision, and prepare/apply stage")
	}
	return r.Set.Validate()
}

type ProfileUpdateResponse struct {
	NodeID    string              `json:"nodeId"`
	PeerID    string              `json:"peerId"`
	Revision  uint64              `json:"revision"`
	Stage     string              `json:"stage"`
	Effective RuntimeProfilePatch `json:"effective"`
}
