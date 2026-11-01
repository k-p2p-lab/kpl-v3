package model

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// ProbabilityAttack is opt-in. Nil is omitted, not false/zero, until overlays
// have been merged. A configured but inactive policy retains its probability.
type ProbabilityAttack struct {
	Enabled     *bool    `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Probability *float64 `json:"probability,omitempty" yaml:"probability,omitempty"`
}

func (p *ProbabilityAttack) Active() bool { return p != nil && p.Enabled != nil && *p.Enabled }
func (p ProbabilityAttack) WithDefaults() ProbabilityAttack {
	if p.Enabled == nil {
		p.Enabled = boolPointer(false)
	}
	if p.Probability == nil {
		value := 1.0
		p.Probability = &value
	}
	return p
}
func (p ProbabilityAttack) Validate() error {
	if p.Probability != nil && (math.IsNaN(*p.Probability) || math.IsInf(*p.Probability, 0) || *p.Probability < 0 || *p.Probability > 1) {
		return fmt.Errorf("probability must be finite and in [0, 1]")
	}
	return nil
}

type AttackSelector struct {
	Groups  []string `json:"groups,omitempty" yaml:"groups,omitempty"`
	NodeIDs []string `json:"nodeIds,omitempty" yaml:"nodeIds,omitempty"`
	PeerIDs []string `json:"peerIds,omitempty" yaml:"peerIds,omitempty"`
}

func (s *AttackSelector) Empty() bool {
	return s == nil || len(s.Groups)+len(s.NodeIDs)+len(s.PeerIDs) == 0
}
func (s *AttackSelector) Matches(group, nodeID, peerID string) bool {
	return s == nil || (len(s.Groups) == 0 || slices.Contains(s.Groups, group)) && (len(s.NodeIDs) == 0 || slices.Contains(s.NodeIDs, nodeID)) && (len(s.PeerIDs) == 0 || slices.Contains(s.PeerIDs, peerID))
}
func (s *AttackSelector) Validate() error {
	if s == nil {
		return nil
	}
	for name, values := range map[string][]string{"groups": s.Groups, "nodeIds": s.NodeIDs, "peerIds": s.PeerIDs} {
		seen := map[string]bool{}
		for _, value := range values {
			if value == "" || strings.TrimSpace(value) != value || seen[value] {
				return fmt.Errorf("%s must contain distinct nonblank values without surrounding whitespace", name)
			}
			seen[value] = true
			if name == "peerIds" {
				if _, err := peer.Decode(value); err != nil {
					return fmt.Errorf("invalid attack peerId %q", value)
				}
			}
		}
	}
	return nil
}

type CensorshipConfig struct {
	ProbabilityAttack `yaml:",inline"`
	Topics            []string        `json:"topics,omitempty" yaml:"topics,omitempty"`
	Publishers        *AttackSelector `json:"publishers,omitempty" yaml:"publishers,omitempty"`
}
type MessageDelayConfig struct {
	ProbabilityAttack  `yaml:",inline"`
	Delay              string `json:"delay,omitempty" yaml:"delay,omitempty"`
	MaxPendingMessages *int   `json:"maxPendingMessages,omitempty" yaml:"maxPendingMessages,omitempty"`
	MaxPendingBytes    *int   `json:"maxPendingBytes,omitempty" yaml:"maxPendingBytes,omitempty"`
}
type ConnectionAttackConfig struct {
	Enabled           *bool           `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Targets           *AttackSelector `json:"targets,omitempty" yaml:"targets,omitempty"`
	TargetConnections *int            `json:"targetConnections,omitempty" yaml:"targetConnections,omitempty"`
	RetryInterval     string          `json:"retryInterval,omitempty" yaml:"retryInterval,omitempty"`
}
type ControlAttackConfig struct {
	Enabled  *bool           `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Topics   []string        `json:"topics,omitempty" yaml:"topics,omitempty"`
	Targets  *AttackSelector `json:"targets,omitempty" yaml:"targets,omitempty"`
	Interval string          `json:"interval,omitempty" yaml:"interval,omitempty"`
}
type GraftFloodConfig struct {
	ControlAttackConfig `yaml:",inline"`
	GraftsPerPeer       *int  `json:"graftsPerPeer,omitempty" yaml:"graftsPerPeer,omitempty"`
	RespectBackoff      *bool `json:"respectBackoff,omitempty" yaml:"respectBackoff,omitempty"`
}
type IHaveSpamConfig struct {
	ControlAttackConfig `yaml:",inline"`
	IDsPerRPC           *int `json:"idsPerRPC,omitempty" yaml:"idsPerRPC,omitempty"`
	RPCsPerPeer         *int `json:"rpcsPerPeer,omitempty" yaml:"rpcsPerPeer,omitempty"`
}
type IWantWithholdingConfig struct {
	ProbabilityAttack `yaml:",inline"`
	Topics            []string        `json:"topics,omitempty" yaml:"topics,omitempty"`
	Targets           *AttackSelector `json:"targets,omitempty" yaml:"targets,omitempty"`
}

// AttackTarget is an internal, run/generation-scoped registry record. AuthorPeerID
// is the configured signing identity, which may differ from the transport ID.
type AttackTarget struct {
	NodeID       string   `json:"nodeId"`
	Group        string   `json:"group"`
	PeerID       string   `json:"peerId"`
	AuthorPeerID string   `json:"authorPeerId,omitempty"`
	Addresses    []string `json:"addresses,omitempty"`
	Ready        bool     `json:"ready"`
}

func attackEnabled(value *bool) bool { return value != nil && *value }
func (c NodeConfig) HasAttackOptions() bool {
	return c.Sink != nil || c.Censorship != nil || c.MessageDelay != nil || c.ConnectionAttack != nil || c.GraftFlood != nil || c.IHaveSpam != nil || c.IWantWithholding != nil
}
func (c NodeConfig) HasActiveAttacks() bool {
	return c.Sink.Active() || c.Censorship != nil && c.Censorship.Active() || c.MessageDelay != nil && c.MessageDelay.Active() || c.ConnectionAttack != nil && attackEnabled(c.ConnectionAttack.Enabled) || c.GraftFlood != nil && attackEnabled(c.GraftFlood.Enabled) || c.IHaveSpam != nil && attackEnabled(c.IHaveSpam.Enabled) || c.IWantWithholding != nil && c.IWantWithholding.Active()
}
func (c NodeConfig) AttackSelectors() []*AttackSelector {
	var values []*AttackSelector
	if c.Censorship != nil {
		values = append(values, c.Censorship.Publishers)
	}
	if c.ConnectionAttack != nil {
		values = append(values, c.ConnectionAttack.Targets)
	}
	if c.GraftFlood != nil {
		values = append(values, c.GraftFlood.Targets)
	}
	if c.IHaveSpam != nil {
		values = append(values, c.IHaveSpam.Targets)
	}
	if c.IWantWithholding != nil {
		values = append(values, c.IWantWithholding.Targets)
	}
	return values
}
func (c NodeConfig) ValidateAttackRole(role string) error {
	if c.HasActiveAttacks() && role != "worker" {
		return fmt.Errorf("active attack options require role worker")
	}
	return nil
}
func mergeAttack[T any](base, overlay *T) *T {
	if base == nil && overlay == nil {
		return nil
	}
	var result T
	if base != nil {
		result = *base
	}
	if overlay != nil {
		mergeNonZero(reflect.ValueOf(&result).Elem(), reflect.ValueOf(*overlay))
	}
	return &result
}
func (c *NodeConfig) mergeAttacks(base, overlay NodeConfig) {
	c.Sink = mergeAttack(base.Sink, overlay.Sink)
	c.Censorship = mergeAttack(base.Censorship, overlay.Censorship)
	c.MessageDelay = mergeAttack(base.MessageDelay, overlay.MessageDelay)
	c.ConnectionAttack = mergeAttack(base.ConnectionAttack, overlay.ConnectionAttack)
	c.GraftFlood = mergeAttack(base.GraftFlood, overlay.GraftFlood)
	c.IHaveSpam = mergeAttack(base.IHaveSpam, overlay.IHaveSpam)
	c.IWantWithholding = mergeAttack(base.IWantWithholding, overlay.IWantWithholding)
}
func (c *NodeConfig) defaultAttacks() {
	c.mergeAttacks(*c, NodeConfig{})
	if c.Censorship != nil {
		c.Censorship.Publishers = normalizeAttackSelector(c.Censorship.Publishers)
	}
	if c.ConnectionAttack != nil {
		c.ConnectionAttack.Targets = normalizeAttackSelector(c.ConnectionAttack.Targets)
	}
	if c.GraftFlood != nil {
		c.GraftFlood.Targets = normalizeAttackSelector(c.GraftFlood.Targets)
	}
	if c.IHaveSpam != nil {
		c.IHaveSpam.Targets = normalizeAttackSelector(c.IHaveSpam.Targets)
	}
	if c.IWantWithholding != nil {
		c.IWantWithholding.Targets = normalizeAttackSelector(c.IWantWithholding.Targets)
	}

	if c.Sink != nil {
		*c.Sink = c.Sink.WithDefaults()
	}
	if c.Censorship != nil {
		c.Censorship.ProbabilityAttack = c.Censorship.ProbabilityAttack.WithDefaults()
	}
	if c.MessageDelay != nil {
		p := c.MessageDelay
		p.ProbabilityAttack = p.ProbabilityAttack.WithDefaults()
		if p.MaxPendingMessages == nil {
			p.MaxPendingMessages = intPointer(1024)
		}
		if p.MaxPendingBytes == nil {
			p.MaxPendingBytes = intPointer(16 << 20)
		}
	}
	if c.ConnectionAttack != nil {
		p := c.ConnectionAttack
		if p.Enabled == nil {
			p.Enabled = boolPointer(false)
		}
		if p.TargetConnections == nil {
			p.TargetConnections = intPointer(8)
		}
		if p.RetryInterval == "" {
			p.RetryInterval = "1s"
		}
	}
	defaults := func(p *ControlAttackConfig) {
		if p.Enabled == nil {
			p.Enabled = boolPointer(false)
		}
		if p.Interval == "" {
			p.Interval = "1s"
		}
	}
	if c.GraftFlood != nil {
		p := c.GraftFlood
		defaults(&p.ControlAttackConfig)
		if p.GraftsPerPeer == nil {
			p.GraftsPerPeer = intPointer(1)
		}
		if p.RespectBackoff == nil {
			p.RespectBackoff = boolPointer(false)
		}
	}
	if c.IHaveSpam != nil {
		p := c.IHaveSpam
		defaults(&p.ControlAttackConfig)
		if p.IDsPerRPC == nil {
			p.IDsPerRPC = intPointer(100)
		}
		if p.RPCsPerPeer == nil {
			p.RPCsPerPeer = intPointer(1)
		}
	}
	if c.IWantWithholding != nil {
		c.IWantWithholding.ProbabilityAttack = c.IWantWithholding.ProbabilityAttack.WithDefaults()
	}
}
func (c NodeConfig) ValidateAttacks() error {
	if err := c.ValidateAttackSyntax(); err != nil {
		return err
	}
	c.defaultAttacks()
	for name, p := range map[string]*ProbabilityAttack{"sink": c.Sink, "censorship": nil, "messageDelay": nil, "iwantWithholding": nil} {
		switch name {
		case "censorship":
			if c.Censorship != nil {
				p = &c.Censorship.ProbabilityAttack
			}
		case "messageDelay":
			if c.MessageDelay != nil {
				p = &c.MessageDelay.ProbabilityAttack
			}
		case "iwantWithholding":
			if c.IWantWithholding != nil {
				p = &c.IWantWithholding.ProbabilityAttack
			}
		}
		if p != nil {
			if err := p.Validate(); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	for _, s := range c.AttackSelectors() {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	topics := func(values []string, required bool) error {
		if required && len(values) == 0 {
			return fmt.Errorf("active control attack requires explicit topics")
		}
		for _, t := range values {
			if t == "" || !slices.Contains(c.GossipSub.Topics, t) {
				return fmt.Errorf("attack topic %q is not a configured topic", t)
			}
		}
		return nil
	}
	if c.Censorship != nil {
		p := c.Censorship
		if err := topics(p.Topics, false); err != nil {
			return err
		}
		if p.Active() && len(p.Topics) == 0 && p.Publishers.Empty() {
			return fmt.Errorf("active censorship requires topics or publishers")
		}
		if p.Active() && !p.Publishers.Empty() && c.GossipSub.SignaturePolicy != "" && c.GossipSub.SignaturePolicy != "strict-sign" {
			return fmt.Errorf("publisher censorship requires strict-sign")
		}
	}
	if c.MessageDelay != nil {
		p := c.MessageDelay
		if p.Delay != "" || p.Active() {
			d, err := time.ParseDuration(p.Delay)
			if err != nil || d <= 0 {
				return fmt.Errorf("messageDelay.delay must be a positive duration")
			}
		}
		if *p.MaxPendingMessages <= 0 || *p.MaxPendingBytes <= 0 {
			return fmt.Errorf("messageDelay queue limits must be positive")
		}
	}
	if c.ConnectionAttack != nil {
		p := c.ConnectionAttack
		if p.TargetConnections == nil || *p.TargetConnections <= 0 {
			return fmt.Errorf("targetConnections must be positive")
		}
		if err := pubsubDuration("connectionAttack.retryInterval", p.RetryInterval, true); err != nil {
			return err
		}
		if attackEnabled(p.Enabled) && p.Targets.Empty() {
			return fmt.Errorf("active connectionAttack requires targets")
		}
		if attackEnabled(p.Enabled) && c.Libp2p.ConnectionLimit != nil && *c.Libp2p.ConnectionLimit > 0 && *p.TargetConnections > *c.Libp2p.ConnectionLimit {
			return fmt.Errorf("targetConnections exceeds the libp2p connection limit")
		}
	}
	controls := func(p ControlAttackConfig) error {
		if err := topics(p.Topics, attackEnabled(p.Enabled)); err != nil {
			return err
		}
		return pubsubDuration("control attack interval", p.Interval, true)
	}
	if c.GraftFlood != nil {
		if err := controls(c.GraftFlood.ControlAttackConfig); err != nil {
			return err
		}
		if *c.GraftFlood.GraftsPerPeer <= 0 || *c.GraftFlood.GraftsPerPeer > 65536 {
			return fmt.Errorf("graftsPerPeer must be in [1, 65536]")
		}
	}
	if c.IHaveSpam != nil {
		if err := controls(c.IHaveSpam.ControlAttackConfig); err != nil {
			return err
		}
		if *c.IHaveSpam.IDsPerRPC <= 0 || *c.IHaveSpam.IDsPerRPC > 65536 || *c.IHaveSpam.RPCsPerPeer <= 0 || *c.IHaveSpam.RPCsPerPeer > 65536 {
			return fmt.Errorf("IHAVE counts must be in [1, 65536]")
		}
	}
	if c.IWantWithholding != nil {
		if err := topics(c.IWantWithholding.Topics, false); err != nil {
			return err
		}
	}
	if c.HasActiveAttacks() {
		if c.GossipSub.Enabled == nil || !*c.GossipSub.Enabled || c.GossipSub.Router != "gossipsub" {
			return fmt.Errorf("active attacks require enabled GossipSub")
		}
		if (c.Sink.Active() || c.Censorship != nil && c.Censorship.Active() || c.MessageDelay != nil && c.MessageDelay.Active()) && c.GossipSub.TopicMode != "subscribe" && c.GossipSub.TopicMode != "relay" {
			return fmt.Errorf("receive attacks require subscribe or relay topic mode")
		}
	}
	return nil
}

// ValidateAttackSyntax checks declared values even when a later overlay replaces
// them. Cross-field/router/topic requirements use the final effective config.
func (c NodeConfig) ValidateAttackSyntax() error {
	var topicLists [][]string
	if c.Censorship != nil {
		topicLists = append(topicLists, c.Censorship.Topics)
	}
	if c.GraftFlood != nil {
		topicLists = append(topicLists, c.GraftFlood.Topics)
	}
	if c.IHaveSpam != nil {
		topicLists = append(topicLists, c.IHaveSpam.Topics)
	}
	if c.IWantWithholding != nil {
		topicLists = append(topicLists, c.IWantWithholding.Topics)
	}
	for _, topics := range topicLists {
		seen := map[string]bool{}
		for _, topic := range topics {
			if topic == "" || seen[topic] {
				return fmt.Errorf("attack topics must contain distinct nonblank values")
			}
			seen[topic] = true
		}
	}
	if c.GraftFlood != nil && c.GraftFlood.GraftsPerPeer != nil && *c.GraftFlood.GraftsPerPeer > 65536 {
		return fmt.Errorf("graftsPerPeer must not exceed 65536")
	}
	if c.IHaveSpam != nil {
		for _, value := range []*int{c.IHaveSpam.IDsPerRPC, c.IHaveSpam.RPCsPerPeer} {
			if value != nil && *value > 65536 {
				return fmt.Errorf("IHAVE counts must not exceed 65536")
			}
		}
	}

	policies := []*ProbabilityAttack{c.Sink}
	if c.Censorship != nil {
		policies = append(policies, &c.Censorship.ProbabilityAttack)
	}
	if c.MessageDelay != nil {
		policies = append(policies, &c.MessageDelay.ProbabilityAttack)
	}
	if c.IWantWithholding != nil {
		policies = append(policies, &c.IWantWithholding.ProbabilityAttack)
	}
	for _, p := range policies {
		if p != nil {
			if err := p.Validate(); err != nil {
				return err
			}
		}
	}
	for _, selector := range c.AttackSelectors() {
		if err := selector.Validate(); err != nil {
			return err
		}
	}
	for _, value := range []struct {
		name  string
		value string
	}{
		{"messageDelay.delay", func() string {
			if c.MessageDelay != nil {
				return c.MessageDelay.Delay
			}
			return ""
		}()},
		{"connectionAttack.retryInterval", func() string {
			if c.ConnectionAttack != nil {
				return c.ConnectionAttack.RetryInterval
			}
			return ""
		}()},
		{"graftFlood.interval", func() string {
			if c.GraftFlood != nil {
				return c.GraftFlood.Interval
			}
			return ""
		}()},
		{"ihaveSpam.interval", func() string {
			if c.IHaveSpam != nil {
				return c.IHaveSpam.Interval
			}
			return ""
		}()},
	} {
		if value.value != "" {
			if err := pubsubDuration(value.name, value.value, true); err != nil {
				return err
			}
		}
	}
	var positive []*int
	if c.MessageDelay != nil {
		positive = append(positive, c.MessageDelay.MaxPendingBytes, c.MessageDelay.MaxPendingMessages)
	}
	if c.ConnectionAttack != nil {
		positive = append(positive, c.ConnectionAttack.TargetConnections)
	}
	if c.GraftFlood != nil {
		positive = append(positive, c.GraftFlood.GraftsPerPeer)
	}
	if c.IHaveSpam != nil {
		positive = append(positive, c.IHaveSpam.IDsPerRPC, c.IHaveSpam.RPCsPerPeer)
	}
	for _, value := range positive {
		if value != nil && *value <= 0 {
			return fmt.Errorf("attack counts and queue limits must be positive")
		}
	}
	return nil
}

func normalizeAttackSelector(value *AttackSelector) *AttackSelector {
	if value == nil {
		return nil
	}
	out := *value
	out.Groups = slices.Clone(value.Groups)
	out.NodeIDs = slices.Clone(value.NodeIDs)
	out.PeerIDs = slices.Clone(value.PeerIDs)
	for i, raw := range out.PeerIDs {
		if id, err := peer.Decode(raw); err == nil {
			out.PeerIDs[i] = id.String()
		}
	}
	return &out
}
