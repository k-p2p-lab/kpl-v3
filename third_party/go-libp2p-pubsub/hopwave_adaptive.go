package pubsub

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// HopwavePayloadRange assigns a trough factor to [MinBytes, MaxBytes).
// A nil MaxBytes includes every payload at or above MinBytes.
type HopwavePayloadRange struct {
	MinBytes int
	MaxBytes *int
	Factor   float64
}

// HopwaveAdaptivePolicy controls per-publication trough forwarding. Unmatched
// payloads use the router's current HopwaveFactor. PayloadSize can supply the
// application's payload length when the PubSub data includes framing. A nil
// callback uses len(data). The callback runs on the router event loop.
type HopwaveAdaptivePolicy struct {
	PayloadRanges   []HopwavePayloadRange
	RetentionWindow time.Duration // 0 disables observation; 60s enables it
	PayloadSize     func(data []byte) int
}

// WithAdaptiveHopwave configures a policy for WithHopwavePublish(true).
// The two options can be supplied in either order.
func WithAdaptiveHopwave(policy HopwaveAdaptivePolicy) Option {
	// An Option can be reused across nodes. Copy both the slice and its bounds
	// so later edits to the caller's config cannot change live forwarding.
	ranges := make([]HopwavePayloadRange, len(policy.PayloadRanges))
	for i, rule := range policy.PayloadRanges {
		if rule.MaxBytes != nil {
			upper := *rule.MaxBytes
			rule.MaxBytes = &upper
		}
		ranges[i] = rule
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].MinBytes < ranges[j].MinBytes })
	policy.PayloadRanges = ranges
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("adaptive Hopwave requires the GossipSub router")
		}
		if policy.RetentionWindow != 0 && policy.RetentionWindow != time.Minute {
			return fmt.Errorf("Hopwave retention window must be 0s or 60s")
		}
		for i, rule := range ranges {
			if rule.MinBytes < 0 || (rule.MaxBytes != nil && *rule.MaxBytes <= rule.MinBytes) {
				return fmt.Errorf("invalid Hopwave payload byte range at %d", rule.MinBytes)
			}
			if math.IsNaN(rule.Factor) || rule.Factor < 0 || rule.Factor > 1 {
				return fmt.Errorf("Hopwave payload factor must be finite and in [0, 1]")
			}
			if i > 0 && (ranges[i-1].MaxBytes == nil || *ranges[i-1].MaxBytes > rule.MinBytes) {
				return fmt.Errorf("overlapping Hopwave payload ranges at %d", rule.MinBytes)
			}
		}
		gs.hopwavePolicy = policy
		gs.hopwaveWindows = nil
		if policy.RetentionWindow != 0 {
			gs.hopwaveWindows = make(map[string]hopwaveRetentionWindow)
		}
		return nil
	}
}

type hopwaveRetentionWindow struct {
	start     time.Time
	neighbors map[peer.ID]struct{}
	retention float64
	valid     bool
}

// Sample after heartbeat mesh maintenance. Only endpoints of non-overlapping
// windows are needed, so intervening heartbeats need no peer-set copies.
func (gs *GossipSubRouter) observeHopwaveMesh(now time.Time) {
	for topic, members := range gs.mesh {
		if !gs.meshTopicActive(topic) {
			continue
		}
		prev, exists := gs.hopwaveWindows[topic]
		if exists && now.Sub(prev.start) < gs.hopwavePolicy.RetentionWindow {
			continue
		}
		next := hopwaveRetentionWindow{start: now, neighbors: make(map[peer.ID]struct{}, len(members))}
		retained := 0
		for pid := range members {
			// A frozen mesh retains pinned IDs even across disconnections and
			// unsubscribes. Observe only active mesh links at each endpoint.
			if gs.meshPeerActive(topic, pid) {
				next.neighbors[pid] = struct{}{}
				if _, present := prev.neighbors[pid]; present {
					retained++
				}
			}
		}
		if len(prev.neighbors) != 0 {
			next.retention = float64(retained) / float64(len(prev.neighbors))
			next.valid = true
		}
		gs.hopwaveWindows[topic] = next
	}
}

func (gs *GossipSubRouter) hopwaveTroughFactor(topic string, data []byte) float64 {
	base := gs.params.HopwaveFactor
	if ranges := gs.hopwavePolicy.PayloadRanges; len(ranges) != 0 {
		size := len(data)
		if measure := gs.hopwavePolicy.PayloadSize; measure != nil {
			size = measure(data)
		}
		i := sort.Search(len(ranges), func(i int) bool { return ranges[i].MinBytes > size }) - 1
		if i >= 0 && (ranges[i].MaxBytes == nil || size < *ranges[i].MaxBytes) {
			base = ranges[i].Factor
		}
	}
	if window := gs.hopwaveWindows[topic]; window.valid {
		return math.Min(0.9, base+(1-base)*(1-window.retention))
	}
	return base
}
