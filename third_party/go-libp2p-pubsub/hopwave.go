package pubsub

import (
	"fmt"
	"math"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// WithHopwave enables the Hopwave hop/propagation metadata extension to
// GossipSub. Routing, mesh selection and scoring remain GossipSub's. All hops
// in an experiment must support the extension for a complete hop count.
// Message ID functions receive a copy without the mutable Hopwave fields.
func WithHopwave() Option {
	return func(p *PubSub) error {
		if _, ok := p.rt.(*GossipSubRouter); !ok {
			return fmt.Errorf("Hopwave requires the GossipSub router")
		}
		p.hopwave = true
		return nil
	}
}

// WithHopwavePublish enables periodic full forwarding with random fractional
// forwarding between waves. See KPL-CHANGES.md for source references.
// It also enables the mutable wire metadata. Disabled is the default.
func WithHopwavePublish(enabled bool) Option {
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("Hopwave requires the GossipSub router")
		}
		if enabled {
			if err := validateHopwaveParams(gs.params); err != nil {
				return err
			}
			p.hopwave = true
		}
		gs.hopwavePublish = enabled
		return nil
	}
}

func validateHopwaveParams(params GossipSubParams) error {
	if math.IsNaN(params.HopwaveFactor) || math.IsInf(params.HopwaveFactor, 0) || params.HopwaveFactor < 0 || params.HopwaveFactor > 1 {
		return fmt.Errorf("HopwaveFactor must be finite and in [0, 1]")
	}
	if params.HopwaveInterval < 1 || params.HopwaveInterval > math.MaxInt32 {
		return fmt.Errorf("HopwaveInterval must be in [1, 2147483647]")
	}
	return nil
}

func (gs *GossipSubRouter) hopwaveMessage(message *pb.Message, propagation pb.PropagationType) *pb.Message {
	out := hopwaveMessage(message, propagation)
	if gs.hopwavePublish && out.HopCount != nil && *out.HopCount >= int32(gs.params.HopwaveInterval) {
		*out.HopCount = 0
	}
	return out
}

// Select only after excluding the previous sender, author and IDONTWANT peers.
// Positive factors retain at least one eligible recipient, matching the fork.
func selectHopwavePeers(candidates map[peer.ID]struct{}, factor float64) map[peer.ID]struct{} {
	if factor >= 1 {
		return candidates
	}
	count := int(float64(len(candidates)) * factor)
	if factor > 0 && len(candidates) > 0 && count == 0 {
		count = 1
	}
	selected := make(map[peer.ID]struct{}, count)
	if count == 0 {
		return selected
	}
	peers := make([]peer.ID, 0, len(candidates))
	for p := range candidates {
		peers = append(peers, p)
	}
	shufflePeers(peers)
	for _, p := range peers[:count] {
		selected[p] = struct{}{}
	}
	return selected
}

// Each outgoing transmission gets its own metadata. The inbound message is
// shared with subscribers, tracers and the cache and must remain immutable.
// Repeated IWANT replies increment the cached arrival count once per hop,
// without accumulating retransmissions or contaminating eager deliveries.
func hopwaveMessage(message *pb.Message, propagation pb.PropagationType) *pb.Message {
	out := *message
	out.PropaType = propagation.Enum()
	out.HopCount = nil
	if message.HopCount != nil && *message.HopCount >= 0 && *message.HopCount < math.MaxInt32 {
		hops := *message.HopCount + 1
		out.HopCount = &hops
	}
	// Missing, negative or overflowing hop counts remain unknown. In particular,
	// receiving a message from an uninstrumented peer cannot establish hop zero.
	return &out
}
