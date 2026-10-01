package pubsub

import (
	"fmt"
	"math"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// WithHopWave enables the HopWave hop/propagation metadata extension to
// GossipSub. Routing, mesh selection and scoring remain GossipSub's. All hops
// in an experiment must support the extension for a complete hop count.
func WithHopWave() Option {
	return func(p *PubSub) error {
		if _, ok := p.rt.(*GossipSubRouter); !ok {
			return fmt.Errorf("HopWave requires the GossipSub router")
		}
		p.hopWave = true
		return nil
	}
}

// WithHopWavePublish enables periodic full forwarding with random fractional
// forwarding between waves, as implemented by kmu-comnet's hop-wave branch.
// It also enables the mutable wire metadata. Disabled is the default.
func WithHopWavePublish(enabled bool) Option {
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("HopWave requires the GossipSub router")
		}
		if enabled {
			if err := validateHopWaveParams(gs.params); err != nil {
				return err
			}
			p.hopWave = true
		}
		gs.hopWavePublish = enabled
		return nil
	}
}

func validateHopWaveParams(params GossipSubParams) error {
	if math.IsNaN(params.HopWaveFactor) || math.IsInf(params.HopWaveFactor, 0) || params.HopWaveFactor < 0 || params.HopWaveFactor > 1 {
		return fmt.Errorf("HopWaveFactor must be finite and in [0, 1]")
	}
	if params.HopWaveInterval < 1 || params.HopWaveInterval > math.MaxInt32 {
		return fmt.Errorf("HopWaveInterval must be in [1, 2147483647]")
	}
	return nil
}

func (gs *GossipSubRouter) hopWaveMessage(message *pb.Message, propagation pb.PropagationType) *pb.Message {
	out := hopWaveMessage(message, propagation)
	if gs.hopWavePublish && out.HopCount != nil && *out.HopCount >= int32(gs.params.HopWaveInterval) {
		*out.HopCount = 0
	}
	return out
}

// Select only after excluding the previous sender, author and IDONTWANT peers.
// Positive factors retain at least one eligible recipient, matching the fork.
func selectHopWavePeers(candidates map[peer.ID]struct{}, factor float64) map[peer.ID]struct{} {
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
func hopWaveMessage(message *pb.Message, propagation pb.PropagationType) *pb.Message {
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
