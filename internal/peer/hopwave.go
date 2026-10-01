package peer

import pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

// These are unauthenticated wire reports, separate from Controller estimates
// based on IHAVE/IWANT/mesh evidence. Preserve absence instead of inventing zero.
func addHopwaveFields(fields map[string]any, message *pubsubpb.Message) {
	if message.HopCount == nil && message.PropaType == nil {
		return
	}
	fields["hopwaveMetadataVersion"] = 1
	fields["hopwaveSource"] = "wire"
	fields["hopwaveHopCountAvailable"] = message.HopCount != nil && *message.HopCount >= 0
	if message.HopCount != nil && *message.HopCount >= 0 {
		fields["hopwaveHopCount"] = *message.HopCount
	}
	if message.PropaType != nil {
		switch *message.PropaType {
		case pubsubpb.PropagationType_EAGER_PUSH:
			fields["hopwavePropagationType"] = "eager-push"
		case pubsubpb.PropagationType_LAZY_PULL:
			fields["hopwavePropagationType"] = "lazy-pull"
		default:
			fields["hopwavePropagationType"] = "unknown"
		}
	}
}
