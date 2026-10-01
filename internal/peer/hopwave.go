package peer

import pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

// These are unauthenticated wire reports, separate from Controller estimates
// based on IHAVE/IWANT/mesh evidence. Preserve absence instead of inventing zero.
func addHopWaveFields(fields map[string]any, message *pubsubpb.Message) {
	if message.HopCount == nil && message.PropaType == nil {
		return
	}
	fields["hopWaveMetadataVersion"] = 1
	fields["hopWaveSource"] = "wire"
	fields["hopWaveHopCountAvailable"] = message.HopCount != nil && *message.HopCount >= 0
	if message.HopCount != nil && *message.HopCount >= 0 {
		fields["hopWaveHopCount"] = *message.HopCount
	}
	if message.PropaType != nil {
		switch *message.PropaType {
		case pubsubpb.PropagationType_EAGER_PUSH:
			fields["hopWavePropagationType"] = "eager-push"
		case pubsubpb.PropagationType_LAZY_PULL:
			fields["hopWavePropagationType"] = "lazy-pull"
		default:
			fields["hopWavePropagationType"] = "unknown"
		}
	}
}
