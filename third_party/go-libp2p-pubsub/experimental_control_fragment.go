package pubsub

import (
	"math/bits"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// experimentalControlFragments preserves appendOrMergeRPC's GRAFT/IHAVE packing
// without rescanning a growing protobuf for each appended item. Large explicit
// attack batches must not monopolize the event loop quadratically. The ordinary
// router's general-purpose fragmentation path is unchanged.
// A nil result means cancellation; individual oversize entries remain oversize
// fragments for the caller to drop, just as in appendOrMergeRPC.
func experimentalControlFragments(control *pb.ControlMessage, limit int, canceled func() bool) []*RPC {
	if canceled() {
		return nil
	}
	rpc := &RPC{RPC: pb.RPC{Control: control}}
	if rpc.Size() < limit {
		return []*RPC{rpc}
	}
	out := []*RPC{{}}
	last := out[0]
	last.Control = &pb.ControlMessage{}
	controlSize, ihaveSize := 0, 0
	newRPC := func() {
		last = &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{}}}
		out = append(out, last)
		controlSize, ihaveSize = 0, 0
	}
	if experimentalDelimitedSize(0) > limit {
		last.Control = nil
		newRPC()
	}
	for _, graft := range control.Graft {
		if canceled() {
			return nil
		}
		size := experimentalDelimitedSize(graft.Size())
		last.Control.Graft = append(last.Control.Graft, graft)
		controlSize += size
		if experimentalDelimitedSize(controlSize) > limit {
			last.Control.Graft = last.Control.Graft[:len(last.Control.Graft)-1]
			newRPC()
			last.Control.Graft = append(last.Control.Graft, graft)
			controlSize = size
		}
	}
	for _, ihave := range control.Ihave {
		if canceled() {
			return nil
		}
		if len(last.Control.Ihave) == 0 || last.Control.Ihave[len(last.Control.Ihave)-1].TopicID != ihave.TopicID {
			entry := &pb.ControlIHave{TopicID: ihave.TopicID}
			ihaveSize = entry.Size()
			size := experimentalDelimitedSize(ihaveSize)
			last.Control.Ihave = append(last.Control.Ihave, entry)
			controlSize += size
			if experimentalDelimitedSize(controlSize) > limit {
				last.Control.Ihave = last.Control.Ihave[:len(last.Control.Ihave)-1]
				newRPC()
				last.Control.Ihave = append(last.Control.Ihave, entry)
				controlSize, ihaveSize = size, entry.Size()
			}
		}
		for _, mid := range ihave.MessageIDs {
			if canceled() {
				return nil
			}
			entry := last.Control.Ihave[len(last.Control.Ihave)-1]
			entry.MessageIDs = append(entry.MessageIDs, mid)
			oldSize := ihaveSize
			ihaveSize += experimentalDelimitedSize(len(mid))
			controlSize += experimentalDelimitedSize(ihaveSize) - experimentalDelimitedSize(oldSize)
			if experimentalDelimitedSize(controlSize) > limit {
				entry.MessageIDs = entry.MessageIDs[:len(entry.MessageIDs)-1]
				newRPC()
				entry = &pb.ControlIHave{TopicID: ihave.TopicID, MessageIDs: []string{mid}}
				last.Control.Ihave = append(last.Control.Ihave, entry)
				ihaveSize = entry.Size()
				controlSize = experimentalDelimitedSize(ihaveSize)
			}
		}
	}
	return out
}

// Every field used here has a one-byte protobuf tag. Include the length prefix
// at both nested boundaries (RPC.control and ControlMessage.ihave/graft).
func experimentalDelimitedSize(payload int) int {
	return 1 + (bits.Len64(uint64(payload)|1)+6)/7 + payload
}
