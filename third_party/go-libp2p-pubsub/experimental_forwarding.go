package pubsub

import (
	"errors"

	"github.com/libp2p/go-libp2p/core/peer"
)

// WithGossipSubForwardFilter filters accepted remote messages by recipient on
// eager/direct/fanout sends, IWANT responses and cached IHAVE advertisements.
// The event-loop callback must not block. Local delivery/cache and locally
// originated publications are unaffected. path is "publish" or "ihave".
func WithGossipSubForwardFilter(allow func(peer.ID, *Message, string) bool, active func() bool) Option {
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return errors.New("forward filter requires GossipSub")
		}
		gs.forwardFilter = allow
		gs.forwardFilterActive = active
		return nil
	}
}

func (gs *GossipSubRouter) filterForwardRPC(id peer.ID, rpc *RPC) *RPC {
	if gs.forwardFilter == nil || gs.forwardFilterActive != nil && !gs.forwardFilterActive() {
		return rpc
	}
	allow := func(mid, path string) bool {
		msg, ok := gs.mcache.Get(mid)
		return !ok || msg.ReceivedFrom == gs.p.host.ID() || gs.forwardFilter(id, msg, path)
	}
	// RPCs and their protobuf slices can be shared between recipients. Own only
	// the containers we modify; messages themselves remain immutable.
	out := *rpc
	out.Publish = nil
	for _, msg := range rpc.Publish {
		mid := gs.p.idGen.ID(&Message{Message: msg})
		if allow(mid, "publish") {
			out.Publish = append(out.Publish, msg)
		}
	}
	if rpc.Control != nil {
		ctl := *rpc.Control
		ctl.Ihave = nil
		for _, ihave := range rpc.Control.Ihave {
			entry := *ihave
			entry.MessageIDs = nil
			for _, mid := range ihave.MessageIDs {
				if allow(mid, "ihave") {
					entry.MessageIDs = append(entry.MessageIDs, mid)
				}
			}
			if len(entry.MessageIDs) > 0 {
				ctl.Ihave = append(ctl.Ihave, &entry)
			}
		}
		out.Control = &ctl
	}
	if len(out.Publish)+len(out.Subscriptions) == 0 && (out.Control == nil || out.Control.Size() == 0) {
		return nil
	}
	return &out
}
