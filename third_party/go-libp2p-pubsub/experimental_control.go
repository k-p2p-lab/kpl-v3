package pubsub

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/gogo/protobuf/proto"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// WithIWantResponseFilter selects individual otherwise eligible cached IWANT
// responses. False withholds that message without changing cache or eager push.
func WithIWantResponseFilter(allow func(peer.ID, *Message) bool) Option {
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return errors.New("IWANT response filter requires GossipSub")
		}
		gs.iwantResponseFilter = allow
		return nil
	}
}

type ExperimentalControlResult struct{ Queued, Dropped, Skipped, SuppressedGrafts int }

// SendExperimentalGossipSubControl injects only GRAFT/IHAVE on existing PubSub
// transports. It uses finite outbound queues and normal RPC size fragmentation,
// never creates a connection, changes local mesh membership, or retries drops.
// Result counts RPC fragments admitted/dropped, not remote delivery. IHAVE IDs
// colliding with the local cache are regenerated. Cancellation can race sending.
func (p *PubSub) SendExperimentalGossipSubControl(ctx context.Context, id peer.ID, control *pb.ControlMessage, respectBackoff bool) (ExperimentalControlResult, error) {
	var result ExperimentalControlResult
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return result, errors.New("experimental control requires GossipSub")
	}
	if control == nil || len(control.Iwant)+len(control.Prune)+len(control.Idontwant) > 0 {
		return result, errors.New("experimental control supports only GRAFT and IHAVE")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := p.ctx.Err(); err != nil {
		return result, err
	}
	owned := proto.Clone(control).(*pb.ControlMessage)
	canceled := func() bool { return ctx.Err() != nil || p.ctx.Err() != nil }
	done := make(chan ExperimentalControlResult, 1)
	_, err := p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		result := ExperimentalControlResult{}
		defer func() { done <- result }()
		q, connected := p.peers[id]
		if !connected {
			result.Skipped++
			return MeshFreezeSnapshot{}
		}
		grafts := owned.Graft[:0]
		for _, graft := range owned.Graft {
			if canceled() {
				return MeshFreezeSnapshot{}
			}
			if gs.meshFrozen || respectBackoff && time.Now().Before(gs.backoff[graft.GetTopicID()][id]) {
				result.SuppressedGrafts++
				continue
			}
			grafts = append(grafts, graft)
		}
		owned.Graft = grafts
		for _, ihave := range owned.Ihave {
			ids := ihave.MessageIDs[:0]
			for _, mid := range ihave.MessageIDs {
				for {
					if canceled() {
						return MeshFreezeSnapshot{}
					}
					if _, exists := gs.mcache.Get(mid); !exists {
						break
					}
					digest := sha256.Sum256([]byte(mid + "\x00experimental-bogus"))
					mid = string(digest[:])
				}
				ids = append(ids, mid)
			}
			ihave.MessageIDs = ids
		}
		if len(owned.Graft)+len(owned.Ihave) == 0 {
			return MeshFreezeSnapshot{}
		}
		for _, rpc := range experimentalControlFragments(owned, p.maxMessageSize, canceled) {
			if canceled() {
				return MeshFreezeSnapshot{}
			}
			if rpc.Size() > p.maxMessageSize {
				gs.tracer.DropRPC(rpc, id)
				result.Dropped++
				continue
			}
			if err := q.Push(rpc, false); err != nil {
				gs.tracer.DropRPC(rpc, id)
				result.Dropped++
				continue
			}
			gs.tracer.SendRPC(rpc, id)
			result.Queued++
		}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		return ExperimentalControlResult{}, err
	}
	// An in-progress batch may have stopped at a cancellation checkpoint while
	// the evaluation result and cancellation became ready together. Preserve
	// uncertain admission instead of reporting a successful zero-count send.
	if err := ctx.Err(); err != nil {
		return ExperimentalControlResult{}, err
	}
	if err := p.ctx.Err(); err != nil {
		return ExperimentalControlResult{}, err
	}
	return <-done, nil
}
