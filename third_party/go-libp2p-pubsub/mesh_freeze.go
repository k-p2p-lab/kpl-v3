package pubsub

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	ErrMeshFreezeUnsupported = errors.New("mesh freeze requires the GossipSub router")
	ErrMeshFreezeDisabled    = errors.New("mesh freeze is not enabled")
)

// WithMeshFreeze permits a later FreezeMesh call. It does not freeze the initial,
// empty mesh. The capability is disabled by default.
func WithMeshFreeze() Option {
	return func(p *PubSub) error {
		gs, ok := p.rt.(*GossipSubRouter)
		if !ok {
			return ErrMeshFreezeUnsupported
		}
		gs.meshFreezeEnabled = true
		return nil
	}
}

// FreezeMesh permanently pins the current topic mesh memberships. It is
// idempotent and returns success only after the router event loop applies it.
// Automatic and incoming GRAFT/PRUNE no longer change membership. Topic joins,
// leaves and disconnects cannot add or remove pinned IDs, but transport and
// subscription cleanup still happen. Gossip, fanout and message validation
// continue normally. Control messages already dequeued by a stream writer or
// transmitted before application cannot be recalled.
//
// Canceling ctx can race with application: callers may inspect MeshFreezeSnapshot
// or retry safely. A context canceled before application does not freeze the mesh.
func (p *PubSub) FreezeMesh(ctx context.Context) error {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return ErrMeshFreezeUnsupported
	}
	if !gs.meshFreezeEnabled {
		return ErrMeshFreezeDisabled
	}
	_, err := p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		gs.freezeMesh()
		return MeshFreezeSnapshot{}
	})
	return err
}

// freezeMesh is called only by the PubSub event loop.
func (gs *GossipSubRouter) freezeMesh() {
	if gs.meshFrozen {
		return
	}
	gs.meshFrozen = true
	gs.p.meshFrozen.Store(true)
	gs.frozenMeshActive = make(map[string]map[peer.ID]bool, len(gs.mesh))
	for topic, peers := range gs.mesh {
		gs.frozenMeshActive[topic] = make(map[peer.ID]bool, len(peers))
		for pid := range peers {
			// Before freezing, each mesh ID is accounted as in-mesh.
			gs.frozenMeshActive[topic][pid] = true
			gs.frozenMeshAccounting(pid, topic, gs.meshTopicActive(topic) && gs.meshPeerActive(topic, pid))
		}
	}
	clear(gs.control)
	for _, q := range gs.p.peers {
		q.queueMu.Lock()
		for i, rpc := range q.queue.normal {
			q.queue.normal[i] = withoutMeshControl(rpc)
		}
		for i, rpc := range q.queue.priority {
			q.queue.priority[i] = withoutMeshControl(rpc)
		}
		q.queueMu.Unlock()
	}
}

// MeshFreezeSnapshot distinguishes logical membership from currently usable
// mesh links. Active requires local participation, a connected mesh-capable
// PubSub stream and a current remote topic subscription. Mesh remains bounded by
// the membership at freeze time even if links disappear. The maps and slices are
// caller-owned.
type MeshFreezeSnapshot struct {
	Frozen     bool
	ObservedAt time.Time
	Mesh       map[string][]peer.ID
	Active     map[string][]peer.ID
}

func (p *PubSub) MeshFreezeSnapshot(ctx context.Context) (MeshFreezeSnapshot, error) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return MeshFreezeSnapshot{}, ErrMeshFreezeUnsupported
	}
	return p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		now := time.Now()
		if !now.After(gs.meshFreezeObservedAt) {
			now = gs.meshFreezeObservedAt.Add(time.Nanosecond)
		}
		gs.meshFreezeObservedAt = now
		snapshot := MeshFreezeSnapshot{
			Frozen: gs.meshFrozen, ObservedAt: now,
			Mesh: make(map[string][]peer.ID, len(gs.mesh)), Active: make(map[string][]peer.ID, len(gs.mesh)),
		}
		for topic, peers := range gs.mesh {
			snapshot.Mesh[topic] = make([]peer.ID, 0, len(peers))
			snapshot.Active[topic] = make([]peer.ID, 0, len(peers))
			for pid := range peers {
				snapshot.Mesh[topic] = append(snapshot.Mesh[topic], pid)
				if gs.meshTopicActive(topic) && gs.meshPeerActive(topic, pid) {
					snapshot.Active[topic] = append(snapshot.Active[topic], pid)
				}
			}
			sort.Slice(snapshot.Mesh[topic], func(i, j int) bool { return snapshot.Mesh[topic][i] < snapshot.Mesh[topic][j] })
			sort.Slice(snapshot.Active[topic], func(i, j int) bool { return snapshot.Active[topic][i] < snapshot.Active[topic][j] })
		}
		return snapshot
	})
}

func (p *PubSub) evalMeshFreeze(ctx context.Context, apply func() MeshFreezeSnapshot) (MeshFreezeSnapshot, error) {
	return p.evalMeshFreezeResult(ctx, func() (MeshFreezeSnapshot, error) { return apply(), nil })
}

func (p *PubSub) evalMeshFreezeResult(ctx context.Context, apply func() (MeshFreezeSnapshot, error)) (MeshFreezeSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return MeshFreezeSnapshot{}, err
	}
	if err := p.ctx.Err(); err != nil {
		return MeshFreezeSnapshot{}, err
	}
	type result struct {
		snapshot MeshFreezeSnapshot
		err      error
	}
	done := make(chan result, 1)
	select {
	case p.eval <- func() {
		if err := ctx.Err(); err != nil {
			done <- result{err: err}
			return
		}
		if err := p.ctx.Err(); err != nil {
			done <- result{err: err}
			return
		}
		snapshot, err := apply()
		done <- result{snapshot: snapshot, err: err}
	}:
	case <-ctx.Done():
		return MeshFreezeSnapshot{}, ctx.Err()
	case <-p.ctx.Done():
		return MeshFreezeSnapshot{}, p.ctx.Err()
	}
	// The buffered response owns its snapshot, so cancellation can return at
	// either stage without racing a callback or leaving a blocked sender.
	select {
	case out := <-done:
		return out.snapshot, out.err
	case <-ctx.Done():
		return MeshFreezeSnapshot{}, ctx.Err()
	case <-p.ctx.Done():
		return MeshFreezeSnapshot{}, p.ctx.Err()
	}
}

func withoutMeshControl(rpc *RPC) *RPC {
	if rpc.Control == nil || (len(rpc.Control.Graft) == 0 && len(rpc.Control.Prune) == 0) {
		return rpc
	}
	out := copyRPC(rpc)
	out.Control.Graft = nil
	out.Control.Prune = nil
	if out.Control.Size() == 0 {
		out.Control = nil
	}
	return out
}

func (gs *GossipSubRouter) meshTopicActive(topic string) bool {
	return len(gs.p.mySubs[topic]) != 0 || gs.p.myRelays[topic] != 0
}

func (gs *GossipSubRouter) meshPeerActive(topic string, pid peer.ID) bool {
	proto, connected := gs.peers[pid]
	_, subscribed := gs.p.topics[topic][pid]
	return connected && subscribed && gs.feature(GossipSubFeatureMesh, proto)
}

// Topics first joined after freezing have no pinned mesh entry. They still
// participate in lazy gossip, like an ordinary joined topic with an empty mesh.
// Subscription and relay references can overlap; emit once per topic.
func (gs *GossipSubRouter) emitGossipForUnpinnedTopics() {
	for topic, subs := range gs.p.mySubs {
		if _, pinned := gs.mesh[topic]; !pinned && len(subs) > 0 {
			gs.emitGossip(topic, nil)
		}
	}
	for topic, relays := range gs.p.myRelays {
		if _, pinned := gs.mesh[topic]; !pinned && relays > 0 && len(gs.p.mySubs[topic]) == 0 {
			gs.emitGossip(topic, nil)
		}
	}
}

// Update internal score and connection-manager accounting without emitting a
// synthetic wire GRAFT/PRUNE event. Logical frozen membership stays unchanged.
func (gs *GossipSubRouter) frozenMeshAccounting(pid peer.ID, topic string, active bool) {
	previous, pinned := gs.frozenMeshActive[topic][pid]
	if !pinned || previous == active {
		return
	}
	gs.frozenMeshActive[topic][pid] = active
	gs.meshAccounting(pid, topic, active)
}

// Account for an actual local membership transition without claiming a wire
// GRAFT/PRUNE exchange. Existing members retain their score age and deliveries.
func (gs *GossipSubRouter) meshAccounting(pid peer.ID, topic string, active bool) {
	if active {
		if gs.score != nil {
			gs.score.Graft(pid, topic)
		}
		if gs.tagTracer != nil {
			gs.tagTracer.Graft(pid, topic)
		}
	} else {
		if gs.score != nil {
			gs.score.Prune(pid, topic)
		}
		if gs.tagTracer != nil {
			gs.tagTracer.Prune(pid, topic)
		}
	}
}
