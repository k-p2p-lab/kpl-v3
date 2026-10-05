package pubsub

import (
	"context"
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	ErrMeshPlanInvalid    = errors.New("invalid mesh plan")
	ErrMeshPlanNotReady   = errors.New("mesh plan peers are not ready")
	ErrMeshFreezeConflict = errors.New("mesh is already frozen with different membership")
)

// ValidateMeshPlan checks whether SetMeshAndFreeze can install this local topic
// membership. It changes no mesh, score, fanout or freeze state. A successful
// check reserves nothing; SetMeshAndFreeze validates again when applying it.
//
// Neighbors must be distinct, valid, nonlocal, connected mesh-capable peers that
// subscribe to the topic. Direct peers and rejected peer filters are excluded.
// The local topic must have an active subscription or relay. Missing connections
// or subscriptions return ErrMeshPlanNotReady; invalid input returns
// ErrMeshPlanInvalid. An already frozen identical topic membership succeeds even
// if its links have since disappeared; any different plan conflicts.
func (p *PubSub) ValidateMeshPlan(ctx context.Context, topic string, peers []peer.ID) error {
	return p.meshPlan(ctx, topic, peers, false)
}

// SetMeshAndFreeze installs exactly the supplied topic membership and freezes
// all current topic meshes in one PubSub event-loop operation. Other topics keep
// their existing members. WithMeshFreeze is required. No automatic degree,
// score or backoff selection changes the supplied plan, and this operation emits
// no GRAFT/PRUNE exchange. Apply corresponding plans to remote participants to
// establish reciprocal meshes; this call is not a network-wide atomic operation.
//
// Validation failure leaves all state unchanged. A repeated identical plan is
// idempotent; a frozen different plan returns ErrMeshFreezeConflict. Canceling
// ctx before application leaves the mesh unchanged. Cancellation racing with
// application can be resolved by a safe retry or MeshFreezeSnapshot.
func (p *PubSub) SetMeshAndFreeze(ctx context.Context, topic string, peers []peer.ID) error {
	return p.meshPlan(ctx, topic, peers, true)
}

func (p *PubSub) meshPlan(ctx context.Context, topic string, peers []peer.ID, apply bool) error {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return ErrMeshFreezeUnsupported
	}
	if !gs.meshFreezeEnabled {
		return ErrMeshFreezeDisabled
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if topic == "" {
		return fmt.Errorf("%w: topic is empty", ErrMeshPlanInvalid)
	}
	// Own the membership before queueing; callers may reuse the input slice
	// after returning, including when cancellation races event-loop application.
	members := make(map[peer.ID]struct{}, len(peers))
	for _, pid := range peers {
		if _, err := peer.IDFromBytes([]byte(pid)); err != nil {
			return fmt.Errorf("%w: malformed peer ID", ErrMeshPlanInvalid)
		}
		if pid == p.host.ID() {
			return fmt.Errorf("%w: local peer cannot be its own neighbor", ErrMeshPlanInvalid)
		}
		if _, duplicate := members[pid]; duplicate {
			return fmt.Errorf("%w: duplicate neighbor %s", ErrMeshPlanInvalid, pid)
		}
		members[pid] = struct{}{}
	}
	_, err := p.evalMeshFreezeResult(ctx, func() (MeshFreezeSnapshot, error) {
		if err := gs.validateMeshPlan(topic, members); err != nil {
			return MeshFreezeSnapshot{}, err
		}
		if err := ctx.Err(); err != nil {
			return MeshFreezeSnapshot{}, err
		}
		if err := p.ctx.Err(); err != nil {
			return MeshFreezeSnapshot{}, err
		}
		if !apply || gs.meshFrozen {
			return MeshFreezeSnapshot{}, nil
		}
		previous := gs.mesh[topic]
		for pid := range previous {
			if _, retained := members[pid]; !retained {
				gs.meshAccounting(pid, topic, false)
			}
		}
		for pid := range members {
			if _, retained := previous[pid]; !retained {
				gs.meshAccounting(pid, topic, true)
			}
		}
		gs.mesh[topic] = members
		delete(gs.fanout, topic)
		delete(gs.lastpub, topic)
		gs.freezeMesh()
		return MeshFreezeSnapshot{}, nil
	})
	return err
}

// validateMeshPlan runs only on the PubSub event loop, immediately before an
// optional installation. No part of its validation mutates router state.
func (gs *GossipSubRouter) validateMeshPlan(topic string, members map[peer.ID]struct{}) error {
	if gs.meshFrozen {
		pinned, exists := gs.mesh[topic]
		if !exists || len(pinned) != len(members) {
			return ErrMeshFreezeConflict
		}
		for pid := range members {
			if _, exists := pinned[pid]; !exists {
				return ErrMeshFreezeConflict
			}
		}
		return nil
	}
	// Reject permanent incompatibilities before reporting transient readiness,
	// independent of map iteration order when several neighbors are supplied.
	for pid := range members {
		if _, direct := gs.direct[pid]; direct {
			return fmt.Errorf("%w: direct peer %s cannot enter the mesh", ErrMeshPlanInvalid, pid)
		}
		if gs.p.blacklist != nil && gs.p.blacklist.Contains(pid) {
			return fmt.Errorf("%w: neighbor %s is blacklisted", ErrMeshPlanInvalid, pid)
		}
		if gs.p.peerFilter != nil && !gs.p.peerFilter(pid, topic) {
			return fmt.Errorf("%w: neighbor %s is rejected by the topic peer filter", ErrMeshPlanInvalid, pid)
		}
		if proto, connected := gs.peers[pid]; connected && !gs.feature(GossipSubFeatureMesh, proto) {
			return fmt.Errorf("%w: neighbor %s does not support mesh routing", ErrMeshPlanInvalid, pid)
		}
	}
	if !gs.meshTopicActive(topic) {
		return fmt.Errorf("%w: local topic %q has no subscription or relay", ErrMeshPlanNotReady, topic)
	}
	for pid := range members {
		if _, connected := gs.peers[pid]; !connected || gs.p.peers[pid] == nil {
			return fmt.Errorf("%w: neighbor %s has no connected PubSub stream", ErrMeshPlanNotReady, pid)
		}
		if _, subscribed := gs.p.topics[topic][pid]; !subscribed {
			return fmt.Errorf("%w: neighbor %s has not subscribed to topic %q", ErrMeshPlanNotReady, pid, topic)
		}
	}
	return nil
}
