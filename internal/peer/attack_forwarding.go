package peer

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
)

func (r *attackRuntime) forwardActive() bool {
	c := r.policy.Load().config.Forwarding
	return c != nil && c.Active()
}

func (r *attackRuntime) allowForward(recipient corepeer.ID, msg *pubsub.Message, path string) bool {
	state := r.policy.Load()
	c := state.config.Forwarding
	if c == nil || !c.Active() || !matchesAttackTopic(c.Topics, msg.GetTopic()) {
		return true
	}
	matched, known := r.matchTransport(c.Targets, recipient.String())
	if !known {
		r.count("forwarding", "unknown", 1)
	}
	if !matched {
		return true
	}
	r.count("forwarding", path+"Considered", 1)
	// The path is intentionally absent from the hash: gossip/IWANT must not
	// reroll the decision made for this recipient's eager push.
	if r.selected("forwarding", msg, *c.Probability, recipient.String()) {
		r.count("forwarding", path+"Withheld", 1)
		r.detail("forwarding", "withhold-"+path, msg, state.revision, recipient.String())
		return false
	}
	return true
}
