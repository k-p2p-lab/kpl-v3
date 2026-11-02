package pubsub

import (
	"context"
	"errors"
	"sync"
	"time"
)

// InboundMessageDecision applies after signature verification and seen-ID
// admission, before application validators, delivery and router caching.
// Local publications bypass this hook. A delayed message resumes validation
// once, without re-running the policy or registering its seen ID again.
type InboundMessageDecision struct {
	Drop  bool
	Delay time.Duration
	// Tag is an opaque local observation token, such as a policy revision.
	// It is not serialized or stored in the application's ValidatorData.
	Tag uint64
}
type InboundMessagePolicyLimits struct {
	// Active permits a dormant runtime policy without changing unsigned fast paths.
	// A nil predicate means active. It must be concurrency-safe and nonblocking.
	Active             func() bool
	MaxPendingMessages int
	MaxPendingBytes    int
	// Observe receives queued, released, overflow or canceled. It must not block.
	Observe func(*Message, string)
	// ObserveDecision preserves the selecting decision across delayed events.
	// The original Observe callback remains supported independently.
	ObserveDecision func(*Message, string, InboundMessageDecision)
}

func (p *inboundMessagePolicy) observe(msg *Message, event string, decision InboundMessageDecision) {
	if p.limits.Observe != nil {
		p.limits.Observe(msg, event)
	}
	if p.limits.ObserveDecision != nil {
		p.limits.ObserveDecision(msg, event, decision)
	}
}

type inboundMessagePolicy struct {
	wg      sync.WaitGroup
	decide  func(*Message) InboundMessageDecision
	limits  InboundMessagePolicyLimits
	mu      sync.Mutex
	pending int
	bytes   int
}

func WithInboundMessagePolicy(decide func(*Message) InboundMessageDecision, limits InboundMessagePolicyLimits) Option {
	return func(p *PubSub) error {
		if decide == nil || limits.MaxPendingMessages <= 0 || limits.MaxPendingBytes <= 0 {
			return errors.New("inbound policy requires a callback and positive queue limits")
		}
		p.inboundPolicy = &inboundMessagePolicy{decide: decide, limits: limits}
		return nil
	}
}
func (v *validation) applyInboundPolicy(msg *Message, resume func()) bool {
	policy := v.p.inboundPolicy
	if !v.p.inboundPolicyEnabled() || msg.ReceivedFrom == v.p.host.ID() {
		return false
	}
	policy.mu.Lock()
	if v.p.ctx.Err() != nil {
		policy.mu.Unlock()
		return true
	}
	policy.wg.Add(1)
	policy.mu.Unlock()
	defer policy.wg.Done()
	decision := policy.decide(msg)
	if decision.Drop {
		v.tracer.RejectMessage(msg, RejectValidationIgnored)
		return true
	}
	if decision.Delay <= 0 {
		return false
	}
	size := len(msg.Data)
	policy.mu.Lock()
	if v.p.ctx.Err() != nil {
		policy.mu.Unlock()
		return true
	}
	if policy.pending >= policy.limits.MaxPendingMessages || size > policy.limits.MaxPendingBytes-policy.bytes {
		policy.mu.Unlock()
		policy.observe(msg, "overflow", decision)
		v.tracer.RejectMessage(msg, RejectValidationThrottled)
		return true
	}
	policy.wg.Add(1)
	policy.pending++
	policy.bytes += size
	policy.mu.Unlock()
	policy.observe(msg, "queued", decision)
	go func() {
		defer policy.wg.Done()
		timer := time.NewTimer(decision.Delay)
		defer timer.Stop()
		event := "canceled"
		select {
		case <-v.p.ctx.Done():
		case <-timer.C:
			if v.p.ctx.Err() == nil {
				event = "released"
			}
		}
		policy.mu.Lock()
		policy.pending--
		policy.bytes -= size
		policy.mu.Unlock()
		policy.observe(msg, event, decision)
		if event == "released" {
			resume()
		}
	}()
	return true
}

// WaitInboundMessagePolicy drains canceled inbound decisions and delay work,
// and waits for event-loop routing callbacks before final attack telemetry.
// The PubSub context must already be canceled, preventing new policy admissions.
// The caller context bounds waiting for any callback already in progress.
func (p *PubSub) WaitInboundMessagePolicy(ctx context.Context) error {
	if p.inboundPolicy == nil {
		return nil
	}
	if p.ctx.Err() == nil {
		return errors.New("cancel PubSub before draining inbound policy")
	}
	if p.processLoopDone != nil {
		select {
		case <-p.processLoopDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.inboundPolicy.mu.Lock()
	p.inboundPolicy.mu.Unlock()
	done := make(chan struct{})
	go func() { p.inboundPolicy.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PubSub) inboundPolicyEnabled() bool {
	return p.inboundPolicy != nil && (p.inboundPolicy.limits.Active == nil || p.inboundPolicy.limits.Active())
}
