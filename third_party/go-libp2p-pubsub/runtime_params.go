package pubsub

import (
	"context"
	"errors"
	"math"
)

// GossipSubRuntimeParams contains only fields read on the router event loop.
// Timers, queues, caches and scoring options keep their construction settings.
type GossipSubRuntimeParams struct {
	D, Dlo, Dhi, Dscore, Dout, Dlazy int
	GossipFactor, HopwaveFactor      float64
	HopwaveInterval                  int
}

func (v GossipSubRuntimeParams) Validate() error {
	if v.D <= 0 || v.Dlo < 0 || v.Dhi <= 0 || v.Dlo > v.D || v.D > v.Dhi || v.Dscore < 0 || v.Dscore > v.D || v.Dout < 0 || v.Dout >= v.Dlo || v.Dout > v.D/2 || v.Dlazy < 0 {
		return errors.New("invalid runtime GossipSub degree bounds")
	}
	for _, factor := range []float64{v.GossipFactor, v.HopwaveFactor} {
		if math.IsNaN(factor) || math.IsInf(factor, 0) || factor < 0 || factor > 1 {
			return errors.New("runtime GossipSub factors must be finite and in [0, 1]")
		}
	}
	if v.HopwaveInterval < 1 || v.HopwaveInterval > 2147483647 {
		return errors.New("runtime HopwaveInterval must be in [1, 2147483647]")
	}
	return nil
}

// SetGossipSubRuntimeParams changes subsequent routing decisions without
// restarting PubSub. Mesh freeze remains in force. Degree repair happens on the
// normal heartbeat. A cancellation racing application may have taken effect.
func (p *PubSub) SetGossipSubRuntimeParams(ctx context.Context, values GossipSubRuntimeParams) error {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return errors.New("runtime GossipSub parameters require the GossipSub router")
	}
	if err := values.Validate(); err != nil {
		return err
	}
	_, err := p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		// Do not replace the entire struct: timer/connector workers read other
		// immutable fields outside this event loop.
		gs.params.D, gs.params.Dlo, gs.params.Dhi = values.D, values.Dlo, values.Dhi
		gs.params.Dscore, gs.params.Dout, gs.params.Dlazy = values.Dscore, values.Dout, values.Dlazy
		gs.params.GossipFactor, gs.params.HopwaveFactor = values.GossipFactor, values.HopwaveFactor
		gs.params.HopwaveInterval = values.HopwaveInterval
		return MeshFreezeSnapshot{}
	})
	return err
}

// GossipSubRuntimeParamsSnapshot returns the router's currently applied values.
func (p *PubSub) GossipSubRuntimeParamsSnapshot(ctx context.Context) (GossipSubRuntimeParams, error) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return GossipSubRuntimeParams{}, errors.New("runtime GossipSub parameters require the GossipSub router")
	}
	done := make(chan GossipSubRuntimeParams, 1)
	_, err := p.evalMeshFreeze(ctx, func() MeshFreezeSnapshot {
		v := gs.params
		done <- GossipSubRuntimeParams{D: v.D, Dlo: v.Dlo, Dhi: v.Dhi, Dscore: v.Dscore, Dout: v.Dout, Dlazy: v.Dlazy, GossipFactor: v.GossipFactor, HopwaveFactor: v.HopwaveFactor, HopwaveInterval: v.HopwaveInterval}
		return MeshFreezeSnapshot{}
	})
	if err != nil {
		return GossipSubRuntimeParams{}, err
	}
	return <-done, nil
}
