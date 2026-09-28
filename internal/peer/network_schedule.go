package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/netem"
)

type peerNetworkStep struct {
	after  time.Duration
	config model.NetworkConfig
}
type peerNetworkSchedule struct {
	reference string
	base      time.Time
	steps     []peerNetworkStep
	initial   model.NetworkConfig
	next      int
	estimate  *controllerClockEstimate
	sampledAt time.Time
}

type networkAppliedState struct {
	configJSON string
	revision   int
	appliedAt  time.Time
}

func newPeerNetworkSchedule(ctx context.Context, config model.PeerProcessConfig, joinedAt time.Time, clock func(context.Context, string, *http.Client) (controllerClockEstimate, error)) (*peerNetworkSchedule, error) {
	network := config.NodeConfig.Network
	if err := network.Validate(); err != nil {
		return nil, err
	}
	if !network.Scheduled() {
		return nil, nil
	}
	p := &peerNetworkSchedule{reference: network.Schedule.Clock(), base: joinedAt, initial: network.Initial()}
	if p.reference == "experiment-start" {
		if config.ExperimentStartedAt.IsZero() {
			return nil, fmt.Errorf("experiment-start network schedule requires experimentStartedAt")
		}
		estimate, err := clock(ctx, config.ControllerURL, &http.Client{Timeout: time.Second})
		if err != nil {
			return nil, fmt.Errorf("network schedule needs a synchronized Controller clock: %w", err)
		}
		now := time.Now()
		// Preserve a monotonic local anchor. Subsequent Controller outages or clock
		// refreshes cannot restart the countdown or accumulate scheduling drift.
		p.base = now.Add(config.ExperimentStartedAt.Sub(now.Add(estimate.offset)))
		p.estimate = &estimate
		p.sampledAt = now
	}
	current := p.initial
	if current.DelayDistribution != nil {
		return nil, fmt.Errorf("network schedule delayDistribution must be resolved by the Agent")
	}
	for _, change := range network.Schedule.Changes {
		after, _ := time.ParseDuration(change.After)
		current = current.Merge(change.Set)
		if current.DelayDistribution != nil {
			return nil, fmt.Errorf("network schedule delayDistribution must be resolved by the Agent")
		}
		p.steps = append(p.steps, peerNetworkStep{after: after, config: current})
	}
	// A late-joining Peer starts with the latest already-due state, never briefly
	// installs obsolete intermediate conditions before opening P2P sockets.
	now := time.Now()
	for p.next < len(p.steps) && !p.base.Add(p.steps[p.next].after).After(now) {
		p.initial = p.steps[p.next].config
		p.next++
	}
	return p, nil
}

func (s *Server) recordNetworkApplied(config model.NetworkConfig, revision int, deadline time.Time, after string, initial bool) {
	now := time.Now()
	raw, _ := json.Marshal(config)
	s.networkApplied.Store(&networkAppliedState{configJSON: string(raw), revision: revision, appliedAt: now.UTC()})
	lag := max(time.Duration(0), now.Sub(deadline))
	fields := map[string]any{"network": config, "reference": s.networkSchedule.reference, "after": after, "revision": revision, "initial": initial, "lateByMs": float64(lag) / float64(time.Millisecond)}
	s.telemetry.emitObservedPriority(func(controllerClockReading) (model.TraceEvent, bool) {
		return model.TraceEvent{Type: "network_config_applied", Fields: fields}, true
	}, true)
	s.logger.Info("peer network conditions applied", "node", s.config.Node.ID, "reference", s.networkSchedule.reference, "after", after, "revision", revision, "initial", initial, "lateBy", lag, "network", config)
}

func (s *Server) runNetworkSchedule(ctx context.Context, apply func(context.Context, model.NetworkConfig, model.NetworkConfig, int) error) error {
	p := s.networkSchedule
	current := p.initial
	for i := p.next; i < len(p.steps); i++ {
		step := p.steps[i]
		deadline := p.base.Add(step.after)
		timer := time.NewTimer(max(time.Duration(0), time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// Catch up to the latest due state if CPU pressure delayed this wakeup.
		for i+1 < len(p.steps) && !p.base.Add(p.steps[i+1].after).After(time.Now()) {
			i++
			step = p.steps[i]
			deadline = p.base.Add(step.after)
		}
		applyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := apply(applyCtx, current, step.config, netem.DefaultP2PPort)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failure := fmt.Errorf("network schedule change %d after %s: %w", i+1, step.after, err)
			s.telemetry.emitObservedPriority(func(controllerClockReading) (model.TraceEvent, bool) {
				return model.TraceEvent{Type: "network_config_failed", Fields: map[string]any{"reference": p.reference, "after": step.after.String(), "revision": i + 1, "error": failure.Error()}}, true
			}, true)
			s.logger.Error("scheduled network change failed", "node", s.config.Node.ID, "error", failure)
			return failure
		}
		current = step.config
		s.recordNetworkApplied(current, i+1, deadline, step.after.String(), false)
	}
	return nil
}

func (s *Server) addNetworkStatus(node *model.Node) {
	applied := s.networkApplied.Load()
	if applied == nil {
		return
	}
	node.Metadata = maps.Clone(node.Metadata)
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}
	node.Metadata["network"] = applied.configJSON
	node.Metadata["networkPending"] = "false"
	node.Metadata["networkRevision"] = strconv.Itoa(applied.revision)
	node.Metadata["networkAppliedAt"] = applied.appliedAt.Format(time.RFC3339Nano)
}
