package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
)

// Preparation may race startup, earlier discovery dials, or the remote Peer
// dialing this one. Keep transient dial failures inside the command budget
// instead of canceling preparation of the entire graph on the first failure.
func (s *Server) connectTopologyPeer(ctx context.Context, info peer.AddrInfo) error {
	delay := 250 * time.Millisecond
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("connect topology neighbor %s: %w", info.ID, errors.Join(err, lastErr))
		}
		// Preserve the configured transport dial timeout and any experiment
		// delay. The command context bounds all attempts and retry waits.
		err := s.host.Connect(ctx, info)
		if err == nil {
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return fmt.Errorf("connect topology neighbor %s: %w", info.ID, errors.Join(ctx.Err(), err, lastErr))
		}
		// A later backoff is a consequence of an earlier failed dial. Retain
		// the actual transport failure for diagnostics at the deadline.
		if lastErr == nil || !errors.Is(err, swarm.ErrDialBackoff) {
			lastErr = err
		}
		if !retryTopologyDial(err) {
			return fmt.Errorf("connect topology neighbor %s: %w", info.ID, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("connect topology neighbor %s: %w", info.ID, errors.Join(ctx.Err(), lastErr))
		case <-timer.C:
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("connect topology neighbor %s: %w", info.ID, errors.Join(err, lastErr))
		}
		// Only an initial backoff can predate this explicit preparation.
		// Give that selected neighbor one fresh chance; failures from our own
		// later attempts retain libp2p's backoff. Other peers are untouched.
		if attempt == 0 && errors.Is(err, swarm.ErrDialBackoff) {
			if network, ok := s.host.Network().(*swarm.Swarm); ok {
				network.Backoff().Clear(info.ID)
			}
		}
		delay = min(2*delay, 2*time.Second)
	}
}

func retryTopologyDial(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	var networkErr net.Error
	return errors.Is(err, swarm.ErrDialBackoff) ||
		errors.Is(err, swarm.ErrAllDialsFailed) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &networkErr)
}
