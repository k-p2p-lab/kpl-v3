package master

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Docker acknowledges overlay mutations before every manager view converges.
// Wait by full identity; never remove or adopt a concurrent replacement.
func (s *swarm) waitResetNetworkView(ctx context.Context, retiring, expected string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		out, err := s.output(ctx, "network", "ls", "--quiet", "--no-trunc", "--filter", "name=^"+regexp.QuoteMeta(s.network)+"$")
		if err != nil {
			return fmt.Errorf("verify Peer network transition: %w", err)
		}
		ids, err := swarmIDs(out)
		if err != nil {
			return err
		}
		if expected == "" {
			if len(ids) == 0 {
				return nil
			}
		} else if len(ids) == 1 && ids[0] == expected {
			return nil
		}
		for _, id := range ids {
			if id != retiring && (expected == "" || id != expected) {
				return fmt.Errorf("unexpected Peer network replacement during reset: observed IDs=%q; no replacement was removed", ids)
			}
		}
		if err := swarmSleep(ctx, 250*time.Millisecond); err != nil {
			return fmt.Errorf("Peer network view did not converge: %w", err)
		}
	}
}

func (s *swarm) createResetPeerNetwork(ctx context.Context, args []string, oldID string) (string, error) {
	// Share the Docker timeout across removal visibility, name-conflict retries
	// and creation visibility, rather than restarting it after every poll.
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	for {
		if err := s.waitResetNetworkView(ctx, oldID, ""); err != nil {
			return "", err
		}
		id, err := s.output(ctx, args...)
		if err != nil {
			// This explicit daemon refusal proves this attempt created nothing.
			// Other failures can follow successful creation and must not replay.
			if !strings.Contains(err.Error(), "network with name "+s.network+" already exists") {
				return "", err
			}
			if pauseErr := swarmSleep(ctx, 250*time.Millisecond); pauseErr != nil {
				return "", errors.Join(err, pauseErr)
			}
			continue
		}
		if !swarmID.MatchString(id) || id == oldID {
			return "", errors.New("Peer network recreation returned invalid or unchanged ID; inspect Docker before deploying")
		}
		if err := s.waitResetNetworkView(ctx, oldID, id); err != nil {
			return "", err
		}
		return id, nil
	}
}
