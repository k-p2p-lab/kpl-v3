package experimentnet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const networkTransitionTimeout = time.Minute

func waitNetworkTransition(ctx context.Context) error {
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Swarm removal acknowledgment does not wait for the manager daemon's local
// overlay view to disappear. Never delete an unexpected replacement by name.
func (m *Manager) waitNetworkRemoved(ctx context.Context, oldID string) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wait for Peer network %q (old ID=%q) to disappear: %w", m.config.PeerNetwork, oldID, err)
		}
		ids, err := m.networkIDs(ctx)
		if err != nil {
			return fmt.Errorf("verify Peer network removal: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		if len(ids) != 1 || oldID == "" || ids[0] != oldID {
			return fmt.Errorf("Peer network %q was replaced during initialization: old ID=%q observed IDs=%q; no replacement was removed", m.config.PeerNetwork, oldID, ids)
		}
		if err := waitNetworkTransition(ctx); err != nil {
			return fmt.Errorf("wait for Peer network %q (old ID=%q) to disappear: %w", m.config.PeerNetwork, oldID, err)
		}
	}
}

func verifyFreshNetwork(n *networkInfo, request model.ExperimentNetworkRequest, allocation networkAllocation, oldID, expectedID string) error {
	if n.ID == oldID || (expectedID != "" && n.ID != expectedID) || n.Labels["io.kpl.execution"] != request.Epoch || n.IPAM.Config[0].Subnet != allocation.Subnet || n.IPAM.Config[0].Gateway != allocation.Gateway {
		return fmt.Errorf("fresh Peer network identity or allocation mismatch: ID=%q old ID=%q expected ID=%q epoch=%q subnet=%q gateway=%q", n.ID, oldID, expectedID, n.Labels["io.kpl.execution"], n.IPAM.Config[0].Subnet, n.IPAM.Config[0].Gateway)
	}
	return nil
}

func (m *Manager) waitFreshNetwork(ctx context.Context, request model.ExperimentNetworkRequest, allocation networkAllocation, oldID, id string) (string, error) {
	if !identifier.MatchString(id) || id == oldID {
		return "", errors.New("fresh Peer network creation returned an invalid or reused ID")
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("fresh Peer network %q did not become visible: %w", id, err)
		}
		created, err := m.inspect(ctx)
		if err != nil {
			return "", fmt.Errorf("fresh Peer network could not be verified: %w", err)
		}
		if created != nil {
			if err := verifyFreshNetwork(created, request, allocation, oldID, id); err != nil {
				return "", err
			}
			return id, nil
		}
		if err := waitNetworkTransition(ctx); err != nil {
			return "", fmt.Errorf("fresh Peer network %q did not become visible: %w", id, err)
		}
	}
}

func (m *Manager) createFreshNetwork(ctx context.Context, request model.ExperimentNetworkRequest, allocation networkAllocation, oldID string) (string, error) {
	// Share one bound across removal convergence, name-conflict retries and
	// creation visibility, within the existing three-minute preparation budget.
	ctx, cancel := context.WithTimeout(ctx, networkTransitionTimeout)
	defer cancel()
	args := []string{"network", "create", "--driver", "overlay", "--attachable", "--subnet", allocation.Subnet,
		"--label", "io.kpl.application=kp2plab-v3", "--label", "io.kpl.stack=" + m.config.Stack, "--label", "io.kpl.execution=" + request.Epoch}
	if allocation.Gateway != "" {
		args = append(args, "--gateway", allocation.Gateway)
	}
	args = append(args, m.config.PeerNetwork)
	for {
		if err := m.waitNetworkRemoved(ctx, oldID); err != nil {
			return "", err
		}
		data, createErr := m.command(ctx, nil, args...)
		if createErr == nil {
			return m.waitFreshNetwork(ctx, request, allocation, oldID, strings.TrimSpace(string(data)))
		}
		failure := fmt.Errorf("create fresh Peer network (response may be ambiguous): %w", createErr)
		if err := ctx.Err(); err != nil {
			return "", errors.Join(failure, err)
		}
		// A failed CLI response may still have created the network. Reconcile by
		// full identity, epoch, ownership and allocation; never blindly create twice.
		ids, err := m.networkIDs(ctx)
		if err != nil {
			return "", errors.Join(failure, fmt.Errorf("list after creation failure: %w", err))
		}
		if len(ids) > 1 {
			return "", errors.Join(failure, fmt.Errorf("ambiguous Peer network after creation failure: IDs=%q", ids))
		}
		// Do not inspect the retiring ID: it can legitimately disappear between
		// ls and inspect. Only an unexpected/new ID needs full verification.
		if len(ids) == 1 && ids[0] != oldID {
			observed, err := m.inspect(ctx)
			if err != nil {
				return "", errors.Join(failure, fmt.Errorf("inspect after creation failure: %w", err))
			}
			if observed == nil || observed.ID != ids[0] {
				return "", errors.Join(failure, errors.New("Peer network identity changed while reconciling creation"))
			}
			if err := verifyFreshNetwork(observed, request, allocation, oldID, ""); err != nil {
				return "", errors.Join(failure, err)
			}
			// No gateway or Peer has been attached by this preparation yet. An
			// occupied replacement is not ours to adopt after an uncertain response.
			if len(observed.Containers) != 0 {
				return "", errors.Join(failure, errors.New("new Peer network already has endpoints; refusing to adopt it"))
			}
			if err := m.unusedNetwork(ctx, observed); err != nil {
				return "", errors.Join(failure, err)
			}
			return observed.ID, nil
		}
		// This specific daemon rejection did not create a network. Its name view
		// may lag a successful removal even when the previous list was empty.
		if !strings.Contains(createErr.Error(), "network with name "+m.config.PeerNetwork+" already exists") {
			return "", failure
		}
		if err := waitNetworkTransition(ctx); err != nil {
			return "", errors.Join(failure, fmt.Errorf("Peer network name remained unavailable: %w", err))
		}
	}
}
