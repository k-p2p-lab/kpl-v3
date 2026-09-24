package peer

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p/core/peer"
)

func connectBootstrapPeers(parent context.Context, config model.PeerProcessConfig, ownID peer.ID, fetch func(context.Context) ([]bootstrapNode, error), connect func(context.Context, peer.AddrInfo) error) error {
	config.NodeConfig = config.NodeConfig.WithDefaults()
	bootstrapTimeout, _ := time.ParseDuration(config.NodeConfig.Kademlia.BootstrapTimeout)
	retryInterval, _ := time.ParseDuration(config.NodeConfig.Kademlia.BootstrapRetryInterval)
	dialTimeout := 5 * time.Second
	if config.NodeConfig.Libp2p.DialTimeout != "" {
		dialTimeout, _ = time.ParseDuration(config.NodeConfig.Libp2p.DialTimeout)
	}
	ctx, cancel := context.WithTimeout(parent, bootstrapTimeout)
	defer cancel()
	rng := rand.New(rand.NewSource(config.Seed))
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("bootstrap did not complete within %s: %w", bootstrapTimeout, err)
		}
		nodes, err := fetch(ctx)
		if err == nil {
			if config.Node.Role != "boot" {
				// v2 workers connect to one randomly selected reachable bootstrap.
				// Use the assigned peer seed so retries/order can be reproduced.
				nodes = append([]bootstrapNode(nil), nodes...)
				rng.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
			}
			for _, node := range nodes {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("bootstrap did not complete within %s: %w", bootstrapTimeout, err)
				}
				if node.PeerID == ownID.String() {
					continue
				}
				info, err := addrInfo(node)
				if err != nil {
					continue
				}
				connectCtx, cancelDial := context.WithTimeout(ctx, dialTimeout)
				err = connect(connectCtx, info)
				cancelDial()
				if err == nil && ctx.Err() == nil && config.Node.Role != "boot" {
					return nil
				}
			}
			// The first bootstrap is allowed to start an empty network.
			if config.Node.Role == "boot" && ctx.Err() == nil {
				return nil
			}
		}
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("bootstrap did not complete within %s: %w", bootstrapTimeout, ctx.Err())
		case <-timer.C:
		}
	}
}
