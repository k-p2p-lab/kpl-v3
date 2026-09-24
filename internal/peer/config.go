package peer

import (
	"fmt"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/libp2p/go-libp2p"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

func hostOptions(config model.NodeConfig) ([]libp2p.Option, error) {
	// Keep the v2 transport/security stack explicit: libp2p defaults also
	// enable TLS and additional transports, changing connection startup costs.
	options := []libp2p.Option{
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
	}
	if config.Libp2p.UserAgent != "" {
		options = append(options, libp2p.UserAgent(config.Libp2p.UserAgent))
	}
	if config.Libp2p.DialTimeout != "" {
		dialTimeout, err := time.ParseDuration(config.Libp2p.DialTimeout)
		if err != nil {
			return nil, fmt.Errorf("parse libp2p dial timeout: %w", err)
		}
		options = append(options, libp2p.WithDialTimeout(dialTimeout))
	}
	if config.Libp2p.NATPortMap != nil && *config.Libp2p.NATPortMap {
		options = append(options, libp2p.NATPortMap())
	}
	if config.Libp2p.Relay != nil {
		if *config.Libp2p.Relay {
			options = append(options, libp2p.EnableRelay())
		} else {
			options = append(options, libp2p.DisableRelay())
		}
	}
	if config.Libp2p.RelayService != nil && *config.Libp2p.RelayService {
		options = append(options, libp2p.EnableRelayService())
	}

	cmConfig := config.Libp2p.ConnectionManager
	if cmConfig != nil {
		var managerOptions []connmgr.Option
		if cmConfig.GracePeriod != "" {
			grace, err := time.ParseDuration(cmConfig.GracePeriod)
			if err != nil {
				return nil, fmt.Errorf("parse connection manager grace period: %w", err)
			}
			managerOptions = append(managerOptions, connmgr.WithGracePeriod(grace))
		}
		manager, err := connmgr.NewConnManager(cmConfig.LowWater, cmConfig.HighWater, managerOptions...)
		if err != nil {
			return nil, fmt.Errorf("create connection manager: %w", err)
		}
		options = append(options, libp2p.ConnectionManager(manager))
	}
	if config.Libp2p.ConnectionLimit != nil && *config.Libp2p.ConnectionLimit > 0 {
		limits := rcmgr.PartialLimitConfig{
			System: rcmgr.ResourceLimits{Conns: rcmgr.LimitVal(*config.Libp2p.ConnectionLimit)},
		}
		manager, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Build(rcmgr.DefaultLimits.AutoScale())))
		if err != nil {
			return nil, fmt.Errorf("create resource manager: %w", err)
		}
		options = append(options, libp2p.ResourceManager(manager))
	}
	return options, nil
}
