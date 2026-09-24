package peer

import (
	"math/rand"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

// gossipDiscoverOptions configures PubSub's discovery engine; the caller chooses
// its Controller or Kademlia discovery provider separately.
func gossipDiscoverOptions(config model.PubSubDiscoveryConfig) ([]pubsub.DiscoverOpt, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var options []pubsub.DiscoverOpt
	var discoveryOptions []discovery.Option
	if config.TTL != "" {
		ttl, _ := time.ParseDuration(config.TTL)
		discoveryOptions = append(discoveryOptions, discovery.TTL(ttl))
	}
	if config.Limit != nil {
		discoveryOptions = append(discoveryOptions, discovery.Limit(*config.Limit))
	}
	if len(discoveryOptions) > 0 {
		options = append(options, pubsub.WithDiscoveryOpts(discoveryOptions...))
	}
	if config.Connector != nil {
		value := *config.Connector
		cacheSize := 100
		if value.CacheSize != nil {
			cacheSize = *value.CacheSize
		}
		dial, min, max, unit, offset := 2*time.Minute, 10*time.Second, time.Hour, time.Second, time.Duration(0)
		for target, raw := range map[*time.Duration]string{&dial: value.DialTimeout, &min: value.MinBackoff, &max: value.MaxBackoff, &unit: value.TimeUnit, &offset: value.Offset} {
			if raw != "" {
				*target, _ = time.ParseDuration(raw)
			}
		}
		base := 5.0
		if value.Base != nil {
			base = *value.Base
		}
		seed := int64(1)
		if value.Seed != nil {
			seed = *value.Seed
		}
		jitter := backoff.FullJitter
		if value.Jitter == "none" {
			jitter = backoff.NoJitter
		}
		options = append(options, pubsub.WithDiscoverConnector(func(h host.Host) (*backoff.BackoffConnector, error) {
			strategy := backoff.NewExponentialBackoff(min, max, jitter, unit, base, offset, rand.NewSource(seed))
			return backoff.NewBackoffConnector(h, cacheSize, dial, strategy)
		}))
	}
	return options, nil
}
