package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"time"

	lru "github.com/hashicorp/golang-lru/simplelru"
	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	corepeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

func dhtOptions(config model.KademliaConfig) ([]dht.Option, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	mode := dht.ModeServer
	switch config.Mode {
	case "auto":
		mode = dht.ModeAuto
	case "auto-server":
		mode = dht.ModeAutoServer
	case "client":
		mode = dht.ModeClient
	case "server":
		mode = dht.ModeServer
	default:
		return nil, fmt.Errorf("invalid kademlia mode %q", config.Mode)
	}
	options := []dht.Option{dht.Mode(mode)}
	if config.ProtocolID != "" {
		// V1ProtocolOverride changes the wire ID but leaves upstream's /ipfs
		// validation prefix intact. A private exact ID must also opt out of
		// Amino-only tuning restrictions, without changing the advertised ID.
		validationPrefix := protocol.ID("/k-p2p-lab/exact" + config.ProtocolID)
		if config.ProtocolID == "/ipfs/kad/1.0.0" {
			validationPrefix = dht.DefaultPrefix
		}
		options = append(options, dht.ProtocolPrefix(validationPrefix), dht.V1ProtocolOverride(protocol.ID(config.ProtocolID)))
	} else if config.ProtocolPrefix != "" {
		options = append(options, dht.ProtocolPrefix(protocol.ID(config.ProtocolPrefix)))
	}
	if config.ProtocolExtension != "" {
		options = append(options, dht.ProtocolExtension(protocol.ID(config.ProtocolExtension)))
	}
	if config.BucketSize != nil {
		options = append(options, dht.BucketSize(*config.BucketSize))
	}
	if config.Concurrency != nil {
		options = append(options, dht.Concurrency(*config.Concurrency))
	}
	if config.Resiliency != nil {
		options = append(options, dht.Resiliency(*config.Resiliency))
	}
	if config.LookupCheckConcurrency != nil {
		options = append(options, dht.LookupCheckConcurrency(*config.LookupCheckConcurrency))
	}
	durations := []struct {
		name  string
		value string
		apply func(time.Duration) dht.Option
	}{
		{"routingTableLatencyTolerance", config.RoutingTableLatencyTolerance, dht.RoutingTableLatencyTolerance},
		{"routingTableRefreshPeriod", config.RoutingTableRefreshPeriod, dht.RoutingTableRefreshPeriod},
		{"routingTableRefreshTimeout", config.RoutingTableRefreshTimeout, dht.RoutingTableRefreshQueryTimeout},
		{"maxRecordAge", config.MaxRecordAge, dht.MaxRecordAge},
	}
	for _, item := range durations {
		if item.value == "" {
			continue
		}
		value, err := time.ParseDuration(item.value)
		if err != nil {
			return nil, fmt.Errorf("parse kademlia %s: %w", item.name, err)
		}
		options = append(options, item.apply(value))
	}
	if config.DisableAutoRefresh != nil && *config.DisableAutoRefresh {
		options = append(options, dht.DisableAutoRefresh())
	}
	if config.DisableProviders != nil && *config.DisableProviders {
		options = append(options, dht.DisableProviders())
	}
	if config.DisableValues != nil && *config.DisableValues {
		options = append(options, dht.DisableValues())
	}
	if config.OptimisticProvide != nil && *config.OptimisticProvide {
		options = append(options, dht.EnableOptimisticProvide())
	}
	if config.OptimisticProvideJobsPoolSize != nil {
		options = append(options, dht.OptimisticProvideJobsPoolSize(*config.OptimisticProvideJobsPoolSize))
	}
	if config.QueryFilter != nil {
		filter := compileKademliaFilter(*config.QueryFilter)
		options = append(options, dht.QueryFilter(func(instance interface{}, info corepeer.AddrInfo) bool {
			allowed := true
			switch filter.policy {
			case "public":
				allowed = dht.PublicQueryFilter(instance, info)
			// Upstream's private query policy accepts any nonempty address set.
			// Use allowCIDRs when a strict address range is required.
			case "private":
				allowed = dht.PrivateQueryFilter(instance, info)
			case "non-loopback":
				allowed = filter.hasNonLoopback(info.Addrs)
			}
			return allowed && filter.acceptPeer(info.Addrs)
		}))
	}
	if config.RoutingTableFilter != nil {
		filter := compileKademliaFilter(*config.RoutingTableFilter)
		options = append(options, dht.RoutingTableFilter(func(instance interface{}, id corepeer.ID) bool {
			owner := instance.(interface{ Host() host.Host }).Host()
			var addresses []ma.Multiaddr
			for _, conn := range owner.Network().ConnsToPeer(id) {
				addresses = append(addresses, conn.RemoteMultiaddr())
			}
			allowed := true
			switch filter.policy {
			case "public":
				allowed = dht.PublicRoutingTableFilter(instance, id)
			case "private":
				allowed = dht.PrivateRoutingTableFilter(instance, id)
			case "non-loopback":
				allowed = filter.hasNonLoopback(addresses)
			}
			return allowed && filter.acceptPeer(addresses)
		}))
	}
	if config.AddressFilter != nil {
		filter := compileKademliaFilter(*config.AddressFilter)
		options = append(options, dht.AddressFilter(func(addresses []ma.Multiaddr) []ma.Multiaddr {
			result := make([]ma.Multiaddr, 0, len(addresses))
			for _, address := range addresses {
				allowed := true
				switch filter.policy {
				case "public":
					allowed = dht.PublicQueryFilter(nil, corepeer.AddrInfo{Addrs: []ma.Multiaddr{address}})
				case "private":
					allowed = manet.IsPrivateAddr(address)
				case "non-loopback":
					allowed = filter.hasNonLoopback([]ma.Multiaddr{address})
				}
				if allowed && filter.acceptPeer([]ma.Multiaddr{address}) {
					result = append(result, address)
				}
			}
			return result
		}))
	}
	if config.BootstrapSource == "static" {
		options = append(options, dht.BootstrapPeers(kademliaStaticBootstrap(config.BootstrapPeers)...))
	} else if config.BootstrapSource == "none" {
		options = append(options, dht.BootstrapPeers())
	}
	if config.Datastore != "" {
		options = append(options, dht.Datastore(kademliaDatastore(config.Datastore)))
	}
	return options, nil
}

// newDHT adds policies that need this Peer's host or run-scoped registry. Only
// the two resource-free datastore policies are exposed. DHT owns ProviderStore
// after successful construction; on a constructor failure we close it here.
func (s *Server) newDHT(ctx context.Context) (*dht.IpfsDHT, error) {
	config := s.config.NodeConfig.Kademlia
	options, err := dhtOptions(config)
	if err != nil {
		return nil, err
	}
	if diversity := config.RoutingTableDiversity; diversity != nil && (diversity.Enabled == nil || *diversity.Enabled) {
		perCpl, perTable := amino.DefaultMaxPeersPerIPGroupPerCpl, amino.DefaultMaxPeersPerIPGroup
		if diversity.MaxPerCpl != nil {
			perCpl = *diversity.MaxPerCpl
		}
		if diversity.MaxForTable != nil {
			perTable = *diversity.MaxForTable
		}
		options = append(options, dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(s.host, perCpl, perTable)))
	}
	if config.BootstrapSource == "controller" {
		options = append(options, dht.BootstrapPeersFunc(func() []corepeer.AddrInfo {
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			nodes, err := s.fetchBootstrap(queryCtx)
			if err != nil {
				return nil
			}
			var result []corepeer.AddrInfo
			seen := make(map[corepeer.ID]bool)
			for _, node := range nodes {
				info, err := addrInfo(node)
				if err == nil && info.ID != s.host.ID() && !seen[info.ID] {
					result = append(result, info)
					seen[info.ID] = true
				}
			}
			return result
		}))
	}
	if validation := config.Validator; validation != nil {
		validators := record.NamespacedValidator{}
		if validation.Mode != "namespaced" {
			validators["pk"] = record.PublicKeyValidator{}
			validators["ipns"] = ipns.Validator{KeyBook: s.host.Peerstore()}
		}
		for namespace, kind := range validation.Namespaces {
			switch kind {
			case "public-key":
				validators[namespace] = record.PublicKeyValidator{}
			case "ipns":
				validators[namespace] = ipns.Validator{KeyBook: s.host.Peerstore()}
			case "bytes":
				validators[namespace] = kademliaBytesValidator{}
			case "reject":
				validators[namespace] = kademliaRejectValidator{}
			}
		}
		options = append(options, dht.Validator(validators))
	}
	var providerStore providers.ProviderStore
	if settings := config.ProviderStore; settings != nil {
		if settings.Mode == "null" {
			providerStore = kademliaNullProviderStore{}
		} else {
			store := kademliaDatastore(config.Datastore)
			options = append(options, dht.Datastore(store))
			var providerOptions []providers.Option
			if settings.CleanupInterval != "" {
				interval, _ := time.ParseDuration(settings.CleanupInterval)
				providerOptions = append(providerOptions, providers.CleanupInterval(interval))
			}
			if settings.CacheSize != nil {
				cache, err := lru.NewLRU(*settings.CacheSize, nil)
				if err != nil {
					return nil, fmt.Errorf("provider cache: %w", err)
				}
				providerOptions = append(providerOptions, providers.Cache(cache))
			}
			providerStore, err = providers.NewProviderManager(s.host.ID(), s.host.Peerstore(), store, providerOptions...)
			if err != nil {
				return nil, fmt.Errorf("provider manager: %w", err)
			}
		}
		options = append(options, dht.ProviderStore(providerStore))
	}
	if config.RequestHook == "log" {
		logger := s.logger
		if logger == nil {
			logger = slog.Default()
		}
		options = append(options, dht.OnRequestHook(func(_ context.Context, stream network.Stream, request *pb.Message) {
			logger.Info("DHT request", "node", s.config.Node.ID, "peer", stream.Conn().RemotePeer(), "type", request.GetType().String())
		}))
	}
	instance, err := dht.New(ctx, s.host, options...)
	if err != nil && providerStore != nil {
		err = errors.Join(err, providerStore.Close())
	}
	return instance, err
}

type kademliaFilter struct {
	policy      string
	allow, deny []*net.IPNet
}

func compileKademliaFilter(config model.KademliaFilterConfig) kademliaFilter {
	result := kademliaFilter{policy: config.Policy}
	for _, text := range config.AllowCIDRs {
		_, network, _ := net.ParseCIDR(text)
		result.allow = append(result.allow, network)
	}
	for _, text := range config.DenyCIDRs {
		_, network, _ := net.ParseCIDR(text)
		result.deny = append(result.deny, network)
	}
	return result
}

// A deny match rejects the entire query/RT candidate, because those callbacks
// cannot remove a denied address from the dialer's candidate address list.
func (f kademliaFilter) acceptPeer(addresses []ma.Multiaddr) bool {
	allowed := len(f.allow) == 0
	for _, address := range addresses {
		ip, err := manet.ToIP(address)
		if err != nil {
			// CIDR admission cannot prove the resolved address of a hostname.
			if len(f.allow) > 0 || len(f.deny) > 0 {
				return false
			}
			continue
		}
		for _, block := range f.deny {
			if block.Contains(ip) {
				return false
			}
		}
		for _, block := range f.allow {
			if block.Contains(ip) {
				allowed = true
			}
		}
	}
	return allowed
}

func (f kademliaFilter) hasNonLoopback(addresses []ma.Multiaddr) bool {
	for _, address := range addresses {
		ip, err := manet.ToIP(address)
		if err == nil && ip.IsGlobalUnicast() && !ip.IsLoopback() {
			return true
		}
	}
	return false
}

func kademliaStaticBootstrap(addresses []string) []corepeer.AddrInfo {
	peers := make(map[corepeer.ID][]ma.Multiaddr)
	for _, raw := range addresses {
		address, _ := ma.NewMultiaddr(raw)
		info, _ := corepeer.AddrInfoFromP2pAddr(address)
		duplicate := false
		for _, previous := range peers[info.ID] {
			if previous.Equal(info.Addrs[0]) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			peers[info.ID] = append(peers[info.ID], info.Addrs[0])
		}
	}
	result := make([]corepeer.AddrInfo, 0, len(peers))
	for id, addresses := range peers {
		result = append(result, corepeer.AddrInfo{ID: id, Addrs: addresses})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func kademliaDatastore(kind string) ds.Batching {
	if kind == "null" {
		return ds.NewNullDatastore()
	}
	return dssync.MutexWrap(ds.NewMapDatastore())
}

// The bytes policy intentionally accepts opaque experiment records and chooses
// the lexicographically greatest bytes, independent of arrival order.
type kademliaBytesValidator struct{}

func (kademliaBytesValidator) Validate(string, []byte) error { return nil }
func (kademliaBytesValidator) Select(_ string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("cannot select from empty records")
	}
	selected := 0
	for index := 1; index < len(values); index++ {
		if bytes.Compare(values[index], values[selected]) > 0 {
			selected = index
		}
	}
	return selected, nil
}

type kademliaRejectValidator struct{}

func (kademliaRejectValidator) Validate(string, []byte) error {
	return fmt.Errorf("record rejected by configured namespace policy")
}
func (kademliaRejectValidator) Select(string, [][]byte) (int, error) {
	return 0, fmt.Errorf("record rejected by configured namespace policy")
}

type kademliaNullProviderStore struct{}

func (kademliaNullProviderStore) AddProvider(context.Context, []byte, corepeer.AddrInfo) error {
	return nil
}
func (kademliaNullProviderStore) GetProviders(context.Context, []byte) ([]corepeer.AddrInfo, error) {
	return nil, nil
}
func (kademliaNullProviderStore) Close() error { return nil }
