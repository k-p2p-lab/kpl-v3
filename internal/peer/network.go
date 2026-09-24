package peer

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/netem"
	"github.com/multiformats/go-multiaddr"
)

type routeSource func(context.Context, string) (net.IP, error)

// Resolve the Agent-facing address before installing netem: scope all can
// otherwise drop the DNS request needed to discover the Agent's overlay IP.
func preparePeerNetwork(ctx context.Context, config model.PeerProcessConfig, route routeSource, apply func(context.Context, model.PeerProcessConfig) error) (model.PeerProcessConfig, error) {
	resolved, err := resolveDockerP2PListen(ctx, config, route)
	if err != nil {
		return config, err
	}
	if err := apply(ctx, resolved); err != nil {
		return config, err
	}
	return resolved, nil
}

func resolveDockerP2PListen(ctx context.Context, config model.PeerProcessConfig, route routeSource) (model.PeerProcessConfig, error) {
	ip, port, err := dockerP2PAddress(config.P2PListen)
	if err != nil {
		return config, err
	}
	routeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	localIP, err := route(routeCtx, config.AgentURL)
	if err != nil {
		return config, fmt.Errorf("resolve Docker peer IPv4 route to Agent: %w", err)
	}
	if localIP.To4() == nil || !localIP.IsGlobalUnicast() || localIP.IsLoopback() {
		return config, fmt.Errorf("Docker peer requires a non-loopback IPv4 route to its Agent")
	}
	if !ip.IsUnspecified() && !ip.Equal(localIP) {
		return config, fmt.Errorf("Docker P2P listen address %s must match the Agent-facing IPv4 address %s", ip, localIP)
	}
	config.P2PListen = fmt.Sprintf("/ip4/%s/tcp/%d", localIP.String(), port)
	return config, nil
}

func dockerP2PAddress(value string) (net.IP, int, error) {
	address, err := multiaddr.NewMultiaddr(value)
	if err != nil {
		return nil, 0, fmt.Errorf("Docker peer requires an IPv4 TCP P2P listen address: %w", err)
	}
	protocols := address.Protocols()
	if len(protocols) != 2 || protocols[0].Code != multiaddr.P_IP4 || protocols[1].Code != multiaddr.P_TCP {
		return nil, 0, fmt.Errorf("Docker peer requires an IPv4 TCP P2P listen address")
	}
	rawIP, _ := address.ValueForProtocol(multiaddr.P_IP4)
	rawPort, _ := address.ValueForProtocol(multiaddr.P_TCP)
	ip := net.ParseIP(rawIP)
	port, portErr := strconv.Atoi(rawPort)
	if ip == nil || ip.To4() == nil || ip.IsLoopback() || !ip.IsUnspecified() && !ip.IsGlobalUnicast() || portErr != nil || port < 1 || port > 65535 {
		return nil, 0, fmt.Errorf("Docker peer requires a non-loopback IPv4 TCP address and port between 1 and 65535")
	}
	return ip, port, nil
}

// UDP connect chooses a source address through the kernel routing table without
// sending a UDP payload or requiring the Agent to expose a UDP listener.
func agentRouteSourceIPv4(ctx context.Context, agentURL string) (net.IP, error) {
	endpoint, err := url.Parse(agentURL)
	if err != nil {
		return nil, fmt.Errorf("invalid Agent URL: %w", err)
	}
	if endpoint.Hostname() == "" || endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("Agent URL must use http or https with a hostname")
	}
	port := endpoint.Port()
	if port == "" {
		port = "80"
		if endpoint.Scheme == "https" {
			port = "443"
		}
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "udp4", net.JoinHostPort(endpoint.Hostname(), port))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	address, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || address.IP.To4() == nil {
		return nil, fmt.Errorf("Agent route did not select an IPv4 source address")
	}
	return append(net.IP(nil), address.IP...), nil
}

// Apply impairments before opening any P2P connections. The Agent validates
// the Swarm overlay and creates a private peer namespace: /.dockerenv alone
// does not distinguish --network=host or shared container namespaces from an
// isolated container.
func prepareNetwork(ctx context.Context, config model.PeerProcessConfig) error {
	if err := config.NodeConfig.WithDefaults().Validate(); err != nil {
		return fmt.Errorf("validate peer config before network setup: %w", err)
	}
	if err := requirePeerContainer(runtime.GOOS, os.Stat); err != nil {
		return err
	}
	if !config.NodeConfig.Network.Enabled() {
		return nil
	}
	if _, port, err := dockerP2PAddress(config.P2PListen); err != nil || port != netem.DefaultP2PPort {
		return fmt.Errorf("Docker network conditions require the fixed P2P TCP port 20000")
	}
	if err := netem.Apply(ctx, config.NodeConfig.Network, 20000); err != nil {
		return fmt.Errorf("apply peer network conditions: %w", err)
	}
	return nil
}

func requirePeerContainer(goos string, stat func(string) (os.FileInfo, error)) error {
	if goos != "linux" {
		return fmt.Errorf("peers require Linux Docker containers on a Swarm overlay")
	}
	if _, err := stat("/.dockerenv"); err != nil {
		return fmt.Errorf("refusing peer startup outside a Docker container: %w", err)
	}
	return nil
}
