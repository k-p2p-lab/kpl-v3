package experimentnet

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,199}$`)
var dockerEndpointID = regexp.MustCompile(`^[a-f0-9]{64}$`)

type ManagerConfig struct {
	Stack, PeerNetwork, ControlNetwork, Subnet, ControllerURL, Token, SelfContainer, DataDir string
}

type Manager struct {
	config  ManagerConfig
	mu      sync.Mutex
	command func(context.Context, io.Reader, ...string) ([]byte, error)
	client  *http.Client
}

type networkInfo struct {
	ID                                                    string `json:"Id"`
	Name, Driver, Scope                                   string
	Attachable, Internal, Ingress, ConfigOnly, EnableIPv6 bool
	ConfigFrom                                            struct{ Network string }
	IPAM                                                  struct {
		Driver  string
		Options map[string]string
		Config  []struct {
			Subnet, Gateway, IPRange string
			AuxiliaryAddresses       map[string]string
		}
	}
	Labels, Options map[string]string
	Containers      map[string]json.RawMessage
}

type networkAllocation struct{ Subnet, Gateway string }

func NewManager(config ManagerConfig) (*Manager, error) {
	if !identifier.MatchString(config.Stack) || !identifier.MatchString(config.PeerNetwork) || !identifier.MatchString(config.ControlNetwork) || config.PeerNetwork == config.ControlNetwork || config.Token == "" || config.SelfContainer == "" || config.DataDir == "" {
		return nil, errors.New("network-manager requires distinct network names, stack, authentication, task and data directory")
	}
	if _, err := controlURL(config.ControllerURL); err != nil {
		return nil, err
	}
	return &Manager{config: config, command: dockerCommand, client: &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// Limit daemon output, including failure messages, without buffering inventories.
type limitedOutput struct{ bytes.Buffer }

func (b *limitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if b.Len() < 1<<20 {
		_, _ = b.Buffer.Write(p[:min(len(p), (1<<20)-b.Len())])
	}
	return n, nil
}

func dockerCommand(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = input
	var stdout, stderr limitedOutput
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("Docker %s: %w: %.4096s", args[0], err, stderr.String())
	}
	if stdout.Len() >= 1<<20 {
		return nil, errors.New("Docker response exceeds limit")
	}
	return stdout.Bytes(), nil
}

func (m *Manager) networkIDs(ctx context.Context) ([]string, error) {
	// Listing distinguishes confirmed absence from an ambiguous inspect failure.
	// --quiet alone still truncates IDs; inspect returns the complete identity.
	ids, err := m.command(ctx, nil, "network", "ls", "--quiet", "--no-trunc", "--filter", "name=^"+regexp.QuoteMeta(m.config.PeerNetwork)+"$")
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(ids))
	for _, id := range fields {
		if !identifier.MatchString(id) {
			return nil, errors.New("invalid Peer network ID in Docker list")
		}
	}
	return fields, nil
}

func (m *Manager) inspect(ctx context.Context) (*networkInfo, error) {
	fields, err := m.networkIDs(ctx)
	if err != nil || len(fields) == 0 {
		return nil, err
	}
	if len(fields) != 1 {
		return nil, fmt.Errorf("ambiguous Peer network %q: IDs=%q", m.config.PeerNetwork, fields)
	}
	data, err := m.command(ctx, nil, "network", "inspect", fields[0])
	if err != nil {
		return nil, err
	}
	var networks []networkInfo
	if err := json.Unmarshal(data, &networks); err != nil || len(networks) != 1 {
		return nil, errors.New("invalid Peer network inspection")
	}
	n := &networks[0]
	if n.ID != fields[0] || n.Name != m.config.PeerNetwork {
		return nil, fmt.Errorf("Peer network identity mismatch: listed ID=%q, inspected ID=%q, name=%q (expected %q)", fields[0], n.ID, n.Name, m.config.PeerNetwork)
	}
	if n.Driver != "overlay" || n.Scope != "swarm" || !n.Attachable || n.Internal || n.Ingress || n.ConfigOnly || n.EnableIPv6 || n.ConfigFrom.Network != "" {
		return nil, fmt.Errorf("Peer network must be an attachable IPv4 Swarm overlay: driver=%q scope=%q attachable=%t internal=%t ingress=%t configOnly=%t ipv6=%t configFrom=%q", n.Driver, n.Scope, n.Attachable, n.Internal, n.Ingress, n.ConfigOnly, n.EnableIPv6, n.ConfigFrom.Network)
	}
	if n.IPAM.Driver != "default" || len(n.IPAM.Options) != 0 || len(n.IPAM.Config) != 1 {
		return nil, fmt.Errorf("Peer network must use default IPAM with one IPv4 allocation and no custom IPAM options: driver=%q options=%d allocations=%d", n.IPAM.Driver, len(n.IPAM.Options), len(n.IPAM.Config))
	}
	if n.Labels["io.kpl.application"] != "kp2plab-v3" || n.Labels["io.kpl.stack"] != m.config.Stack {
		return nil, errors.New("Peer network ownership mismatch")
	}
	for key := range n.Labels {
		if key != "io.kpl.application" && key != "io.kpl.stack" && key != "io.kpl.execution" {
			return nil, errors.New("custom Peer network labels are not supported")
		}
	}
	for key := range n.Options {
		if key != "com.docker.network.driver.overlay.vxlanid_list" {
			return nil, errors.New("custom Peer network options are not supported")
		}
	}
	ipam := n.IPAM.Config[0]
	ip, subnet, err := net.ParseCIDR(ipam.Subnet)
	if err != nil || ip.To4() == nil || !ip.Equal(subnet.IP) || ipam.IPRange != "" || len(ipam.AuxiliaryAddresses) != 0 {
		return nil, errors.New("unsupported Peer network allocation")
	}
	if m.config.Subnet != "" && ipam.Subnet != m.config.Subnet {
		return nil, errors.New("Peer network subnet differs from configured subnet")
	}
	return n, nil
}

func (m *Manager) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !authorized(r, m.config.Token) {
			http.Error(w, "valid bearer token required", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/network/prepare" {
			http.NotFound(w, r)
			return
		}
		var request model.ExperimentNetworkRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid network request", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected a single network request", http.StatusBadRequest)
			return
		}
		result, err := m.prepare(r.Context(), request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}

func RunManager(ctx context.Context, config ManagerConfig) error {
	m, err := NewManager(config)
	if err != nil {
		return err
	}
	defer m.client.CloseIdleConnections()
	return serve(ctx, ":18082", m.Handler())
}

func (m *Manager) prepare(ctx context.Context, request model.ExperimentNetworkRequest) (model.ExperimentNetwork, error) {
	var result model.ExperimentNetwork
	if !identifier.MatchString(request.RunID) || !identifier.MatchString(request.Epoch) || request.RequestedAt.IsZero() || len(request.Agents) == 0 || len(request.Agents) > 1024 {
		return result, errors.New("invalid experiment network identity or Agent set")
	}
	for id, raw := range request.Agents {
		if _, err := controlURL(raw); err != nil || !identifier.MatchString(id) {
			return result, errors.New("invalid Agent endpoint")
		}
	}
	if !m.mu.TryLock() {
		return result, errors.New("network preparation is already in progress")
	}
	defer m.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// A restarted Controller or canceled HTTP caller cannot replay an older
	// preparation after a newer attempt. Persist before the first mutation.
	var previous model.ExperimentNetworkRequest
	if data, err := os.ReadFile(filepath.Join(m.config.DataDir, "request.json")); err == nil {
		if err := json.Unmarshal(data, &previous); err != nil {
			return result, err
		}
		if previous.Epoch != request.Epoch && !request.RequestedAt.After(previous.RequestedAt) {
			return result, errors.New("stale network preparation")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := m.persist("request.json", request); err != nil {
		return result, err
	}
	if err := m.clusterReady(ctx); err != nil {
		return result, err
	}
	network, err := m.inspect(ctx)
	if err != nil {
		return result, err
	}
	if network != nil && network.Labels["io.kpl.execution"] == request.Epoch {
		data, err := os.ReadFile(filepath.Join(m.config.DataDir, "active.json"))
		if err == nil && json.Unmarshal(data, &result) == nil && result.NetworkID == network.ID && result.Epoch == request.Epoch && result.RunID == request.RunID {
			if err := m.gatewayReady(ctx, result.GatewayURL); err != nil {
				return model.ExperimentNetwork{}, err
			}
			return result, nil
		}
		return model.ExperimentNetwork{}, errors.New("previous preparation is incomplete; start a new experiment attempt after cleanup")
	}
	var allocation networkAllocation
	if network != nil {
		if err := m.unusedNetwork(ctx, network); err != nil {
			return result, err
		}
		allocation = networkAllocation{network.IPAM.Config[0].Subnet, network.IPAM.Config[0].Gateway}
		if err := m.persist("allocation.json", allocation); err != nil {
			return result, err
		}
	} else {
		data, err := os.ReadFile(filepath.Join(m.config.DataDir, "allocation.json"))
		if err != nil {
			return result, fmt.Errorf("Peer network missing and original allocation unavailable: %w", err)
		}
		if err := json.Unmarshal(data, &allocation); err != nil {
			return result, err
		}
	}
	if err := validateAllocation(allocation, m.config.Subnet); err != nil {
		return result, err
	}
	// All Agents have fenced admission and confirmed Peer removal. Docker still
	// rejects deletion if a service or remote attachment remains; never force it.
	if err := m.removeGateway(ctx); err != nil {
		return result, err
	}
	if network != nil {
		if _, err := m.command(ctx, nil, "network", "rm", network.ID); err != nil {
			return result, err
		}
	}
	oldID := ""
	if network != nil {
		oldID = network.ID
	}
	id, err := m.createFreshNetwork(ctx, request, allocation, oldID)
	if err != nil {
		return result, err
	}
	result, err = m.createGateway(ctx, request, id, allocation.Subnet)
	if err != nil {
		return model.ExperimentNetwork{}, err
	}
	if err := m.persist("active.json", result); err != nil {
		return model.ExperimentNetwork{}, err
	}
	return result, nil
}

func validateAllocation(allocation networkAllocation, configured string) error {
	ip, subnet, err := net.ParseCIDR(allocation.Subnet)
	if err != nil || ip.To4() == nil || !ip.Equal(subnet.IP) {
		return errors.New("invalid saved network subnet")
	}
	ones, _ := subnet.Mask.Size()
	if ones < 1 || ones > 30 || (configured != "" && configured != allocation.Subnet) {
		return errors.New("saved network subnet differs from configuration or has no usable addresses")
	}
	if allocation.Gateway != "" {
		gateway := net.ParseIP(allocation.Gateway)
		if gateway == nil || gateway.To4() == nil || !subnet.Contains(gateway) || gateway.Equal(subnet.IP) {
			return errors.New("invalid saved network gateway")
		}
		broadcast := append(net.IP(nil), subnet.IP.To4()...)
		for i := range broadcast {
			broadcast[i] |= ^subnet.Mask[i]
		}
		if gateway.Equal(broadcast) {
			return errors.New("network gateway is a broadcast address")
		}
	}
	return nil
}

func (m *Manager) unusedNetwork(ctx context.Context, network *networkInfo) error {
	// Refuse legacy deployments with Controller/Agent services still attached,
	// and unrelated local endpoints, before removing even our own gateway.
	data, err := m.command(ctx, nil, "service", "ls", "--quiet")
	if err != nil {
		return err
	}
	ids := strings.Fields(string(data))
	if len(ids) > 1024 {
		return errors.New("service inventory exceeds network reset limit")
	}
	for _, id := range ids {
		if !identifier.MatchString(id) {
			return errors.New("invalid service ID")
		}
	}
	if len(ids) > 0 {
		args := append([]string{"service", "inspect", "--format", "{{range .Spec.TaskTemplate.Networks}}{{println .Target}}{{end}}"}, ids...)
		data, err = m.command(ctx, nil, args...)
		if err != nil {
			return err
		}
		for _, target := range strings.Fields(string(data)) {
			if target == network.ID {
				return errors.New("a Swarm service still uses the Peer overlay; move control services to a separate network before enabling automatic reset")
			}
		}
	}
	for id, raw := range network.Containers {
		var endpoint struct{ Name, EndpointID string }
		if err := json.Unmarshal(raw, &endpoint); err != nil {
			return fmt.Errorf("invalid Peer network endpoint %q: %w", id, err)
		}
		// Docker reports its per-overlay LB sandbox in Containers, using
		// lb-<network name> and <network name>-endpoint. It has no container
		// object to inspect. Match that exact pair, never just the lb- prefix
		// or a container's display name, and leave its removal to Docker.
		if network.Driver == "overlay" && network.Scope == "swarm" && !network.Ingress &&
			id == "lb-"+network.Name && endpoint.Name == network.Name+"-endpoint" && dockerEndpointID.MatchString(endpoint.EndpointID) {
			continue
		}
		if !identifier.MatchString(id) {
			return fmt.Errorf("invalid Peer network endpoint identity %q", id)
		}
		data, err := m.command(ctx, nil, "inspect", "--type", "container", "--format", "{{index .Config.Labels \"io.kpl.network-gateway\"}}", id)
		if err != nil {
			return fmt.Errorf("cannot verify Peer network endpoint %q (name=%q): %w", id, endpoint.Name, err)
		}
		if strings.TrimSpace(string(data)) != m.config.Stack {
			return fmt.Errorf("Peer network has an endpoint other than the experiment gateway: id=%q name=%q", id, endpoint.Name)
		}
	}
	return nil
}

func (m *Manager) persist(name string, value any) error {
	if err := os.MkdirAll(m.config.DataDir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	path := filepath.Join(m.config.DataDir, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	err = errors.Join(writeErr, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

func (m *Manager) clusterReady(ctx context.Context) error {
	data, err := m.command(ctx, nil, "info", "--format", "{{.Swarm.ControlAvailable}}")
	if err != nil || strings.TrimSpace(string(data)) != "true" {
		return errors.New("network-manager requires a Swarm manager")
	}
	data, err = m.command(ctx, nil, "node", "ls", "--format", "{{.Status}}")
	if err != nil {
		return err
	}
	states := strings.Fields(string(data))
	if len(states) == 0 {
		return errors.New("cannot verify Swarm nodes")
	}
	for _, state := range states {
		if state != "Ready" {
			return errors.New("all Swarm nodes must be Ready before network reset")
		}
	}
	return nil
}

func (m *Manager) removeGateway(ctx context.Context) error {
	data, err := m.command(ctx, nil, "ps", "--all", "--quiet", "--no-trunc", "--filter", "label=io.kpl.network-gateway="+m.config.Stack)
	if err != nil {
		return err
	}
	for _, id := range strings.Fields(string(data)) {
		if !identifier.MatchString(id) {
			return errors.New("invalid gateway container ID")
		}
		if _, err := m.command(ctx, nil, "rm", "--force", id); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) createGateway(ctx context.Context, request model.ExperimentNetworkRequest, networkID, subnet string) (model.ExperimentNetwork, error) {
	var result model.ExperimentNetwork
	image, err := m.command(ctx, nil, "inspect", "--format", "{{.Image}}", m.config.SelfContainer)
	if err != nil {
		return result, err
	}
	imageID := strings.TrimSpace(string(image))
	if !strings.HasPrefix(imageID, "sha256:") {
		return result, errors.New("cannot resolve network gateway image")
	}
	config := GatewayConfig{Token: m.config.Token, ControllerURL: m.config.ControllerURL, Agents: request.Agents, Subnet: subnet}
	data, err := json.Marshal(config)
	if err != nil {
		return result, err
	}
	var archive bytes.Buffer
	tw := tar.NewWriter(&archive)
	if err := tw.WriteHeader(&tar.Header{Name: "etc/kpl-network-gateway.json", Mode: 0600, Size: int64(len(data))}); err != nil {
		return result, err
	}
	if _, err := tw.Write(data); err != nil {
		return result, err
	}
	if err := tw.Close(); err != nil {
		return result, err
	}
	output, err := m.command(ctx, nil, "create", "--name", m.config.Stack+"-network-gateway", "--network", m.config.ControlNetwork,
		"--label", "io.kpl.network-gateway="+m.config.Stack, "--label", "io.kpl.execution="+request.Epoch,
		"--label", "com.docker.stack.namespace="+m.config.Stack, "--label", "io.kpl.resource-monitor=true",
		"--user", "0:0", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--init",
		"--log-driver", "json-file", "--log-opt", "max-size=10m", "--log-opt", "max-file=2",
		imageID, "network-gateway", "--config", "/etc/kpl-network-gateway.json")
	if err != nil {
		return result, err
	}
	id := strings.TrimSpace(string(output))
	if !identifier.MatchString(id) {
		return result, errors.New("invalid gateway container ID")
	}
	if _, err := m.command(ctx, &archive, "cp", "-", id+":/"); err != nil {
		return result, err
	}
	if _, err := m.command(ctx, nil, "network", "connect", networkID, id); err != nil {
		return result, err
	}
	if _, err := m.command(ctx, nil, "start", id); err != nil {
		return result, err
	}
	output, err = m.command(ctx, nil, "inspect", "--format", "{{json .NetworkSettings.Networks}}", id)
	if err != nil {
		return result, err
	}
	var endpoints map[string]struct {
		IPAddress string
		NetworkID string
	}
	if err := json.Unmarshal(output, &endpoints); err != nil {
		return result, err
	}
	control, peer := net.ParseIP(endpoints[m.config.ControlNetwork].IPAddress), net.ParseIP(endpoints[m.config.PeerNetwork].IPAddress)
	if control == nil || control.To4() == nil || peer == nil || peer.To4() == nil || endpoints[m.config.PeerNetwork].NetworkID != networkID {
		return result, errors.New("gateway network attachments could not be verified")
	}
	result = model.ExperimentNetwork{RunID: request.RunID, Epoch: request.Epoch, NetworkID: networkID, NetworkName: m.config.PeerNetwork,
		GatewayURL: "http://" + net.JoinHostPort(control.String(), "18081"), PeerGatewayURL: "http://" + net.JoinHostPort(peer.String(), "18081")}
	if err := m.gatewayReady(ctx, result.GatewayURL); err != nil {
		return model.ExperimentNetwork{}, err
	}
	return result, nil
}

func (m *Manager) gatewayReady(ctx context.Context, endpoint string) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/health", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+m.config.Token)
		response, err := m.client.Do(req)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return nil
			}
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("experiment network gateway not ready: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
