package experimentnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Inspect only the fields needed for gateway ownership and attachment proof.
// Container configuration and environment contain credentials.
const gatewayInspectionFormat = `{"id":{{json .Id}},"name":{{json .Name}},"running":{{json .State.Running}},"pid":{{json .State.Pid}},"labels":{{json .Config.Labels}},"networks":{{json .NetworkSettings.Networks}}}`

type gatewayNetworkEndpoint struct {
	IPAddress string
	NetworkID string
}

type gatewayInspection struct {
	ID, Name string
	Running  bool
	PID      int
	Labels   map[string]string
	Networks map[string]gatewayNetworkEndpoint
}

func (m *Manager) acknowledgedGatewayID(network *networkInfo) (string, error) {
	id := ""
	for candidate, raw := range network.Containers {
		var endpoint struct{ Name string }
		if err := json.Unmarshal(raw, &endpoint); err != nil {
			return "", fmt.Errorf("inspect acknowledged gateway endpoint: %w", err)
		}
		if endpoint.Name != m.config.Stack+"-network-gateway" {
			continue
		}
		if !identifier.MatchString(candidate) || id != "" {
			return "", errors.New("acknowledged gateway endpoint identity is invalid or ambiguous")
		}
		id = candidate
	}
	if id == "" {
		return "", errors.New("acknowledged gateway is no longer attached to the Peer overlay")
	}
	return id, nil
}

func (m *Manager) inspectGateway(ctx context.Context, request model.ExperimentNetworkRequest, id, networkID, subnet string) (model.ExperimentNetwork, error) {
	var result model.ExperimentNetwork
	output, err := m.command(ctx, nil, "inspect", "--type", "container", "--format", "{{json .NetworkSettings.Networks}}", m.config.SelfContainer)
	if err != nil {
		return result, err
	}
	var managerNetworks map[string]gatewayNetworkEndpoint
	if err := json.Unmarshal(output, &managerNetworks); err != nil {
		return result, fmt.Errorf("inspect Manager control network: %w", err)
	}
	controlID := managerNetworks[m.config.ControlNetwork].NetworkID
	if !identifier.MatchString(controlID) {
		return result, errors.New("Manager control network identity could not be verified")
	}
	output, err = m.command(ctx, nil, "inspect", "--type", "container", "--format", gatewayInspectionFormat, id)
	if err != nil {
		return result, err
	}
	var gateway gatewayInspection
	if err := json.Unmarshal(output, &gateway); err != nil {
		return result, fmt.Errorf("inspect gateway identity: %w", err)
	}
	if gateway.ID != id || gateway.Name != "/"+m.config.Stack+"-network-gateway" || !gateway.Running || gateway.PID <= 0 ||
		gateway.Labels["io.kpl.network-gateway"] != m.config.Stack || gateway.Labels["com.docker.stack.namespace"] != m.config.Stack || gateway.Labels["io.kpl.execution"] != request.Epoch {
		return result, errors.New("gateway container ownership, execution identity or running state could not be verified")
	}
	controlEndpoint, peerEndpoint := gateway.Networks[m.config.ControlNetwork], gateway.Networks[m.config.PeerNetwork]
	control, peer := net.ParseIP(controlEndpoint.IPAddress), net.ParseIP(peerEndpoint.IPAddress)
	_, allocation, err := net.ParseCIDR(subnet)
	if err != nil || len(gateway.Networks) != 2 || invalidGatewayAddress(control) || invalidGatewayAddress(peer) || control.Equal(peer) ||
		controlEndpoint.NetworkID != controlID || peerEndpoint.NetworkID != networkID || !allocation.Contains(peer) {
		return result, errors.New("gateway network attachments could not be verified")
	}
	return model.ExperimentNetwork{RunID: request.RunID, Epoch: request.Epoch, NetworkID: networkID, NetworkName: m.config.PeerNetwork,
		GatewayURL: "http://" + net.JoinHostPort(control.String(), "18081"), PeerGatewayURL: "http://" + net.JoinHostPort(peer.String(), "18081")}, nil
}

func invalidGatewayAddress(ip net.IP) bool {
	return ip == nil || ip.To4() == nil || ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast()
}
