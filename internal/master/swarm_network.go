package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
)

const swarmResetFormat = `{{.Name}}|{{.Driver}}|{{.Scope}}|{{.Attachable}}|{{.Internal}}|{{.Ingress}}|{{.ConfigOnly}}|{{.EnableIPv6}}|{{.ConfigFrom.Network}}|{{.IPAM.Driver}}|{{len .IPAM.Options}}|{{len .IPAM.Config}}|{{len .Containers}}|{{index .Labels "io.kpl.application"}}|{{index .Labels "io.kpl.stack"}}|{{range $key, $value := .Labels}}{{if and (ne $key "io.kpl.application") (ne $key "io.kpl.stack") (ne $key "io.kpl.execution")}}custom{{end}}{{end}}|{{range $key, $value := .Options}}{{if ne $key "com.docker.network.driver.overlay.vxlanid_list"}}custom{{end}}{{end}}`
const swarmIPAMFormat = `{{range .IPAM.Config}}{{json .Subnet}}|{{json .Gateway}}|{{json .IPRange}}|{{len .AuxAddress}}{{end}}`

func (s *swarm) peerNetworkUnused(ctx context.Context) error {
	services, err := s.output(ctx, "service", "ls", "--quiet", "--filter", "label=com.docker.stack.namespace="+s.stack)
	if err != nil {
		return err
	}
	if services != "" {
		return errors.New("Peer network reset requires all stack services removed first; no services were stopped")
	}
	out, err := s.output(ctx, "node", "ls", "--quiet")
	if err != nil {
		return err
	}
	nodes, err := swarmIDs(out)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("cannot verify an empty Swarm node list")
	}
	states, err := s.output(ctx, append([]string{"node", "inspect", "--format", "{{.Status.State}}"}, nodes...)...)
	if err != nil {
		return err
	}
	fields := strings.Fields(states)
	if len(fields) != len(nodes) {
		return errors.New("Docker returned incomplete node readiness information")
	}
	for _, state := range fields {
		if state != "ready" {
			return errors.New("every Swarm node must be Ready before resetting the Peer network")
		}
	}
	return nil
}

func parseSwarmIPAM(raw string) (subnet, gateway string, err error) {
	parts := strings.Split(raw, "|")
	if len(parts) != 4 || parts[2] != `""` || parts[3] != "0" {
		return "", "", errors.New("Peer network reset does not support IP ranges or auxiliary addresses")
	}
	if json.Unmarshal([]byte(parts[0]), &subnet) != nil || json.Unmarshal([]byte(parts[1]), &gateway) != nil {
		return "", "", errors.New("Docker returned invalid IPv4 allocation")
	}
	encodedSubnet, _ := json.Marshal(subnet)
	encodedGateway, _ := json.Marshal(gateway)
	if parts[0] != string(encodedSubnet) || parts[1] != string(encodedGateway) {
		return "", "", errors.New("Docker returned non-string or ambiguous IPv4 allocation")
	}
	if err := ValidateSetting("KPL_PEER_SUBNET", subnet); err != nil || subnet == "" {
		return "", "", errors.New("Peer network has no valid IPv4 subnet to preserve")
	}
	prefix, err := netip.ParsePrefix(subnet)
	if err != nil || !prefix.Addr().Is4() {
		return "", "", errors.New("Peer subnet must be IPv4")
	}
	if gateway != "" {
		addr, err := netip.ParseAddr(gateway)
		if err != nil || !addr.Is4() || !prefix.Contains(addr) || addr == prefix.Addr() {
			return "", "", errors.New("Docker returned an invalid IPv4 gateway")
		}
		network := prefix.Addr().As4()
		broadcast := network
		hostBits := 32 - prefix.Bits()
		for bit := 0; bit < hostBits; bit++ {
			broadcast[3-bit/8] |= byte(1 << uint(bit%8))
		}
		if addr == netip.AddrFrom4(broadcast) {
			return "", "", errors.New("Docker returned a broadcast gateway")
		}
	}
	return subnet, gateway, nil
}

func (s *swarm) resetPeerNetwork(ctx context.Context) error {
	if err := s.peerNetworkUnused(ctx); err != nil {
		return err
	}
	id, err := s.output(ctx, "network", "inspect", "--format", "{{.Id}}", s.network)
	if err != nil {
		return err
	}
	if !swarmID.MatchString(id) {
		return errors.New("Docker returned invalid or ambiguous Peer network ID")
	}
	expected := s.network + "|overlay|swarm|true|false|false|false|false||default|0|1|0|" + swarmApplication + "|" + s.stack + "||"
	metadata, err := s.output(ctx, "network", "inspect", "--format", swarmResetFormat, id)
	if err != nil {
		return err
	}
	if metadata != expected {
		return errors.New("Peer network reset requires an empty helper-owned IPv4 overlay with default IPAM and no custom settings")
	}
	ipam, err := s.output(ctx, "network", "inspect", "--format", swarmIPAMFormat, id)
	if err != nil {
		return err
	}
	subnet, gateway, err := parseSwarmIPAM(ipam)
	if err != nil {
		return err
	}
	if configured := s.config["KPL_PEER_SUBNET"]; configured != "" && configured != subnet {
		return errors.New("existing Peer subnet differs from KPL_PEER_SUBNET; reset does not change subnet")
	}
	args := []string{"network", "create", "--driver", "overlay", "--attachable", "--label", "io.kpl.application=" + swarmApplication, "--label", "io.kpl.stack=" + s.stack, "--subnet", subnet}
	if gateway != "" {
		args = append(args, "--gateway", gateway)
	}
	args = append(args, s.network)
	if err := s.peerNetworkUnused(ctx); err != nil {
		return err
	}
	metadata, err = s.output(ctx, "network", "inspect", "--format", swarmResetFormat, id)
	if err != nil {
		return err
	}
	if metadata != expected {
		return errors.New("Peer network changed or acquired containers before reset; no changes made")
	}
	fmt.Fprintf(s.a.Stdout, "Recreating Peer network %s (%s), preserving subnet %s and gateway %s.\n", s.network, id, subnet, gateway)
	// Every argument here comes from validated identifiers or IP addresses.
	// Emit the exact reconstruction command before the first destructive step.
	fmt.Fprintln(s.a.Stdout, "Recovery command (only if the network is absent): docker "+strings.Join(args, " "))
	if err := s.run(ctx, "network", "rm", id); err != nil {
		return fmt.Errorf("Peer network removal failed or is unconfirmed; no replacement requested: %w", err)
	}
	newID, err := s.output(ctx, args...)
	if err != nil {
		return fmt.Errorf("Peer network was removed but recreation failed or is unconfirmed; use the recovery command only if absent: %w", err)
	}
	if !swarmID.MatchString(newID) || newID == id {
		return errors.New("Peer network recreation returned invalid or unchanged ID; inspect Docker before deploying")
	}
	metadata, err = s.output(ctx, "network", "inspect", "--format", swarmResetFormat, newID)
	if err != nil {
		return err
	}
	currentIPAM, err := s.output(ctx, "network", "inspect", "--format", swarmIPAMFormat, newID)
	if err != nil {
		return err
	}
	if metadata != expected || currentIPAM != ipam {
		return errors.New("recreated Peer network settings differ from original; inspect Docker before deploying")
	}
	fmt.Fprintf(s.a.Stdout, "Peer network recreated with new ID %s. Services, containers, placement labels, configuration and data volumes were not changed. Deploy separately, then verify cross-host connectivity.\n", newID)
	return nil
}

func (s *swarm) captureGatewayTasks(ctx context.Context) ([]string, error) {
	if !s.present["agent"] {
		return nil, errors.New("Agent task history is required before gateway cleanup")
	}
	control := s.config["KPL_CONTROL_NODE_ID"]
	if control == "" {
		return nil, errors.New("gateway cleanup requires configured control node")
	}
	node, err := s.output(ctx, "info", "--format", "{{.Swarm.NodeID}}")
	if err != nil {
		return nil, err
	}
	if node != control {
		return nil, errors.New("run remove against the control node Docker daemon to clean its gateway")
	}
	manager, err := s.captureTasks(ctx, s.stack+"_network-manager", "")
	if err != nil {
		return nil, err
	}
	agents, err := s.captureTasks(ctx, s.stack+"_agent", "")
	if err != nil {
		return nil, err
	}
	return append(manager, agents...), nil
}
func (s *swarm) prepareGatewayRemoval(ctx context.Context) error {
	tasks, err := s.captureGatewayTasks(ctx)
	if err != nil {
		return err
	}
	for _, component := range []string{"network-manager", "agent"} {
		if err := s.pauseService(ctx, s.stack+"_"+component); err != nil {
			return err
		}
	}
	if err := s.waitTasks(ctx, tasks, s.agentTimeout); err != nil {
		return err
	}
	tasks, err = s.captureGatewayTasks(ctx)
	if err != nil {
		return err
	}
	return s.waitTasks(ctx, tasks, s.agentTimeout)
}
func (s *swarm) removeGateway(ctx context.Context) error {
	services, err := s.output(ctx, "service", "ls", "--quiet", "--filter", "label=com.docker.stack.namespace="+s.stack)
	if err != nil {
		return err
	}
	if services != "" {
		return errors.New("stack services still exist; gateway retained")
	}
	peers, err := s.output(ctx, "ps", "--all", "--quiet", "--filter", "label=io.kpl.managed=true", "--filter", "label=io.kpl.network="+s.network)
	if err != nil {
		return err
	}
	if peers != "" {
		return errors.New("Peer containers remain on control node; gateway retained")
	}
	out, err := s.output(ctx, "ps", "--all", "--quiet", "--no-trunc", "--filter", "label=io.kpl.network-gateway="+s.stack)
	if err != nil {
		return err
	}
	ids, err := swarmIDs(out)
	if err != nil {
		return err
	}
	for _, id := range ids {
		owner, err := s.output(ctx, "inspect", "--type", "container", "--format", `{{index .Config.Labels "io.kpl.network-gateway"}}|{{index .Config.Labels "com.docker.stack.namespace"}}|{{index .Config.Labels "io.kpl.managed"}}`, id)
		if err != nil {
			return err
		}
		if owner != s.stack+"|"+s.stack+"|" && owner != s.stack+"|"+s.stack+"|<no value>" {
			return errors.New("gateway ownership mismatch; container retained")
		}
		if err := s.run(ctx, "rm", "--force", id); err != nil {
			return err
		}
	}
	return nil
}
