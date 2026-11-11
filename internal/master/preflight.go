package master

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Preflight checks deployment settings and cluster state without pulling images
// or changing services, networks, node labels, or the Docker context.
func (a *App) Preflight(ctx context.Context, cfg Config, configOnly bool) error {
	c := Config{}
	for key, value := range cfg {
		c[key] = value
	}
	ConfigDefaults(c)
	for _, key := range []string{"KPL_CONTROL_NODE_ID", "KPL_IMAGE", "KPL_USER", "KPL_PASSWORD"} {
		if c[key] == "" {
			return fmt.Errorf("set %s before checking deployment", key)
		}
	}
	if err := validateConfig(c); err != nil {
		return err
	}
	if configOnly {
		return nil
	}
	ctx, cancel := configTimeout(ctx, c, "KPL_DOCKER_TIMEOUT")
	defer cancel()
	configured := a.WithConfig(c)
	dock := func(args ...string) (string, error) { return configured.Output(ctx, "docker", args...) }
	swarm, err := dock("info", "--format", "{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}")
	if err != nil {
		return fmt.Errorf("inspect Swarm manager: %w", err)
	}
	if swarm != "active true" {
		return errors.New("run this check against an active Swarm manager")
	}
	control, err := dock("node", "inspect", "--format", "{{.ID}} {{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}} {{.Spec.Role}}", c["KPL_CONTROL_NODE_ID"])
	if err != nil {
		return fmt.Errorf("inspect control node: %w", err)
	}
	if control != c["KPL_CONTROL_NODE_ID"]+" linux ready active manager" {
		return errors.New("the exact control node must be a Linux, Ready, Active Swarm manager")
	}
	if err := checkAnalysisSwarm(c, dock); err != nil {
		return err
	}
	network, err := dock("network", "inspect", "--format", "{{.Name}} {{.Driver}} {{.Attachable}}", c["KPL_PEER_NETWORK"])
	if err != nil {
		return fmt.Errorf("inspect Peer network: %w", err)
	}
	if network != c["KPL_PEER_NETWORK"]+" overlay true" {
		return errors.New("the named Peer network must be an attachable overlay")
	}
	if c["KPL_PEER_SUBNET"] != "" {
		subnet, err := dock("network", "inspect", "--format", "{{range .IPAM.Config}}{{println .Subnet}}{{end}}", c["KPL_PEER_NETWORK"])
		if err != nil {
			return fmt.Errorf("inspect Peer network subnets: %w", err)
		}
		if subnet != c["KPL_PEER_SUBNET"] {
			return errors.New("the existing Peer network subnet differs from KPL_PEER_SUBNET")
		}
	}
	label := "kpl." + c["KPL_STACK_NAME"] + ".agent"
	nodes, err := dock("node", "ls", "--quiet", "--filter", "node.label="+label+"=true")
	if err != nil {
		return fmt.Errorf("list selected Agent nodes: %w", err)
	}
	minimum, _ := strconv.ParseInt(c["KPL_MIN_AGENTS"], 10, 64)
	ids := strings.Fields(nodes)
	insufficient := func() error {
		return fmt.Errorf("at least %d %s=true Linux, Ready, Active nodes are required", minimum, label)
	}
	if len(ids) == 0 {
		return insufficient()
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !nodeIDPattern.MatchString(id) || seen[id] {
			return errors.New("Docker returned an invalid or duplicate node ID")
		}
		seen[id] = true
	}
	args := append([]string{"node", "inspect", "--format", "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}"}, ids...)
	agents, err := dock(args...)
	if err != nil {
		return fmt.Errorf("inspect selected Agent nodes: %w", err)
	}
	lines := strings.Split(agents, "\n")
	if len(lines) != len(ids) {
		return errors.New("Docker returned an incomplete Agent node inventory")
	}
	eligible := int64(0)
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return errors.New("Docker returned a malformed Agent node inventory")
		}
		if fields[0] == "linux" && fields[1] == "ready" && fields[2] == "active" {
			eligible++
		}
	}
	if eligible < minimum {
		return insufficient()
	}
	fmt.Fprintf(a.Stdout, "PASS: Swarm manager, pinned control node, attachable overlay, %d eligible Agents for stack %s, image reference, capacity %s and Agent metrics port %s.\n", eligible, c["KPL_STACK_NAME"], c["KPL_AGENT_CAPACITY"], c["KPL_AGENT_METRICS_PORT"])
	fmt.Fprintln(a.Stdout, "This checks deployment settings only; it does not validate registry access, physical-node kernels, VXLAN connectivity, host-port availability, firewall reachability or capacity under load.")
	return nil
}
