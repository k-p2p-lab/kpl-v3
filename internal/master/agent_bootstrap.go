package master

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"time"
)

func (a *App) runSwarmAgent(ctx context.Context, logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("swarm-agent", flag.ContinueOnError)
	flags.SetOutput(a.Stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError("swarm-agent uses the Swarm task environment and takes no arguments")
	}
	resolved, err := a.swarmAgentArguments(ctx, swarmSleep)
	if err != nil {
		return err
	}
	return runAgent(ctx, logger, resolved)
}

func (a *App) swarmAgentArguments(ctx context.Context, pause func(context.Context, time.Duration) error) ([]string, error) {
	for _, key := range []string{"KPL_SWARM_TASK_NAME", "KPL_SWARM_SERVICE_NAME", "KPL_SWARM_NODE_ID", "KPL_PEER_NETWORK"} {
		if a.Env(key) == "" {
			return nil, fmt.Errorf("%s is required for Swarm Agent startup", key)
		}
	}
	for _, key := range []string{"KPL_SWARM_TASK_NAME", "KPL_SWARM_SERVICE_NAME", "KPL_PEER_NETWORK"} {
		if !networkNamePattern.MatchString(a.Env(key)) {
			return nil, fmt.Errorf("invalid %s", key)
		}
	}
	if !nodeIDPattern.MatchString(a.Env("KPL_SWARM_NODE_ID")) {
		return nil, errors.New("invalid Swarm node ID")
	}
	hostRebootEnabled, err := parseHostRebootEnabled(a.Env("KPL_AGENT_HOST_REBOOT_ENABLED"))
	if err != nil {
		return nil, err
	}
	controlNodeID := a.Env("KPL_CONTROL_NODE_ID")
	if hostRebootEnabled && !nodeIDPattern.MatchString(controlNodeID) {
		return nil, errors.New("KPL_CONTROL_NODE_ID must be an exact node ID when host reboot is enabled")
	}
	peerNetwork := a.Env("KPL_PEER_NETWORK")
	control := a.Env("KPL_CONTROL_NETWORK")
	autoReset := control != ""
	if control == "" {
		control = peerNetwork
	} else if control == peerNetwork {
		return nil, errors.New("control and Peer networks must differ for automatic network initialization")
	}
	if !networkNamePattern.MatchString(control) {
		return nil, errors.New("invalid control network")
	}
	port := a.Env("KPL_AGENT_METRICS_PORT")
	if port == "" {
		port = "9091"
	}
	if err := ValidateSetting("KPL_AGENT_METRICS_PORT", port); err != nil {
		return nil, err
	}
	capacity := a.Env("KPL_AGENT_CAPACITY")
	if capacity == "" {
		capacity = "20"
	}
	if err := ValidateSetting("KPL_AGENT_CAPACITY", capacity); err != nil {
		return nil, err
	}
	read := func(args ...string) (string, error) {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		command := a.command("docker", args)
		command.CancelGrace = time.Second
		out, err := a.Runner.Output(callCtx, command)
		return strings.TrimSpace(string(out)), err
	}
	var address, nodeAddress, image string
	stage := "task control overlay address"
	for attempt := 1; attempt <= 5; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err := func() error {
			stage = "task control overlay address"
			rows, err := read("inspect", "--type", "container", "--format", `{{range $name, $net := .NetworkSettings.Networks}}{{printf "%s %s\n" $name $net.IPAddress}}{{end}}`, a.Env("KPL_SWARM_TASK_NAME"))
			if err != nil {
				return err
			}
			address = ""
			for _, row := range strings.Split(rows, "\n") {
				fields := strings.Fields(row)
				if len(fields) > 0 && fields[0] == control {
					if len(fields) == 1 {
						continue
					}
					ip, err := netip.ParseAddr(fields[1])
					if len(fields) != 2 || err != nil || !ip.Is4() || address != "" {
						return &agentMetadataError{"no unambiguous IPv4 address on the control overlay"}
					}
					address = ip.String()
				}
			}
			if address == "" {
				return errors.New("control overlay address is not ready")
			}
			stage = "Swarm node address"
			nodeAddress, err = read("info", "--format", "{{.Swarm.NodeAddr}}")
			if err != nil {
				return err
			}
			if nodeAddress == "" {
				return errors.New("node address is not ready")
			}
			if _, err := swarmURLHost(nodeAddress); err != nil {
				return &agentMetadataError{"Docker returned an invalid Swarm node address"}
			}
			stage = "running task image"
			image, err = read("inspect", "--type", "container", "--format", "{{.Image}}", a.Env("KPL_SWARM_TASK_NAME"))
			if err != nil {
				return err
			}
			if image == "" {
				return errors.New("running task image is not ready")
			}
			if !swarmDigest.MatchString(image) {
				return &agentMetadataError{"cannot resolve the running task image content ID"}
			}
			return nil
		}()
		if err == nil {
			break
		}
		var malformed *agentMetadataError
		if errors.As(err, &malformed) {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if attempt == 5 {
			return nil, fmt.Errorf("cannot resolve %s after five attempts: %w", stage, err)
		}
		fmt.Fprintf(a.Stderr, "Swarm Agent: Waiting for %s (attempt %d/5); retrying in 2s.\n", stage, attempt)
		if err := pause(ctx, 2*time.Second); err != nil {
			return nil, err
		}
	}
	metricsHost, _ := swarmURLHost(nodeAddress)
	name := a.Env("KPL_SWARM_NODE_HOSTNAME")
	if name == "" {
		name = a.Env("KPL_SWARM_NODE_ID")
	}
	controller := a.Env("KPL_CONTROLLER_URL")
	if controller == "" {
		controller = "http://controller:8080"
	}
	var args []string
	if autoReset {
		args = append(args, "--auto-reset-network", "--control-network", control)
	}
	if hostRebootEnabled {
		args = append(args, "--host-reboot-enabled", "--control-node-id", controlNodeID)
	}
	args = append(args, "--id", a.Env("KPL_SWARM_SERVICE_NAME")+"-"+a.Env("KPL_SWARM_NODE_ID"), "--name", name,
		"--labels", "swarmNodeId="+a.Env("KPL_SWARM_NODE_ID"), "--listen", ":8090", "--advertise-url", "http://"+address+":8090", "--self-url", "http://"+address+":8090",
		"--metrics-listen", ":9091", "--metrics-url", "http://"+metricsHost+":"+port+"/metrics", "--controller-url", controller,
		"--capacity", capacity, "--data-dir", "/var/lib/kpl/agent", "--docker-image", image, "--docker-network", peerNetwork)
	return args, nil
}

type agentMetadataError struct{ message string }

func (e *agentMetadataError) Error() string { return e.message }
