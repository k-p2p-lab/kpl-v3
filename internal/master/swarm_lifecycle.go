package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const swarmTaskFormat = `{{.NodeID}}|{{.Status.State}}|{{if .Status.ContainerStatus}}{{.Status.ContainerStatus.ContainerID}}|{{.Status.ContainerStatus.PID}}|{{.Status.ContainerStatus.ExitCode}}{{else}}-|0|-{{end}}|{{if .Status.Err}}error{{else}}ok{{end}}`

type swarmTask struct{ node, state, container, pid, exit, problem string }

func parseSwarmTask(record string) (swarmTask, error) {
	fields := strings.Split(record, "|")
	if len(fields) != 6 {
		return swarmTask{}, errors.New("Docker returned incomplete task metadata")
	}
	t := swarmTask{fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]}
	if t.node != "" && !swarmID.MatchString(t.node) || !swarmNodeName.MatchString(t.state) || t.container != "-" && !swarmID.MatchString(t.container) || t.problem != "ok" && t.problem != "error" {
		return t, errors.New("Docker returned invalid task metadata")
	}
	if _, err := strconv.ParseUint(t.pid, 10, 64); err != nil {
		return t, errors.New("Docker returned invalid task PID")
	}
	if t.exit != "-" {
		if _, err := strconv.ParseInt(t.exit, 10, 64); err != nil {
			return t, errors.New("Docker returned invalid task exit code")
		}
	}
	return t, nil
}
func (t swarmTask) terminal() bool { return t.state == "complete" || t.state == "shutdown" }
func (t swarmTask) clean() bool {
	return t.terminal() && t.container != "" && t.container != "-" && t.pid == "0" && t.exit == "0" && t.problem == "ok"
}
func (t swarmTask) failed() bool {
	switch t.state {
	case "failed", "rejected", "orphaned", "remove":
		return true
	}
	return false
}
func (s *swarm) task(ctx context.Context, id string) (swarmTask, error) {
	out, err := s.output(ctx, "inspect", "--type", "task", "--format", swarmTaskFormat, id)
	if err != nil {
		return swarmTask{}, err
	}
	return parseSwarmTask(out)
}

func (s *swarm) captureTasks(ctx context.Context, service, node string) ([]string, error) {
	args := []string{"service", "ps", "--quiet", "--no-trunc"}
	if node != "" {
		args = append(args, "--filter", "node="+node)
	}
	args = append(args, service)
	out, err := s.output(ctx, args...)
	if err != nil {
		return nil, err
	}
	ids, err := swarmIDs(out)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no task history for %s; cleanup cannot be verified", service)
	}
	seen := map[string]bool{}
	var captured []string
	for _, id := range ids {
		t, err := s.task(ctx, id)
		if err != nil {
			return nil, err
		}
		if t.node == "" {
			if t.container == "-" && t.pid == "0" && t.exit == "-" && (t.state == "new" || t.state == "pending" || t.state == "allocated" || t.state == "shutdown") {
				continue
			}
			return nil, fmt.Errorf("task %s has no verifiable node/container assignment", id)
		}
		include := !(t.clean() && seen[t.node])
		seen[t.node] = true
		if include {
			ready, err := s.inspectNode(ctx, "{{.Description.Platform.OS}} {{.Status.State}}", t.node)
			if err != nil {
				return nil, err
			}
			if ready != "linux ready" {
				return nil, fmt.Errorf("task %s is on unavailable node %s; cleanup cannot be verified", id, t.node)
			}
			captured = append(captured, id)
		}
	}
	return captured, nil
}

func (s *swarm) waitTasks(ctx context.Context, ids []string, budget time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for _, id := range ids {
		for {
			t, err := s.task(waitCtx, id)
			if err != nil {
				return fmt.Errorf("cannot inspect task %s; cleanup is unverified: %w", id, err)
			}
			state, err := s.inspectNode(waitCtx, "{{.Status.State}}", t.node)
			if err != nil {
				return err
			}
			if state != "ready" {
				return fmt.Errorf("node %s became unavailable; cleanup is unverified", t.node)
			}
			if t.terminal() {
				if !t.clean() {
					return fmt.Errorf("task %s stopped without a clean container exit (state=%s, exit=%s)", id, t.state, t.exit)
				}
				break
			}
			if t.failed() {
				return fmt.Errorf("task %s is %s; cleanup is unverified", id, t.state)
			}
			if err := swarmSleep(waitCtx, 2*time.Second); err != nil {
				return fmt.Errorf("timed out waiting for task %s; stack retained for inspection: %w", id, err)
			}
		}
	}
	return nil
}

func (s *swarm) placementExclusions(ctx context.Context, action string, nodes []string) error {
	if len(nodes) == 0 {
		return nil
	}
	args := []string{"service", "update", "--detach=true", "--no-resolve-image"}
	for _, node := range nodes {
		args = append(args, "--constraint-"+action, "node.id!="+node)
	}
	args = append(args, s.stack+"_agent")
	return s.run(ctx, args...)
}

func (s *swarm) pauseService(ctx context.Context, service string) error {
	return s.run(ctx, "service", "update", "--detach=true", "--no-resolve-image", "--constraint-add", "node.role==manager", "--constraint-add", "node.role==worker", service)
}

func (s *swarm) validateAgentPlacement(ctx context.Context, nodes []string) error {
	selected, err := s.selectedNodes(ctx)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	eligible := int64(0)
	for _, node := range append(selected, nodes...) {
		if seen[node] {
			continue
		}
		seen[node] = true
		state, err := s.inspectNode(ctx, "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}", node)
		if err != nil {
			return err
		}
		if state == "linux ready active" {
			eligible++
		}
	}
	minimum, err := positiveSwarmInteger(s.config["KPL_MIN_AGENTS"])
	if err != nil {
		return err
	}
	if eligible < minimum {
		return fmt.Errorf("%s requires at least %d eligible Agent nodes; specify nodes or a selector", s.options.name, minimum)
	}
	return nil
}

func (s *swarm) prepareRestart(ctx context.Context, nodes []string) ([]string, error) {
	if !s.present["controller"] || !s.present["agent"] {
		return nil, errors.New("restart requires existing Controller and Agent services; use deploy for initial deployment")
	}
	if err := s.validateAgentPlacement(ctx, nodes); err != nil {
		return nil, err
	}
	var tasks []string
	for _, service := range s.services {
		captured, err := s.captureTasks(ctx, service, "")
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, captured...)
	}
	for _, id := range tasks {
		t, err := s.task(ctx, id)
		if err != nil {
			return nil, err
		}
		if t.failed() || t.terminal() && !t.clean() {
			return nil, fmt.Errorf("task %s has unverified cleanup (%s); services were not stopped", id, t.state)
		}
	}
	return tasks, nil
}

func (s *swarm) stopService(ctx context.Context, service string, budget time.Duration) error {
	tasks, err := s.captureTasks(ctx, service, "")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.a.Stdout, "Stopping %s and verifying clean exits...\n", service)
	if err := s.pauseService(ctx, service); err != nil {
		return fmt.Errorf("cannot pause %s; restart stopped before deployment: %w", service, err)
	}
	if err := s.waitTasks(ctx, tasks, budget); err != nil {
		return err
	}
	tasks, err = s.captureTasks(ctx, service, "")
	if err != nil {
		return err
	}
	return s.waitTasks(ctx, tasks, budget)
}

// Archive paths belong to the Docker control host, which can be remote. Do not
// stat or create the source directory on the machine running this operator.
// Compose merges volumes by target, retaining the local Controller data volume.
func (s *swarm) deploymentComposeFiles() ([]string, func(), error) {
	args := []string{"--compose-file", filepath.Join(s.a.Repo, "stack.swarm.yaml")}
	var temporary []string
	cleanup := func() {
		for _, name := range temporary {
			_ = os.Remove(name)
		}
	}
	addOverride := func(pattern string, write func(io.Writer) error) error {
		file, err := os.CreateTemp("", pattern)
		if err != nil {
			return err
		}
		name := file.Name()
		temporary = append(temporary, name)
		args = append(args, "--compose-file", name)
		return errors.Join(write(file), file.Close())
	}
	source := s.config["KPL_ARCHIVE_HOST_PATH"]
	if err := ValidateSetting("KPL_ARCHIVE_HOST_PATH", source); err != nil {
		return nil, cleanup, err
	}
	if source == "" {
		return args, cleanup, nil
	}
	// YAML quoting does not prevent Compose interpolation. Escape every dollar
	// before JSON quoting so both '${...}' and ordinary '$name' remain literal.
	quoted, err := json.Marshal(strings.ReplaceAll(source, "$", "$$"))
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	if err := addOverride("kpl-archive-stack-*.yaml", func(file io.Writer) error {
		_, err := fmt.Fprintf(file, "version: \"3.8\"\nservices:\n  controller:\n    volumes:\n      - type: bind\n        source: %s\n        target: /var/lib/kpl/data/runs\n", quoted)
		return err
	}); err != nil {
		cleanup()
		return nil, func() {}, err
	}
	return args, cleanup, nil
}

func (s *swarm) deploy(ctx context.Context) error {
	for _, key := range []string{"KPL_IMAGE", "KPL_CONTROL_NODE_ID", "KPL_USER", "KPL_PASSWORD", "GRAFANA_ADMIN_PASSWORD"} {
		if s.config[key] == "" {
			return fmt.Errorf("%s is required", key)
		}
	}
	if err := s.a.Preflight(ctx, s.config, true); err != nil {
		return err
	}
	if err := s.loadAnalysisRetirementJournal(ctx); err != nil {
		return err
	}
	host, err := s.controlHost(ctx)
	if err != nil {
		return err
	}
	metrics := "http://" + host + ":" + s.config["KPL_HTTP_PORT"] + "/metrics"
	prometheus := "http://" + host + ":" + s.config["PROMETHEUS_PORT"] + "/"
	s.a = s.a.WithEnv(map[string]*string{"KPL_CONTROLLER_METRICS_URL": &metrics, "KPL_PROMETHEUS_EXTERNAL_URL": &prometheus})
	compose, cleanup, err := s.deploymentComposeFiles()
	if err != nil {
		return err
	}
	defer cleanup()
	// stack config interpolates credentials; validate it without emitting YAML.
	quiet := *s.a
	quiet.Stdout = io.Discard
	checkCtx, cancel := context.WithTimeout(ctx, s.timeout)
	err = quiet.Run(checkCtx, "docker", append([]string{"stack", "config"}, compose...)...)
	cancel()
	if err != nil {
		return err
	}
	control := s.config["KPL_CONTROL_NODE_ID"]
	if err := s.readyNode(ctx, control); err != nil {
		return err
	}
	role, err := s.inspectNode(ctx, "{{.Spec.Role}}", control)
	if err != nil {
		return err
	}
	if role != "manager" {
		return errors.New("control node must be a Swarm manager")
	}
	nodes, err := s.commandNodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := s.readyNode(ctx, node); err != nil {
			return err
		}
	}
	s.reportNodes(nodes)
	if s.options.name == "restart" {
		if _, err := s.prepareRestart(ctx, nodes); err != nil {
			return err
		}
	} else if err := s.validateAgentPlacement(ctx, nodes); err != nil {
		// Reject known-insufficient placement before adding labels can start a
		// global Agent, or an initial deployment can leave a new overlay behind.
		return err
	}
	if err := s.resolveImage(ctx); err != nil {
		return err
	}
	if s.options.name == "deploy" {
		// Tag resolution can outlast an external service replacement. Do not
		// overwrite a new owner using the inventory captured before the pull.
		if err := s.loadServices(ctx); err != nil {
			return err
		}
	}
	networks, err := s.output(ctx, "network", "ls", "--quiet", "--filter", "name=^"+regexp.QuoteMeta(s.network)+"$")
	if err != nil {
		return err
	}
	if networks == "" {
		if s.options.name == "restart" {
			return errors.New("Peer network is missing; repair it before restarting; services were not stopped")
		}
		args := []string{"network", "create", "--driver", "overlay", "--attachable", "--label", "io.kpl.application=" + swarmApplication, "--label", "io.kpl.stack=" + s.stack}
		if subnet := s.config["KPL_PEER_SUBNET"]; subnet != "" {
			args = append(args, "--subnet", subnet)
		}
		args = append(args, s.network)
		if err := s.run(ctx, args...); err != nil {
			return err
		}
	} else {
		if _, err := swarmIDs(networks); err != nil {
			return err
		}
		metadata, err := s.output(ctx, "network", "inspect", "--format", "{{.Name}} {{.Driver}} {{.Attachable}}", s.network)
		if err != nil {
			return err
		}
		if metadata != s.network+" overlay true" {
			return errors.New("Peer network must be an attachable overlay")
		}
		if subnet := s.config["KPL_PEER_SUBNET"]; subnet != "" {
			actual, err := s.output(ctx, "network", "inspect", "--format", "{{range .IPAM.Config}}{{println .Subnet}}{{end}}", s.network)
			if err != nil {
				return err
			}
			if actual != subnet {
				return errors.New("existing Peer network subnet differs; no network or placement changes made")
			}
		}
	}
	if err := s.loadRetiredAnalysisProof(ctx); err != nil {
		return err
	}
	if s.options.name == "restart" {
		// An image pull can take minutes. Confirm ownership again before any stop;
		// a service with the same name may have been replaced in that interval.
		if err := s.loadServices(ctx); err != nil {
			return err
		}
		tasks, err := s.prepareRestart(ctx, nodes)
		if err != nil {
			return err
		}
		if err := s.stopService(ctx, s.stack+"_controller", s.controllerTimeout); err != nil {
			return err
		}
		if err := s.stopService(ctx, s.stack+"_agent", s.agentTimeout); err != nil {
			return err
		}
		for _, service := range s.services {
			if service == s.stack+"_controller" || service == s.stack+"_agent" {
				continue
			}
			if err := s.stopService(ctx, service, s.agentTimeout); err != nil {
				return err
			}
		}
		if err := s.waitTasks(ctx, tasks, s.agentTimeout); err != nil {
			return err
		}
		if err := s.removeLegacyAnalysisServices(ctx); err != nil {
			return err
		}
	} else {
		if err := s.stopLegacyAnalysisServices(ctx); err != nil {
			return err
		}
		if err := s.removeLegacyAnalysisServices(ctx); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		if err := s.run(ctx, "node", "update", "--label-add", s.label+"=true", node); err != nil {
			return err
		}
	}
	if err := s.a.Preflight(ctx, s.config, false); err != nil {
		return err
	}
	s.withRetiredAnalysisProof()
	deployArgs := append([]string{"stack", "deploy", "--with-registry-auth", "--resolve-image", "always", "--detach=true"}, compose...)
	if err := s.run(ctx, append(deployArgs, s.stack)...); err != nil {
		return err
	}
	fmt.Fprintln(s.a.Stdout, "Deployment submitted. Use status, then check Agent status in the Controller. Use Discover Agents to recover missing registrations before running an experiment.")
	return nil
}

func (s *swarm) addNodes(ctx context.Context) error {
	if !s.present["agent"] {
		return errors.New("deploy the stack before adding nodes")
	}
	nodes, err := s.commandNodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := s.readyNode(ctx, node); err != nil {
			return err
		}
	}
	s.reportNodes(nodes)
	for _, node := range nodes {
		if err := s.run(ctx, "node", "update", "--label-add", s.label+"=true", node); err != nil {
			return err
		}
	}
	if err := s.placementExclusions(ctx, "rm", nodes); err != nil {
		return err
	}
	fmt.Fprintln(s.a.Stdout, "Agent placement updated. Use status and wait for Controller registration.")
	return nil
}

func (s *swarm) readyCleanupNode(ctx context.Context, node string) error {
	state, err := s.inspectNode(ctx, "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}", node)
	if err != nil {
		return err
	}
	fields := strings.Fields(state)
	if len(fields) != 3 || fields[0] != "linux" || fields[1] != "ready" || fields[2] != "active" && fields[2] != "pause" && fields[2] != "drain" {
		return fmt.Errorf("node %s must be Linux and Ready with Active, Pause or Drain availability to verify cleanup; got %s", node, state)
	}
	return nil
}

func (s *swarm) removeNodes(ctx context.Context) error {
	if !s.present["agent"] {
		return errors.New("Agent service does not exist")
	}
	nodes, err := s.commandNodes(ctx)
	if err != nil {
		return err
	}
	var tasks []string
	for _, node := range nodes {
		if err := s.readyCleanupNode(ctx, node); err != nil {
			return err
		}
		captured, err := s.captureTasks(ctx, s.stack+"_agent", node)
		if err != nil {
			return err
		}
		tasks = append(tasks, captured...)
	}
	s.reportNodes(nodes)
	if err := s.placementExclusions(ctx, "add", nodes); err != nil {
		return err
	}
	if err := s.waitTasks(ctx, tasks, s.agentTimeout); err != nil {
		return err
	}
	for _, node := range nodes {
		remaining, err := s.captureTasks(ctx, s.stack+"_agent", node)
		if err != nil {
			return err
		}
		if err := s.waitTasks(ctx, remaining, s.agentTimeout); err != nil {
			return err
		}
	}
	for _, node := range nodes {
		if err := s.run(ctx, "node", "update", "--label-rm", s.label, node); err != nil {
			return err
		}
	}
	if err := s.placementExclusions(ctx, "rm", nodes); err != nil {
		return err
	}
	fmt.Fprintln(s.a.Stdout, "Selected Agents stopped with clean exits. Other node labels were unchanged.")
	return nil
}

func (s *swarm) waitRemovedServices(ctx context.Context) error {
	allowed := make(map[string]bool, len(s.serviceIDs))
	for _, id := range s.serviceIDs {
		allowed[id] = true
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	for {
		remaining, err := s.output(waitCtx, "service", "ls", "--quiet", "--filter", "label=com.docker.stack.namespace="+s.stack)
		if err != nil {
			return fmt.Errorf("cannot verify stack service removal: %w", err)
		}
		ids, err := swarmIDs(remaining)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !allowed[id] {
				return fmt.Errorf("new stack service %s appeared during removal; inspect status before retrying", id)
			}
		}
		if len(ids) == 0 {
			return nil
		}
		if err := swarmSleep(waitCtx, 2*time.Second); err != nil {
			return fmt.Errorf("stack service removal is still pending; inspect status: %w", err)
		}
	}
}

func (s *swarm) remove(ctx context.Context) error {
	gatewayReady := false
	if s.present["network-manager"] {
		if err := s.prepareGatewayRemoval(ctx); err != nil {
			fmt.Fprintf(s.a.Stderr, "Gateway cleanup is unverified (%v); services will be deleted, but the gateway and attached control network are retained for inspection.\n", err)
		} else {
			gatewayReady = true
		}
	}
	if len(s.serviceIDs) > 0 {
		// Delete only the immutable service IDs whose ownership was checked.
		// Gateway cleanup may wait while a same-name replacement appears.
		if err := s.run(ctx, append([]string{"service", "rm"}, s.serviceIDs...)...); err != nil {
			return fmt.Errorf("service removal failed; inspect status and retry remove: %w", err)
		}
	}
	if err := s.waitRemovedServices(ctx); err != nil {
		return fmt.Errorf("service removal is unverified; stack resource cleanup was not attempted: %w", err)
	}
	if gatewayReady {
		if err := s.removeGateway(ctx); err != nil {
			fmt.Fprintf(s.a.Stderr, "Gateway removal is unverified; inspect retained gateway and control network: %v\n", err)
		}
	}
	// Gateway inspection can block. Refuse a replacement created in that
	// interval before asking Docker's name-based stack removal to clean assets.
	if err := s.waitRemovedServices(ctx); err != nil {
		return fmt.Errorf("stack service inventory changed; stack resource cleanup was not attempted: %w", err)
	}
	if err := s.run(ctx, "stack", "rm", s.stack); err != nil {
		return err
	}
	if err := s.waitRemovedServices(ctx); err != nil {
		return err
	}
	nodes, err := s.selectedNodes(ctx)
	if err != nil {
		fmt.Fprintln(s.a.Stderr, "Could not list Agent labels; retry remove to clear remaining labels.")
	} else {
		for _, node := range nodes {
			if err := s.run(ctx, "node", "update", "--label-rm", s.label, node); err != nil {
				fmt.Fprintf(s.a.Stderr, "Could not clear Agent label on %s; retry remove to clear remaining labels.\n", node)
			}
		}
	}
	fmt.Fprintln(s.a.Stdout, "Stack services removed. Data volumes and Peer network preserved.\nService deletion does not verify final experiment logs or standalone Peer cleanup.")
	return nil
}
