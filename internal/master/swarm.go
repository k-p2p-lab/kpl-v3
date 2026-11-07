package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const swarmApplication = "kp2plab-v3"

var swarmID = regexp.MustCompile(`^[a-zA-Z0-9]+$`)
var swarmNodeName = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
var swarmPlatform = regexp.MustCompile(`^linux/[a-z0-9][a-z0-9_]*(/[a-z0-9][a-z0-9_.-]*)?$`)
var swarmDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type swarmOptions struct {
	name, selector, platforms, image, component, tail, logContext string
	nodes                                                         []string
}

type swarm struct {
	a                     *App
	config                Config
	configuredImage       string
	options               swarmOptions
	stack, network, label string
	timeout               time.Duration
	pullTimeout           time.Duration
	buildTimeout          time.Duration
	pushTimeout           time.Duration
	agentTimeout          time.Duration
	controllerTimeout     time.Duration
	services              []string
	serviceIDs            []string
	present               map[string]bool
}

func parseSwarmOptions(name string, args []string) (swarmOptions, error) {
	o := swarmOptions{name: name, component: "controller", tail: "100"}
	bad := func(message string) (swarmOptions, error) { return o, &UsageError{Message: message} }
	switch name {
	case "nodes", "login", "check", "access", "status", "remove", "reset-peer-network":
		if len(args) != 0 {
			return bad(name + " takes no arguments")
		}
		return o, nil
	case "image-inspect":
		if len(args) > 1 {
			return bad("image inspect accepts at most one image reference")
		}
		if len(args) == 1 {
			if err := validateRequestedImage(args[0], false); err != nil {
				return bad(err.Error())
			}
			o.image = args[0]
		}
		return o, nil
	case "logs":
		if len(args) > 0 && !strings.HasPrefix(args[0], "--") {
			o.component = args[0]
			args = args[1:]
		}
		switch o.component {
		case "controller", "agent", "network-manager", "resource-monitor", "prometheus", "grafana", "access", "auth":
		default:
			return bad("unknown log component")
		}
		for len(args) > 0 {
			if len(args) < 2 {
				return bad("log option requires a value")
			}
			switch args[0] {
			case "--tail":
				o.tail = args[1]
				if o.tail != "all" {
					if _, err := positiveSwarmInteger(o.tail); err != nil {
						return bad("--tail needs a positive integer without leading zeros, or all")
					}
				}
			case "--context":
				o.logContext = args[1]
				if o.logContext == "" || strings.HasPrefix(o.logContext, "-") || strings.ContainsAny(o.logContext, "\r\n\x00") {
					return bad("invalid Docker context name")
				}
			default:
				return bad("unknown logs option: " + args[0])
			}
			args = args[2:]
		}
		if o.logContext != "" && o.component != "access" && o.component != "auth" {
			return bad("--context only applies to access/auth file reads")
		}
		return o, nil
	case "publish", "restart", "deploy", "add-node", "remove-node":
	default:
		return bad("unknown Swarm command: " + name)
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		key, value, supplied := strings.Cut(arg, "=")
		if key == "--image" || key == "--platforms" {
			if key == "--platforms" && name == "restart" {
				return bad("restart pulls the configured registry image; use kpl image publish --platforms CSV to build and publish first")
			}
			if key == "--image" && name != "deploy" && name != "restart" || key == "--platforms" && name != "publish" {
				return bad("unsupported option for " + name + ": " + key)
			}
			if !supplied {
				if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
					if key == "--platforms" {
						return bad("--platforms requires Linux OS/architecture[/variant] tuples")
					}
					return bad("--image requires a registry sha256 digest or repository@sha256:digest")
				}
				i++
				value = args[i]
			}
			if key == "--image" {
				if o.image != "" {
					return bad("--image may be specified only once")
				}
				if err := validateRequestedImage(value, true); err != nil {
					return bad(err.Error())
				}
				o.image = value
			} else {
				if o.platforms != "" {
					return bad("--platforms may be specified only once")
				}
				for _, platform := range strings.Split(value, ",") {
					if !swarmPlatform.MatchString(platform) {
						return bad("--platforms requires Linux OS/architecture[/variant] tuples")
					}
				}
				o.platforms = value
			}
			continue
		}
		if name == "publish" {
			return bad("publish accepts only --platforms CSV")
		}
		switch arg {
		case "--all", "--all-excluding-self", "--workers":
			if o.selector != "" || len(o.nodes) > 0 {
				return bad("use exactly one selector without explicit nodes")
			}
			o.selector = arg
		default:
			if o.selector != "" {
				return bad("use exactly one selector without explicit nodes")
			}
			if !swarmNodeName.MatchString(arg) || strings.HasPrefix(arg, "-") {
				return bad("invalid node or option: " + arg)
			}
			o.nodes = append(o.nodes, arg)
		}
	}
	if o.image != "" && o.platforms != "" {
		return bad("--image and --platforms cannot be combined; selecting a digest reuses an existing image")
	}
	if (name == "add-node" || name == "remove-node") && o.selector == "" && len(o.nodes) == 0 {
		return bad(name + " requires nodes or a selector")
	}
	return o, nil
}

func positiveSwarmInteger(value string) (int64, error) {
	if value == "" || value[0] == '0' {
		return 0, errors.New("expected positive integer")
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, errors.New("expected positive integer")
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, errors.New("integer out of range")
	}
	return n, nil
}

func (a *App) RunSwarm(ctx context.Context, name string, args []string) error {
	if name == "scenario" {
		if len(args) > 1 || len(args) == 1 && strings.HasPrefix(args[0], "-") {
			return &UsageError{Message: "scenario accepts one file"}
		}
		path := filepath.Join(a.Repo, "examples", "swarm-smoke.yaml")
		if len(args) == 1 {
			path = args[0]
		}
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("scenario must be a regular file")
		}
		_, err = io.Copy(a.Stdout, file)
		return err
	}
	o, err := parseSwarmOptions(name, args)
	if err != nil {
		return err
	}
	config, err := a.LoadConfig()
	if err != nil {
		return err
	}
	configuredImage := config["KPL_IMAGE"]
	if o.image != "" {
		image, err := requestedImageReference(config["KPL_IMAGE"], o.image)
		if err != nil {
			return err
		}
		config["KPL_IMAGE"] = image
	}
	if name == "restart" && o.platforms != "" && strings.Contains(config["KPL_IMAGE"], "@") {
		return usageError("--platforms cannot be used with a configured image digest; configure a mutable tag to build an image")
	}
	s := &swarm{a: a.WithConfig(config), config: config, configuredImage: configuredImage, options: o, stack: config["KPL_STACK_NAME"], network: config["KPL_PEER_NETWORK"], present: make(map[string]bool)}
	s.label = "kpl." + s.stack + ".agent"
	for key, target := range map[string]*time.Duration{"KPL_DOCKER_TIMEOUT": &s.timeout, "KPL_IMAGE_PULL_TIMEOUT": &s.pullTimeout, "KPL_IMAGE_BUILD_TIMEOUT": &s.buildTimeout, "KPL_IMAGE_PUSH_TIMEOUT": &s.pushTimeout, "KPL_AGENT_STOP_TIMEOUT": &s.agentTimeout, "KPL_CONTROLLER_STOP_TIMEOUT": &s.controllerTimeout} {
		n, err := positiveSwarmInteger(config[key])
		if err != nil || n > math.MaxInt64/int64(time.Second) {
			return fmt.Errorf("%s exceeds the supported timeout range", key)
		}
		*target = time.Duration(n) * time.Second
	}
	switch name {
	case "login":
		return s.login(ctx)
	case "publish":
		return s.publish(ctx)
	case "image-inspect":
		return s.inspectImage(ctx)
	}
	manager, err := s.output(ctx, "info", "--format", "{{.Swarm.LocalNodeState}} {{.Swarm.ControlAvailable}}")
	if err != nil {
		return err
	}
	if manager != "active true" {
		return errors.New("run against an active Swarm manager")
	}
	switch name {
	case "nodes":
		return s.run(ctx, "node", "ls")
	case "check":
		return s.a.Preflight(ctx, config, false)
	case "access":
		return s.access(ctx)
	case "reset-peer-network":
		return s.resetPeerNetwork(ctx)
	}
	if err := s.loadServices(ctx); err != nil {
		return err
	}
	switch name {
	case "logs":
		return s.logs(ctx)
	case "status":
		if err := s.run(ctx, "stack", "services", s.stack); err != nil {
			return err
		}
		if s.present["agent"] {
			if err := s.run(ctx, "service", "ps", "--no-trunc", s.stack+"_agent"); err != nil {
				return err
			}
		}
		fmt.Fprintf(a.Stdout, "\nAgent placement label: %s=true\n", s.label)
		return s.run(ctx, "node", "ls", "--filter", "node.label="+s.label+"=true")
	case "deploy", "restart":
		return s.deploy(ctx)
	case "add-node":
		return s.addNodes(ctx)
	case "remove-node":
		return s.removeNodes(ctx)
	case "remove":
		return s.remove(ctx)
	}
	return &UsageError{Message: "unknown Swarm command: " + name}
}

func (s *swarm) run(ctx context.Context, args ...string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.a.Run(ctx, "docker", args...)
}
func (s *swarm) output(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.a.Output(ctx, "docker", args...)
}
func (s *swarm) inspectNode(ctx context.Context, format, node string) (string, error) {
	return s.output(ctx, "node", "inspect", "--format", format, node)
}
func (s *swarm) selectedNodes(ctx context.Context) ([]string, error) {
	out, err := s.output(ctx, "node", "ls", "--quiet", "--filter", "node.label="+s.label+"=true")
	if err != nil {
		return nil, err
	}
	return swarmIDs(out)
}
func swarmIDs(out string) ([]string, error) {
	ids := strings.Fields(out)
	for _, id := range ids {
		if !swarmID.MatchString(id) {
			return nil, errors.New("Docker returned an invalid identifier")
		}
	}
	return ids, nil
}
func (s *swarm) loadServices(ctx context.Context) error {
	out, err := s.output(ctx, "service", "ls", "--quiet", "--filter", "label=com.docker.stack.namespace="+s.stack)
	if err != nil {
		return err
	}
	ids, err := swarmIDs(out)
	if err != nil {
		return err
	}
	var services []string
	present := make(map[string]bool)
	for _, id := range ids {
		identity, err := s.output(ctx, "service", "inspect", "--format", `{{.Spec.Name}}|{{index .Spec.Labels "io.kpl.application"}}`, id)
		if err != nil {
			return err
		}
		parts := strings.Split(identity, "|")
		if len(parts) != 2 || parts[1] != swarmApplication || !strings.HasPrefix(parts[0], s.stack+"_") {
			return errors.New("existing stack contains an unrecognized service; no changes made")
		}
		component := strings.TrimPrefix(parts[0], s.stack+"_")
		switch component {
		case "controller", "agent", "prometheus", "grafana", "resource-monitor", "network-manager":
		default:
			return errors.New("existing stack contains an unrecognized service; no changes made")
		}
		if present[component] {
			return errors.New("Docker returned duplicate service ownership")
		}
		services = append(services, parts[0])
		present[component] = true
	}
	s.services, s.serviceIDs, s.present = services, ids, present
	return nil
}

func (s *swarm) readyNode(ctx context.Context, node string) error {
	state, err := s.inspectNode(ctx, "{{.Description.Platform.OS}} {{.Status.State}} {{.Spec.Availability}}", node)
	if err != nil {
		return err
	}
	if state != "linux ready active" {
		return fmt.Errorf("node %s must be Linux, Ready and Active; got %s", node, state)
	}
	return nil
}
func (s *swarm) commandNodes(ctx context.Context) ([]string, error) {
	var candidates []string
	if s.options.selector == "" {
		seen := map[string]bool{}
		var result []string
		for _, name := range s.options.nodes {
			id, err := s.inspectNode(ctx, "{{.ID}}", name)
			if err != nil {
				return nil, err
			}
			if !swarmID.MatchString(id) {
				return nil, errors.New("Docker returned invalid node ID")
			}
			if !seen[id] {
				result = append(result, id)
				seen[id] = true
			}
		}
		return result, nil
	}
	var err error
	if s.options.name == "remove-node" {
		candidates, err = s.selectedNodes(ctx)
	} else {
		var out string
		out, err = s.output(ctx, "node", "ls", "--quiet")
		if err == nil {
			candidates, err = swarmIDs(out)
		}
	}
	if err != nil {
		return nil, err
	}
	self := ""
	if s.options.selector == "--all-excluding-self" {
		self, err = s.output(ctx, "info", "--format", "{{.Swarm.NodeID}}")
		if err != nil {
			return nil, err
		}
		if !swarmID.MatchString(self) {
			return nil, errors.New("Docker returned invalid current node ID")
		}
	}
	seen := map[string]bool{}
	var result []string
	for _, candidate := range candidates {
		record, err := s.inspectNode(ctx, "{{.ID}}|{{.Spec.Role}}|{{.Description.Platform.OS}}|{{.Status.State}}|{{.Spec.Availability}}", candidate)
		if err != nil {
			return nil, err
		}
		fields := strings.Split(record, "|")
		if len(fields) != 5 || !swarmID.MatchString(fields[0]) || (fields[1] != "manager" && fields[1] != "worker") {
			return nil, errors.New("Docker returned incomplete node metadata")
		}
		for _, field := range fields[2:] {
			if !swarmNodeName.MatchString(field) {
				return nil, errors.New("Docker returned invalid node metadata")
			}
		}
		id := fields[0]
		if seen[id] {
			continue
		}
		seen[id] = true
		if id == self || s.options.selector == "--workers" && fields[1] != "worker" {
			continue
		}
		if s.options.name != "remove-node" && strings.Join(fields[2:], " ") != "linux ready active" {
			fmt.Fprintf(s.a.Stderr, "Skipping node %s: requires Linux, Ready and Active; got %s.\n", id, strings.Join(fields[2:], " "))
			continue
		}
		result = append(result, id)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("selector %s matched no eligible nodes", s.options.selector)
	}
	return result, nil
}
func (s *swarm) reportNodes(nodes []string) {
	if len(nodes) > 0 {
		fmt.Fprintf(s.a.Stderr, "Selected %d node(s): %s\n", len(nodes), strings.Join(nodes, " "))
	}
}

func (s *swarm) controlHost(ctx context.Context) (string, error) {
	id := s.config["KPL_CONTROL_NODE_ID"]
	if err := ValidateSetting("KPL_CONTROL_NODE_ID", id); err != nil {
		return "", err
	}
	host, err := s.inspectNode(ctx, "{{.Status.Addr}}", id)
	if err != nil {
		return "", err
	}
	return swarmURLHost(host)
}
func swarmURLHost(raw string) (string, error) {
	addr, err := netip.ParseAddr(raw)
	if err != nil || addr.Zone() != "" {
		return "", errors.New("Docker returned invalid node address")
	}
	if addr.Is6() {
		return "[" + raw + "]", nil
	}
	return raw, nil
}
func (s *swarm) access(ctx context.Context) error {
	host, err := s.controlHost(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(s.a.Stdout, "Controller: http://%s:%s\nPrometheus: http://%s:%s\nGrafana: http://%s:%s\n", host, s.config["KPL_HTTP_PORT"], host, s.config["PROMETHEUS_PORT"], host, s.config["GRAFANA_PORT"])
	nodes, err := s.selectedNodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		raw, err := s.inspectNode(ctx, "{{.Status.Addr}}", node)
		if err != nil {
			return err
		}
		host, err := swarmURLHost(raw)
		if err != nil {
			return err
		}
		fmt.Fprintf(s.a.Stdout, "Agent metrics (%s): http://%s:%s/metrics\n", node, host, s.config["KPL_AGENT_METRICS_PORT"])
	}
	if len(nodes) == 0 {
		fmt.Fprintln(s.a.Stdout, "Agent metrics: no nodes are selected for this stack.")
	}
	fmt.Fprintln(s.a.Stdout, "These configured URLs use Swarm node addresses; reachability, firewall rules and service readiness are not checked.")
	return nil
}

func (s *swarm) login(ctx context.Context) error {
	image := s.config["KPL_IMAGE"]
	if err := ValidateSetting("KPL_IMAGE", image); err != nil {
		return err
	}
	args := []string{"login"}
	if first, _, ok := strings.Cut(image, "/"); ok && first != "docker.io" && first != "index.docker.io" && first != "registry-1.docker.io" && (strings.ContainsAny(first, ".:") || first == "localhost") {
		args = append(args, first)
	}
	return s.a.RunInteractive(ctx, "docker", args...)
}

func (s *swarm) publish(ctx context.Context) error {
	image := s.config["KPL_IMAGE"]
	if err := ValidateSetting("KPL_IMAGE", image); err != nil {
		return err
	}
	if strings.Contains(image, "@") {
		return errors.New("publish requires a mutable image tag")
	}
	if err := s.checkPublishContext(); err != nil {
		return err
	}
	buildArgs, err := s.sourceBuildArguments(ctx)
	if err != nil {
		return err
	}
	a := s.a.WithEnv(map[string]*string{"DOCKER_DEFAULT_PLATFORM": nil})
	buildCtx, cancel := context.WithTimeout(ctx, s.buildTimeout)
	defer cancel()
	if s.options.platforms != "" {
		args := append([]string{"buildx", "build", "--platform", s.options.platforms, "--tag", image, "--push"}, buildArgs...)
		args = append(args, s.a.Repo)
		if err := a.Run(buildCtx, "docker", args...); err != nil {
			return fmt.Errorf("multi-platform build/push failed: %w", err)
		}
	} else {
		args := append([]string{"build", "--tag", image}, buildArgs...)
		args = append(args, s.a.Repo)
		if err := a.Run(buildCtx, "docker", args...); err != nil {
			return fmt.Errorf("image build failed; push was not started: %w", err)
		}
		pushCtx, pushCancel := context.WithTimeout(ctx, s.pushTimeout)
		defer pushCancel()
		if err := a.Run(pushCtx, "docker", "push", image); err != nil {
			return fmt.Errorf("image push failed: %w", err)
		}
	}
	fmt.Fprintf(s.a.Stdout, "Published %s. Deploy resolves this tag to its current registry digest.\n", image)
	return nil
}

func (s *swarm) checkPublishContext() error {
	envFile := s.a.configPath()
	info, err := os.Stat(filepath.Join(s.a.Repo, "Dockerfile"))
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("Dockerfile not found in repository root")
	}
	if _, err := os.Lstat(filepath.Join(s.a.Repo, "Dockerfile.dockerignore")); !errors.Is(err, os.ErrNotExist) {
		return errors.New("Dockerfile.dockerignore overrides verified config exclusions; remove it before publishing")
	}
	if _, err := os.Stat(envFile); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	repo, err := filepath.EvalSymlinks(s.a.Repo)
	if err != nil {
		return err
	}
	repo, err = filepath.Abs(repo)
	if err != nil {
		return err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(envFile))
	if err != nil {
		return err
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return err
	}
	real, err := filepath.EvalSymlinks(envFile)
	if err != nil {
		return err
	}
	real, err = filepath.Abs(real)
	if err != nil {
		return err
	}
	for _, path := range []string{filepath.Join(parent, filepath.Base(envFile)), real} {
		rel, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		rule := ""
		if rel == ".env" {
			rule = ".env"
		} else if strings.HasPrefix(rel, ".env.") && !strings.Contains(rel, string(filepath.Separator)) {
			rule = ".env.*"
		} else {
			return errors.New("config is inside the build context without supported exclusions; move it outside or use a root .env.* file")
		}
		data, err := os.ReadFile(filepath.Join(repo, ".dockerignore"))
		if err != nil {
			return errors.New("cannot verify config build-context exclusion")
		}
		found := false
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "!") {
				return errors.New("cannot verify config exclusion with negated .dockerignore rules")
			}
			if line == rule {
				found = true
			}
		}
		if !found {
			return errors.New(".dockerignore does not exclude the config file")
		}
	}
	return nil
}

func (s *swarm) resolveImage(ctx context.Context) error {
	image := s.config["KPL_IMAGE"]
	_, pinnedDigest, pinned := strings.Cut(image, "@")
	if pinned {
		fmt.Fprintf(s.a.Stdout, "Checking pinned image %s (pull timeout: %s)...\n", image, s.pullTimeout)
	} else {
		fmt.Fprintf(s.a.Stdout, "Resolving image tag %s (pull timeout: %s)...\n", image, s.pullTimeout)
	}
	pullCtx, cancel := context.WithTimeout(ctx, s.pullTimeout)
	defer cancel()
	pullApp := s.a.WithEnv(map[string]*string{"DOCKER_DEFAULT_PLATFORM": nil})
	// Keep exact digest lines: the generic Output helper trims whitespace,
	// which would accidentally accept malformed leading/trailing digest spaces.
	raw, err := pullApp.Runner.Output(pullCtx, pullApp.command("docker", []string{"pull", image}))
	if err != nil {
		return fmt.Errorf("image pull failed; no deployment changes made: %w", err)
	}
	digest := ""
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "Digest:") {
			count++
			fields := strings.Split(line, " ")
			if len(fields) != 2 || fields[0] != "Digest:" || !swarmDigest.MatchString(fields[1]) {
				return errors.New("pull returned an invalid registry digest; no deployment changes made")
			}
			digest = fields[1]
		}
	}
	if count != 1 {
		return errors.New("pull must return exactly one registry digest; no deployment changes made")
	}
	if pinned {
		if digest != pinnedDigest {
			return errors.New("pull returned a different registry digest than requested; no deployment changes made")
		}
	} else {
		colon := strings.LastIndex(image, ":")
		if colon <= strings.LastIndex(image, "/") {
			return errors.New("image tag is missing")
		}
		s.config["KPL_IMAGE"] = image[:colon] + "@" + digest
	}
	s.a = s.a.WithConfig(s.config)
	if err := s.verifyImageLayout(ctx); err != nil {
		return err
	}
	if err := s.a.Preflight(ctx, s.config, true); err != nil {
		return err
	}
	fmt.Fprintf(s.a.Stdout, "Deploying resolved image %s\n", s.config["KPL_IMAGE"])
	return nil
}

func validateRequestedImage(value string, digestRequired bool) error {
	if value == "" {
		return usageError("image reference cannot be empty")
	}
	if swarmDigest.MatchString(value) {
		return nil
	}
	if err := ValidateSetting("KPL_IMAGE", value); err != nil {
		return usageError("invalid image reference: " + err.Error())
	}
	if digestRequired && !strings.Contains(value, "@sha256:") {
		return usageError("--image requires a registry sha256 digest or repository@sha256:digest; use config set KPL_IMAGE=repository:tag to select a tag")
	}
	return nil
}

func requestedImageReference(configured, requested string) (string, error) {
	if !swarmDigest.MatchString(requested) {
		return requested, nil
	}
	if configured == "" {
		return "", usageError("a bare registry digest requires KPL_IMAGE to identify its repository; use repository@sha256:digest")
	}
	if err := ValidateSetting("KPL_IMAGE", configured); err != nil {
		return "", err
	}
	repository, _, _ := strings.Cut(configured, "@")
	if colon := strings.LastIndex(repository, ":"); colon > strings.LastIndex(repository, "/") {
		repository = repository[:colon]
	}
	return repository + "@" + requested, nil
}

// inspectImage only consults the selected daemon's local image metadata. Registry
// digests identify distributable images; an Image ID identifies local content.
func (s *swarm) inspectImage(ctx context.Context) error {
	image := s.config["KPL_IMAGE"]
	if image == "" {
		return usageError("configure KPL_IMAGE or supply an image reference to inspect")
	}
	configured := s.configuredImage
	if configured == "" {
		configured = s.config["KPL_IMAGE"]
	}
	fmt.Fprintf(s.a.Stdout, "Configured image: %s\nInspecting local image: %s\n", configured, image)
	out, err := s.output(ctx, "image", "inspect", "--format", "{{json .}}", image)
	if err != nil {
		return fmt.Errorf("inspect local image (no image was pulled or changed): %w", err)
	}
	var metadata struct {
		ID          string
		RepoDigests []string
	}
	if err := json.Unmarshal([]byte(out), &metadata); err != nil || !swarmDigest.MatchString(metadata.ID) {
		return errors.New("Docker returned invalid local image metadata")
	}
	fmt.Fprintf(s.a.Stdout, "Local image ID: %s\n", metadata.ID)
	if len(metadata.RepoDigests) == 0 {
		fmt.Fprintln(s.a.Stdout, "Registry digests: unknown (no RepoDigests recorded locally)")
		return nil
	}
	for _, digest := range metadata.RepoDigests {
		if !strings.Contains(digest, "@sha256:") || ValidateSetting("KPL_IMAGE", digest) != nil {
			return errors.New("Docker returned an invalid registry digest reference")
		}
		fmt.Fprintf(s.a.Stdout, "Registry digest: %s\n", digest)
	}
	return nil
}

func (s *swarm) logs(ctx context.Context) error {
	o := s.options
	if o.component != "access" && o.component != "auth" {
		return s.run(ctx, "service", "logs", "--tail", o.tail, "--timestamps", s.stack+"_"+o.component)
	}
	if !s.present["controller"] {
		return errors.New("Controller service does not exist")
	}
	out, err := s.output(ctx, "service", "ps", "--quiet", "--no-trunc", "--filter", "desired-state=running", s.stack+"_controller")
	if err != nil {
		return err
	}
	ids, err := swarmIDs(out)
	if err != nil {
		return err
	}
	var node, container string
	for _, id := range ids {
		record, err := s.output(ctx, "inspect", "--type", "task", "--format", "{{.NodeID}}|{{.Status.State}}|{{if .Status.ContainerStatus}}{{.Status.ContainerStatus.ContainerID}}{{end}}", id)
		if err != nil {
			return err
		}
		fields := strings.Split(record, "|")
		if len(fields) != 3 {
			return errors.New("Docker returned incomplete Controller task metadata")
		}
		if fields[1] != "running" {
			continue
		}
		if !swarmID.MatchString(fields[0]) || !swarmID.MatchString(fields[2]) {
			return errors.New("Docker returned invalid Controller task metadata")
		}
		if container != "" {
			return errors.New("multiple Controller tasks are running; retry after update settles")
		}
		node, container = fields[0], fields[2]
	}
	if container == "" {
		return errors.New("no running Controller task")
	}
	var prefix []string
	if o.logContext != "" {
		prefix = []string{"--context", o.logContext}
	}
	daemon, err := s.output(ctx, append(append([]string{}, prefix...), "info", "--format", "{{.Swarm.NodeID}}")...)
	if err != nil {
		return err
	}
	if daemon != node {
		return fmt.Errorf("Controller runs on node %s; use logs %s --context NAME connected to that node", node, o.component)
	}
	args := append(append([]string{}, prefix...), "exec", container)
	if o.tail == "all" {
		args = append(args, "cat", "--")
	} else {
		args = append(args, "tail", "-n", o.tail, "--")
	}
	args = append(args, "/var/lib/kpl/data/logs/"+o.component+".jsonl")
	return s.run(ctx, args...)
}

func swarmSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
