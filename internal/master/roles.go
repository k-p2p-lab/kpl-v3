package master

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/agent"
	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/controller"
	"github.com/k-p2p-lab/kpl-v3/internal/experimentnet"
	"github.com/k-p2p-lab/kpl-v3/internal/peer"
	"github.com/k-p2p-lab/kpl-v3/internal/runtimehealth"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

var roleCommands = []string{"controller", "agent", "swarm-agent", "resource-monitor", "network-manager", "network-gateway", "peer"}

func IsRuntimeCommand(name string) bool {
	if name == "internal-worker" {
		return true
	}
	for _, role := range roleCommands {
		if role == name {
			return true
		}
	}
	return false
}

func parseRoleFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return usageError(flags.Name() + " accepts flags only; unexpected positional arguments")
	}
	return nil
}

func roleDefaultError(flags *flag.FlagSet, name string, err error) error {
	if err == nil {
		return nil
	}
	explicit := false
	flags.Visit(func(value *flag.Flag) {
		if value.Name == name {
			explicit = true
		}
	})
	if explicit {
		return nil
	}
	return err
}

// Container roles share this executable and retain their own flag parsing.
// They run before operator option handling and do not need a source checkout.
func (a *App) executeRuntime(ctx context.Context, args []string) error {
	logger := slog.New(slog.NewJSONHandler(a.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	var err error
	switch args[0] {
	case "controller":
		err = runController(ctx, logger, args[1:])
	case "agent":
		err = runAgent(ctx, logger, args[1:])
	case "swarm-agent":
		err = a.runSwarmAgent(ctx, logger, args[1:])
	case "resource-monitor":
		err = runServiceMonitor(ctx, logger, args[1:])
	case "network-manager":
		err = runNetworkManager(ctx, args[1:])
	case "network-gateway":
		err = runNetworkGateway(ctx, args[1:])
	case "peer":
		err = runPeer(ctx, logger, args[1:])
	case "internal-worker":
		err = runInternalWorker(ctx, logger, args[1:])
	}
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		logger.Error("kpl stopped", "role", args[0], "error", err)
	}
	return err
}

// This role requires the private inherited IPC descriptors checked by Serve.
// It is deliberately absent from public help and completion role lists.
func runInternalWorker(ctx context.Context, logger *slog.Logger, args []string) error {
	if len(args) != 1 {
		return usageError("internal worker requires one private role")
	}
	kind := args[0]
	switch kind {
	case "controller-run", "controller-analysis", "controller-summary", "controller-export", "controller-journal", "agent-run":
	default:
		return usageError("unknown internal worker role")
	}
	return workerproc.Serve(ctx, kind, func(workerCtx context.Context, workerKind string, bootstrap json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		workerLogger := logger.With("workerKind", workerKind, "runId", peer.RunID(), "workerEpoch", peer.Epoch())
		slog.SetDefault(workerLogger)
		var handler workerproc.Handler
		var err error
		if workerKind == "agent-run" {
			handler, err = agent.NewRunWorkerHandler(workerCtx, workerKind, bootstrap, peer)
		} else {
			handler, err = controller.WorkerFactory(workerCtx, workerKind, bootstrap, peer)
		}
		if err != nil {
			return nil, err
		}
		go runtimehealth.Run(workerCtx, workerLogger, workerKind)
		return handler, nil
	})
}

func runServiceMonitor(ctx context.Context, logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("kpl run resource-monitor", flag.ContinueOnError)
	socket := flags.String("docker-socket", "/var/run/docker.sock", "local Docker Engine socket")
	controllerURL := flags.String("controller", os.Getenv("KPL_CONTROLLER_URL"), "Controller base URL")
	stack := flags.String("stack", os.Getenv("KPL_STACK_NAME"), "Swarm stack to monitor")
	nodeID := flags.String("node-id", os.Getenv("KPL_SWARM_NODE_ID"), "stable Swarm node ID")
	controlNode := flags.String("control-node-id", os.Getenv("KPL_CONTROL_NODE_ID"), "node hosting the default control services")
	nodeName := flags.String("node-name", os.Getenv("KPL_SWARM_NODE_HOSTNAME"), "Docker host name")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	user, password := os.Getenv("KPL_USER"), os.Getenv("KPL_PASSWORD")
	if err := auth.Validate(user, password); err != nil {
		return err
	}
	return agent.RunServiceMonitor(ctx, agent.ServiceMonitorConfig{DockerSocket: *socket, ControllerURL: *controllerURL,
		Token: auth.InternalToken(user, password), Stack: *stack, NodeID: *nodeID, NodeName: *nodeName, ControlNodeID: *controlNode}, logger)
}

func runController(ctx context.Context, logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("kpl run controller", flag.ContinueOnError)
	listen := flags.String("listen", ":8080", "HTTP listen address")
	networkManager := flags.String("network-manager", os.Getenv("KPL_NETWORK_MANAGER_URL"), "experiment network manager URL; reset the Peer overlay before every run")
	agentDiscoveryDNS := flags.String("agent-discovery-dns", os.Getenv("KPL_AGENT_DISCOVERY_DNS"), "optional Swarm Agent task DNS name for Dashboard discovery (port 8090)")
	dataDir := flags.String("data-dir", "data", "experiment data directory")
	minimumFree := uint64(1 << 30)
	var minimumFreeErr error
	if raw := os.Getenv("KPL_RUN_MIN_FREE_BYTES"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			minimumFreeErr = fmt.Errorf("invalid KPL_RUN_MIN_FREE_BYTES: %w", err)
		} else {
			minimumFree = value
		}
	}
	runMinFree := flags.Uint64("run-min-free-bytes", minimumFree, "minimum local free bytes before starting each run (0 disables admission guard)")
	archiveRate := controller.DefaultArchiveMiBPerSecond
	var archiveRateErr error
	if raw := os.Getenv("KPL_ARCHIVE_MIB_PER_SECOND"); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			archiveRateErr = fmt.Errorf("invalid KPL_ARCHIVE_MIB_PER_SECOND: %w", err)
		} else {
			archiveRate = value
		}
	}
	archiveMiB := flags.Uint64("archive-mib-per-second", archiveRate, "idle archive copy/verification limit in MiB/s (0 disables throttling; experiment pauses remain)")
	analysisRate := uint64(32)
	var analysisRateErr error
	if raw := os.Getenv("KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND"); raw != "" {
		analysisRate, analysisRateErr = strconv.ParseUint(raw, 10, 64)
		if analysisRateErr != nil {
			analysisRateErr = errors.New("invalid KPL_ANALYSIS_TRANSFER_MIB_PER_SECOND")
		}
	}
	analysisTransfer := flags.Uint64("analysis-transfer-mib-per-second", analysisRate, "remote result analysis transfer limit per worker in MiB/s (1-1024)")
	analysisPrefetch := uint64(64)
	var analysisPrefetchErr error
	if raw := os.Getenv("KPL_ANALYSIS_PREFETCH_MIB"); raw != "" {
		analysisPrefetch, analysisPrefetchErr = strconv.ParseUint(raw, 10, 64)
		if analysisPrefetchErr != nil {
			analysisPrefetchErr = errors.New("invalid KPL_ANALYSIS_PREFETCH_MIB")
		}
	}
	analysisPrefetchMiB := flags.Uint64("analysis-prefetch-mib", analysisPrefetch, "result analysis read-ahead queue per calculation in MiB (1-1024)")
	analysisCPUs, analysisCPUsErr := analysisResourceDefault("KPL_ANALYSIS_CPUS", 4)
	analysisCPUCount := flags.Uint64("analysis-cpus", analysisCPUs, "maximum Go execution lanes per analysis process (1-4096, bounded by available CPUs)")
	analysisMemory, analysisMemoryErr := analysisResourceDefault("KPL_ANALYSIS_MEMORY_MIB", 4096)
	analysisMemoryMiB := flags.Uint64("analysis-memory-mib", analysisMemory, "Go soft memory target per analysis process in MiB")
	user, password := os.Getenv("KPL_USER"), os.Getenv("KPL_PASSWORD")
	metricsURL := flags.String("metrics-url", os.Getenv("KPL_CONTROLLER_METRICS_URL"), "public Controller /metrics URL advertised to Prometheus")
	prometheusURL := flags.String("prometheus-url", os.Getenv("KPL_PROMETHEUS_URL"), "Controller-reachable Prometheus base URL for resource history exports (default http://127.0.0.1:9090)")
	prometheusPort := flags.String("prometheus-port", os.Getenv("PROMETHEUS_PORT"), "public Prometheus port advertised to the Dashboard (default 9090)")
	grafanaPort := flags.String("grafana-port", os.Getenv("GRAFANA_PORT"), "public Grafana port advertised to the Dashboard (default 3000)")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	if err := roleDefaultError(flags, "run-min-free-bytes", minimumFreeErr); err != nil {
		return err
	}
	if err := roleDefaultError(flags, "archive-mib-per-second", archiveRateErr); err != nil {
		return err
	}
	if err := roleDefaultError(flags, "analysis-transfer-mib-per-second", analysisRateErr); err != nil {
		return err
	}
	if *analysisTransfer < 1 || *analysisTransfer > 1024 {
		return errors.New("analysis transfer limit must be between 1 and 1024 MiB/s")
	}
	if err := roleDefaultError(flags, "analysis-prefetch-mib", analysisPrefetchErr); err != nil {
		return err
	}
	if *analysisPrefetchMiB < 1 || *analysisPrefetchMiB > 1024 {
		return errors.New("analysis prefetch queue must be between 1 and 1024 MiB")
	}
	if err := roleDefaultError(flags, "analysis-cpus", analysisCPUsErr); err != nil {
		return err
	}
	if *analysisCPUCount < 1 || *analysisCPUCount > 4096 {
		return errors.New("analysis CPU count must be between 1 and 4096")
	}
	if err := roleDefaultError(flags, "analysis-memory-mib", analysisMemoryErr); err != nil {
		return err
	}
	if *analysisMemoryMiB < 1 || *analysisMemoryMiB > uint64(^uint64(0)>>1)/(1<<20) {
		return errors.New("analysis memory target exceeds the supported range")
	}
	retiredAnalysisEndpoints := strings.Fields(os.Getenv("KPL_RETIRED_ANALYSIS_ENDPOINTS"))
	if err := controller.ValidateRetiredAnalysisEndpoints(retiredAnalysisEndpoints); err != nil {
		return err
	}
	parsedPrometheusPort, err := parsePort(*prometheusPort, 9090, "Prometheus")
	if err != nil {
		return err
	}
	parsedGrafanaPort, err := parsePort(*grafanaPort, 3000, "Grafana")
	if err != nil {
		return err
	}
	server := controller.New(controller.ServerConfig{
		RunWorkers:                   true,
		ControlNodeID:                strings.TrimSpace(os.Getenv("KPL_CONTROL_NODE_ID")),
		RuntimeImage:                 strings.TrimSpace(os.Getenv("KPL_RUNTIME_IMAGE")),
		NetworkManagerURL:            strings.TrimSpace(*networkManager),
		AgentDiscoveryDNS:            strings.TrimSpace(*agentDiscoveryDNS),
		Listen:                       *listen,
		DataDir:                      *dataDir,
		RunMinFreeBytes:              *runMinFree,
		ArchiveMiBPerSecond:          archiveMiB,
		AnalysisTransferMiBPerSecond: *analysisTransfer,
		AnalysisPrefetchMiB:          *analysisPrefetchMiB,
		AnalysisCPUs:                 *analysisCPUCount,
		AnalysisMemoryMiB:            *analysisMemoryMiB,
		RetiredAnalysisEndpoints:     retiredAnalysisEndpoints,
		User:                         user,
		Password:                     password,
		MetricsURL:                   *metricsURL,
		PrometheusURL:                *prometheusURL,
		PrometheusPort:               parsedPrometheusPort,
		GrafanaPort:                  parsedGrafanaPort,
	}, logger)
	// Keep diagnostics alive during graceful finalization after the service
	// context is canceled; stop them when the runtime actually returns.
	healthCtx, stopHealth := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHealth()
	go runtimehealth.Run(healthCtx, logger, "controller")
	return server.Run(ctx)
}

func analysisResourceDefault(key string, fallback uint64) (uint64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return value, nil
}

func parsePort(raw string, fallback int, name string) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	port, err := strconv.Atoi(raw)
	if err != nil || port < 1 || port > 65535 {
		return 0, fmt.Errorf("%s port must be an integer from 1 to 65535", name)
	}
	return port, nil
}

func runAgent(ctx context.Context, logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("kpl run agent", flag.ContinueOnError)
	hostRebootDefault, hostRebootErr := parseHostRebootEnabled(os.Getenv("KPL_AGENT_HOST_REBOOT_ENABLED"))
	hostRebootEnabled := flags.Bool("host-reboot-enabled", hostRebootDefault, "allow confirmed Linux worker host reboot through the Dashboard")
	controlNodeID := flags.String("control-node-id", os.Getenv("KPL_CONTROL_NODE_ID"), "Swarm node hosting the Controller; host reboot refuses this node")
	id := flags.String("id", "", "stable unique agent id")
	name := flags.String("name", "", "human-readable agent name")
	listen := flags.String("listen", ":8090", "HTTP listen address")
	advertiseURL := flags.String("advertise-url", "", "controller-reachable agent URL")
	metricsListen := flags.String("metrics-listen", "", "optional metrics-only HTTP listen address")
	metricsURL := flags.String("metrics-url", "", "public metrics URL advertised to the Controller")
	selfURL := flags.String("self-url", "", "agent overlay URL reachable from peer containers (defaults to advertise-url)")
	controllerURL := flags.String("controller-url", "", "controller URL on the Swarm peer overlay")
	capacity := flags.Int("capacity", 100, "default maximum active peers (overridable per Agent in Dashboard)")
	dataDir := flags.String("data-dir", "data-agent", "agent data directory")
	user, password := os.Getenv("KPL_USER"), os.Getenv("KPL_PASSWORD")
	labels := flags.String("labels", "", "comma-separated key=value labels")
	dockerSocket := flags.String("docker-socket", "/var/run/docker.sock", "local Docker Engine socket for Peer exit waits and KPL resource measurement")
	selfContainer := flags.String("self-container", os.Getenv("KPL_SWARM_TASK_NAME"), "Agent container name for Agent + Peer resource measurement")
	dockerBinary := flags.String("docker-binary", "docker", "Docker CLI executable")
	dockerImage := flags.String("docker-image", "", "peer image resolved from the running Swarm Agent task")
	dockerNetwork := flags.String("docker-network", "", "attachable Swarm peer overlay network")
	autoResetNetwork := flags.Bool("auto-reset-network", false, "require an initialized network before admitting Peers")
	controlNetwork := flags.String("control-network", "", "stable attachable overlay for Agent control")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	if err := roleDefaultError(flags, "host-reboot-enabled", hostRebootErr); err != nil {
		return err
	}
	if err := auth.Validate(user, password); err != nil {
		return err
	}
	analysisHandler, stopAnalysis := controller.NewAgentAnalysisWorker(ctx, auth.AnalysisWorkerToken(user, password), os.Getenv("KPL_RUNTIME_IMAGE"), logger)
	server, err := agent.New(authenticatedAgentConfig(user, password, agent.Config{
		AnalysisHandler: analysisHandler,
		StopAnalysis:    stopAnalysis,
		RunWorkerFactory: func(workerCtx context.Context, config agent.RunWorkerConfig, broker workerproc.Handler) (*workerproc.Process, error) {
			return workerproc.Start(workerCtx, "agent-run", config, broker)
		},
		HostRebootEnabled: *hostRebootEnabled, ControlNodeID: *controlNodeID,
		AutoResetNetwork: *autoResetNetwork, ControlNetwork: *controlNetwork,
		ID: *id, Name: *name, Listen: *listen, AdvertiseURL: *advertiseURL,
		MetricsListen: *metricsListen, MetricsURL: *metricsURL, SelfURL: *selfURL,
		ControllerURL: *controllerURL, Capacity: *capacity, DataDir: *dataDir,
		Labels:       parseLabels(*labels),
		DockerSocket: *dockerSocket, SelfContainer: *selfContainer,
		DockerBinary: *dockerBinary, DockerImage: *dockerImage, DockerNetwork: *dockerNetwork,
	}), logger)
	if err != nil {
		_ = stopAnalysis(context.Background())
		return err
	}
	healthCtx, stopHealth := context.WithCancel(context.WithoutCancel(ctx))
	defer stopHealth()
	go runtimehealth.Run(healthCtx, logger, "agent")
	return server.Run(ctx)
}

// Host management credentials stay separate from the key distributed to Peers.
// Both are derived from the configured account without another operator input.
func authenticatedAgentConfig(user, password string, config agent.Config) agent.Config {
	config.Token = auth.InternalToken(user, password)
	config.HostRebootToken = auth.HostRebootToken(user, password)
	return config
}

func parseHostRebootEnabled(raw string) (bool, error) {
	if raw == "" {
		return false, nil
	}
	if err := ValidateSetting("KPL_AGENT_HOST_REBOOT_ENABLED", raw); err != nil {
		return false, err
	}
	return raw == "true", nil
}

func runNetworkManager(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("kpl run network-manager", flag.ContinueOnError)
	dataDir := flags.String("data-dir", "/var/lib/kpl/network", "network recovery state directory")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	user, password := os.Getenv("KPL_USER"), os.Getenv("KPL_PASSWORD")
	if err := auth.Validate(user, password); err != nil {
		return err
	}
	return experimentnet.RunManager(ctx, experimentnet.ManagerConfig{Stack: os.Getenv("KPL_STACK_NAME"), PeerNetwork: os.Getenv("KPL_PEER_NETWORK"),
		ControlNetwork: os.Getenv("KPL_CONTROL_NETWORK"), Subnet: os.Getenv("KPL_PEER_SUBNET"), ControllerURL: os.Getenv("KPL_CONTROLLER_URL"),
		Token: auth.InternalToken(user, password), SelfContainer: os.Getenv("KPL_SWARM_TASK_NAME"), DataDir: *dataDir})
}

func runNetworkGateway(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("kpl run network-gateway", flag.ContinueOnError)
	configPath := flags.String("config", "", "gateway configuration file")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}
	var config experimentnet.GatewayConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	return experimentnet.RunGateway(ctx, config)
}

func runPeer(ctx context.Context, logger *slog.Logger, args []string) error {
	flags := flag.NewFlagSet("kpl run peer", flag.ContinueOnError)
	configPath := flags.String("config", "", "peer container JSON config")
	if err := parseRoleFlags(flags, args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}
	return peer.Run(ctx, *configPath, logger)
}

func parseLabels(raw string) map[string]string {
	result := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && key != "" {
			result[key] = value
		}
	}
	return result
}
