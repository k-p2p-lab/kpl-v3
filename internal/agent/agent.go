package agent

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var errCapacityReached = errors.New("agent capacity reached")

type Config struct {
	DockerBinary  string
	DockerImage   string
	DockerNetwork string
	ID            string
	Name          string
	Listen        string
	AdvertiseURL  string
	MetricsListen string
	MetricsURL    string
	SelfURL       string
	ControllerURL string
	Token         string
	Capacity      int
	DataDir       string
	Labels        map[string]string
}

type process struct {
	node                  model.Node
	apiURL                string
	configPath            string
	cancel                context.CancelFunc
	exited                bool
	containerID           string
	done                  chan struct{}
	cleanupErr            error
	heartbeatAcknowledged bool
	// Peer source time is distinct from node.LastSeen, which is the Agent's
	// receipt time. Only timestamps from this process are compared for order.
	peerStatusObservedAt time.Time
}

type Server struct {
	startupReconciled bool
	spool             *telemetrySpool
	spoolError        error
	config            Config
	logger            *slog.Logger
	client            *http.Client
	docker            *dockerRuntime
	startedAt         time.Time
	mu                sync.RWMutex
	heartbeatMu       sync.Mutex
	capacityLimit     int // protected by mu; zero uses the CLI default
	capacityRevision  string
	processes         map[string]*process
	runFences         map[string]uint64
	shuttingDown      bool
	eventsMu          sync.Mutex
	events            []model.TraceEvent
	eventsInFlight    int
	inFlightRuns      map[string]int
	terminations      map[string]model.TraceEvent
	telemetryClosed   bool
	flushNow          chan struct{}
}

func New(config Config, logger *slog.Logger) (*Server, error) {
	if config.DockerBinary == "" {
		config.DockerBinary = "docker"
	}
	if config.DockerImage == "" {
		return nil, fmt.Errorf("docker peer image is required")
	}
	if config.DockerNetwork == "" {
		return nil, fmt.Errorf("docker peer overlay network is required")
	}
	if config.ID == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	if config.Name == "" {
		config.Name = config.ID
	}
	if config.Listen == "" {
		config.Listen = ":8090"
	}
	if config.ControllerURL == "" || config.AdvertiseURL == "" {
		return nil, fmt.Errorf("controller url and advertise url are required")
	}
	if err := validateMetricsEndpoint(config.MetricsListen, config.MetricsURL); err != nil {
		return nil, err
	}
	if config.Capacity <= 0 {
		config.Capacity = 100
	}
	if config.DataDir == "" {
		config.DataDir = "data-agent"
	}
	if config.SelfURL == "" {
		config.SelfURL = config.AdvertiseURL
	}
	for name, raw := range map[string]string{"controller-url": config.ControllerURL, "self-url": config.SelfURL} {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Hostname() == "" || parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("%s must be an HTTP(S) URL reachable from peer containers", name)
		}
		ip := net.ParseIP(parsed.Hostname())
		if strings.EqualFold(parsed.Hostname(), "localhost") || ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
			return nil, fmt.Errorf("%s must be reachable from peer containers; loopback and unspecified addresses refer to the peer itself", name)
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		config:    config,
		logger:    logger,
		client:    &http.Client{Timeout: 10 * time.Second},
		docker:    &dockerRuntime{binary: config.DockerBinary, image: config.DockerImage, network: config.DockerNetwork, commandContext: exec.CommandContext},
		startedAt: time.Now().UTC(),
		processes: make(map[string]*process),
		runFences: make(map[string]uint64),
		flushNow:  make(chan struct{}, 1),
	}
	var spoolErr error
	s.spool, s.events, spoolErr = openTelemetrySpool(config.DataDir)
	if spoolErr != nil {
		return nil, spoolErr
	}
	return s, nil
}

func (s *Server) Run(ctx context.Context) error {
	// Bind before reconciling: a duplicate Agent on the same host must not remove the
	// running Agent's peers and only then discover that one of its ports is
	// occupied.
	listener, err := net.Listen("tcp", s.config.Listen)
	if err != nil {
		return fmt.Errorf("listen on Agent control API: %w", err)
	}
	defer listener.Close()
	var metricsListener net.Listener
	if s.config.MetricsListen != "" {
		metricsListener, err = net.Listen("tcp", s.config.MetricsListen)
		if err != nil {
			return fmt.Errorf("listen on Agent metrics endpoint: %w", err)
		}
		defer metricsListener.Close()
	}
	return s.serve(ctx, listener, metricsListener)
}

func (s *Server) serve(ctx context.Context, listener, metricsListener net.Listener) (resultErr error) {
	s.logger.Info("agent startup checking Docker runtime", "id", s.config.ID)
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := s.docker.check(checkCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("Docker runtime: %w", err)
	}
	imageCtx, imageCancel := context.WithTimeout(ctx, 30*time.Second)
	imageID, imageErr := s.docker.run(imageCtx, nil, "image", "inspect", "--format", "{{.Id}}", s.config.DockerImage)
	imageCancel()
	if imageErr != nil {
		return fmt.Errorf("resolve immutable Peer image: %w", imageErr)
	}
	if value := strings.TrimSpace(string(imageID)); strings.HasPrefix(value, "sha256:") {
		s.docker.image = value
	}
	s.logger.Info("agent startup cleaning previous Peer containers", "id", s.config.ID)
	cleanupStarted := time.Now()
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, containerStopTimeout)
	err = s.docker.removeAgentContainers(cleanupCtx, s.config.ID)
	cleanupCancel()
	if err != nil {
		return fmt.Errorf("clean up previous peer containers: %w", err)
	}
	s.startupReconciled = true
	s.logger.Info("agent startup cleanup complete", "id", s.config.ID, "elapsed", time.Since(cleanupStarted))
	server := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      190 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	var metricsServer *http.Server
	if metricsListener != nil {
		metricsServer = &http.Server{
			Handler:           s.metricsOnlyHandler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       30 * time.Second,
		}
	}
	// Continue forwarding Peer telemetry while shutdown waits for the Peers.
	controlCtx, controlCancel := context.WithCancel(context.WithoutCancel(ctx))
	controlDone := make(chan struct{})
	shutdownRequested, requestShutdown := context.WithCancel(ctx)
	shutdownDone := make(chan error, 1)
	var metricsServeDone chan error
	defer func() {
		requestShutdown()
		if err := <-shutdownDone; err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("Agent shutdown incomplete: %w", err))
		}
		if metricsServeDone != nil {
			if err := <-metricsServeDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
				resultErr = errors.Join(resultErr, fmt.Errorf("serve Agent metrics endpoint: %w", err))
			}
		}
		controlCancel()
		<-controlDone
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer drainCancel()
		resultErr = errors.Join(resultErr, s.drainEvents(drainCtx))
	}()
	go func() {
		defer close(controlDone)
		s.controlLoop(controlCtx)
	}()
	go func() {
		<-shutdownRequested.Done()
		s.beginShutdown()
		// Keep the telemetry endpoint open while Peers send their final session
		// markers. Shutting down HTTP first would turn graceful exits into gaps.
		cleanupErr := s.waitStopped(containerStopTimeout)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if metricsServer != nil {
			if err := metricsServer.Shutdown(shutdownCtx); err != nil {
				_ = metricsServer.Close()
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("finish metrics HTTP handlers: %w", err))
			}
		}
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("finish HTTP handlers: %w", err))
		}
		// Serve returns before Shutdown finishes its handlers. Fence admission
		// before the final drain so a slow request cannot be acknowledged later.
		s.closeTelemetry()
		shutdownDone <- cleanupErr
	}()
	if metricsServer != nil {
		metricsServeDone = make(chan error, 1)
		go func() {
			err := metricsServer.Serve(metricsListener)
			metricsServeDone <- err
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				requestShutdown()
			}
		}()
		s.logger.Info("agent metrics listening", "id", s.config.ID, "address", metricsListener.Addr(), "advertise", s.config.MetricsURL)
	}
	s.logger.Info("agent listening", "id", s.config.ID, "address", listener.Addr(), "advertise", s.config.AdvertiseURL)
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) controlLoop(ctx context.Context) {
	// A congested event sink must not prevent the Agent from renewing its
	// lease. Otherwise healthy capacity disappears from placement while peers
	// continue to expire, reducing the live population during churn.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		s.heartbeatLoop(ctx)
	}()
	defer func() { <-heartbeatDone }()
	flush := time.NewTicker(500 * time.Millisecond)
	defer flush.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-flush.C:
			s.flushEvents(ctx)
		case <-s.flushNow:
			s.flushEvents(ctx)
		}
	}
}

func (s *Server) heartbeatLoop(ctx context.Context) {
	registered := false
	heartbeat := time.NewTicker(2 * time.Second)
	defer heartbeat.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if !registered {
			if err := s.register(ctx); err != nil {
				s.logger.Warn("agent registration failed", "error", err)
			} else {
				registered = true
				s.logger.Info("agent registered", "id", s.config.ID, "controller", s.config.ControllerURL)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if registered {
				if err := s.heartbeat(ctx); err != nil {
					s.logger.Warn("agent heartbeat failed", "error", err)
					// A timeout or temporary Controller outage does not invalidate
					// registration. Replaying all acknowledged history on every
					// transport failure can prevent a busy Agent from recovering.
					var responseErr *controllerResponseError
					if errors.As(err, &responseErr) && responseErr.statusCode == http.StatusConflict {
						registered = false
					}
				}
			}
		}
	}
}

// Capture node states, occupied capacity and observation time under one lock.
// The Controller can compare heartbeat and status responses even when those
// requests cross in flight or an Agent task has restarted on the same host.
func (s *Server) snapshot() model.AgentHeartbeat {
	return s.snapshotWithHistory(true)
}

func (s *Server) snapshotAgent() model.Agent {
	hostname, _ := os.Hostname()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.agentStatusLocked(hostname)
}

// Caller holds mu. Registration needs only the lease/capacity, not copies of
// every departed peer's status. Inventory acknowledgments remain independent.
func (s *Server) agentStatusLocked(hostname string) model.Agent {
	image := s.config.DockerImage
	if s.docker != nil {
		image = s.docker.image
	}
	return model.Agent{
		ID:         s.config.ID,
		Name:       s.config.Name,
		URL:        strings.TrimRight(s.config.AdvertiseURL, "/"),
		MetricsURL: s.config.MetricsURL,
		Hostname:   hostname,
		Version:    model.BuildVersion(),
		PeerImage:  image, RunDrain: true, StartupReconciled: s.startupReconciled,
		Capacity:         s.capacityLocked(),
		DefaultCapacity:  s.config.Capacity,
		CapacityRevision: s.capacityRevision,
		ActiveNodes:      s.capacityUsedLocked(),
		State:            model.AgentOnline,
		Labels:           s.config.Labels,
		StartedAt:        s.startedAt,
		LastSeen:         time.Now().UTC(),
	}
}

func (s *Server) snapshotWithHistory(includeAcknowledged bool) model.AgentHeartbeat {
	hostname, _ := os.Hostname()
	s.mu.RLock()
	capacity := len(s.processes)
	if !includeAcknowledged {
		capacity = min(capacity, max(0, s.capacityLocked()))
	}
	h := model.AgentHeartbeat{
		Agent: s.agentStatusLocked(hostname),
		Nodes: make([]model.Node, 0, capacity),
	}
	for _, proc := range s.processes {
		if !includeAcknowledged && proc.heartbeatAcknowledged && processSuccessfullyStopped(proc) {
			continue
		}
		h.Nodes = append(h.Nodes, heartbeatNodeStatus(proc))
	}
	s.mu.RUnlock()
	sort.Slice(h.Nodes, func(i, j int) bool { return h.Nodes[i].ID < h.Nodes[j].ID })
	return h
}

func (s *Server) createNode(ctx context.Context, request model.CreateNodeRequest) (model.Node, error) {
	if request.ID == "" || request.RunID == "" || request.Group == "" {
		return model.Node{}, fmt.Errorf("id, runId, and group are required")
	}
	if request.Lifetime != "" {
		lifetime, err := time.ParseDuration(request.Lifetime)
		if err != nil || lifetime < 0 {
			return model.Node{}, fmt.Errorf("lifetime must be a non-negative duration")
		}
	}
	resolvedConfig, nodeType := resolveCreateNodeConfig(request)
	if err := resolvedConfig.Validate(); err != nil {
		return model.Node{}, fmt.Errorf("invalid node config: %w", err)
	}
	requestedNetwork, _ := json.Marshal(resolvedConfig.Network)
	networkConfig, err := resolvedConfig.Network.Resolve(rand.New(rand.NewSource(request.Seed)))
	if err != nil {
		return model.Node{}, fmt.Errorf("resolve peer network conditions: %w", err)
	}
	resolvedConfig.Network = networkConfig
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return model.Node{}, fmt.Errorf("agent is shutting down")
	}
	if fence, exists := s.runFences[request.RunID]; exists && request.Generation <= fence {
		s.mu.Unlock()
		return model.Node{}, fmt.Errorf("run %q generation %d is fenced at generation %d", request.RunID, request.Generation, fence)
	}
	if _, exists := s.processes[request.ID]; exists {
		s.mu.Unlock()
		return model.Node{}, fmt.Errorf("node %q already exists", request.ID)
	}
	if s.capacityUsedLocked() >= s.capacityLocked() {
		s.mu.Unlock()
		return model.Node{}, errCapacityReached
	}
	now := time.Now().UTC()
	profile := request.Profile
	if profile == "" {
		profile = nodeType
	}
	topicsJSON, _ := json.Marshal(resolvedConfig.GossipSub.Topics)
	node := model.Node{
		ID:         request.ID,
		RunID:      request.RunID,
		Generation: request.Generation,
		AgentID:    s.config.ID,
		Group:      request.Group,
		Role:       request.Role,
		Type:       nodeType,
		Profile:    profile,
		State:      model.NodeStarting,
		Metadata: map[string]string{
			"runtime":       "docker",
			"profile":       profile,
			"pubsubRouter":  resolvedConfig.GossipSub.Router,
			"pubsubEnabled": strconv.FormatBool(resolvedConfig.GossipSub.Enabled != nil && *resolvedConfig.GossipSub.Enabled),
			"scoreEnabled":  strconv.FormatBool(resolvedConfig.GossipSub.Enabled != nil && *resolvedConfig.GossipSub.Enabled && resolvedConfig.GossipSub.Router == "gossipsub" && resolvedConfig.GossipSub.Score != nil && resolvedConfig.GossipSub.Score.IsEnabled()),
			"allowPublish":  strconv.FormatBool(resolvedConfig.PublishAllowed()),
			"topicMode":     resolvedConfig.GossipSub.TopicMode,
			"topics":        strings.Join(resolvedConfig.GossipSub.Topics, ","),
			"topicsJSON":    string(topicsJSON),
			"dhtEnabled":    strconv.FormatBool(resolvedConfig.Kademlia.Enabled != nil && *resolvedConfig.Kademlia.Enabled),
			"dhtMode":       resolvedConfig.Kademlia.Mode,
		},
		StartedAt: now,
		LastSeen:  now,
	}
	processCtx, cancel := context.WithCancel(ctx)
	networkJSON, _ := json.Marshal(resolvedConfig.Network)
	node.Metadata["network"] = string(networkJSON)
	node.Metadata["networkRequested"] = string(requestedNetwork)
	node.Metadata["seed"] = strconv.FormatInt(request.Seed, 10)
	if request.Lifetime != "" {
		node.Metadata["lifetime"] = request.Lifetime
		node.Metadata["lifetimeBasis"] = "container-created"
	}
	peerConfig := model.PeerProcessConfig{
		Node:          node,
		NodeConfig:    resolvedConfig,
		Seed:          request.Seed,
		ControllerURL: strings.TrimRight(s.config.ControllerURL, "/"),
		AgentURL:      strings.TrimRight(s.config.SelfURL, "/"),
		APListen:      "0.0.0.0:18000",
		P2PListen:     "/ip4/0.0.0.0/tcp/20000",
		Token:         s.config.Token,
	}
	// Reserve capacity before preparing files so duplicate creates and run
	// fences see the in-flight peer. Keep its mutable status separate from the
	// configuration/response, which are encoded without the state lock.
	proc := &process{node: cloneNodeStatus(node), cancel: cancel, done: make(chan struct{})}
	s.processes[request.ID] = proc
	s.mu.Unlock()

	// Disk stalls must not block heartbeats, lifetime stops or other admissions.
	configPath, err := s.writePeerConfig(peerConfig)
	var data []byte
	if err == nil {
		data, err = json.Marshal(peerConfig)
	}
	if err == nil {
		err = processCtx.Err()
	}
	if err != nil {
		cancel()
		s.finishProcess(request.ID, proc, err, nil)
		return model.Node{}, err
	}
	s.mu.Lock()
	proc.configPath = configPath
	s.mu.Unlock()
	go s.runDockerProcess(processCtx, proc, data, request.Lifetime)
	return node, nil
}

func resolveCreateNodeConfig(request model.CreateNodeRequest) (model.NodeConfig, string) {
	kind := request.Type
	if kind == "" {
		kind = request.Config.Type
	}
	if kind == "" {
		if request.Role == "boot" {
			kind = "boot"
		} else {
			kind = "full"
		}
	}
	if base, ok := model.BuiltInNodeConfig(kind); ok {
		resolved := base.Merge(request.Config).WithDefaults()
		// The chosen built-in preset is authoritative, including aliases such as
		// worker, which canonicalize to full.
		resolved.Type = base.Type
		return resolved, resolved.Type
	}
	base, _ := model.BuiltInNodeConfig("full")
	base.Type = kind
	resolved := base.Merge(request.Config)
	resolved.Type = kind
	resolved = resolved.WithDefaults()
	return resolved, resolved.Type
}

func scheduleLifetimeStop(ctx context.Context, value string, stop func()) bool {
	if value == "" {
		return false
	}
	lifetime, err := time.ParseDuration(value)
	if err != nil || lifetime < 0 {
		return false
	}
	go func() {
		timer := time.NewTimer(lifetime)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
			stop()
		}
	}()
	return true
}

func (s *Server) writePeerConfig(config model.PeerProcessConfig) (string, error) {
	dir := filepath.Join(s.config.DataDir, "nodes", safeName(config.Node.ID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "peer.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Server) finishProcess(nodeID string, proc *process, runErr, cleanupErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	proc.exited = true
	proc.cleanupErr = cleanupErr
	current, ok := s.processes[nodeID]
	if ok && current == proc {
		current.node.LastSeen = time.Now().UTC()
		if cleanupErr == nil {
			// This is an upper bound on the exit time, never evidence that the
			// process stayed subscribed until the Agent observed its exit.
			s.queueTermination(model.TraceEvent{EventID: cryptorand.Text(), RunID: current.node.RunID, NodeID: nodeID, AgentID: s.config.ID, Type: "measurement_terminated", Timestamp: current.node.LastSeen})
		}
		setProcessMetadata(current, "stoppedAt", current.node.LastSeen.Format(time.RFC3339Nano))
		if cleanupErr == nil && (current.node.State == model.NodeStopping || runErr == nil) {
			current.node.State = model.NodeStopped
			current.node.Error = ""
		} else {
			current.node.State = model.NodeFailed
			current.node.Error = errors.Join(runErr, cleanupErr).Error()
		}
	}
	if proc.exited && proc.cleanupErr == nil && proc.node.State == model.NodeStopped {
		proc.node = heartbeatNodeStatus(proc)
		proc.apiURL = ""
	}
	if proc.done != nil {
		close(proc.done)
	}
}

func (s *Server) stopNode(nodeID string) error {
	s.mu.Lock()
	proc, ok := s.processes[nodeID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("node %q not found", nodeID)
	}
	if proc.node.State == model.NodeStopped || proc.node.State == model.NodeFailed {
		if proc.cleanupErr != nil && proc.containerID != "" {
			s.retryContainerCleanupLocked(proc)
		}
		s.mu.Unlock()
		return nil
	}
	proc.node.State = model.NodeStopping
	proc.node.LastSeen = time.Now().UTC()
	if proc.node.Metadata["stopRequestedAt"] == "" {
		setProcessMetadata(proc, "stopRequestedAt", proc.node.LastSeen.Format(time.RFC3339Nano))
	}
	cancel := proc.cancel
	s.mu.Unlock()
	cancel()
	return nil
}

func (s *Server) stopAll() {
	s.mu.RLock()
	ids := make([]string, 0, len(s.processes))
	for id, proc := range s.processes {
		if !proc.exited || proc.cleanupErr != nil {
			ids = append(ids, id)
		}
	}
	s.mu.RUnlock()
	for _, id := range ids {
		_ = s.stopNode(id)
	}
}

// stopRunGeneration establishes the fence before marking matching processes as
// stopping. A create for the stopped generation can therefore never commit
// after this method has taken the lock: it either committed first and is in the
// cancellation set, or observes the fence and is rejected.
func (s *Server) stopRunGeneration(runID string, generation uint64) {
	s.mu.Lock()
	if s.runFences == nil {
		s.runFences = make(map[string]uint64)
	}
	fence, exists := s.runFences[runID]
	if !exists || generation > fence {
		fence = generation
		s.runFences[runID] = fence
	}
	now := time.Now().UTC()
	cancels := make([]context.CancelFunc, 0)
	for _, proc := range s.processes {
		if proc.node.RunID != runID || proc.node.Generation > fence {
			continue
		}
		if proc.exited {
			if proc.cleanupErr != nil && proc.containerID != "" {
				s.retryContainerCleanupLocked(proc)
			}
			continue
		}
		if proc.node.State == model.NodeStopping || proc.node.State == model.NodeStopped || proc.node.State == model.NodeFailed {
			continue
		}
		proc.node.State = model.NodeStopping
		proc.node.LastSeen = now
		if proc.node.Metadata["stopRequestedAt"] == "" {
			setProcessMetadata(proc, "stopRequestedAt", now.Format(time.RFC3339Nano))
		}
		if proc.cancel != nil {
			cancels = append(cancels, proc.cancel)
		}
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (s *Server) updateNode(update model.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	proc, ok := s.processes[update.ID]
	if !ok {
		return fmt.Errorf("node %q not found", update.ID)
	}
	if proc.exited || proc.node.State == model.NodeStopping || proc.node.State == model.NodeStopped || proc.node.State == model.NodeFailed {
		return nil
	}
	// An HTTP retry or crossed status request must not restore older state.
	// Never compare the Peer clock with the Agent's local receipt timestamp.
	// Legacy reports without source times work until an ordered report arrives.
	if !proc.peerStatusObservedAt.IsZero() && (update.LastSeen.IsZero() || !update.LastSeen.After(proc.peerStatusObservedAt)) {
		return nil
	}
	if !update.LastSeen.IsZero() {
		proc.peerStatusObservedAt = update.LastSeen
	}
	proc.node.PeerID = update.PeerID
	// A Docker peer may report before inspect has resolved its control endpoint.
	// Keep it starting until Agent can actually route publish requests to it.
	if update.State != model.NodeReady || proc.apiURL != "" {
		proc.node.State = update.State
	}
	if proc.node.State == model.NodeReady && proc.node.Metadata["readyAt"] == "" {
		setProcessMetadata(proc, "readyAt", time.Now().UTC().Format(time.RFC3339Nano))
	}
	proc.node.Addresses = slices.Clone(update.Addresses)
	proc.node.ConnectedPeers = slices.Clone(update.ConnectedPeers)
	proc.node.TopicPeers = maps.Clone(update.TopicPeers)
	proc.node.PeerScores = maps.Clone(update.PeerScores)
	// Observation time makes an empty routing/mesh snapshot authoritative.
	// Missing legacy fields cannot erase an already observed overlay, and a
	// stale overlay cannot replace a newer snapshot in an otherwise new report.
	if !update.OverlayObservedAt.IsZero() && (proc.node.OverlayObservedAt.IsZero() || update.OverlayObservedAt.After(proc.node.OverlayObservedAt)) {
		proc.node.RoutingPeers = slices.Clone(update.RoutingPeers)
		proc.node.MeshPeers = cloneMeshPeers(update.MeshPeers)
		proc.node.OverlayObservedAt = update.OverlayObservedAt
	}
	proc.node.LastSeen = time.Now().UTC()
	proc.node.Error = update.Error
	return nil
}

func (s *Server) nodes() []model.Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]model.Node, 0, len(s.processes))
	for _, proc := range s.processes {
		result = append(result, cloneNodeStatus(proc.node))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func cloneMeshPeers(peers map[string][]string) map[string][]string {
	if peers == nil {
		return nil
	}
	cloned := make(map[string][]string, len(peers))
	for topic, members := range peers {
		cloned[topic] = slices.Clone(members)
	}
	return cloned
}

// Keep every terminal ID in the full inventory so the Controller can reconcile
// reservations even when it missed the entire lifetime. Successful exits no
// longer have a live overlay; resending their last connections, scores and
// resolved configuration makes churn history overflow the heartbeat body limit.
// Successfully removed peers retain this compact record in memory as well.
func heartbeatNodeStatus(proc *process) model.Node {
	if !proc.exited || proc.cleanupErr != nil || proc.node.State != model.NodeStopped {
		return cloneNodeStatus(proc.node)
	}
	node := proc.node
	node.Addresses = nil
	node.ConnectedPeers = nil
	node.RoutingPeers = nil
	node.MeshPeers = nil
	node.TopicPeers = nil
	node.PeerScores = nil
	node.OverlayObservedAt = time.Time{}
	node.Metadata = make(map[string]string)
	// Preserve lifecycle evidence and topic labels used by Controller metrics.
	for _, key := range []string{"runtime", "containerId", "containerCreatedAt", "containerStartedAt", "lifetimeBasis", "stoppedAt", "stopRequestedAt", "topics", "topicsJSON", "pubsubEnabled", "topicMode"} {
		if value, exists := proc.node.Metadata[key]; exists {
			node.Metadata[key] = value
		}
	}
	return node
}

// Returned status may be encoded after unlocking; it must own every mutable
// collection so concurrent peer reports cannot change a captured heartbeat.
func cloneNodeStatus(node model.Node) model.Node {
	node.Addresses = slices.Clone(node.Addresses)
	node.ConnectedPeers = slices.Clone(node.ConnectedPeers)
	node.RoutingPeers = slices.Clone(node.RoutingPeers)
	node.MeshPeers = cloneMeshPeers(node.MeshPeers)
	node.TopicPeers = maps.Clone(node.TopicPeers)
	node.PeerScores = maps.Clone(node.PeerScores)
	node.Metadata = maps.Clone(node.Metadata)
	return node
}

// Caller holds mu. The startup config remains the reset/default value.
func (s *Server) capacityLocked() int {
	if s.capacityLimit > 0 {
		return s.capacityLimit
	}
	return s.config.Capacity
}

// A Docker slot is occupied from admission until its container is confirmed
// removed. Stopping and failed-cleanup peers still use the host's resources.
// Caller holds Server.mu.
func (s *Server) capacityUsedLocked() int {
	count := 0
	for _, proc := range s.processes {
		if !proc.exited || proc.cleanupErr != nil {
			count++
		}
	}
	return count
}

func (s *Server) enqueueEvents(batch model.EventBatch) bool {
	s.eventsMu.Lock()
	// Reserve space for batches already being sent. A failed Controller request
	// must be requeued without discarding events the Agent has acknowledged.
	if s.telemetryClosed || len(s.events)+s.eventsInFlight+len(batch.Events) > 50000 {
		s.eventsMu.Unlock()
		return false
	}
	for i := range batch.Events {
		batch.Events[i].AgentID = s.config.ID
	}
	if err := s.spool.append(batch.Events); err != nil {
		s.spoolError = err
		s.eventsMu.Unlock()
		s.logger.Error("persist telemetry spool", "error", err)
		return false
	}
	s.events = append(s.events, batch.Events...)
	length := len(s.events)
	s.eventsMu.Unlock()
	if length >= 250 {
		select {
		case s.flushNow <- struct{}{}:
		default:
		}
	}
	return true
}

func (s *Server) closeTelemetry() {
	s.eventsMu.Lock()
	s.telemetryClosed = true
	s.eventsMu.Unlock()
}

func (s *Server) flushEvents(ctx context.Context) {
	s.eventsMu.Lock()
	// Cleanup handlers can drain while controlLoop is forwarding a batch. Keep
	// a single request in flight so retries remain ahead of subsequent events.
	if s.eventsInFlight > 0 {
		s.eventsMu.Unlock()
		return
	}
	s.admitTerminationsLocked()
	if len(s.events) == 0 {
		s.eventsMu.Unlock()
		return
	}
	data, count, err := model.MarshalEventBatchPrefix(s.config.ID, s.events)
	if err != nil {
		s.eventsMu.Unlock()
		s.logger.Error("encode telemetry batch", "error", err)
		return
	}
	batchEvents := append([]model.TraceEvent(nil), s.events[:count]...)
	s.eventsInFlight += count
	s.inFlightRuns = map[string]int{}
	for _, event := range batchEvents {
		s.inFlightRuns[event.RunID]++
	}
	s.events = append([]model.TraceEvent(nil), s.events[count:]...)
	s.eventsMu.Unlock()
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = s.postData(flushCtx, "/api/v1/events/batch", data, nil)
	s.eventsMu.Lock()
	s.eventsInFlight -= count
	s.inFlightRuns = nil
	if err == nil {
		err = s.spool.acknowledge(batchEvents)
	}
	if err == nil && len(s.events) == 0 && len(s.terminations) == 0 {
		s.spoolError = nil
	}
	if err != nil {
		s.events = append(batchEvents, s.events...)
		s.logger.Warn("telemetry flush failed", "events", len(batchEvents), "error", err)
	}
	s.eventsMu.Unlock()
}

func (s *Server) drainEvents(ctx context.Context) error {
	for ctx.Err() == nil {
		s.eventsMu.Lock()
		remaining := len(s.events) + s.eventsInFlight + len(s.terminations)
		s.eventsMu.Unlock()
		if remaining == 0 {
			return nil
		}
		s.flushEvents(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	s.eventsMu.Lock()
	remaining := len(s.events) + s.eventsInFlight + len(s.terminations)
	s.eventsMu.Unlock()
	if remaining > 0 {
		s.logger.Warn("telemetry drain incomplete", "events", remaining, "error", ctx.Err())
		return fmt.Errorf("drain Agent telemetry (%d events remaining): %w", remaining, ctx.Err())
	}
	return nil
}

// Retain one pending exit marker per process instead of silently losing it
// when the trace queue is full. The process inventory already bounds this map.
func (s *Server) queueTermination(event model.TraceEvent) {
	s.eventsMu.Lock()
	defer s.eventsMu.Unlock()
	if s.terminations == nil {
		s.terminations = make(map[string]model.TraceEvent)
	}
	if existing, ok := s.terminations[event.NodeID]; !ok || event.Timestamp.Before(existing.Timestamp) {
		if err := s.spool.append([]model.TraceEvent{event}); err != nil {
			s.spoolError = err
			s.logger.Error("persist termination evidence", "error", err)
		}
		s.terminations[event.NodeID] = event
	}
}

func (s *Server) admitTerminationsLocked() {
	for id, event := range s.terminations {
		if len(s.events)+s.eventsInFlight >= 50000 {
			return
		}
		s.events = append(s.events, event)
		delete(s.terminations, id)
	}
}

func (s *Server) postJSON(ctx context.Context, path string, input, output any) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return s.postData(ctx, path, data, output)
}

type controllerResponseError struct {
	statusCode int
	status     string
	message    string
}

func (e *controllerResponseError) Error() string {
	return fmt.Sprintf("controller returned %s: %s", e.status, e.message)
}

func (s *Server) postData(ctx context.Context, path string, data []byte, output any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.config.ControllerURL, "/")+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &controllerResponseError{resp.StatusCode, resp.Status, strings.TrimSpace(string(message))}
	}
	if path == "/api/v1/agents/register" || path == "/api/v1/agents/heartbeat" {
		if raw := resp.Header.Get("X-KPL-Agent-Capacity"); raw != "" {
			capacity, err := strconv.Atoi(raw)
			if err != nil || capacity <= 0 {
				return fmt.Errorf("Controller returned invalid Agent capacity %q", raw)
			}
			s.mu.Lock()
			s.capacityLimit = capacity
			s.capacityRevision = resp.Header.Get("X-KPL-Agent-Capacity-Revision")
			s.mu.Unlock()
		}
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	// Registration returns JSON even when the caller only needs its headers.
	// Consume small responses so net/http can reuse the Controller connection.
	// The success status already acknowledged the request; a drain failure must
	// not turn accepted telemetry into another send.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	return nil
}

func safeName(value string) string {
	return url.PathEscape(strings.ReplaceAll(value, "..", "_"))
}
