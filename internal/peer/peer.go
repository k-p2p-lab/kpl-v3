package peer

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type Server struct {
	config            model.PeerProcessConfig
	host              host.Host
	dht               *dht.IpfsDHT
	pubsub            *pubsub.PubSub
	topics            map[string]*pubsub.Topic
	subs              []*pubsub.Subscription
	relays            []pubsub.RelayCancelFunc
	scoreMu           sync.RWMutex
	peerScores        map[string]float64
	bandwidth         *bandwidthReporter
	telemetry         *telemetry
	logger            *slog.Logger
	startedAt         time.Time
	publishSeq        atomic.Uint64
	publishGate       publicationGate
	publishing        publicationBridge
	mesh              meshTracker
	discoveryMu       sync.Mutex
	discoveryFailures map[peer.ID]time.Time
	closeOnce         sync.Once
}

type envelope struct {
	ID                 string `json:"id"`
	RunID              string `json:"runId"`
	Publisher          string `json:"publisher"`
	SentAt             int64  `json:"sentAt"`
	ClockBasis         string `json:"clockBasis,omitempty"`
	ClockUncertaintyNS int64  `json:"clockUncertaintyNs,omitempty"`
	Payload            []byte `json:"payload"`
}

type bootstrapNode struct {
	NodeID     string   `json:"nodeId"`
	PeerID     string   `json:"peerId"`
	Addresses  []string `json:"addresses"`
	Subscribed bool     `json:"subscribed,omitempty"`
}

func Run(ctx context.Context, configPath string, logger *slog.Logger) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read peer config: %w", err)
	}
	var config model.PeerProcessConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("decode peer config: %w", err)
	}
	config, err = preparePeerNetwork(ctx, config, agentRouteSourceIPv4, prepareNetwork)
	if err != nil {
		return err
	}
	// Address discovery has already completed before netem. Avoid repeating a
	// DNS lookup here, since scope all can deliberately impair that lookup.
	server, err := newServer(ctx, config, logger)
	if err != nil {
		return err
	}
	return server.Run(ctx)
}

func newServer(ctx context.Context, config model.PeerProcessConfig, logger *slog.Logger) (*Server, error) {
	if config.Node.ID == "" || config.Node.RunID == "" || config.AgentURL == "" || config.ControllerURL == "" {
		return nil, fmt.Errorf("peer config requires node, run, agent, and controller")
	}
	if logger == nil {
		logger = slog.Default()
	}
	config.NodeConfig = config.NodeConfig.WithDefaults()
	if err := config.NodeConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validate node config: %w", err)
	}
	identity, err := deterministicIdentity(config.Node.RunID, config.Seed)
	if err != nil {
		return nil, fmt.Errorf("create identity: %w", err)
	}
	options, err := hostOptions(config.NodeConfig)
	if err != nil {
		return nil, err
	}
	options = append(options, libp2p.Identity(identity), libp2p.ListenAddrStrings(config.P2PListen))
	address, err := multiaddr.NewMultiaddr(config.P2PListen)
	if err != nil {
		return nil, fmt.Errorf("Docker P2P address: %w", err)
	}
	// Overlay containers can also have a host-local docker_gwbridge
	// address. Neither that address nor observed NAT/relay addresses
	// should enter Identify or the bootstrap registry for this peer.
	options = append(options, libp2p.AddrsFactory(func([]multiaddr.Multiaddr) []multiaddr.Multiaddr {
		return []multiaddr.Multiaddr{address}
	}))
	bandwidth := newBandwidthReporter()
	options = append(options, libp2p.BandwidthReporter(bandwidth))
	h, err := libp2p.New(options...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}
	telemetry := newTelemetry(config.Node, config.AgentURL, config.Token, logger)
	server := &Server{
		config:     config,
		host:       h,
		topics:     make(map[string]*pubsub.Topic),
		peerScores: make(map[string]float64),
		telemetry:  telemetry,
		bandwidth:  bandwidth,
		logger:     logger,
		startedAt:  time.Now().UTC(),
	}
	if *config.NodeConfig.Kademlia.Enabled {
		dhtInstance, err := server.newDHT(ctx)
		if err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("create dht: %w", err)
		}
		server.dht = dhtInstance
	}
	return server, nil
}

func (s *Server) Run(parentCtx context.Context) (runErr error) {
	// Record the subscription boundary before cancellation reaches PubSub.
	ctx, cancel := context.WithCancel(context.WithoutCancel(parentCtx))
	stopParentWatch := context.AfterFunc(parentCtx, func() {
		s.telemetry.stopMeasurement()
		cancel()
	})
	telemetryCtx, stopTelemetry := context.WithCancel(context.Background())
	telemetryDone := make(chan error, 1)
	go func() { telemetryDone <- s.telemetry.run(telemetryCtx) }()
	bandwidthCtx, stopBandwidth := context.WithCancel(context.Background())
	var bandwidthDone chan struct{}
	defer func() {
		stopParentWatch()
		s.telemetry.stopMeasurement()
		cancel()
		s.Close()
		stopBandwidth()
		if bandwidthDone != nil {
			<-bandwidthDone
		}
		s.emitBandwidth(true)
		stopTelemetry()
		runErr = errors.Join(runErr, <-telemetryDone)
	}()
	if err := s.telemetry.synchronizeClock(ctx, s.config.ControllerURL); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.logger.Warn("start peer without synchronized clock", "error", err)
	}
	bandwidthDone = make(chan struct{})
	go func() { defer close(bandwidthDone); s.bandwidthLoop(bandwidthCtx) }()
	go s.telemetry.clockSyncLoop(ctx, s.config.ControllerURL)
	if err := s.connectBootstrap(ctx); err != nil {
		return err
	}
	if s.dht != nil {
		if err := s.dht.Bootstrap(ctx); err != nil {
			return fmt.Errorf("bootstrap dht: %w", err)
		}
	}
	if *s.config.NodeConfig.GossipSub.Enabled {
		if err := s.startPubSub(ctx); err != nil {
			return err
		}
	} else if s.telemetry.startMeasurement(nil) {
		go s.telemetry.measurementLoop(ctx)
	}
	apiServer := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	listener, err := net.Listen("tcp", s.config.APListen)
	if err != nil {
		return fmt.Errorf("listen on peer api: %w", err)
	}
	if err := s.reportStatus(ctx, model.NodeReady, ""); err != nil {
		_ = listener.Close()
		return fmt.Errorf("report ready status: %w", err)
	}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := apiServer.Shutdown(shutdownCtx); err != nil {
			_ = apiServer.Close()
		}
	}()
	go s.statusLoop(ctx)
	s.logger.Info("peer ready", "node", s.config.Node.ID, "peer", s.host.ID(), "api", s.config.APListen)
	err = apiServer.Serve(listener)
	s.telemetry.stopMeasurement()
	cancel()
	<-shutdownDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) startPubSub(ctx context.Context) error {
	config := s.config.NodeConfig.GossipSub
	tracer := &gossipTracer{telemetry: s.telemetry, peerID: s.host.ID().String(), publishing: &s.publishing}
	options, err := gossipSubOptions(config, tracer)
	if err != nil {
		return err
	}
	authorOptions, err := gossipAuthorOptions(config, s.host)
	if err != nil {
		return err
	}
	options = append(options, authorOptions...)
	discoveryOptions, err := s.gossipDiscoveryOptions()
	if err != nil {
		return err
	}
	options = append(options, discoveryOptions...)
	options = append(options, pubsub.WithRawTracer(&messageTracer{server: s}))
	s.mesh.configure(config.Enabled != nil && *config.Enabled && config.Router == "gossipsub")
	if config.Score != nil && config.Score.IsEnabled() && config.ScoreInspectInterval != "" {
		interval, parseErr := time.ParseDuration(config.ScoreInspectInterval)
		if parseErr != nil {
			return fmt.Errorf("parse score inspect interval: %w", parseErr)
		}
		if interval > 0 {
			options = append(options, pubsub.WithPeerScoreInspect(pubsub.PeerScoreInspectFn(s.recordPeerScores), interval))
		}
	}
	var ps *pubsub.PubSub
	switch config.Router {
	case "gossipsub":
		ps, err = pubsub.NewGossipSub(ctx, s.host, options...)
	case "floodsub":
		if len(config.Protocols) > 0 {
			ps, err = pubsub.NewFloodsubWithProtocols(ctx, s.host, gossipProtocolIDs(config), options...)
		} else {
			ps, err = pubsub.NewFloodSub(ctx, s.host, options...)
		}
	case "randomsub":
		pubsub.RandomSubD = *config.RandomDegree
		ps, err = pubsub.NewRandomSub(ctx, s.host, *config.RandomNetworkSize, options...)
	default:
		return fmt.Errorf("unknown pubsub router %q", config.Router)
	}
	if err != nil {
		return fmt.Errorf("create %s: %w", config.Router, err)
	}
	s.pubsub = ps
	if err := registerGossipValidators(ps, config); err != nil {
		return err
	}
	var subscribedTopics []string
	for _, topicName := range config.Topics {
		topic, err := ps.Join(topicName, gossipTopicOptions(config, topicName)...)
		if err != nil {
			return fmt.Errorf("join topic %s: %w", topicName, err)
		}
		s.topics[topicName] = topic
		switch config.TopicMode {
		case "subscribe":
			subscriptionOptions := gossipSubscriptionOptions(config, topicName)
			sub, err := topic.Subscribe(subscriptionOptions...)
			if err != nil {
				return fmt.Errorf("subscribe topic %s: %w", topicName, err)
			}
			s.subs = append(s.subs, sub)
			subscribedTopics = append(subscribedTopics, topicName)
		case "relay":
			cancel, err := topic.Relay()
			if err != nil {
				return fmt.Errorf("relay topic %s: %w", topicName, err)
			}
			s.relays = append(s.relays, cancel)
		case "publish":
		}
	}
	if s.telemetry.startMeasurement(subscribedTopics) {
		go s.telemetry.measurementLoop(ctx)
	}
	// Start consumers only after every subscription and the source start event
	// are established. Already buffered messages remain available to Next.
	for i, topicName := range subscribedTopics {
		go s.consume(ctx, topicName, s.subs[i])
	}
	if config.Discovery == nil && strings.TrimSpace(s.config.ControllerURL) != "" {
		go s.discoveryLoop(ctx)
	}
	return nil
}

func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready", "nodeId": s.config.Node.ID, "peerId": s.host.ID().String()})
	})
	mux.HandleFunc("/publish", s.handlePublish)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.config.Token != "" && r.Method != http.MethodGet && r.Header.Get("Authorization") != "Bearer "+s.config.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if nodeID := r.Header.Get("X-KPL-Node-ID"); nodeID != "" && nodeID != s.config.Node.ID {
		http.Error(w, "peer identity does not match publish target", http.StatusConflict)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.config.NodeConfig.PublishAllowed() {
		http.Error(w, "publishing is disabled for this node type", http.StatusForbidden)
		return
	}
	var request model.PublishRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		http.Error(w, "invalid request: expected a single JSON value", http.StatusBadRequest)
		return
	}
	if request.PayloadSize <= 0 {
		request.PayloadSize = 32
	}
	if request.PayloadSize > model.MaxPublishPayloadBytes {
		http.Error(w, "payload exceeds 16 MiB", http.StatusBadRequest)
		return
	}
	if request.PayloadEncoding != "" && request.PayloadEncoding != "envelope" && request.PayloadEncoding != "raw" {
		http.Error(w, "payloadEncoding must be envelope or raw", http.StatusBadRequest)
		return
	}
	if request.DeliveryWindow == "" {
		request.DeliveryWindow = "10s"
	}
	deliveryWindow, err := time.ParseDuration(request.DeliveryWindow)
	if err != nil || deliveryWindow <= 0 || deliveryWindow > time.Hour {
		http.Error(w, "deliveryWindow must be a positive duration no greater than 1h", http.StatusBadRequest)
		return
	}
	request.DeliveryWindow = deliveryWindow.String()
	topicNames, err := s.publishTopics(request.Topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	publishCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	results := make([]map[string]any, 0, len(topicNames))
	for _, topicName := range topicNames {
		// Start the application clock after acquiring the publication gate.
		// Queueing behind another HTTP publish is not propagation latency.
		if err := s.publishGate.acquire(publishCtx); err != nil {
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
		if err := s.waitPublishReady(publishCtx, topicName); err != nil {
			s.publishGate.release()
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
		message, err := s.preparePublicationWithClock(request, s.telemetry.clockReading())
		if err != nil {
			s.publishGate.release()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		message, err = s.publishPreparedMessage(publishCtx, topicName, message)
		s.publishGate.release()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		targetNodes := request.TargetNodes
		if perTopic, exists := request.TargetNodesByTopic[topicName]; exists {
			targetNodes = perTopic
		}
		fields := map[string]any{"publisher": s.config.Node.ID, "payloadBytes": request.PayloadSize, "wireBytes": len(message.wire), "payloadEncoding": message.encoding, "pubsubMessageId": message.pubsubID, "targetNodes": targetNodes}
		fields["measurementDefinition"] = "session-window-v1"
		fields["deliveryWindow"] = request.DeliveryWindow
		cohort, known := request.TargetNodeIDs, request.TargetNodeIDs != nil
		if perTopic, exists := request.TargetNodeIDsByTopic[topicName]; exists {
			cohort, known = perTopic, true
		}
		if known {
			// A known empty cohort must serialize as [], never as null or missing.
			fields["targetNodeIds"] = append([]string{}, cohort...)
		}
		if s.localPublication() {
			fields["localPublication"] = true
			fields["targetNodes"] = 0
			fields["targetNodeIds"] = []string{}
		}
		if !request.CohortCapturedAt.IsZero() {
			fields["cohortCapturedAt"] = request.CohortCapturedAt.UTC().Format(time.RFC3339Nano)
		}
		s.telemetry.emitWithClock(model.TraceEvent{PeerID: s.host.ID().String(), Type: "publish", MessageID: message.id, Topic: topicName, Timestamp: message.sentAt, Fields: fields}, message.clock)
		results = append(results, map[string]any{"messageId": message.id, "topic": topicName, "payloadBytes": request.PayloadSize, "wireBytes": len(message.wire), "payloadEncoding": message.encoding})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if request.Topic != "*" {
		_ = json.NewEncoder(w).Encode(results[0])
	} else {
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": results})
	}
}

func (s *Server) consume(ctx context.Context, topic string, sub *pubsub.Subscription) {
	for {
		message, err := sub.Next(ctx)
		if err != nil {
			s.telemetry.stopMeasurement()
			return
		}
		s.telemetry.emitObserved(func(reading controllerClockReading) (model.TraceEvent, bool) {
			return s.deliveryEventWithClock(message, topic, reading)
		})
	}
}

func (s *Server) connectBootstrap(ctx context.Context) error {
	return connectBootstrapPeers(ctx, s.config, s.host.ID(), s.fetchBootstrap, s.host.Connect)
}

func (s *Server) fetchBootstrap(ctx context.Context) ([]bootstrapNode, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.config.ControllerURL, "/")+"/api/v1/bootstrap", nil)
	if err != nil {
		return nil, err
	}
	query := req.URL.Query()
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	query.Set("runId", s.config.Node.RunID)
	req.URL.RawQuery = query.Encode()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bootstrap registry returned %s", resp.Status)
	}
	var nodes []bootstrapNode
	if err := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func addrInfo(node bootstrapNode) (peer.AddrInfo, error) {
	id, err := peer.Decode(node.PeerID)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	info := peer.AddrInfo{ID: id}
	for _, raw := range node.Addresses {
		address, err := multiaddr.NewMultiaddr(raw)
		if err == nil {
			info.Addrs = append(info.Addrs, address)
		}
	}
	if len(info.Addrs) == 0 {
		return peer.AddrInfo{}, fmt.Errorf("bootstrap node has no valid address")
	}
	return info, nil
}

func (s *Server) statusLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statusCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.reportStatus(statusCtx, model.NodeReady, ""); err != nil {
				s.logger.Warn("report peer status", "error", err)
			}
			cancel()
		}
	}
}

func (s *Server) reportStatus(ctx context.Context, state, message string) error {
	node := s.config.Node
	node.PeerID = s.host.ID().String()
	node.State = state
	node.Error = message
	node.StartedAt = s.startedAt
	node.LastSeen = time.Now().UTC()
	node.Addresses = nil
	for _, address := range s.host.Addrs() {
		node.Addresses = append(node.Addresses, address.String())
	}
	node.ConnectedPeers = nil
	for _, connection := range s.host.Network().Conns() {
		node.ConnectedPeers = append(node.ConnectedPeers, connection.RemotePeer().String())
	}
	sort.Strings(node.ConnectedPeers)
	node.ConnectedPeers = unique(node.ConnectedPeers)
	node.TopicPeers = make(map[string]int)
	if s.pubsub != nil {
		for topic := range s.topics {
			node.TopicPeers[topic] = len(s.pubsub.ListPeers(topic))
		}
	}
	s.scoreMu.RLock()
	if len(s.peerScores) > 0 {
		node.PeerScores = make(map[string]float64, len(s.peerScores))
		for peerID, score := range s.peerScores {
			node.PeerScores[peerID] = score
		}
	}
	s.scoreMu.RUnlock()
	node.RoutingPeers, node.MeshPeers, node.OverlayObservedAt = s.overlaySnapshot()
	return s.telemetry.reportNode(ctx, node)
}

func (s *Server) recordPeerScores(scores map[peer.ID]float64) {
	copyScores := make(map[string]float64, len(scores))
	for peerID, score := range scores {
		copyScores[peerID.String()] = score
	}
	s.scoreMu.Lock()
	s.peerScores = copyScores
	s.scoreMu.Unlock()
}

func deterministicIdentity(runID string, seed int64) (libcrypto.PrivKey, error) {
	if runID == "" {
		return nil, fmt.Errorf("identity run namespace is required")
	}
	var runIDLength [8]byte
	var seedBytes [8]byte
	binary.BigEndian.PutUint64(runIDLength[:], uint64(len(runID)))
	binary.BigEndian.PutUint64(seedBytes[:], uint64(seed))
	hash := sha256.New()
	_, _ = hash.Write([]byte("kpl-v3/libp2p-identity/v1"))
	_, _ = hash.Write(runIDLength[:])
	_, _ = hash.Write([]byte(runID))
	_, _ = hash.Write(seedBytes[:])
	privateKey := ed25519.NewKeyFromSeed(hash.Sum(nil))
	return libcrypto.UnmarshalEd25519PrivateKey(privateKey)
}

func unique(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}

func (s *Server) Close() {
	s.closeOnce.Do(func() {
		if s.telemetry != nil {
			s.telemetry.stopMeasurement()
		}
		for _, cancel := range s.relays {
			cancel()
		}
		for _, sub := range s.subs {
			sub.Cancel()
		}
		for _, topic := range s.topics {
			_ = topic.Close()
		}
		if s.dht != nil {
			_ = s.dht.Close()
		}
		if s.host != nil {
			_ = s.host.Close()
		}
	})
}
