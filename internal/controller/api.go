package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/k-p2p-lab/v3/internal/auth"
	"github.com/k-p2p-lab/v3/internal/model"
	"github.com/k-p2p-lab/v3/internal/webui"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (s *Server) Run(ctx context.Context) error {
	if err := auth.Validate(s.config.User, s.config.Password); err != nil {
		return err
	}
	if s.config.MetricsURL != "" {
		if _, _, _, ok := prometheusTargetFromURL(s.config.MetricsURL); !ok {
			return errors.New("Controller metrics URL must be a non-loopback HTTP(S) URL ending in /metrics, without credentials, query or fragment")
		}
	}
	if err := prepareDataDir(s.config.DataDir); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.config.Listen)
	if err != nil {
		return err
	}
	return s.serve(ctx, listener)
}

func (s *Server) serve(ctx context.Context, listener net.Listener) error {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	// Agents must still be able to deliver final Peer events after run cancellation.
	ingestCtx, cancelIngest := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelIngest()
	server := &http.Server{
		Addr:              s.config.Listen,
		Handler:           s.Handler(runCtx),
		BaseContext:       func(net.Listener) context.Context { return ingestCtx },
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				s.state.markStaleAgents(agentStaleAfter)
			}
		}
	}()
	analysisDone := make(chan struct{})
	go func() {
		defer close(analysisDone)
		s.recordAnalysisLoop(runCtx)
	}()
	s.logger.Info("controller listening", "address", s.config.Listen)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var err error
	select {
	case err = <-served:
	case <-runCtx.Done():
	}
	// Close admission before waiting: no concurrent POST may add a run after
	// Wait starts, including requests already accepted by the HTTP listener.
	s.cancelMu.Lock()
	s.shuttingDown = true
	for _, cancel := range s.cancels {
		cancel()
	}
	s.cancelMu.Unlock()
	cancelRun()
	// First finish all producers and their generation fences. Then stop peers
	// retained by completed experiments and flush every Agent while ingestion
	// is still available. Swarm removes the Agent services only after we exit.
	s.runs.Wait()
	cleanupErr := s.cleanupAgents()
	s.analysisWorkers.Wait()
	<-analysisDone
	cancelIngest()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelShutdown()
	if shutdownErr != nil {
		_ = server.Close()
	}

	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, cleanupErr, shutdownErr)
}

func (s *Server) Handler(ctx context.Context) http.Handler {
	return s.withMiddleware(s.withAuthentication(s.routes(ctx)))
}

func (s *Server) routes(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("/api/v1/auth/session", s.handleSession)
	mux.HandleFunc("/api/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFileFS(w, r, webui.FS(), "login.html")
	})
	mux.Handle("/metrics", promhttp.HandlerFor(s.state.metrics.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/ui-config", s.handleUIConfig)
	mux.HandleFunc("/api/v1/prometheus/agent-targets", s.handlePrometheusAgentTargets)
	mux.HandleFunc("/api/v1/prometheus/controller-targets", s.handlePrometheusControllerTargets)
	mux.HandleFunc("/api/v1/snapshot", s.handleSnapshot)
	mux.HandleFunc("/api/v1/agents", s.handleAgents)
	mux.HandleFunc("/api/v1/agents/register", s.handleAgentRegister)
	mux.HandleFunc("/api/v1/agents/heartbeat", s.handleAgentHeartbeat)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/network", s.handleNetwork)
	mux.HandleFunc("/api/v1/bootstrap", s.handleBootstrap)
	mux.HandleFunc("/api/v1/discovery", s.handleDiscovery)
	mux.HandleFunc("/api/v1/events", s.handleEvents)
	mux.HandleFunc("/api/v1/events/batch", s.handleEventBatch)
	mux.HandleFunc("/api/v1/scenarios/validate", s.handleScenarioValidation)
	mux.HandleFunc("/api/v1/scenarios", s.handleScenarios)
	mux.HandleFunc("/api/v1/scenarios/", s.handleScenarioAction)
	mux.HandleFunc("/api/v1/experiments", s.handleExperiments(ctx))
	mux.HandleFunc("/api/v1/results", s.handleResults)
	mux.HandleFunc("/api/v1/results/", s.handleResultAction)
	mux.HandleFunc("POST /api/v1/result-batches/{batchID}/resume", s.handleBatchResume(ctx, false))
	mux.HandleFunc("POST /api/v1/result-batches/{batchID}/retry", s.handleBatchResume(ctx, true))
	mux.HandleFunc("/api/v1/result-batches/", s.handleResultBatchAction)
	mux.HandleFunc("/api/v1/experiments/", s.handleExperimentAction)
	mux.HandleFunc("/api/v1/analysis-jobs/", s.handleAnalysisJob(ctx))
	mux.HandleFunc("/api/v1/batch-analysis-jobs/", s.handleBatchAnalysis(ctx))
	mux.HandleFunc("/api/v1/stream", func(w http.ResponseWriter, r *http.Request) {
		streamCtx, cancel := context.WithCancel(r.Context())
		defer cancel()
		stop := context.AfterFunc(ctx, cancel)
		defer stop()
		if ctx.Err() != nil {
			cancel()
		}
		s.handleStream(w, r.WithContext(streamCtx))
	})
	mux.Handle("/", http.FileServer(http.FS(webui.FS())))
	return mux
}

type prometheusTargetGroup struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func (s *Server) handlePrometheusControllerTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	groups := make([]prometheusTargetGroup, 0, 1)
	if target, scheme, _, ok := prometheusTargetFromURL(s.config.MetricsURL); ok {
		groups = append(groups, prometheusTargetGroup{
			Targets: []string{target},
			Labels:  map[string]string{"__scheme__": scheme},
		})
	}
	writeJSON(w, http.StatusOK, groups)
}

func (s *Server) handlePrometheusAgentTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	type candidate struct {
		id, name, metricsURL, swarmNodeID string
	}
	now := time.Now()
	candidates := make([]candidate, 0)
	s.state.mu.RLock()
	for _, agent := range s.state.agents {
		if !agentIsOnline(agent, now) {
			continue
		}
		candidates = append(candidates, candidate{
			id: agent.ID, name: agent.Name, metricsURL: agent.MetricsURL,
			swarmNodeID: agent.Labels["swarmNodeId"],
		})
	}
	s.state.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].id < candidates[j].id })

	groups := make([]prometheusTargetGroup, 0, len(candidates))
	for _, agent := range candidates {
		target, scheme, metricsPath, ok := prometheusTargetFromURL(agent.metricsURL)
		if !ok {
			continue
		}
		labels := map[string]string{"agent_id": agent.id, "agent_name": agent.name}
		if agent.swarmNodeID != "" {
			labels["swarm_node_id"] = agent.swarmNodeID
		}
		if scheme != "http" {
			labels["__scheme__"] = scheme
		}
		if metricsPath != "/metrics" {
			labels["__metrics_path__"] = metricsPath
		}
		groups = append(groups, prometheusTargetGroup{Targets: []string{target}, Labels: labels})
	}
	writeJSON(w, http.StatusOK, groups)
}

func prometheusTargetFromURL(raw string) (target, scheme, metricsPath string, ok bool) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.Contains(raw, "#") {
		return "", "", "", false
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Opaque != "" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" {
		return "", "", "", false
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "localhost" || host == "localhost.localdomain" || strings.HasSuffix(host, ".localhost") {
		return "", "", "", false
	}
	if ip := net.ParseIP(host); ip != nil && (ip.IsLoopback() || ip.IsUnspecified()) {
		return "", "", "", false
	}
	scheme = parsed.Scheme
	if scheme != "http" && scheme != "https" {
		return "", "", "", false
	}
	port := parsed.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", "", "", false
	}
	metricsPath = parsed.EscapedPath()
	if metricsPath != "/metrics" {
		return "", "", "", false
	}
	return net.JoinHostPort(parsed.Hostname(), strconv.Itoa(portNumber)), scheme, metricsPath, true
}

func (s *Server) handleUIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		PrometheusPort int `json:"prometheusPort"`
		GrafanaPort    int `json:"grafanaPort"`
	}{
		PrometheusPort: s.config.PrometheusPort,
		GrafanaPort:    s.config.GrafanaPort,
	})
}

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC()})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.state.snapshot())
}

func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.state.inventory().Agents)
}

func (s *Server) handleAgentRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var agent model.Agent
	if err := decodeJSON(w, r, &agent); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	registered, err := s.state.registerAgent(agent)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, registered)
}

func (s *Server) handleAgentHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var heartbeat model.AgentHeartbeat
	if err := decodeJSON(w, r, &heartbeat); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.state.heartbeat(heartbeat); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.state.inventory().Nodes)
}

func (s *Server) handleNetwork(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	snapshot := s.state.snapshot()
	writeJSON(w, http.StatusOK, map[string]any{
		"generatedAt": snapshot.GeneratedAt,
		"nodes":       snapshot.Nodes,
		"edges":       snapshot.Edges,
		"metrics":     snapshot.Metrics,
	})
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	runID := strings.TrimSpace(r.URL.Query().Get("runId"))
	if runID == "" {
		writeError(w, http.StatusBadRequest, "runId query parameter is required")
		return
	}
	snapshot := s.state.snapshot()
	type bootstrapNode struct {
		NodeID    string   `json:"nodeId"`
		PeerID    string   `json:"peerId"`
		Addresses []string `json:"addresses"`
	}
	var result []bootstrapNode
	for _, node := range snapshot.Nodes {
		if node.RunID == runID && node.Role == "boot" && node.State == model.NodeReady && node.PeerID != "" && len(node.Addresses) > 0 {
			result = append(result, bootstrapNode{NodeID: node.ID, PeerID: node.PeerID, Addresses: node.Addresses})
		}
	}
	writeJSON(w, http.StatusOK, result)
}

// handleDiscovery exposes configured topic membership rather than observed
// graph outcomes. Peers use it only to discover transports; GossipSub still
// decides which connected topic peers enter its mesh.
func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	runID := strings.TrimSpace(r.URL.Query().Get("runId"))
	topic := strings.TrimSpace(r.URL.Query().Get("topic"))
	requesterNodeID := strings.TrimSpace(r.URL.Query().Get("requesterNodeId"))
	if runID == "" || topic == "" || requesterNodeID == "" {
		writeError(w, http.StatusBadRequest, "runId, topic, and requesterNodeId query parameters are required")
		return
	}
	type discoveryNode struct {
		NodeID     string   `json:"nodeId"`
		PeerID     string   `json:"peerId"`
		Addresses  []string `json:"addresses"`
		Subscribed bool     `json:"subscribed"`
	}
	result := make([]discoveryNode, 0)
	now := time.Now().UTC()
	s.state.mu.RLock()
	for _, node := range s.state.nodes {
		if node.ID == requesterNodeID || node.RunID != runID || node.State != model.NodeReady ||
			node.PeerID == "" || len(node.Addresses) == 0 ||
			!agentIsOnline(s.state.agents[node.AgentID], now) ||
			!nodeFeatureEnabled(node, "pubsubEnabled", node.Type != "boot" && node.Type != "dht-only") ||
			!nodeHasTopic(node, topic) {
			continue
		}
		result = append(result, discoveryNode{
			NodeID:     node.ID,
			PeerID:     node.PeerID,
			Addresses:  append([]string(nil), node.Addresses...),
			Subscribed: nodeSubscribesToTopic(node, topic),
		})
	}
	s.state.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].Subscribed != result[j].Subscribed {
			return result[i].Subscribed
		}
		return result[i].NodeID < result[j].NodeID
	})
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, s.state.inventory().Events)
}

func (s *Server) handleEventBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var batch model.EventBatch
	if err := decodeJSON(w, r, &batch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(batch.Events) > model.MaxEventBatchEvents {
		writeError(w, http.StatusRequestEntityTooLarge, "event batch cannot exceed 5000 events")
		return
	}
	normalizeControllerEventTimes(&batch, time.Now().UTC())
	if err := batch.ValidateEventSizes(); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if err := s.state.appendEvents(batch); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// An Agent emits measurement_terminated only after it has observed process or
// container exit. Controller receipt is therefore a conservative, comparable
// upper bound even when Agent host clocks differ.
func normalizeControllerEventTimes(batch *model.EventBatch, observedAt time.Time) {
	for index := range batch.Events {
		event := &batch.Events[index]
		if event.Type != "measurement_terminated" {
			continue
		}
		if event.Fields == nil {
			event.Fields = make(map[string]any)
		}
		if !event.Timestamp.IsZero() {
			event.Fields["sourceTimestamp"] = event.Timestamp.UTC().Format(time.RFC3339Nano)
		}
		event.Fields["timestampBasis"] = "controller-receipt-upper-bound-v1"
		event.Timestamp = observedAt
	}
}

func (s *Server) handleExperiments(ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, s.state.inventory().Experiments)
		case http.MethodPost:
			// Cancel a slow upload immediately during shutdown; canceling a
			// request context alone does not unblock a server-side body Read.
			stopDeadline := context.AfterFunc(ctx, func() {
				_ = http.NewResponseController(w).SetReadDeadline(time.Now())
			})
			defer stopDeadline()
			mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			bodyLimit := int64(scenarioYAMLLimit)
			if mediaType == "application/json" {
				bodyLimit = scenarioJSONRequestBodyLimit
			}
			raw, err := readLimitedRequestBody(w, r, bodyLimit)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					writeError(w, http.StatusRequestEntityTooLarge, "experiment request is too large")
					return
				}
				writeError(w, http.StatusBadRequest, "cannot read scenario")
				return
			}
			repetitions := 1
			if mediaType == "application/json" {
				if !utf8.Valid(raw) {
					writeError(w, http.StatusBadRequest, "experiment request must be valid UTF-8")
					return
				}
				var submission struct {
					Scenario    string `json:"scenario"`
					Repetitions *int   `json:"repetitions"`
				}
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&submission); err != nil {
					writeError(w, http.StatusBadRequest, "invalid experiment submission: "+err.Error())
					return
				}
				var trailing any
				if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
					writeError(w, http.StatusBadRequest, "experiment submission contains trailing data")
					return
				}
				if submission.Repetitions != nil {
					repetitions = *submission.Repetitions
				}
				if len(submission.Scenario) > scenarioYAMLLimit {
					writeError(w, http.StatusBadRequest, fmt.Sprintf("scenario YAML cannot exceed %d bytes", scenarioYAMLLimit))
					return
				}
				raw = []byte(submission.Scenario)
			}
			experiment, err := s.StartScenarioRepeated(ctx, raw, repetitions)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSON(w, http.StatusAccepted, experiment)
		default:
			methodNotAllowed(w)
		}
	}
}

func (s *Server) handleExperimentAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/experiments/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	if parts[1] == "analysis" {
		s.handleResultAnalysis(w, r, parts[0])
		return
	}
	if parts[1] == "download" {
		s.handleResultDownload(w, r, parts[0])
		return
	}
	if parts[1] != "stop" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if err := s.StopScenario(parts[0]); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"runId": parts[0], "status": "stopping"})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	_, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming is unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	updates, unsubscribe := s.state.subscribe()
	defer unsubscribe()
	// Coalesce event notifications: topology/JSON work must not scale with
	// RPC frequency or with the number of open dashboard tabs.
	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	lastSent := time.Time{}
	pendingUpdate := time.Time{}
	send := func() error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		data, generatedAt, err := s.streamSnapshotAt()
		if err != nil {
			return err
		}
		response := http.NewResponseController(w)
		_ = response.SetWriteDeadline(time.Now().Add(10 * time.Second))
		stopWrite := context.AfterFunc(r.Context(), func() { _ = response.SetWriteDeadline(time.Now()) })
		defer stopWrite()
		if _, err := io.WriteString(w, "event: snapshot\ndata: "); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\n\n"); err != nil {
			return err
		}
		if err := response.Flush(); err != nil {
			return err
		}
		lastSent = time.Now()
		// A shared cached snapshot may predate this client's notification.
		// Keep it pending until a newly generated snapshot covers the update.
		if !generatedAt.Before(pendingUpdate) {
			pendingUpdate = time.Time{}
		}
		return nil
	}
	if err := send(); err != nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			select {
			case <-updates:
				if pendingUpdate.IsZero() {
					pendingUpdate = time.Now()
				}
			default:
			}
			if !pendingUpdate.IsZero() || time.Since(lastSent) >= 15*time.Second {
				if err := send(); err != nil {
					return
				}
			}
		}
	}
}

const snapshotInterval = time.Second

func (s *Server) streamSnapshot() ([]byte, error) {
	data, _, err := s.streamSnapshotAt()
	return data, err
}

func (s *Server) streamSnapshotAt() ([]byte, time.Time, error) {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.snapshotData != nil && time.Since(s.snapshotAt) < snapshotInterval {
		return s.snapshotData, s.snapshotAt, nil
	}
	// Timestamp the beginning of the read, not the end of JSON encoding.
	// Notifications during encoding must still be delivered by a later read.
	generatedAt := time.Now()
	data, err := json.Marshal(s.state.snapshot())
	if err != nil {
		return nil, time.Time{}, err
	}
	s.snapshotData, s.snapshotAt = data, generatedAt
	return data, generatedAt, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid JSON: expected a single value")
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func methodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}
