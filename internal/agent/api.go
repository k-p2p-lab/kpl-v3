package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", s.metricsHandler())
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/registration/refresh", s.handleRegistrationRefresh)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodeAction)
	mux.HandleFunc("/api/v1/runs/", s.handleRunAction)
	mux.HandleFunc("/api/v1/telemetry", s.handleTelemetry)
	mux.HandleFunc("/api/v1/network/", s.handleExperimentNetwork)
	mux.HandleFunc("/api/v1/host/reboot", s.handleHostReboot)
	mux.HandleFunc("/api/v1/host/reboot-status", s.handleHostRebootStatus)
	if s.config.AnalysisHandler != nil {
		mux.HandleFunc("/internal/result-analysis", s.handleResultAnalysis)
		mux.HandleFunc("/internal/analysis-idle", s.handleAnalysisIdle)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if s.config.AnalysisHandler != nil && (r.URL.Path == "/internal/result-analysis" || r.URL.Path == "/internal/analysis-idle") {
			mux.ServeHTTP(w, r)
			return
		}
		if s.config.Token != "" && r.Method != http.MethodGet && r.Header.Get("Authorization") != "Bearer "+s.config.Token {
			writeError(w, http.StatusUnauthorized, "valid bearer token required")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleAnalysisIdle(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	active := s.analysisActive
	s.mu.RUnlock()
	if active {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "Agent is calculating a result analysis")
		return
	}
	s.config.AnalysisHandler.ServeHTTP(w, r)
}

func (s *Server) handleResultAnalysis(w http.ResponseWriter, r *http.Request) {
	s.runWorkersMu.Lock()
	s.mu.Lock()
	ready := s.startupReconciled && !s.shuttingDown && !s.fencingAll && !s.analysisActive && (s.host == nil || !s.host.RebootPending) && s.capacityUsedLocked() == 0 && len(s.runWorkers) == 0
	if ready {
		s.analysisActive = true
	}
	s.mu.Unlock()
	s.runWorkersMu.Unlock()
	if !ready {
		writeError(w, http.StatusServiceUnavailable, "Agent is not ready for result analysis")
		return
	}
	defer func() {
		s.mu.Lock()
		s.analysisActive = false
		s.mu.Unlock()
	}()
	s.config.AnalysisHandler.ServeHTTP(w, r)
}

func (s *Server) handleRunAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && parts[0] != "" && parts[1] == "drain" {
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		timeout := model.DefaultRunDrainTimeout
		if raw := r.URL.Query().Get("timeout"); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil || value <= 0 || value > model.MaxRunDrainTimeout {
				writeError(w, http.StatusBadRequest, "timeout must be positive and no greater than 2m")
				return
			}
			timeout = value
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if s.config.RunWorkerFactory != nil {
			s.stopRunGeneration(parts[0], ^uint64(0))
			if s.history != nil {
				s.history.mu.Lock()
				err := s.history.saveFence(parts[0], ^uint64(0))
				s.history.mu.Unlock()
				if err != nil {
					writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunWorkerPendingCode, WorkerPending: true})
					return
				}
			}
			if err := s.waitRunContainers(ctx, parts[0], ^uint64(0)); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunWorkerPendingCode, WorkerPending: true})
				return
			}
			if err := s.waitRunWorkerIngress(ctx); err != nil {
				writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunWorkerPendingCode, WorkerPending: true})
				return
			}
		}
		if err := s.drainRunEvents(ctx, parts[0]); err != nil {
			var pending *runLogsPendingError
			if errors.As(err, &pending) {
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunTelemetryPendingCode, RemainingEvents: pending.remaining})
				return
			}
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if s.config.RunWorkerFactory != nil {
			if err := s.finishRunWorker(ctx, parts[0]); err != nil {
				var pending *runLogsPendingError
				if errors.As(err, &pending) {
					writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunTelemetryPendingCode, RemainingEvents: pending.remaining})
					return
				}
				var failed *runWorkerFailedError
				if errors.As(err, &failed) {
					writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: "run_worker_failed"})
					return
				}
				writeJSON(w, http.StatusServiceUnavailable, model.RunDrainError{Error: err.Error(), Code: model.RunWorkerPendingCode, WorkerPending: true})
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) != 2 || parts[0] == "" || parts[1] != "nodes" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodDelete {
		methodNotAllowed(w)
		return
	}
	rawGeneration := r.URL.Query().Get("generation")
	generation, err := strconv.ParseUint(rawGeneration, 10, 64)
	if err != nil || rawGeneration == "" {
		writeError(w, http.StatusBadRequest, "generation must be an unsigned integer")
		return
	}
	s.stopRunGeneration(parts[0], generation)
	cleanupCtx, cancel := context.WithTimeout(r.Context(), containerStopTimeout)
	defer cancel()
	if err := s.waitRunContainers(cleanupCtx, parts[0], generation); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "agentId": s.config.ID})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	finishView := s.acquireRunView()
	defer finishView()
	snapshot, err := s.snapshotWithHistoryContext(r.Context(), true)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeHeartbeatView(w, snapshot)
}

// The Controller discovers task addresses, but registration still travels over
// the authenticated Agent -> configured Controller path. Never accept a new
// Controller URL from the discovery request or alter Peer lifecycle state.
func (s *Server) handleRegistrationRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !s.heartbeatMu.TryLock() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusConflict, "Agent registration or heartbeat is in progress; retry discovery shortly")
		return
	}
	defer s.heartbeatMu.Unlock()
	s.mu.RLock()
	ready := s.startupReconciled && !s.shuttingDown
	s.mu.RUnlock()
	if !ready {
		writeError(w, http.StatusServiceUnavailable, "Agent is starting or shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	if err := s.registerLocked(ctx); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	finishView := s.acquireRunView()
	defer finishView()
	snapshot, err := s.snapshotWithHistoryContext(r.Context(), true)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeHeartbeatView(w, snapshot)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		finishView := s.acquireRunView()
		defer finishView()
		nodes, err := s.nodesWithContext(r.Context())
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeNodeView(w, nodes)
	case http.MethodDelete:
		s.mu.Lock()
		if s.fencingAll {
			s.mu.Unlock()
			writeError(w, http.StatusConflict, "Peer cleanup is already in progress")
			return
		}
		s.fencingAll = true
		s.historyRevision++
		s.mu.Unlock()
		defer func() { s.mu.Lock(); s.fencingAll = false; s.historyRevision++; s.mu.Unlock() }()
		// Close old run generations without making this Agent permanently unusable
		// after a Controller restart. Future runs get different IDs.
		s.mu.Lock()
		if s.runFences == nil {
			s.runFences = make(map[string]uint64)
		}
		for _, proc := range s.processes {
			s.runFences[proc.node.RunID] = ^uint64(0)
		}
		s.mu.Unlock()
		s.stopAll()
		if err := s.fenceRetiredRuns(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		cleanupCtx, cancel := context.WithTimeout(r.Context(), containerStopTimeout)
		err := s.waitStoppedContext(cleanupCtx)
		cancel()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		drainCtx, cancelDrain := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancelDrain()
		if err := s.drainEvents(drainCtx); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if s.config.RunWorkerFactory != nil {
			s.runWorkersMu.Lock()
			ids := make([]string, 0, len(s.runWorkers))
			for id := range s.runWorkers {
				ids = append(ids, id)
			}
			s.runWorkersMu.Unlock()
			for _, id := range ids {
				if err := s.finishRunWorker(drainCtx, id); err != nil {
					writeError(w, http.StatusServiceUnavailable, err.Error())
					return
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		var request model.CreateNodeRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		node, err := s.admitNode(r.Context(), context.Background(), request)
		if errors.Is(err, errPeerHistoryFull) || errors.Is(err, errTelemetryBacklogFull) || errors.Is(err, errRunResetPending) {
			w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if errors.Is(err, errCapacityReached) {
			writeError(w, http.StatusTooManyRequests, err.Error())
			return
		}
		if err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, node)
	default:
		methodNotAllowed(w)
	}
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	finishView := s.acquireNodeRunView(parts[0])
	defer finishView()
	if len(parts) == 1 && r.Method == http.MethodDelete {
		if err := s.stopNode(parts[0]); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if len(parts) != 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "profile":
		s.handleProfileUpdate(w, r, parts[0])
	case "mesh-freeze":
		s.handleMeshFreeze(w, r, parts[0])
	case "topology":
		s.handleTopology(w, r, parts[0])
	case "status":
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var node model.Node
		if err := decodeJSON(w, r, &node); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if node.ID != parts[0] {
			writeError(w, http.StatusBadRequest, "node id does not match path")
			return
		}
		if err := s.updateNode(node); err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case "publish":
		if r.Method != http.MethodPost {
			methodNotAllowed(w)
			return
		}
		var request model.PublishRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.proxyPublish(r.Context(), parts[0], request); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) proxyPublish(ctx context.Context, nodeID string, request model.PublishRequest) error {
	s.mu.RLock()
	proc, ok := s.processes[nodeID]
	if !ok || proc.exited || proc.node.State != model.NodeReady {
		s.mu.RUnlock()
		return fmt.Errorf("node %q is not ready", nodeID)
	}
	apiURL := proc.apiURL
	s.mu.RUnlock()
	if s.config.RunWorkerFactory != nil {
		return s.runWorkerControl(ctx, proc, "publish", request, nil)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/publish", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// Container IPs can be recycled during churn. A late request must never
	// publish through a different peer that inherited an old IP address.
	req.Header.Set("X-KPL-Node-ID", nodeID)
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	resp, err := mutationHTTPClient(s.client).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}

func (s *Server) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	// Wait briefly without decoding a body. A small bounded queue absorbs Peer
	// bursts instead of forcing hundreds of synchronized retries into the Agent.
	if !s.acquireTelemetryDecoder(r.Context()) {
		s.telemetryDecodersRejected.Add(1)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "telemetry decoder busy; retry this batch")
		return
	}
	defer s.releaseTelemetryDecoder()
	var batch model.EventBatch
	if err := decodeJSON(w, r, &batch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(batch.Events) > model.MaxEventBatchEvents {
		writeError(w, http.StatusRequestEntityTooLarge, "event batch cannot exceed 5000 events")
		return
	}
	// Normalization and canonical JSON escaping can enlarge a valid request.
	// Reject an event that cannot be forwarded before acknowledging any of it.
	for i := range batch.Events {
		batch.Events[i].AgentID = s.config.ID
	}
	batch.AgentID = s.config.ID
	if err := batch.ValidateEventSizes(); err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		return
	}
	if !s.enqueueEvents(batch) {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "telemetry admission is unavailable; retry this batch")
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
