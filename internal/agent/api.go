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

	"github.com/k-p2p-lab/v3/internal/model"
)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", s.metricsHandler())
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodeAction)
	mux.HandleFunc("/api/v1/runs/", s.handleRunAction)
	mux.HandleFunc("/api/v1/telemetry", s.handleTelemetry)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if s.config.Token != "" && r.Method != http.MethodGet && r.Header.Get("Authorization") != "Bearer "+s.config.Token {
			writeError(w, http.StatusUnauthorized, "valid bearer token required")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleRunAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/runs/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
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
	writeJSON(w, http.StatusOK, s.snapshot())
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.nodes())
	case http.MethodDelete:
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
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		var request model.CreateNodeRequest
		if err := decodeJSON(w, r, &request); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		node, err := s.createNode(context.Background(), request)
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
	resp, err := s.client.Do(req)
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
