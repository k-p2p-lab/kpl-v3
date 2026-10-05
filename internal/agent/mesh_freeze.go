package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) handleMeshFreeze(w http.ResponseWriter, r *http.Request, nodeID string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request model.MeshFreezeRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.RunID) == "" {
		writeError(w, http.StatusBadRequest, "runId is required")
		return
	}
	response, err := s.proxyMeshFreeze(r.Context(), nodeID, request)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// validateMeshFreezeTargetLocked checks both incarnation and run fences. It is
// called before dispatch and again before accepting the acknowledgement; no
// network operation holds the Agent state lock.
func (s *Server) validateMeshFreezeTargetLocked(nodeID string, request model.MeshFreezeRequest) (*process, error) {
	proc, ok := s.processes[nodeID]
	if !ok || proc.exited || proc.node.State != model.NodeReady || proc.apiURL == "" {
		return nil, fmt.Errorf("node %q is not ready", nodeID)
	}
	if request.RunID == "" || proc.node.RunID != request.RunID || proc.node.Generation != request.Generation {
		return nil, fmt.Errorf("node %q does not match run/generation", nodeID)
	}
	if fence, exists := s.runFences[request.RunID]; s.fencingAll || exists && request.Generation <= fence {
		return nil, fmt.Errorf("run %q generation %d is fenced", request.RunID, request.Generation)
	}
	if proc.node.Metadata["pubsubRouter"] != "gossipsub" || proc.node.Metadata["pubsubEnabled"] != "true" || proc.node.Metadata["meshFreezeEnabled"] != "true" {
		return nil, fmt.Errorf("node %q does not have GossipSub meshFreeze enabled", nodeID)
	}
	return proc, nil
}

func (s *Server) proxyMeshFreeze(ctx context.Context, nodeID string, request model.MeshFreezeRequest) (model.MeshFreezeResponse, error) {
	var result model.MeshFreezeResponse
	s.mu.RLock()
	proc, err := s.validateMeshFreezeTargetLocked(nodeID, request)
	if err != nil {
		s.mu.RUnlock()
		return result, err
	}
	apiURL, peerID := proc.apiURL, proc.node.PeerID
	s.mu.RUnlock()
	data, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/mesh-freeze", bytes.NewReader(data))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KPL-Node-ID", nodeID)
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return result, fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	acknowledgement, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil {
		return result, fmt.Errorf("read mesh-freeze acknowledgement: %w", err)
	}
	if len(acknowledgement) > 4096 {
		return result, fmt.Errorf("mesh-freeze acknowledgement exceeds 4 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(acknowledgement))
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode mesh-freeze acknowledgement: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("invalid mesh-freeze acknowledgement: expected a single JSON value")
	}
	if !result.Frozen || result.NodeID != nodeID || result.PeerID == "" || peerID != "" && result.PeerID != peerID {
		return result, fmt.Errorf("mesh-freeze acknowledgement does not match target %q", nodeID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.validateMeshFreezeTargetLocked(nodeID, request)
	if err != nil {
		return result, fmt.Errorf("mesh-freeze target changed while waiting for acknowledgement: %w", err)
	}
	if current != proc || current.apiURL != apiURL || current.node.PeerID != "" && current.node.PeerID != result.PeerID {
		return result, fmt.Errorf("mesh-freeze target %q changed while waiting for acknowledgement", nodeID)
	}
	setProcessMetadata(current, "meshFrozen", "true")
	return result, nil
}
