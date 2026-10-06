package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) handleTopology(w http.ResponseWriter, r *http.Request, nodeID string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request model.TopologyRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, model.MaxTopologyRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid topology request: "+err.Error())
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid topology request: expected a single JSON value")
		return
	}
	if _, err := request.PeerInfos(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	response, err := s.proxyTopology(r.Context(), nodeID, request)
	if err != nil {
		writeError(w, topologyProxyErrorStatus(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// Preserve retryable Peer responses separately from permanent plan conflicts
// and malformed acknowledgements. The Controller must not infer retryability
// from an error message returned by a Peer.
type topologyPeerResponseError struct {
	statusCode int
	status     string
	message    string
}

func (e *topologyPeerResponseError) Error() string {
	return fmt.Sprintf("peer returned %s: %s", e.status, e.message)
}

func topologyProxyErrorStatus(err error) int {
	var responseErr *topologyPeerResponseError
	if errors.As(err, &responseErr) {
		switch responseErr.statusCode {
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return responseErr.statusCode
		default:
			return http.StatusBadGateway
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout
	}
	if errors.Is(err, context.Canceled) {
		return http.StatusServiceUnavailable
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		if networkErr.Timeout() {
			return http.StatusGatewayTimeout
		}
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

func (s *Server) proxyTopology(ctx context.Context, nodeID string, request model.TopologyRequest) (model.TopologyResponse, error) {
	var result model.TopologyResponse
	infos, err := request.PeerInfos()
	if err != nil {
		return result, err
	}
	scope := model.MeshFreezeRequest{RunID: request.RunID, Generation: request.Generation}
	s.mu.RLock()
	proc, err := s.validateMeshFreezeTargetLocked(nodeID, scope)
	if err != nil {
		s.mu.RUnlock()
		return result, err
	}
	apiURL, peerID := proc.apiURL, proc.node.PeerID
	s.mu.RUnlock()
	for _, info := range infos {
		if info.ID.String() == peerID {
			return result, fmt.Errorf("node %q cannot be its own topology neighbor", nodeID)
		}
	}
	data, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	if len(data) > model.MaxTopologyRequestBytes {
		return result, fmt.Errorf("topology request exceeds 1 MiB")
	}
	ctx, cancel := context.WithTimeout(ctx, model.TopologyProxyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/topology", bytes.NewReader(data))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KPL-Node-ID", nodeID)
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	// Topology readiness may take longer than the shared ten-second client.
	// Keep its transport configuration and scope only this request to its ctx.
	client := &http.Client{}
	if s.client != nil {
		client.Transport = s.client.Transport
		client.Jar = s.client.Jar
		client.CheckRedirect = s.client.CheckRedirect
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return result, &topologyPeerResponseError{statusCode: resp.StatusCode, status: resp.Status, message: strings.TrimSpace(string(message))}
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, model.MaxTopologyRequestBytes+1))
	if err != nil {
		return result, fmt.Errorf("read topology acknowledgement: %w", err)
	}
	if len(data) > model.MaxTopologyRequestBytes {
		return result, fmt.Errorf("topology acknowledgement exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, fmt.Errorf("decode topology acknowledgement: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return result, fmt.Errorf("invalid topology acknowledgement: expected a single JSON value")
	}
	if err := result.Validate(request, nodeID, peerID); err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.validateMeshFreezeTargetLocked(nodeID, scope)
	if err != nil {
		return result, fmt.Errorf("topology target changed while waiting for acknowledgement: %w", err)
	}
	if current != proc || current.apiURL != apiURL || current.node.PeerID != "" && current.node.PeerID != result.PeerID {
		return result, fmt.Errorf("topology target %q changed while waiting for acknowledgement", nodeID)
	}
	if result.Frozen {
		setProcessMetadata(current, "meshFrozen", "true")
	}
	return result, nil
}
