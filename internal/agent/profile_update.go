package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) handleProfileUpdate(w http.ResponseWriter, r *http.Request, nodeID string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var request model.ProfileUpdateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, model.MaxProfileUpdateBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if decoder.Decode(new(any)) != io.EOF {
		writeError(w, http.StatusBadRequest, "expected one profile update")
		return
	}
	if err := request.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	response, err := s.proxyProfileUpdate(r.Context(), nodeID, request)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) profileTargetLocked(nodeID string, request model.ProfileUpdateRequest) (*process, error) {
	proc := s.processes[nodeID]
	if proc == nil || proc.exited || proc.node.State != model.NodeReady || proc.apiURL == "" {
		return nil, fmt.Errorf("node %q is not ready", nodeID)
	}
	if proc.node.RunID != request.RunID || proc.node.Generation != request.Generation {
		return nil, fmt.Errorf("profile target run/generation changed")
	}
	if fence, exists := s.runFences[request.RunID]; s.fencingAll || exists && request.Generation <= fence {
		return nil, fmt.Errorf("profile target generation is fenced")
	}
	if request.Set.Network != nil && proc.node.Metadata["networkMutable"] != "true" {
		return nil, fmt.Errorf("node %q has no runtime network capability", nodeID)
	}
	if request.Set.GossipSub != nil && (proc.node.Metadata["pubsubEnabled"] != "true" || proc.node.Metadata["pubsubRouter"] != "gossipsub") {
		return nil, fmt.Errorf("node %q requires enabled GossipSub", nodeID)
	}
	return proc, nil
}

func (s *Server) proxyProfileUpdate(ctx context.Context, nodeID string, request model.ProfileUpdateRequest) (result model.ProfileUpdateResponse, err error) {
	s.mu.RLock()
	proc, err := s.profileTargetLocked(nodeID, request)
	if err != nil {
		s.mu.RUnlock()
		return result, err
	}
	apiURL, peerID := proc.apiURL, proc.node.PeerID
	s.mu.RUnlock()
	dispatched := false
	defer func() {
		if err == nil || !dispatched || request.Stage != "apply" {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		current := s.processes[nodeID]
		if current != proc || proc.exited || proc.apiURL != apiURL {
			return
		}
		confirmed, _ := strconv.ParseUint(proc.node.Metadata["profileRevision"], 10, 64)
		if confirmed >= request.Revision {
			return
		}
		// A lost or failed response cannot establish the effective settings.
		setProcessMetadata(proc, "profilePending", "true")
		if request.Set.Network != nil {
			setProcessMetadata(proc, "networkPending", "true")
		}
	}()
	data, err := json.Marshal(request)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/profile", bytes.NewReader(data))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KPL-Node-ID", nodeID)
	if s.config.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.Token)
	}
	dispatched = true
	resp, err := s.client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return result, fmt.Errorf("peer returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, model.MaxProfileUpdateBytes+1))
	if err != nil {
		return result, err
	}
	if len(data) > model.MaxProfileUpdateBytes {
		return result, fmt.Errorf("profile acknowledgement too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return result, err
	}
	if decoder.Decode(new(any)) != io.EOF || result.NodeID != nodeID || result.PeerID == "" || peerID != "" && result.PeerID != peerID || result.Revision != request.Revision || result.Stage != request.Stage {
		return result, fmt.Errorf("invalid profile acknowledgement")
	}
	if err := model.ValidateProfileAcknowledgement(request, result); err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.profileTargetLocked(nodeID, request)
	if err != nil {
		return result, err
	}
	if current != proc || current.apiURL != apiURL || current.node.PeerID != "" && current.node.PeerID != result.PeerID {
		return result, fmt.Errorf("profile target changed while awaiting acknowledgement")
	}
	if request.Stage == "apply" {
		storeProfileAcknowledgement(current, result, time.Now().UTC())
	}
	return result, nil
}

func storeProfileAcknowledgement(proc *process, response model.ProfileUpdateResponse, at time.Time) {
	previous, _ := strconv.ParseUint(proc.node.Metadata["profileRevision"], 10, 64)
	if response.Revision < previous || response.Revision == previous && proc.node.Metadata["runtimeProfile"] != "" {
		return
	}
	effective := response.Effective
	var prior model.RuntimeProfilePatch
	if json.Unmarshal([]byte(proc.node.Metadata["runtimeProfile"]), &prior) == nil && model.ValidateRuntimeProfileSnapshot(prior) == nil {
		if effective.Network == nil {
			effective.Network = prior.Network
		}
		if effective.GossipSub == nil {
			effective.GossipSub = prior.GossipSub
		}
	}
	raw, _ := json.Marshal(effective)
	setProcessMetadata(proc, "runtimeProfile", string(raw))
	setProcessMetadata(proc, "profileRevision", strconv.FormatUint(response.Revision, 10))
	setProcessMetadata(proc, "profilePending", "false")
	setProcessMetadata(proc, "profileAppliedAt", at.UTC().Format(time.RFC3339Nano))
	if response.Effective.Network != nil {
		raw, _ := json.Marshal(response.Effective.Network)
		setProcessMetadata(proc, "network", string(raw))
		setProcessMetadata(proc, "networkRevision", strconv.FormatUint(response.Revision, 10))
		setProcessMetadata(proc, "networkPending", "false")
		setProcessMetadata(proc, "networkAppliedAt", at.UTC().Format(time.RFC3339Nano))
	}
}

// A lost apply acknowledgement can be recovered from a later Peer status.
// Only monotonic revisions from the current process are accepted.
func updateProcessProfile(proc *process, metadata map[string]string) {
	revision, err := strconv.ParseUint(metadata["profileRevision"], 10, 64)
	previous, _ := strconv.ParseUint(proc.node.Metadata["profileRevision"], 10, 64)
	if err != nil || revision == 0 || revision < previous || revision == previous && proc.node.Metadata["runtimeProfile"] != "" || revision > 2147483647 {
		return
	}
	raw := metadata["runtimeProfile"]
	if len(raw) > model.MaxProfileUpdateBytes {
		return
	}
	var effective model.RuntimeProfilePatch
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&effective) != nil || decoder.Decode(new(any)) != io.EOF {
		return
	}
	if model.ValidateRuntimeProfileSnapshot(effective) != nil {
		return
	}
	if effective.Network != nil && proc.node.Metadata["networkMutable"] != "true" {
		return
	}
	if effective.GossipSub != nil && (proc.node.Metadata["pubsubEnabled"] != "true" || proc.node.Metadata["pubsubRouter"] != "gossipsub") {
		return
	}
	appliedAt, err := time.Parse(time.RFC3339Nano, metadata["profileAppliedAt"])
	if err != nil {
		return
	}
	storeProfileAcknowledgement(proc, model.ProfileUpdateResponse{Revision: revision, Effective: effective}, appliedAt)
}
