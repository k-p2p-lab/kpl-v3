package agent

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

var hostRebootProofNonce = regexp.MustCompile(`^[A-Z2-7]{26,128}$`)

func (s *Server) hostRebootActorMatches(request model.HostRebootRequest) (bool, error) {
	boot, err := s.hostBootIDReader()
	if err != nil || !hostBootUUID.MatchString(boot) {
		return false, errors.New("host boot identity is unavailable")
	}
	return request.NodeID == s.config.Labels["swarmNodeId"] && request.BootID == boot && request.AgentStartedAt.Equal(s.startedAt), nil
}

func (s *Server) handleHostRebootStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if s.config.Token == "" || s.config.HostRebootToken == "" || auth.Equal(s.config.Token, s.config.HostRebootToken) || r.Header.Get("Authorization") != "Bearer "+s.config.Token {
		writeError(w, http.StatusUnauthorized, "valid internal authentication and host-control key are required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var request model.HostRebootProofRequest
	if err := decodeJSON(w, r, &request); err != nil || !hostRebootProofNonce.MatchString(request.Nonce) {
		writeError(w, http.StatusBadRequest, "a random base32 nonce of at least 26 characters is required")
		return
	}
	boot, err := s.hostBootIDReader()
	if err != nil || !hostBootUUID.MatchString(boot) {
		writeError(w, http.StatusServiceUnavailable, "host boot identity is unavailable")
		return
	}
	s.mu.RLock()
	proof := model.HostRebootProof{Nonce: request.Nonce, AgentID: s.config.ID, NodeID: s.config.Labels["swarmNodeId"], BootID: boot, AgentStartedAt: s.startedAt.UTC(), StartupReconciled: s.startupReconciled}
	if s.host != nil {
		proof.NodeID = s.host.NodeID
		proof.RebootPending = s.host.RebootPending
		proof.RebootSupported = s.host.RebootSupported
	}
	s.mu.RUnlock()
	data, err := json.Marshal(proof)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "host status proof is unavailable")
		return
	}
	proof.Signature = auth.HostRebootStatusSignature(s.config.HostRebootToken, data)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, proof)
}
