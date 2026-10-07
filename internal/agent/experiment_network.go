package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) handleExperimentNetwork(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !s.config.AutoResetNetwork {
		writeError(w, http.StatusConflict, "Agent was not deployed for automatic network reset")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
	defer cancel()
	if !s.networkMu.TryLock() {
		writeError(w, http.StatusConflict, "network preparation is already in progress")
		return
	}
	defer s.networkMu.Unlock()
	if err := ctx.Err(); err != nil {
		writeError(w, http.StatusRequestTimeout, err.Error())
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	decoder.DisallowUnknownFields()
	decode := func(target any) error {
		if err := decoder.Decode(target); err != nil {
			return err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return errors.New("expected a single network request")
		}
		return nil
	}
	var err error
	switch r.URL.Path {
	case "/api/v1/network/fence":
		var request model.ExperimentNetworkRequest
		if err = decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		err = s.fenceExperimentNetwork(ctx, request)
	case "/api/v1/network/activate":
		var network model.ExperimentNetwork
		if err = decode(&network); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		err = s.activateExperimentNetwork(ctx, network)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) fenceExperimentNetwork(ctx context.Context, request model.ExperimentNetworkRequest) error {
	if request.RunID == "" || request.Epoch == "" || request.RequestedAt.IsZero() {
		return errors.New("experiment network identity is required")
	}
	s.mu.Lock()
	if !s.startupReconciled || s.shuttingDown {
		s.mu.Unlock()
		return errors.New("Agent is not ready for network reset")
	}
	if s.networkRequest.Epoch != request.Epoch && !s.networkRequest.RequestedAt.IsZero() && !request.RequestedAt.After(s.networkRequest.RequestedAt) {
		s.mu.Unlock()
		return errors.New("stale network preparation")
	}
	for _, proc := range s.processes {
		if !proc.exited || proc.cleanupErr != nil {
			s.mu.Unlock()
			return errors.New("Peer removal must finish before network reset")
		}
	}
	s.experimentNetwork = nil
	s.networkRequest = request
	s.mu.Unlock()
	// A daemon inventory also catches containers left by an interrupted create
	// or a previous Agent instance. Do not remove them or report cleanup success.
	data, err := s.docker.run(ctx, nil, "ps", "--all", "--quiet", "--filter", "label=io.kpl.managed=true", "--filter", "label=io.kpl.network="+s.docker.network)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(data)) != "" {
		return errors.New("Docker still has Peer containers on the experiment network")
	}
	return nil
}

func (s *Server) activateExperimentNetwork(ctx context.Context, network model.ExperimentNetwork) error {
	if !dockerNetworkName.MatchString(network.NetworkID) || network.NetworkName != s.docker.network {
		return errors.New("unexpected experiment network")
	}
	for _, endpoint := range []string{network.GatewayURL, network.PeerGatewayURL} {
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "http" || u.User != nil || net.ParseIP(u.Hostname()).To4() == nil || u.Port() != "18081" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("invalid experiment gateway address")
		}
	}
	// Swarm materializes a new overlay on a worker only when its first Peer
	// attaches. The manager verifies global network ownership; create binds the
	// immutable ID and verifies that exact attachment after container startup.
	s.mu.RLock()
	current := s.networkRequest.Epoch == network.Epoch && s.networkRequest.RunID == network.RunID
	s.mu.RUnlock()
	if !current {
		return errors.New("experiment network preparation was superseded")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, network.GatewayURL+"/health", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.config.Token)
	response, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("reach experiment control gateway: %w", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return errors.New("experiment control gateway is not ready")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown || s.networkRequest.Epoch != network.Epoch || s.networkRequest.RunID != network.RunID {
		return errors.New("experiment network preparation was superseded")
	}
	s.experimentNetwork = &network
	return nil
}
