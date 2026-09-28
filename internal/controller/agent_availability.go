package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const agentAvailabilityFile = "agent-availability.json"

func loadDisabledAgents(dir string) (map[string]bool, error) {
	values := make(map[string]bool)
	data, err := os.ReadFile(filepath.Join(dir, agentAvailabilityFile))
	if errors.Is(err, os.ErrNotExist) {
		return values, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load Agent availability: %w", err)
	}
	if err = json.Unmarshal(data, &values); err != nil || values == nil {
		return nil, errors.New("invalid Agent availability settings")
	}
	for id := range values {
		if strings.TrimSpace(id) == "" {
			return nil, errors.New("invalid Agent ID in availability settings")
		}
	}
	return values, nil
}

// A disable is a scheduling/visibility setting, not a process stop. Calls
// already dispatched may finish; retained peers remain reachable for cleanup.
func (s *state) setAgentEnabled(id string, enabled bool) (model.Agent, int, error) {
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentAvailabilityErr != nil {
		return model.Agent{}, http.StatusServiceUnavailable, s.agentAvailabilityErr
	}
	s.mu.RLock()
	agent, exists := s.agents[id]
	disabled := maps.Clone(s.agentDisabled)
	s.mu.RUnlock()
	if !exists {
		return model.Agent{}, http.StatusNotFound, errors.New("Agent not found")
	}
	if agent.Disabled == !enabled {
		return agent, http.StatusOK, nil
	}
	if disabled == nil {
		disabled = make(map[string]bool)
	}
	if enabled {
		delete(disabled, id)
	} else {
		disabled[id] = true
	}
	data, err := json.Marshal(disabled)
	if err == nil {
		err = os.MkdirAll(s.dataDir, 0o755)
	}
	if err == nil {
		err = writeFileAtomic(filepath.Join(s.dataDir, agentAvailabilityFile), append(data, '\n'), 0o600)
	}
	if err != nil {
		return model.Agent{}, http.StatusInternalServerError, fmt.Errorf("save Agent availability: %w", err)
	}
	s.mu.Lock()
	s.agentDisabled = disabled
	agent = s.agents[id]
	agent.Disabled = !enabled
	s.agents[id] = agent
	s.mu.Unlock()
	s.notify()
	return agent, http.StatusOK, nil
}

func (s *Server) handleAgentEnabled(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Enabled *bool `json:"enabled"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if input.Enabled == nil {
		writeError(w, http.StatusBadRequest, "enabled must be true or false")
		return
	}
	agent, status, err := s.state.setAgentEnabled(r.PathValue("agentID"), *input.Enabled)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, agent)
}
