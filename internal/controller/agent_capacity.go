package controller

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	mathrand "math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const agentCapacitiesFile = "agent-capacities.json"

func loadAgentCapacities(dir string) (map[string]int, error) {
	capacities := make(map[string]int)
	data, err := os.ReadFile(filepath.Join(dir, agentCapacitiesFile))
	if errors.Is(err, os.ErrNotExist) {
		return capacities, nil
	}
	if err != nil {
		return capacities, fmt.Errorf("load Agent capacities: %w", err)
	}
	if err := json.Unmarshal(data, &capacities); err != nil {
		return nil, fmt.Errorf("decode Agent capacities: %w", err)
	}
	if capacities == nil {
		return nil, errors.New("Agent capacities must be a JSON object")
	}
	for id, capacity := range capacities {
		if strings.TrimSpace(id) == "" || capacity <= 0 {
			return nil, fmt.Errorf("invalid saved capacity for Agent %q", id)
		}
	}
	return capacities, nil
}

// Caller holds mu. Never trust Controller-owned fields from an Agent report.
func (s *state) observeAgentCapacityLocked(agent *model.Agent) {
	if agent.DefaultCapacity > 0 && s.agentCapacityRevisions[agent.ID] == "" {
		s.agentCapacityRevisions[agent.ID] = rand.Text()
	}
	s.agentReportedCapacities[agent.ID] = agent.Capacity
	s.applyAgentCapacityLocked(agent)
}

func (s *state) applyAgentCapacityLocked(agent *model.Agent) {
	reported := s.agentReportedCapacities[agent.ID]
	agent.CapacityOverride = s.agentCapacityOverrides[agent.ID]
	desired := agent.DefaultCapacity
	if agent.CapacityOverride > 0 {
		desired = agent.CapacityOverride
	}
	agent.Capacity, agent.CapacityPending = reported, false
	if desired > 0 {
		agent.CapacityPending = reported != desired || agent.CapacityRevision != s.agentCapacityRevisions[agent.ID]
		// Reductions limit placement immediately; increases wait for Agent acknowledgment.
		if reported <= 0 || desired < reported {
			agent.Capacity = desired
		}
		// An old report can cross several edits, even if its numeric capacity
		// matches again. Only acknowledgment of the current revision may raise
		// the last safe placement limit.
		if previous := s.agents[agent.ID].Capacity; agent.CapacityPending && previous > 0 {
			agent.Capacity = min(agent.Capacity, previous)
		}
	}
}

func (s *Server) writeAgentCapacityHeader(w http.ResponseWriter, id string) {
	s.state.mu.RLock()
	agent := s.state.agents[id]
	revision := s.state.agentCapacityRevisions[id]
	desired := agent.DefaultCapacity
	if override := s.state.agentCapacityOverrides[id]; override > 0 {
		desired = override
	}
	s.state.mu.RUnlock()
	if desired > 0 {
		w.Header().Set("X-KPL-Agent-Capacity", strconv.Itoa(desired))
		w.Header().Set("X-KPL-Agent-Capacity-Revision", revision)
	}
}

// Saving is serialized independently from placement and heartbeats. A failed
// atomic replacement leaves both the stored settings and live admission intact.
func (s *state) setAgentCapacity(id string, capacity *int) (model.Agent, int, error) {
	if capacity != nil && *capacity <= 0 {
		return model.Agent{}, http.StatusBadRequest, errors.New("capacity must be a positive integer or null to use the CLI default")
	}
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentSettingsErr != nil {
		return model.Agent{}, http.StatusServiceUnavailable, s.agentSettingsErr
	}
	s.mu.RLock()
	agent, exists := s.agents[id]
	capacities := maps.Clone(s.agentCapacityOverrides)
	s.mu.RUnlock()
	if !exists {
		return model.Agent{}, http.StatusNotFound, errors.New("Agent not found")
	}
	if agent.DefaultCapacity <= 0 {
		return model.Agent{}, http.StatusConflict, errors.New("update this Agent to support capacity settings")
	}
	if capacity == nil {
		delete(capacities, id)
	} else {
		capacities[id] = *capacity
	}
	data, err := json.Marshal(capacities)
	if err == nil {
		err = os.MkdirAll(s.dataDir, 0o755)
	}
	if err == nil {
		err = writeFileAtomic(filepath.Join(s.dataDir, agentCapacitiesFile), append(data, '\n'), 0o600)
	}
	if err != nil {
		return model.Agent{}, http.StatusInternalServerError, fmt.Errorf("save Agent capacity: %w", err)
	}
	s.mu.Lock()
	s.agentCapacityOverrides = capacities
	s.agentCapacityRevisions[id] = rand.Text()
	agent = s.agents[id]
	s.applyAgentCapacityLocked(&agent)
	s.agents[id] = agent
	s.mu.Unlock()
	s.notify()
	return agent, http.StatusOK, nil
}

func (s *Server) handleAgentCapacity(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Capacity json.RawMessage `json:"capacity"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(input.Capacity) == 0 {
		writeError(w, http.StatusBadRequest, "capacity is required; use null to restore the CLI default")
		return
	}
	var capacity *int
	if err := json.Unmarshal(input.Capacity, &capacity); err != nil {
		writeError(w, http.StatusBadRequest, "capacity must be a positive integer or null")
		return
	}
	agent, status, err := s.state.setAgentCapacity(r.PathValue("agentID"), capacity)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, status, agent)
}

// Capacity may shrink between a Controller reservation and Agent admission.
// A 429 guarantees the Agent has not created this Peer, so re-placement is safe.
var errAgentCapacityReached = errors.New("Agent capacity reached")

func (s *Server) createReservedNode(ctx context.Context, request model.CreateNodeRequest, agent model.Agent, targetAgentID string, rng *mathrand.Rand) error {
	for {
		if err := s.rememberRunAgent(request.RunID, agent); err != nil {
			s.releaseReservation(request.ID)
			return err
		}
		var node model.Node
		err := s.callAgent(ctx, agent.URL, http.MethodPost, "/api/v1/nodes", request, &node)
		if errors.Is(err, errAgentCapacityReached) {
			s.releaseReservation(request.ID)
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			agent, err = s.acquireAgentWithPlacement(ctx, request.ID, targetAgentID, rng)
			if err != nil {
				return err
			}
			continue
		}
		if err != nil {
			s.releaseReservation(request.ID)
			return fmt.Errorf("create node %s on agent %s: %w", request.ID, agent.ID, err)
		}
		if !s.recordCreatedNode(request, agent.ID, node, agent.StartedAt) {
			s.releaseReservation(request.ID)
			return fmt.Errorf("agent %s changed instance while creating node %s", agent.ID, request.ID)
		}
		return nil
	}
}
