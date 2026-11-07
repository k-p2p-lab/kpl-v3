package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostRebootFencesFile = "host-reboot-fences.json"

var errHostEnableIdentityChanged = errors.New("Agent host identity changed during Enable; refresh before retrying")

type hostRebootFence struct {
	OperationID    string    `json:"operationId"`
	BootID         string    `json:"bootId"`
	AgentStartedAt time.Time `json:"agentStartedAt"`
	RequestedAt    time.Time `json:"requestedAt"`
}

func loadHostRebootFences(directory string) (map[string]hostRebootFence, error) {
	values := make(map[string]hostRebootFence)
	root, err := os.OpenRoot(directory)
	if errors.Is(err, os.ErrNotExist) {
		return values, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := readResumeFile(root, hostRebootFencesFile, resultMetadataLimit)
	if errors.Is(err, os.ErrNotExist) {
		return values, nil
	}
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(data, &values) != nil || values == nil {
		return nil, errors.New("invalid host reboot fences")
	}
	for node, fence := range values {
		if !validResultID(node) || !hostRebootIdentity.MatchString(fence.OperationID) || !validResultID(fence.BootID) || fence.AgentStartedAt.IsZero() || fence.RequestedAt.IsZero() {
			return nil, errors.New("invalid host reboot fence identity")
		}
	}
	return values, nil
}

// A separate durable fence survives request cancellation and a stale cached
// RebootPending=false heartbeat. It is written before invoking the Agent.
func (s *state) setHostRebootFence(nodeID string, fence *hostRebootFence) error {
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentAvailabilityErr != nil {
		return s.agentAvailabilityErr
	}
	s.mu.RLock()
	values := maps.Clone(s.hostRebootFences)
	s.mu.RUnlock()
	if values == nil {
		values = make(map[string]hostRebootFence)
	}
	if fence != nil {
		values[nodeID] = *fence
	} else {
		delete(values, nodeID)
	}
	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.dataDir)
	if err != nil {
		return err
	}
	err = writeAnalysisJSON(root, hostRebootFencesFile, values)
	root.Close()
	if err != nil {
		return fmt.Errorf("save host reboot fence: %w", err)
	}
	s.mu.Lock()
	s.hostRebootFences = values
	s.mu.Unlock()
	return nil
}

func hostHasNewBoot(agent model.Agent, nodeID string, fence hostRebootFence, now time.Time) bool {
	return agent.Host != nil && !agent.Host.RebootPending && agent.Host.NodeID == nodeID &&
		(agent.Labels["swarmNodeId"] == "" || agent.Labels["swarmNodeId"] == nodeID) && agent.Host.BootID != "" && agent.Host.BootID != fence.BootID &&
		agent.StartupReconciled && agentIsOnline(agent, now) && agent.StartedAt.After(fence.AgentStartedAt) && agent.LastSeen.After(fence.RequestedAt)
}

// Registration can replace an offline Agent while local settings are written.
// Release only the host whose proof was checked, never its replacement's host.
func (s *Server) completeHostEnable(verified, current model.Agent) error {
	nodeID := agentHostNodeID(verified)
	if verified.ID != current.ID || nodeID != agentHostNodeID(current) || !verified.StartedAt.Equal(current.StartedAt) || current.Host != nil && current.Host.RebootPending {
		return errHostEnableIdentityChanged
	}
	return s.state.setHostDisabled(nodeID, false)
}

// Caller holds cancelMu, excluding a new maintenance operation during release.
func (s *Server) releaseConfirmedHostRebootFence(agent model.Agent, proofs ...*verifiedHostRebootProof) error {
	nodeID := agentHostNodeID(agent)
	s.state.mu.RLock()
	fence, exists := s.state.hostRebootFences[nodeID]
	s.state.mu.RUnlock()
	if !exists {
		return nil
	}
	var proof *verifiedHostRebootProof
	if len(proofs) > 0 {
		proof = proofs[0]
	}
	confirmed := proof != nil && proof.proof.AgentID == agent.ID && proof.proof.AgentStartedAt.Equal(agent.StartedAt) && proofShowsNewBoot(proof, nodeID, fence)
	if !confirmed {
		// A durable explicit rejection proves no reboot was dispatched. Missing
		// or interrupted metadata provides no such evidence and stays fenced.
		s.hostRebootMu.Lock()
		job, err := s.loadHostRebootLocked(fence.OperationID)
		if err == nil {
			for _, host := range job.status.Hosts {
				if host.NodeID == nodeID && host.BootID == fence.BootID && host.State == "rejected" {
					confirmed = true
					break
				}
			}
		}
		s.hostRebootMu.Unlock()
	}
	if !confirmed {
		return errors.New("host reboot dispatch is unresolved on this boot; wait for a new boot and ready Agent before enabling the host")
	}
	return s.state.setHostRebootFence(nodeID, nil)
}
