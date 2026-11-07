package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const hostAvailabilityFile = "host-availability.json"

func cloneAgentHost(agent model.Agent) model.Agent {
	if agent.Host != nil {
		host := *agent.Host
		agent.Host = &host
	}
	return agent
}

func agentHostNodeID(agent model.Agent) string {
	if agent.Host != nil && agent.Host.NodeID != "" {
		return agent.Host.NodeID
	}
	return agent.Labels["swarmNodeId"]
}

func loadDisabledHosts(directory string) (map[string]bool, error) {
	values := make(map[string]bool)
	root, err := os.OpenRoot(directory)
	if errors.Is(err, os.ErrNotExist) {
		return values, nil
	}
	if err != nil {
		return nil, err
	}
	defer root.Close()
	data, err := readResumeFile(root, hostAvailabilityFile, resultMetadataLimit)
	if errors.Is(err, os.ErrNotExist) {
		return values, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load host availability: %w", err)
	}
	if err := json.Unmarshal(data, &values); err != nil || values == nil {
		return nil, errors.New("invalid host availability settings")
	}
	for id := range values {
		if !validResultID(id) || len(id) > 128 {
			return nil, errors.New("invalid host availability identity")
		}
	}
	return values, nil
}

// A host hold is durable before any reboot request. It also applies to an
// Agent with a new ID registering on the same node after Controller restart.
func (s *state) setHostDisabled(nodeID string, disabled bool) error {
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentAvailabilityErr != nil {
		return s.agentAvailabilityErr
	}
	s.mu.RLock()
	values := maps.Clone(s.hostDisabled)
	s.mu.RUnlock()
	if values[nodeID] == disabled {
		return nil
	}
	if !validResultID(nodeID) || len(nodeID) > 128 {
		return errors.New("invalid host availability identity")
	}
	if values == nil {
		values = make(map[string]bool)
	}
	if disabled {
		values[nodeID] = true
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
	err = writeAnalysisJSON(root, hostAvailabilityFile, values)
	root.Close()
	if err != nil {
		return fmt.Errorf("save host availability: %w", err)
	}
	s.mu.Lock()
	s.hostDisabled = values
	for id, agent := range s.agents {
		agent.Disabled = s.agentDisabled[id] || values[agentHostNodeID(agent)]
		s.agents[id] = agent
	}
	s.mu.Unlock()
	s.notify()
	return nil
}
