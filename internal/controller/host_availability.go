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

const hostAvailabilityFile = "host-availability.json"

// A reboot owns only the hold it acquired. Keeping its previous value and the
// Agent settings in the same durable record prevents a restart from treating a
// pre-existing operator hold as a temporary reboot hold.
type hostRebootHold struct {
	OperationID      string          `json:"operationId"`
	PreviousDisabled bool            `json:"previousDisabled"`
	Agents           map[string]bool `json:"agents"`
	Restored         bool            `json:"restored,omitempty"`
}

type hostAvailability struct {
	Version  int                       `json:"version"`
	Disabled map[string]bool           `json:"disabled"`
	Reboots  map[string]hostRebootHold `json:"reboots,omitempty"`
}

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

func loadHostAvailability(directory string) (hostAvailability, error) {
	value := hostAvailability{Version: 2, Disabled: make(map[string]bool), Reboots: make(map[string]hostRebootHold)}
	root, err := os.OpenRoot(directory)
	if errors.Is(err, os.ErrNotExist) {
		return value, nil
	}
	if err != nil {
		return value, err
	}
	defer root.Close()
	data, err := readResumeFile(root, hostAvailabilityFile, resultMetadataLimit)
	if errors.Is(err, os.ErrNotExist) {
		return value, nil
	}
	if err != nil {
		return value, fmt.Errorf("load host availability: %w", err)
	}
	// Version 1 stored a plain node->bool map. It contains no evidence about
	// the pre-reboot setting, so its holds remain operator-owned.
	var legacy map[string]bool
	if json.Unmarshal(data, &legacy) == nil && legacy != nil {
		value.Disabled = legacy
	} else {
		var stored hostAvailability
		if json.Unmarshal(data, &stored) != nil || stored.Version != 2 || stored.Disabled == nil {
			return value, errors.New("invalid host availability settings")
		}
		value = stored
	}
	for id := range value.Disabled {
		if !validResultID(id) || len(id) > 128 {
			return value, errors.New("invalid host availability identity")
		}
	}
	for node, hold := range value.Reboots {
		if !validResultID(node) || len(node) > 128 || (!hold.Restored && !value.Disabled[node]) || (hold.Restored && value.Disabled[node] != hold.PreviousDisabled) || !hostRebootIdentity.MatchString(hold.OperationID) || len(hold.Agents) == 0 {
			return value, errors.New("invalid host reboot hold")
		}
		for id := range hold.Agents {
			if !validResultID(id) || len(id) > 200 {
				return value, errors.New("invalid host reboot Agent identity")
			}
		}
	}
	if value.Reboots == nil {
		value.Reboots = make(map[string]hostRebootHold)
	}
	return value, nil
}

func loadDisabledHosts(directory string) (map[string]bool, error) {
	value, err := loadHostAvailability(directory)
	return value.Disabled, err
}

// A host hold is durable before any reboot request. It also applies to an
// Agent with a new ID registering on the same node after Controller restart.
func (s *state) setHostDisabled(nodeID string, disabled bool) error {
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentAvailabilityErr != nil {
		return s.agentAvailabilityErr
	}
	value, err := loadHostAvailability(s.dataDir)
	if err != nil {
		return err
	}
	if !validResultID(nodeID) || len(nodeID) > 128 {
		return errors.New("invalid host availability identity")
	}
	if disabled {
		value.Disabled[nodeID] = true
	} else {
		delete(value.Disabled, nodeID)
	}
	// An explicit host setting supersedes automatic restoration by an old job.
	delete(value.Reboots, nodeID)
	return s.saveHostAvailabilityLocked(value)
}

// Caller holds agentSettingsMu. Publish only after rename and directory fsync.
func (s *state) saveHostAvailabilityLocked(value hostAvailability) error {
	if err := os.MkdirAll(s.dataDir, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.dataDir)
	if err != nil {
		return err
	}
	err = writeAnalysisJSON(root, hostAvailabilityFile, value)
	root.Close()
	if err != nil {
		return fmt.Errorf("save host availability: %w", err)
	}
	s.mu.Lock()
	s.hostDisabled = maps.Clone(value.Disabled)
	for id, agent := range s.agents {
		agent.Disabled = s.agentDisabled[id] || value.Disabled[agentHostNodeID(agent)]
		s.agents[id] = agent
	}
	s.mu.Unlock()
	s.notify()
	return nil
}

func (s *state) acquireHostRebootHold(nodeID, operationID string) error {
	s.agentSettingsMu.Lock()
	defer s.agentSettingsMu.Unlock()
	if s.agentAvailabilityErr != nil {
		return s.agentAvailabilityErr
	}
	value, err := loadHostAvailability(s.dataDir)
	if err != nil {
		return err
	}
	if previous, exists := value.Reboots[nodeID]; exists {
		if previous.OperationID != operationID {
			return errors.New("a previous reboot still owns this host hold; wait for recovery or explicitly enable the verified host")
		}
		return s.saveHostAvailabilityLocked(value)
	}
	hold := hostRebootHold{OperationID: operationID, PreviousDisabled: value.Disabled[nodeID], Agents: make(map[string]bool)}
	hasOnlineAgent := false
	s.mu.RLock()
	for id, agent := range s.agents {
		online := agentIsOnline(agent, time.Now())
		if agentHostNodeID(agent) == nodeID && (online || s.agentDisabled[id]) {
			if !validResultID(id) || len(id) > 200 {
				s.mu.RUnlock()
				return errors.New("a host Agent identity cannot be saved for participation restoration")
			}
			hold.Agents[id] = s.agentDisabled[id]
			hasOnlineAgent = hasOnlineAgent || online
		}
	}
	s.mu.RUnlock()
	if !hasOnlineAgent {
		return errors.New("no online Agent remains on the reboot host")
	}
	value.Reboots[nodeID], value.Disabled[nodeID] = hold, true
	return s.saveHostAvailabilityLocked(value)
}
