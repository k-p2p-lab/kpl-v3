package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Every previously disabled identity used to match a mixed host must also
// answer a fresh signed challenge. Service-key heartbeats alone cannot prove
// that an old disabled Agent ID returned instead of an unrelated replacement.
func (s *Server) verifyHostRebootParticipationIdentities(ctx context.Context, operationID, nodeID string, primary *verifiedHostRebootProof) error {
	s.state.agentSettingsMu.Lock()
	value, err := loadHostAvailability(s.state.dataDir)
	s.state.agentSettingsMu.Unlock()
	if err != nil {
		return err
	}
	hold, exists := value.Reboots[nodeID]
	if !exists || hold.OperationID != operationID || hold.PreviousDisabled {
		return nil
	}
	var candidates []model.Agent
	s.state.mu.RLock()
	for id, disabled := range hold.Agents {
		if !disabled && !s.state.agentDisabled[id] {
			continue
		}
		agent, found := s.state.agents[id]
		if !found || !agentIsOnline(agent, time.Now()) || agentHostNodeID(agent) != nodeID || agent.Host == nil || agent.Host.BootID != primary.proof.BootID {
			err = errors.New("a previously disabled Agent has not returned with its original identity; host remains excluded until identities are verified or explicitly enabled")
			break
		}
		if id != primary.proof.AgentID {
			candidates = append(candidates, cloneAgentHost(agent))
		}
	}
	s.state.mu.RUnlock()
	if err != nil {
		return err
	}
	primary.identities = make(map[string]*verifiedHostRebootProof)
	for _, agent := range candidates {
		proof, err := s.requestHostRebootProof(ctx, agent)
		if err != nil || proof.proof.BootID != primary.proof.BootID || !proof.proof.StartupReconciled || proof.proof.RebootPending {
			return errors.New("a previously disabled Agent identity could not provide a ready signed proof; host remains excluded until identities are verified or explicitly enabled")
		}
		primary.identities[agent.ID] = proof
	}
	return nil
}

// Network proof requests happen before these locks. Serialize restoration with
// explicit Enable and new maintenance, then recheck both Agent and operation.
func (s *Server) restoreHostRebootParticipation(status hostRebootStatus, index int, verified model.Agent, proof *verifiedHostRebootProof) error {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	host := status.Hosts[index]
	if operation := s.hostRebootActiveNodes[host.NodeID]; operation != "" && operation != status.ID {
		return errors.New("another reboot operation owns this host")
	}
	s.state.agentSettingsMu.Lock()
	defer s.state.agentSettingsMu.Unlock()
	if s.state.agentAvailabilityErr != nil {
		return s.state.agentAvailabilityErr
	}
	value, err := loadHostAvailability(s.state.dataDir)
	if err != nil {
		return err
	}
	hold, exists := value.Reboots[host.NodeID]
	if !exists {
		// Legacy records have no pre-reboot evidence. An explicit Enable may
		// also have superseded this operation. Neither permits changing a hold.
		return nil
	}
	if hold.OperationID != status.ID {
		return errors.New("host restoration belongs to another reboot operation")
	}
	s.state.mu.RLock()
	current := s.state.agents[verified.ID]
	valid := proof != nil && proof.proof.AgentID == current.ID && proof.proof.NodeID == host.NodeID && proof.proof.AgentStartedAt.Equal(current.StartedAt) && proof.proof.StartupReconciled && !proof.proof.RebootPending && current.ID == verified.ID && agentHostNodeID(current) == host.NodeID && (current.Labels["swarmNodeId"] == "" || current.Labels["swarmNodeId"] == host.NodeID) && current.StartedAt.Equal(verified.StartedAt) && agentIsOnline(current, time.Now()) && current.Host != nil && current.Host.BootID == proof.proof.BootID && !current.Host.RebootPending && current.StartupReconciled
	if valid && !hold.PreviousDisabled {
		// A per-ID manual disable must not silently disappear when an Agent
		// returns under an unrelated ID. Default Swarm IDs are node-stable.
		// Mixed/disabled identities require every previously disabled Agent to
		// return under its known ID; otherwise leave the host excluded.
		for id, wasDisabled := range hold.Agents {
			if !wasDisabled && !s.state.agentDisabled[id] {
				continue
			}
			returned, found := s.state.agents[id]
			identity := proof
			if id != proof.proof.AgentID {
				identity = proof.identities[id]
			}
			if !found || identity == nil || identity.proof.AgentID != id || identity.proof.NodeID != host.NodeID || !identity.proof.AgentStartedAt.Equal(returned.StartedAt) || identity.proof.BootID != proof.proof.BootID || !identity.proof.StartupReconciled || identity.proof.RebootPending || agentHostNodeID(returned) != host.NodeID || !agentIsOnline(returned, time.Now()) || returned.Host == nil || returned.Host.BootID != proof.proof.BootID || returned.Host.RebootPending || !returned.StartupReconciled {
				err = errors.New("a previously disabled Agent has not returned with its original identity; host remains excluded until identities are verified or explicitly enabled")
				break
			}
			for otherID, returned := range s.state.agents {
				if _, known := hold.Agents[otherID]; !known && agentHostNodeID(returned) == host.NodeID && returned.Host != nil && returned.Host.BootID == proof.proof.BootID && agentIsOnline(returned, time.Now()) {
					err = errors.New("disabled Agent settings cannot be matched to replacement Agent identities; host remains excluded until explicitly enabled")
					break
				}
			}
			if err != nil {
				break
			}
		}
	}
	fence, fenced := s.state.hostRebootFences[host.NodeID]
	fences := maps.Clone(s.state.hostRebootFences)
	s.state.mu.RUnlock()
	if !valid {
		return errors.New("Agent identity or readiness changed during host restoration; waiting for a fresh verified return")
	}
	if err != nil {
		return err
	}
	if fenced {
		if fence.OperationID != status.ID {
			return errors.New("a different operation owns the host reboot fence")
		}
		delete(fences, host.NodeID)
		if err := s.state.saveHostRebootFencesLocked(fences); err != nil {
			return err
		}
	}
	if hold.PreviousDisabled {
		value.Disabled[host.NodeID] = true
	} else {
		delete(value.Disabled, host.NodeID)
	}
	// Retain a recovery marker until the corresponding job outcome is durable.
	// In particular, a rename followed by a failed directory fsync must remain
	// discoverable by the recovery loop and must not publish a completed job.
	hold.Restored = true
	value.Reboots[host.NodeID] = hold
	return s.state.saveHostAvailabilityLocked(value)
}

func (s *Server) cleanupRestoredHostRebootHolds(status hostRebootStatus) {
	s.state.agentSettingsMu.Lock()
	defer s.state.agentSettingsMu.Unlock()
	value, err := loadHostAvailability(s.state.dataDir)
	if err != nil {
		return
	}
	changed := false
	for _, host := range status.Hosts {
		hold, exists := value.Reboots[host.NodeID]
		if exists && hold.OperationID == status.ID && hold.Restored {
			delete(value.Reboots, host.NodeID)
			changed = true
		}
	}
	if changed {
		if err := s.state.saveHostAvailabilityLocked(value); err != nil {
			s.logger.Warn("retain restored host reboot marker", "operation", status.ID, "error", err)
		}
	}
}

// Recovery never sends a reboot. Owned holds survive timeouts, cancellation,
// Controller restart, status-write failure and a host returning under a new ID.
func (s *Server) recoverHostRebootHolds(ctx context.Context) error {
	s.state.agentSettingsMu.Lock()
	value, err := loadHostAvailability(s.state.dataDir)
	s.state.agentSettingsMu.Unlock()
	if err != nil {
		return err
	}
	operations := make(map[string]bool)
	for nodeID, hold := range value.Reboots {
		s.cancelMu.Lock()
		active := s.hostRebootActiveNodes[nodeID] != ""
		s.cancelMu.Unlock()
		if !active {
			operations[hold.OperationID] = true
		}
	}
	var failures []error
	for id := range operations {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.hostRebootMu.Lock()
		job, err := s.loadHostRebootLocked(id)
		var status hostRebootStatus
		if err == nil {
			status = cloneHostRebootStatus(job.status)
		}
		s.hostRebootMu.Unlock()
		if err != nil {
			failures = append(failures, fmt.Errorf("load reboot %s for recovery: %w", id, err))
			continue
		}
		before := cloneHostRebootStatus(status)
		needsCommit := false
		// A restored marker means hold persistence completed before publishing
		// the host outcome. A contradictory old outcome must earn a fresh proof.
		for i, host := range status.Hosts {
			if hold, owned := value.Reboots[host.NodeID]; owned && hold.OperationID == id {
				needsCommit = needsCommit || hold.Restored
				if !hold.Restored && host.State == "rebooted" {
					status.Hosts[i].State = "unknown"
				}
			}
		}
		proofErr := s.confirmRebootProofs(ctx, &status)
		complete := len(status.Hosts) > 0
		for _, host := range status.Hosts {
			complete = complete && host.State == "rebooted"
		}
		if complete && proofErr == nil {
			if status.State != "completed" || status.FinishedAt == nil {
				now := time.Now().UTC()
				status.FinishedAt = &now
			}
			status.State, status.Error = "completed", ""
		}
		// Offline and unready hosts commonly leave the outcome unchanged. Avoid
		// rewriting/fsyncing their metadata every poll. A restored marker still
		// requires a durable job commit before it can be removed, even when the
		// status bytes already match after a previous uncertain fsync.
		if needsCommit || !reflect.DeepEqual(before, status) {
			if err := s.updateHostReboot(job, func(current *hostRebootStatus) { *current = status }); err != nil {
				failures = append(failures, fmt.Errorf("persist reboot %s recovery: %w", id, err))
				continue
			}
		}
		if proofErr != nil {
			failures = append(failures, proofErr)
		}
		s.cleanupRestoredHostRebootHolds(status)
	}
	return errors.Join(failures...)
}

func (s *Server) runHostRebootRecoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		if err := s.recoverHostRebootHolds(ctx); err != nil && ctx.Err() == nil {
			s.logger.Warn("recover host reboot participation", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
