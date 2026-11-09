package controller

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const readinessDiagnosticLimit = 3

type readinessOwner struct {
	count    int
	state    string
	lastSeen time.Time
}

type readinessNode struct {
	id, agentID, state string
}

// Collect diagnostics only after the barrier deadline. No network request,
// raw remote error, URL or configuration is included in the observation.
func (s *Server) readyBarrierTimeoutError(runID string, generation uint64, phase scenario.Phase, cause error) error {
	now := time.Now()
	ready, total, failed := 0, 0, 0
	starting, stopping, stopped, unknown, unavailableReady := 0, 0, 0, 0, 0
	owners := make(map[string]readinessOwner)
	samples := make([]readinessNode, 0, readinessDiagnosticLimit)
	s.state.mu.RLock()
	for _, node := range s.state.nodes {
		if node.RunID != runID || node.Generation != generation || phase.Group != "" && node.Group != phase.Group || phase.NodeType != "" && node.Type != phase.NodeType {
			continue
		}
		total++
		agent, registered := s.state.agents[node.AgentID]
		online := registered && agentIsOnline(agent, now)
		label := "unknown"
		switch node.State {
		case model.NodeReady:
			if online {
				ready++
				continue
			}
			unavailableReady++
			label = "ready/agent-unavailable"
		case model.NodeFailed:
			failed++
			label = "failed"
		case model.NodeStarting:
			starting++
			label = "starting"
		case model.NodeStopping:
			stopping++
			label = "stopping"
		case model.NodeStopped:
			stopped++
			label = "stopped"
		default:
			unknown++
		}
		if !online {
			owner := owners[node.AgentID]
			owner.count++
			owner.lastSeen = agent.LastSeen
			owner.state = "unregistered"
			if registered {
				owner.state = "offline"
				if agent.State == model.AgentOnline {
					owner.state = "stale"
				}
			}
			owners[node.AgentID] = owner
		}
		candidate := readinessNode{node.ID, node.AgentID, label}
		if len(samples) < readinessDiagnosticLimit {
			samples = append(samples, candidate)
		} else if candidate.id < samples[len(samples)-1].id {
			samples[len(samples)-1] = candidate
		} else {
			continue
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i].id < samples[j].id })
	}
	s.state.mu.RUnlock()

	ownerIDs := make([]string, 0, len(owners))
	for id := range owners {
		ownerIDs = append(ownerIDs, id)
	}
	sort.Strings(ownerIDs)
	ownerDetails := make([]string, 0, min(len(ownerIDs), readinessDiagnosticLimit)+1)
	for _, id := range ownerIDs[:min(len(ownerIDs), readinessDiagnosticLimit)] {
		owner := owners[id]
		lastSeen := "unknown"
		if !owner.lastSeen.IsZero() {
			lastSeen = owner.lastSeen.UTC().Format(time.RFC3339)
		}
		ownerDetails = append(ownerDetails, fmt.Sprintf("%s:%d/%s/lastSeen=%s", readinessDiagnosticID(id), owner.count, owner.state, lastSeen))
	}
	if len(ownerIDs) > readinessDiagnosticLimit {
		ownerDetails = append(ownerDetails, fmt.Sprintf("+%d more", len(ownerIDs)-readinessDiagnosticLimit))
	}
	nodeDetails := make([]string, 0, len(samples))
	for _, node := range samples {
		nodeDetails = append(nodeDetails, fmt.Sprintf("%s:%s@%s", readinessDiagnosticID(node.id), node.state, readinessDiagnosticID(node.agentID)))
	}
	return fmt.Errorf("ready barrier timed out for group %q in generation %d: %d/%d ready, %d failed; starting=%d, stopping=%d, stopped=%d, ready-on-unavailable-agent=%d, unknown=%d; unavailable owners=[%s]; unready sample=[%s]: %w", phase.Group, generation, ready, total, failed, starting, stopping, stopped, unavailableReady, unknown, strings.Join(ownerDetails, "; "), strings.Join(nodeDetails, "; "), cause)
}

func readinessDiagnosticID(id string) string {
	if id == "" {
		return "unknown"
	}
	runes := []rune(id)
	if len(runes) > 80 {
		id = string(runes[:38]) + "..." + string(runes[len(runes)-39:])
	}
	return strconv.Quote(id)
}
