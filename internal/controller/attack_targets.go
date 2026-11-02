package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// handleAttackTargets exposes only the requester's run and generation. Retained
// terminal identities remain useful for matching late publications, but are not
// dialable. No client-supplied addresses are accepted by the attack interface.
type attackTargetCache struct {
	failed     bool
	mu         sync.Mutex
	runID      string
	generation uint64
	observedAt time.Time
	raw        []byte
}

func (s *Server) handleAttackTargets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	runID := strings.TrimSpace(r.URL.Query().Get("runId"))
	requesterID := strings.TrimSpace(r.URL.Query().Get("requesterNodeId"))
	generation, generationErr := strconv.ParseUint(r.URL.Query().Get("generation"), 10, 64)
	if runID == "" || requesterID == "" || generationErr != nil {
		writeError(w, http.StatusBadRequest, "runId, requesterNodeId and unsigned generation are required")
		return
	}
	result := make([]model.AttackTarget, 0)
	now := time.Now().UTC()
	s.state.mu.RLock()
	requester, exists := s.state.nodes[requesterID]
	if !exists || requester.RunID != runID || requester.Generation != generation || requester.Role != "worker" || !nodeHasLiveState(requester) {
		s.state.mu.RUnlock()
		writeError(w, http.StatusConflict, "requester is not a live worker in this run and generation")
		return
	}
	s.state.mu.RUnlock()
	s.attackTargets.mu.Lock()
	cache := &s.attackTargets
	if cache.runID == runID && cache.generation == requester.Generation && now.Sub(cache.observedAt) < time.Second {
		raw, failed := cache.raw, cache.failed
		s.attackTargets.mu.Unlock()
		if failed {
			writeError(w, http.StatusServiceUnavailable, "attack registry capacity exceeded")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
		return
	}
	s.state.mu.RLock()
	for _, node := range s.state.nodes {
		if node.RunID != runID || node.Generation != requester.Generation || node.PeerID == "" || !nodeFeatureEnabled(node, "pubsubEnabled", node.Type != "boot" && node.Type != "dht-only") {
			continue
		}
		author := node.Metadata["pubsubAuthorId"]
		if author == "" && node.Metadata["pubsubAuthorMode"] != "anonymous" && node.Metadata["pubsubAuthorMode"] != "explicit" {
			author = node.PeerID
		}
		ready := node.State == model.NodeReady && len(node.Addresses) > 0 && agentIsOnline(s.state.agents[node.AgentID], now)
		result = append(result, model.AttackTarget{NodeID: node.ID, Group: node.Group, PeerID: node.PeerID, AuthorPeerID: author, Addresses: append([]string(nil), node.Addresses...), Ready: ready})
		if len(result) > 50000 {
			break
		}
	}
	s.state.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool { return result[i].NodeID < result[j].NodeID })
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > 8<<20 || len(result) > 50000 {
		cache.runID, cache.generation, cache.observedAt, cache.raw, cache.failed = runID, requester.Generation, now, nil, true
		s.attackTargets.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("attack registry capacity exceeded: %d identities", len(result)))
		return
	}
	cache.runID, cache.generation, cache.observedAt, cache.raw = runID, requester.Generation, now, raw
	cache.failed = false
	s.attackTargets.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}
