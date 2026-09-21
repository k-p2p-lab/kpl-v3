package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

const (
	recentEventLimit = 300
	agentStaleAfter  = 10 * time.Second
)

type state struct {
	mu              sync.RWMutex
	persistMu       sync.Mutex
	agents          map[string]model.Agent
	nodes           map[string]model.Node
	reservations    map[string]string
	agentSnapshots  map[string]time.Time
	nodeReportTimes map[string]time.Time
	experiments     map[string]model.Experiment
	runTimings      map[string]*runTiming
	events          []model.TraceEvent
	watchers        map[chan struct{}]struct{}
	dataDir         string
	metrics         *controllerMetrics
	runMetrics      map[string]*runMetricAccumulator
}

func newState(dataDir string) *state {
	s := &state{
		agents:          make(map[string]model.Agent),
		nodes:           make(map[string]model.Node),
		reservations:    make(map[string]string),
		agentSnapshots:  make(map[string]time.Time),
		nodeReportTimes: make(map[string]time.Time),
		experiments:     make(map[string]model.Experiment),
		runTimings:      make(map[string]*runTiming),
		watchers:        make(map[chan struct{}]struct{}),
		dataDir:         dataDir,
		runMetrics:      make(map[string]*runMetricAccumulator),
	}
	s.metrics = newControllerMetrics(s)
	return s
}

func (s *state) registerAgent(agent model.Agent) (model.Agent, error) {
	if strings.TrimSpace(agent.ID) == "" || strings.TrimSpace(agent.URL) == "" {
		return model.Agent{}, fmt.Errorf("agent id and url are required")
	}
	now := time.Now().UTC()
	reportedAt := agent.LastSeen
	s.mu.Lock()
	previous, exists := s.agents[agent.ID]
	if exists && agent.StartedAt.IsZero() {
		agent.StartedAt = previous.StartedAt
	}
	restarted := exists && !agent.StartedAt.Equal(previous.StartedAt)
	if restarted && (agent.StartedAt.Before(previous.StartedAt) || agentIsOnline(previous, now)) {
		s.mu.Unlock()
		return model.Agent{}, fmt.Errorf("agent %q is already registered by another live or newer instance", agent.ID)
	}
	if restarted {
		for nodeID, agentID := range s.reservations {
			if agentID == agent.ID {
				delete(s.reservations, nodeID)
			}
		}
		for nodeID, node := range s.nodes {
			if node.AgentID == agent.ID && node.State != model.NodeStopped && node.State != model.NodeFailed {
				node.State = model.NodeFailed
				node.Error = "Agent instance restarted"
				node.LastSeen = now
				s.nodes[nodeID] = node
			}
		}
		delete(s.agentSnapshots, agent.ID)
	} else if exists {
		agent.MetricsURL = firstNonEmpty(agent.MetricsURL, previous.MetricsURL)
		agent.ActiveNodes = max(agent.ActiveNodes, previous.ActiveNodes)
	} else {
		for _, agentID := range s.reservations {
			if agentID == agent.ID {
				agent.ActiveNodes++
			}
		}
	}
	if agent.StartedAt.IsZero() {
		agent.StartedAt = now
	}
	if !reportedAt.IsZero() && reportedAt.After(s.agentSnapshots[agent.ID]) {
		s.agentSnapshots[agent.ID] = reportedAt
	}
	agent.ActiveNodes = max(0, agent.ActiveNodes)
	agent.LastSeen = now
	agent.State = model.AgentOnline
	s.agents[agent.ID] = agent
	s.mu.Unlock()
	s.notify()
	return agent, nil
}

func (s *state) heartbeat(h model.AgentHeartbeat) error {
	if h.Agent.ID == "" {
		return fmt.Errorf("agent id is required")
	}
	now := time.Now().UTC()
	s.mu.Lock()
	previous, ok := s.agents[h.Agent.ID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("agent %q is not registered", h.Agent.ID)
	}
	if !h.Agent.StartedAt.IsZero() && !h.Agent.StartedAt.Equal(previous.StartedAt) {
		s.mu.Unlock()
		return fmt.Errorf("agent %q heartbeat does not match its registered instance", h.Agent.ID)
	}
	reportedAt := h.Agent.LastSeen
	last := s.agentSnapshots[h.Agent.ID]
	stale := !reportedAt.IsZero() && !last.IsZero() && reportedAt.Before(last)
	if !h.Partial && !reportedAt.IsZero() && !last.IsZero() && !reportedAt.After(last) {
		s.mu.Unlock()
		return nil
	}
	seen := make(map[string]struct{}, len(h.Nodes))
	for _, node := range h.Nodes {
		_, duplicate := seen[node.ID]
		owner := s.nodes[node.ID].AgentID
		reservation := s.reservations[node.ID]
		if node.ID == "" || duplicate || owner != "" && owner != h.Agent.ID || reservation != "" && reservation != h.Agent.ID {
			s.mu.Unlock()
			return fmt.Errorf("agent %q heartbeat contains an empty, duplicate, or foreign node %q", h.Agent.ID, node.ID)
		}
		seen[node.ID] = struct{}{}
	}
	if stale {
		// A newer full status can cross an older partial batch. Successful
		// terminal records are immutable, so acknowledge them even if the
		// newer snapshot did not contain a peer that lived entirely between
		// reports. Never apply older active states or physical occupancy.
		terminal := make([]model.Node, 0, len(h.Nodes))
		for _, node := range h.Nodes {
			if node.State == model.NodeStopped {
				terminal = append(terminal, node)
			}
		}
		h.Nodes = terminal
	}
	if !stale && !reportedAt.IsZero() {
		s.agentSnapshots[h.Agent.ID] = reportedAt
	}
	h.Agent.URL = firstNonEmpty(h.Agent.URL, previous.URL)
	h.Agent.MetricsURL = firstNonEmpty(h.Agent.MetricsURL, previous.MetricsURL)
	h.Agent.Name = firstNonEmpty(h.Agent.Name, previous.Name)
	h.Agent.Hostname = firstNonEmpty(h.Agent.Hostname, previous.Hostname)
	h.Agent.Version = firstNonEmpty(h.Agent.Version, previous.Version)
	if h.Agent.Capacity == 0 {
		h.Agent.Capacity = previous.Capacity
	}
	if h.Agent.StartedAt.IsZero() {
		h.Agent.StartedAt = previous.StartedAt
	}
	h.Agent.LastSeen = now
	h.Agent.State = model.AgentOnline
	reportedOccupied := max(0, h.Agent.ActiveNodes)
	observedActive := 0
	releasedReservations := 0
	for _, node := range h.Nodes {
		// Both input timestamps use the Agent's clock. Preserve LastSeen for
		// status/creation merges, but normalize its age for topology freshness.
		s.nodeReportTimes[node.ID] = topologyReportTime(now, reportedAt, node.LastSeen)
		node.AgentID = h.Agent.ID
		if node.LastSeen.IsZero() {
			node.LastSeen = now
		}
		if node.StartedAt.IsZero() {
			if old, found := s.nodes[node.ID]; found {
				node.StartedAt = old.StartedAt
			} else {
				node.StartedAt = now
			}
		}
		if old, found := s.nodes[node.ID]; found {
			// Node IDs are never reused. Completed exits cannot be revived by
			// a late report, including one carrying an old cleanup failure.
			if old.State == model.NodeStopped && node.State != model.NodeStopped {
				node = old
			} else if old.State == model.NodeFailed && node.State != model.NodeFailed && node.State != model.NodeStopping && node.State != model.NodeStopped {
				// A failed process cannot restart under the same node ID. Keep
				// accepting cleanup progress, but reject delayed live reports.
				node = old
			} else if old.State == model.NodeStopping && node.State != model.NodeStopping && node.State != model.NodeStopped && node.State != model.NodeFailed {
				// A heartbeat assembled before DELETE must not undo the stop.
				node = old
			}
		}
		s.nodes[node.ID] = node
		if s.reservations[node.ID] == h.Agent.ID {
			delete(s.reservations, node.ID)
			releasedReservations++
		}
		if node.State != model.NodeStopping && node.State != model.NodeStopped && node.State != model.NodeFailed {
			observedActive++
		}
	}
	if h.Partial {
		// Other chunks and previously acknowledged records remain part of
		// this Agent's inventory. Omission cannot free their capacity.
		observedActive = 0
		for _, node := range s.nodes {
			if node.AgentID == h.Agent.ID && s.reservations[node.ID] != h.Agent.ID && node.State != model.NodeStopping && node.State != model.NodeStopped && node.State != model.NodeFailed {
				observedActive++
			}
		}
	}
	// Docker cleanup may still occupy capacity after a node stops being
	// logically active. Preserve the Agent's physical occupancy report.
	h.Agent.ActiveNodes = max(reportedOccupied, observedActive)
	for _, agentID := range s.reservations {
		if agentID == h.Agent.ID {
			h.Agent.ActiveNodes++
		}
	}
	for id, node := range s.nodes {
		if node.AgentID != h.Agent.ID {
			continue
		}
		if _, found := seen[id]; !h.Partial && !found && node.State != model.NodeStopped {
			if s.reservations[id] == h.Agent.ID {
				continue
			}
			node.State = model.NodeStopped
			node.LastSeen = now
			s.nodes[id] = node
		}
	}
	if stale {
		h.Agent = previous
		h.Agent.ActiveNodes = max(0, previous.ActiveNodes-releasedReservations)
	}
	s.agents[h.Agent.ID] = h.Agent
	s.mu.Unlock()
	for _, node := range h.Nodes {
		node.AgentID = h.Agent.ID
		s.metrics.initNode(node)
	}
	s.notify()
	return nil
}

func agentIsOnline(agent model.Agent, now time.Time) bool {
	return agent.State == model.AgentOnline && (agent.LastSeen.IsZero() || now.Sub(agent.LastSeen) <= agentStaleAfter)
}

func (s *state) markStaleAgents(maxAge time.Duration) {
	now := time.Now().UTC()
	changed := false
	s.mu.Lock()
	for id, agent := range s.agents {
		if now.Sub(agent.LastSeen) > maxAge && agent.State != model.AgentOffline {
			agent.State = model.AgentOffline
			s.agents[id] = agent
			changed = true
		}
	}
	s.mu.Unlock()
	if changed {
		s.notify()
	}
}

func (s *state) appendEvents(batch model.EventBatch) error {
	if len(batch.Events) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for i := range batch.Events {
		if batch.Events[i].AgentID == "" {
			batch.Events[i].AgentID = batch.AgentID
		}
		// Bandwidth requires its source timestamp; receipt time cannot repair it.
		if batch.Events[i].Timestamp.IsZero() && batch.Events[i].Type != "bandwidth" {
			batch.Events[i].Timestamp = now
		}
	}
	byRun := make(map[string][]model.TraceEvent)
	for _, event := range batch.Events {
		byRun[event.RunID] = append(byRun[event.RunID], event)
	}
	for runID, events := range byRun {
		if err := s.appendRunEvents(runID, events); err != nil {
			return err
		}
	}
	s.notify()
	return nil
}

func (s *state) appendRunEvents(runID string, events []model.TraceEvent) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if runID != "" {
		if deleted, err := s.resultDeletedLocked(runID); err != nil || deleted {
			return err
		}
	}
	s.mu.RLock()
	accumulator := s.runMetrics[runID]
	if accumulator == nil {
		accumulator = newRunMetricAccumulator()
	}
	pending := make(map[string]struct{})
	accepted := make([]model.TraceEvent, 0, len(events))
	for _, event := range events {
		if accumulator.hasEvent(event) {
			continue
		}
		if id := eventIdentity(event); id != "" {
			if _, exists := pending[id]; exists {
				continue
			}
			pending[id] = struct{}{}
		}
		accepted = append(accepted, event)
	}
	s.mu.RUnlock()
	if len(accepted) == 0 {
		return nil
	}
	// Persist before counting. A retry after another run in the same batch
	// fails will skip these already committed event IDs.
	if runID != "" {
		if err := s.persistEventsLocked(runID, accepted); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.runMetrics[runID] = accumulator
	for _, event := range accepted {
		accumulator.observe(event)
	}
	s.events = append(s.events, accepted...)
	if len(s.events) > recentEventLimit {
		s.events = append([]model.TraceEvent(nil), s.events[len(s.events)-recentEventLimit:]...)
	}
	s.mu.Unlock()
	for _, event := range accepted {
		s.metrics.observeEvent(event)
	}
	return nil
}

func (s *state) persistEvents(runID string, events []model.TraceEvent) error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	if deleted, err := s.resultDeletedLocked(runID); err != nil || deleted {
		return err
	}
	return s.persistEventsLocked(runID, events)
}

func (s *state) persistEventsLocked(runID string, events []model.TraceEvent) error {
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
	}
	runID = safeName(runID)
	dir := filepath.Join(s.dataDir, "runs", runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create run directory: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open event log: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data.Bytes()); err != nil {
		rollbackErr := f.Truncate(info.Size())
		f.Close()
		if rollbackErr != nil {
			return fmt.Errorf("write event: %w; rollback: %v", err, rollbackErr)
		}
		return fmt.Errorf("write event: %w", err)
	}
	return f.Close()
}

func (s *state) inventory() model.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.inventoryLocked()
}

func (s *state) inventoryLocked() model.Snapshot {
	result := model.Snapshot{GeneratedAt: time.Now().UTC()}
	for _, agent := range s.agents {
		result.Agents = append(result.Agents, agent)
	}
	for _, node := range s.nodes {
		result.Nodes = append(result.Nodes, node)
	}
	for _, experiment := range s.experiments {
		result.Experiments = append(result.Experiments, experiment)
	}
	s.estimateRunFinishesLocked(result.Experiments, result.GeneratedAt)
	result.Events = append([]model.TraceEvent(nil), s.events...)
	sort.Slice(result.Agents, func(i, j int) bool { return result.Agents[i].ID < result.Agents[j].ID })
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].ID < result.Nodes[j].ID })
	sort.Slice(result.Experiments, func(i, j int) bool {
		return experimentBefore(result.Experiments[i], result.Experiments[j])
	})
	return result
}

func experimentBefore(a, b model.Experiment) bool {
	// Preserve newest-started-first for metric selection. Queued repetitions
	// share a zero start time, so use a complete tie-break order rather than
	// exposing the map's iteration order in every dashboard refresh.
	if !a.StartedAt.Equal(b.StartedAt) {
		return a.StartedAt.After(b.StartedAt)
	}
	aGroup, bGroup := a.BatchID, b.BatchID
	if aGroup == "" {
		aGroup = a.ID
	}
	if bGroup == "" {
		bGroup = b.ID
	}
	if aGroup != bGroup {
		return aGroup < bGroup
	}
	if a.Iteration != b.Iteration {
		return a.Iteration < b.Iteration
	}
	return a.ID < b.ID
}

func (s *state) snapshot() model.Snapshot {
	s.mu.RLock()
	result := s.inventoryLocked()
	agents := make(map[string]model.Agent, len(result.Agents))
	for _, agent := range result.Agents {
		agents[agent.ID] = agent
	}
	topologyNodes := append([]model.Node(nil), result.Nodes...)
	for i := range topologyNodes {
		topologyNodes[i].LastSeen = s.nodeReportTimes[topologyNodes[i].ID]
	}
	// Queued iterations have no observations yet and must not replace the
	// currently running experiment's metrics simply because they were created
	// a few microseconds later.
	runID := ""
	for _, experiment := range result.Experiments {
		if experiment.State == "running" {
			runID = experiment.ID
			break
		}
	}
	if runID == "" {
		for _, experiment := range result.Experiments {
			if !experiment.StartedAt.IsZero() && experiment.State != "queued" {
				runID = experiment.ID
				break
			}
		}
	}
	if accumulator := s.runMetrics[runID]; accumulator != nil {
		result.Metrics, _ = accumulator.liveSummary(runID, result.GeneratedAt)
		if result.Metrics.Bandwidth != nil {
			bandwidth := *result.Metrics.Bandwidth
			bandwidth.CurrentRates = accumulator.bandwidth.currentRates(result.GeneratedAt)
			result.Metrics.Bandwidth = &bandwidth
		}
	} else {
		result.Metrics.RunID = runID
	}
	s.mu.RUnlock()
	result.Edges = networkEdgesAt(topologyNodes, agents, result.GeneratedAt)
	return result
}

func numberField(fields map[string]any, key string) int {
	value, ok := fields[key]
	if !ok {
		return 0
	}
	switch number := value.(type) {
	case int:
		return number
	case int64:
		return int(number)
	case float64:
		return int(number)
	case json.Number:
		parsed, _ := number.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func (s *state) subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		delete(s.watchers, ch)
		s.mu.Unlock()
	}
}

func (s *state) notify() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for watcher := range s.watchers {
		select {
		case watcher <- struct{}{}:
		default:
		}
	}
}

func safeName(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "unnamed"
	}
	return b.String()
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
