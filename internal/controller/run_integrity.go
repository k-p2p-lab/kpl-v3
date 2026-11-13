package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const ownershipDirectory = "run-ownership"

// Persist ownership before sending a create, including requests whose response
// is lost. Metadata survives Controller restart and deletion of an old attempt.
func (s *Server) rememberRunAgent(id string, agent model.Agent) error {
	if s.worker != nil {
		return s.worker.call(context.Background(), "remember", agent, nil)
	}
	return s.rememberRunAgentWithPersistence(id, agent, s.persistExperiment)
}

func (s *Server) rememberRunAgentWithPersistence(id string, agent model.Agent, persist func(model.Experiment) error) error {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	s.state.mu.RLock()
	run, ok := s.state.experiments[id]
	s.state.mu.RUnlock()
	if !ok {
		return nil
	} // manual Peer API has no experiment
	for _, old := range run.Agents {
		if old.ID == agent.ID && old.StartedAt.Equal(agent.StartedAt) && old.URL == agent.URL && old.Capacity == agent.Capacity && old.PeerImage == agent.PeerImage {
			return nil
		}
	}
	run.Agents = append(append([]model.Agent(nil), run.Agents...), agent)
	if err := os.MkdirAll(filepath.Join(s.config.DataDir, ownershipDirectory), 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(s.config.DataDir, ownershipDirectory))
	if err != nil {
		return err
	}
	defer root.Close()
	if err = writeAnalysisJSON(root, id+".json", run.Agents); err != nil {
		return err
	}
	if err = syncRunDirectory(root); err != nil {
		return err
	}
	data, err := os.OpenRoot(s.config.DataDir)
	if err != nil {
		return err
	}
	err = syncRunDirectory(data)
	data.Close()
	if err != nil {
		return err
	}
	if err = persist(run); err != nil {
		return err
	}
	s.state.mu.Lock()
	// Stop requests and storage failures can update live state without
	// persistMu during the writes above. Commit only the ownership field;
	// replacing the earlier Run copy would erase their cancellation/evidence.
	current := s.state.experiments[id]
	current.Agents = run.Agents
	s.state.experiments[id] = current
	s.state.mu.Unlock()
	return nil
}
func (s *Server) cleanupParticipants(id string) ([]model.Agent, error) {
	return s.cleanupParticipantsChecked(id, false)
}

func (s *Server) cleanupParticipantsChecked(id string, requireRecordedDrain bool) ([]model.Agent, error) {
	var owned []model.Agent
	data, err := os.ReadFile(filepath.Join(s.config.DataDir, ownershipDirectory, id+".json"))
	if err == nil {
		if len(data) > resultMetadataLimit {
			return nil, errors.New("run ownership too large")
		}
		if err = json.Unmarshal(data, &owned); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	s.state.mu.RLock()
	known := make(map[string]model.Agent, len(s.state.agents))
	for key, agent := range s.state.agents {
		known[key] = agent
	}
	s.state.mu.RUnlock()
	unique := map[string]model.Agent{}
	for _, agent := range owned {
		current, registered := known[agent.ID]
		compatible := registered && (current.URL == agent.URL || (!agent.StartedAt.IsZero() && current.StartedAt.Equal(agent.StartedAt)) || (current.StartupReconciled && current.StartedAt.After(agent.StartedAt) && sameAgentHost(current, agent)))
		if requireRecordedDrain && agent.RunProcesses {
			compatible = registered && !agent.StartedAt.IsZero() && (current.StartedAt.Equal(agent.StartedAt) || current.StartupReconciled && current.StartedAt.After(agent.StartedAt) && sameRunWorkerAgentHost(current, agent))
			if !compatible {
				return nil, &agentRunWorkerPendingError{message: fmt.Sprintf("Agent %s must register a matching instance before its recorded Run worker cleanup can be confirmed", agent.ID)}
			}
			if !current.RunProcesses || !current.RunDrain {
				return nil, &agentRunWorkerPendingError{message: fmt.Sprintf("Agent %s no longer supports the recorded Run worker cleanup; process reset cannot be confirmed", agent.ID)}
			}
		}
		if compatible {
			if requireRecordedDrain && agent.RunDrain && !current.RunDrain {
				return nil, fmt.Errorf("Agent %s no longer supports the recorded final telemetry drain; delivery cannot be confirmed", agent.ID)
			}
			agent = current
		}
		unique[agent.ID+"@"+agent.URL] = agent
	}
	if len(owned) == 0 {
		for id, agent := range known {
			unique[id] = agent
		}
	}
	if len(unique) == 0 && len(owned) == 0 {
		raw, e := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "scenario.yaml"))
		if e == nil {
			spec, e := scenario.Parse(raw)
			if e != nil {
				return nil, e
			}
			for _, phase := range spec.Phases {
				if phase.Action == "join" {
					return nil, errors.New("cannot verify legacy Peer cleanup before Agents reconnect")
				}
			}
		}
	}
	agents := make([]model.Agent, 0, len(unique))
	for _, agent := range unique {
		agents = append(agents, agent)
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].ID < agents[j].ID })
	return agents, nil
}

func sameRunWorkerAgentHost(current, recorded model.Agent) bool {
	// A missing Swarm node ID cannot downgrade an existing host identity to
	// hostname equality; different machines may legitimately share a hostname.
	currentNode, recordedNode := current.Labels["swarmNodeId"], recorded.Labels["swarmNodeId"]
	if currentNode != "" || recordedNode != "" {
		return currentNode != "" && currentNode == recordedNode
	}
	return sameAgentHost(current, recorded)
}

type agentRunDrainPendingError struct {
	remaining int
	message   string
}

func (e *agentRunDrainPendingError) Error() string { return e.message }

// Keep the displayed failure list short without hiding structured evidence from
// callers that must decide whether final telemetry is still pending.
type runTelemetryDrainError struct {
	message string
	causes  []error
}

func (e *runTelemetryDrainError) Error() string   { return e.message }
func (e *runTelemetryDrainError) Unwrap() []error { return e.causes }

// Only an explicit positive Agent report establishes an outstanding delivery
// obligation. A generic incomplete result or a transport error is not proof.
func hasPendingRunTelemetry(err error) bool {
	if pending, ok := err.(*agentRunDrainPendingError); ok {
		return pending.remaining > 0
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		for _, cause := range wrapped.Unwrap() {
			if hasPendingRunTelemetry(cause) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return hasPendingRunTelemetry(wrapped.Unwrap())
	}
	return false
}

func parseRunDrainPending(data []byte) *agentRunDrainPendingError {
	var response struct {
		Error           string `json:"error"`
		Code            string `json:"code"`
		RemainingEvents *int   `json:"remainingEvents"`
	}
	if json.Unmarshal(data, &response) != nil {
		return nil
	}
	if response.Code == model.RunTelemetryPendingCode && response.RemainingEvents != nil && *response.RemainingEvents > 0 {
		return &agentRunDrainPendingError{remaining: *response.RemainingEvents, message: response.Error}
	}
	// A new Controller can finish draining older Agents that still return only
	// the original error string. No other 503 is assumed to mean queued data.
	if response.Code == "" && strings.HasPrefix(response.Error, "run logs not fully delivered (") {
		raw, _, found := strings.Cut(strings.TrimPrefix(response.Error, "run logs not fully delivered ("), " remaining):")
		remaining, err := strconv.Atoi(raw)
		if found && err == nil && remaining > 0 {
			return &agentRunDrainPendingError{remaining: remaining, message: response.Error}
		}
	}
	return nil
}

const runDrainConcurrency = 4
const runDrainRetryDelay = time.Second

func (s *Server) drainAgentTelemetry(ctx context.Context, id string, agent model.Agent) error {
	var lastPending *agentRunDrainPendingError
	for {
		if err := ctx.Err(); err != nil {
			if lastPending != nil {
				return fmt.Errorf("%w; last Agent report: %w", err, lastPending)
			}
			return err
		}
		wait := model.MaxRunDrainTimeout
		if deadline, ok := ctx.Deadline(); ok {
			wait = min(wait, time.Until(deadline))
		}
		if wait <= 0 {
			continue
		}
		path := "/api/v1/runs/" + url.PathEscape(id) + "/drain?timeout=" + url.QueryEscape(wait.String())
		err := s.callAgent(ctx, agent.URL, http.MethodPost, path, nil, nil)
		if err == nil {
			if agent.RunProcesses {
				return s.confirmRunWorkerDrain(ctx, agent)
			}
			return nil
		}
		var pending *agentRunDrainPendingError
		if !errors.As(err, &pending) {
			if ctx.Err() != nil && lastPending != nil {
				return fmt.Errorf("%w; last Agent report: %w", ctx.Err(), lastPending)
			}
			if lastPending != nil {
				return fmt.Errorf("%w; last Agent report: %w", err, lastPending)
			}
			return err
		}
		if pending.remaining > 0 || lastPending == nil {
			lastPending = pending
		}
		s.logger.Info("waiting for final Agent telemetry", "run", id, "agent", agent.ID, "remaining_events", pending.remaining)
		if err := sleepContext(ctx, runDrainRetryDelay); err != nil {
			return fmt.Errorf("%w; last Agent report: %w", err, lastPending)
		}
	}
}

// A stale task address can now serve an older Agent whose drain acknowledges
// only logs. Check the raw status instead of the merged heartbeat cache, where
// omitted capability fields intentionally preserve the registered values.
func (s *Server) confirmRunWorkerDrain(ctx context.Context, agent model.Agent) error {
	var heartbeat model.AgentHeartbeat
	if err := s.callAgent(ctx, agent.URL, http.MethodGet, "/api/v1/status?view=agent", nil, &heartbeat); err != nil {
		pending := &agentRunWorkerPendingError{message: fmt.Sprintf("Agent %s Run worker cleanup cannot be confirmed: %v", agent.ID, err)}
		return errors.Join(pending, err)
	}
	identity := heartbeat.Agent
	if identity.ID != agent.ID || agent.StartedAt.IsZero() || !identity.StartedAt.Equal(agent.StartedAt) || !identity.RunProcesses || !identity.RunDrain {
		return &agentRunWorkerPendingError{message: fmt.Sprintf("Agent %s Run worker cleanup cannot be confirmed by a different instance or missing process capability", agent.ID)}
	}
	return nil
}

func (s *Server) drainRunTelemetry(ctx context.Context, id string) error {
	if s.worker != nil {
		return s.worker.command(ctx, "drain", nil)
	}
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 3*time.Minute)
		defer cancel()
	}
	agents, err := s.cleanupParticipantsChecked(id, true)
	if err != nil {
		return err
	}
	var capable []model.Agent
	for _, agent := range agents {
		if agent.RunDrain {
			capable = append(capable, agent)
		}
	}
	// Contact owners with bounded concurrency so a slow Agent does not block
	// all others. The parent deadline bounds the whole collection phase.
	jobs := make(chan int, len(capable))
	for index := range capable {
		jobs <- index
	}
	close(jobs)
	errorsByAgent := make([]error, len(capable))
	var workers sync.WaitGroup
	for range min(runDrainConcurrency, len(capable)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				agent := capable[index]
				if err := s.drainAgentTelemetry(ctx, id, agent); err != nil {
					errorsByAgent[index] = fmt.Errorf("collect final logs from Agent %s: %w", agent.ID, err)
				}
			}
		}()
	}
	workers.Wait()
	var failures []error
	var causes []error
	count := 0
	for index, err := range errorsByAgent {
		if err == nil {
			continue
		}
		count++
		causes = append(causes, err)
		s.logger.Warn("final Agent telemetry drain failed", "run", id, "agent", capable[index].ID, "error", err)
		if len(failures) < 4 {
			failures = append(failures, err)
		}
	}
	if count > len(failures) {
		failures = append(failures, fmt.Errorf("%d additional Agents failed final log collection; inspect Controller logs", count-len(failures)))
	}
	if len(causes) == 0 {
		return nil
	}
	return &runTelemetryDrainError{message: errors.Join(failures...).Error(), causes: causes}
}
func (s *Server) flushRunSources(id string) error {
	if s.worker != nil {
		return s.worker.call(context.Background(), "flush", nil, nil)
	}
	root, err := s.analysisDirectory(id)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, name := range []string{"events.jsonl", "observations.jsonl"} {
		f, e := root.Open(name)
		if errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		e = errors.Join(f.Sync(), f.Close())
		if e != nil {
			return e
		}
	}
	return syncRunDirectory(root)
}
func (s *state) recordRunWriteError(id string, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.experiments[id]
	if !ok {
		return
	}
	if run.IntegrityError == "" {
		run.IntegrityError = "Result recording failed: " + err.Error()
	}
	run.DataState = "incomplete"
	if run.State == "completed" {
		run.State = "failed"
		run.Error = run.IntegrityError
	}
	s.experiments[id] = run
	if s.failedRunWrites == nil {
		s.failedRunWrites = map[string]bool{}
	}
	s.failedRunWrites[id] = true
}
func (s *Server) watchRunStorage(ctx context.Context, done <-chan struct{}, id string, cancel context.CancelFunc) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
		}
		err := s.checkRunStorage()
		if err != nil {
			s.state.recordRunWriteError(id, err)
		}
		s.state.mu.RLock()
		failure := s.state.experiments[id].IntegrityError
		s.state.mu.RUnlock()
		if failure != "" {
			cancel()
			return
		}
	}
}
func (s *Server) retryFailedRunWrites() {
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	s.state.mu.RLock()
	var ids []string
	for id := range s.state.failedRunWrites {
		ids = append(ids, id)
	}
	s.state.mu.RUnlock()
	for _, id := range ids {
		if deleted, err := s.state.resultWriteFencedLocked(id); err != nil || deleted {
			continue
		}
		s.state.mu.RLock()
		run := s.state.experiments[id]
		s.state.mu.RUnlock()
		if err := s.persistExperiment(run); err == nil {
			s.state.mu.Lock()
			delete(s.state.failedRunWrites, id)
			s.state.mu.Unlock()
		}
	}
}

func sameAgentHost(a, b model.Agent) bool {
	if node := a.Labels["swarmNodeId"]; node != "" {
		return node == b.Labels["swarmNodeId"]
	}
	return a.Hostname != "" && a.Hostname == b.Hostname
}

// Called under cancelMu. The flag is shared through SSE so a refreshed browser
// observes accepted cancellation; execution identity prevents it leaking to retry.
func (s *Server) markStopRequestedLocked(id, execution string) {
	s.state.mu.Lock()
	for key, run := range s.state.experiments {
		if (execution != "" && run.ExecutionID == execution) || (execution == "" && key == id) {
			if run.State == "running" || run.State == "queued" {
				run.StopRequested = true
				s.state.experiments[key] = run
			}
		}
	}
	s.state.mu.Unlock()
	s.state.notify()
}
