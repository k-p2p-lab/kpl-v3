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
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

const ownershipDirectory = "run-ownership"

// Persist ownership before sending a create, including requests whose response
// is lost. Metadata survives Controller restart and deletion of an old attempt.
func (s *Server) rememberRunAgent(id string, agent model.Agent) error {
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
	if err = s.persistExperiment(run); err != nil {
		return err
	}
	s.state.mu.Lock()
	s.state.experiments[id] = run
	s.state.mu.Unlock()
	return nil
}
func (s *Server) cleanupParticipants(id string) ([]model.Agent, error) {
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
		if current, ok := known[agent.ID]; ok && (current.URL == agent.URL || (!agent.StartedAt.IsZero() && current.StartedAt.Equal(agent.StartedAt)) || (current.StartupReconciled && current.StartedAt.After(agent.StartedAt) && sameAgentHost(current, agent))) {
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
func (s *Server) drainRunTelemetry(ctx context.Context, id string) error {
	agents, err := s.cleanupParticipants(id)
	if err != nil {
		return err
	}
	for _, agent := range agents {
		if !agent.RunDrain {
			continue
		} // old Agents are explicitly marked unverified
		if err = s.callAgent(ctx, agent.URL, http.MethodPost, "/api/v1/runs/"+url.PathEscape(id)+"/drain", nil, nil); err != nil {
			return fmt.Errorf("collect final logs from Agent %s: %w", agent.ID, err)
		}
	}
	return nil
}
func (s *Server) flushRunSources(id string) error {
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
		if deleted, err := s.state.resultDeletedLocked(id); err != nil || deleted {
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
