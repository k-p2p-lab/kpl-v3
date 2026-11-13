package controller

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func (s *Server) prepareExperimentNetwork(ctx context.Context, experiment model.Experiment) (prepareErr error) {
	if s.worker != nil {
		return s.worker.command(ctx, "prepare-network", nil)
	}
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	request := model.ExperimentNetworkRequest{RunID: experiment.ID, Epoch: rand.Text(), RequestedAt: time.Now().UTC()}
	started := time.Now()
	stageStarted := started
	stage := "Agent network fence"
	s.logger.Info("experiment network preparation started", "run", experiment.ID, "epoch", request.Epoch, "stage", stage)
	advance := func(next string) {
		s.logger.Info("experiment network preparation advanced", "run", experiment.ID, "epoch", request.Epoch, "completedStage", stage, "stageElapsed", time.Since(stageStarted), "outcome", "ready", "stage", next, "elapsed", time.Since(started))
		stage, stageStarted = next, time.Now()
	}
	defer func() {
		outcome := "ready"
		if prepareErr != nil {
			outcome = "failed"
			if errors.Is(prepareErr, context.Canceled) {
				outcome = "canceled"
			} else if errors.Is(prepareErr, context.DeadlineExceeded) {
				outcome = "timeout"
			}
		}
		// HTTP errors can contain control URLs. Keep this stage trace limited to
		// identity and timing; the existing Run failure path retains its cause.
		s.logger.Info("experiment network preparation finished", "run", experiment.ID, "epoch", request.Epoch, "stage", stage, "stageElapsed", time.Since(stageStarted), "elapsed", time.Since(started), "outcome", outcome)
	}()
	s.state.mu.RLock()
	agents := make([]model.Agent, 0, len(s.state.agents))
	for _, agent := range s.state.agents {
		agents = append(agents, agent)
	}
	s.state.mu.RUnlock()
	if len(agents) == 0 {
		return errors.New("cannot reset experiment network without registered Agents")
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].ID < agents[j].ID })
	// Include disabled Agents: they can still own Peers and must participate in
	// the cleanup barrier before a shared experiment overlay is removed.
	if err := runOperations(ctx, len(agents), true, 8, nil, false, func(ctx context.Context, i int) error {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		if err := s.callAgent(ctx, agents[i].URL, http.MethodPost, "/api/v1/network/fence", request, nil); err != nil {
			return fmt.Errorf("confirm network cleanup on Agent %s: %w", agents[i].ID, err)
		}
		return nil
	}); err != nil {
		return err
	}
	advance("Manager network preparation")
	request.Agents = make(map[string]string, len(agents))
	for _, agent := range agents {
		request.Agents[agent.ID] = agent.URL
	}
	var network model.ExperimentNetwork
	if err := s.callAgent(ctx, s.config.NetworkManagerURL, http.MethodPost, "/api/v1/network/prepare", request, &network); err != nil {
		return fmt.Errorf("initialize experiment network: %w", err)
	}
	if network.RunID != request.RunID || network.Epoch != request.Epoch || network.NetworkID == "" {
		return errors.New("network-manager returned a different experiment identity")
	}
	advance("Agent network activation")
	if err := runOperations(ctx, len(agents), true, 8, nil, false, func(ctx context.Context, i int) error {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		if err := s.callAgent(ctx, agents[i].URL, http.MethodPost, "/api/v1/network/activate", network, nil); err != nil {
			return fmt.Errorf("activate fresh network on Agent %s: %w", agents[i].ID, err)
		}
		s.state.mu.RLock()
		current, exists := s.state.agents[agents[i].ID]
		s.state.mu.RUnlock()
		if !exists || !current.StartedAt.Equal(agents[i].StartedAt) {
			return fmt.Errorf("Agent %s restarted during network preparation", agents[i].ID)
		}
		return nil
	}); err != nil {
		return err
	}
	s.updateExperiment(experiment.ID, func(run *model.Experiment) { run.PeerNetworkID = network.NetworkID })
	s.logger.Info("experiment network initialized", "run", experiment.ID, "network", network.NetworkName, "networkId", network.NetworkID)
	return nil
}
