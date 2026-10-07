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

func (s *Server) prepareExperimentNetwork(ctx context.Context, experiment model.Experiment) error {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
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
	request := model.ExperimentNetworkRequest{RunID: experiment.ID, Epoch: rand.Text(), RequestedAt: time.Now().UTC()}
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
