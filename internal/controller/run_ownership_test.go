package controller

import (
	"errors"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRememberRunAgentPreservesConcurrentStopAndRecordingFailure(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "owned", ExecutionID: "execution", State: "running", DataState: "collecting"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	agent := model.Agent{ID: "agent", URL: "http://agent", Capacity: 1}
	err := s.rememberRunAgentWithPersistence(run.ID, agent, func(saved model.Experiment) error {
		if err := s.persistExperiment(saved); err != nil {
			return err
		}
		// Stop requests and the storage watcher update live state without
		// persistMu while ownership metadata is being written.
		s.cancelMu.Lock()
		s.markStopRequestedLocked(run.ID, run.ExecutionID)
		s.cancelMu.Unlock()
		s.state.recordRunWriteError(run.ID, errors.New("recording failed during ownership persistence"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	got := s.state.experiments[run.ID]
	s.state.mu.RUnlock()
	if !got.StopRequested || got.IntegrityError == "" || got.DataState != "incomplete" {
		t.Fatalf("ownership persistence discarded newer run state: %+v", got)
	}
	if len(got.Agents) != 1 || got.Agents[0].ID != agent.ID {
		t.Fatalf("ownership missing from current run: %+v", got.Agents)
	}
	s.retryFailedRunWrites()
	saved := persistedExperiment(t, s, run.ID)
	if !saved.StopRequested || saved.IntegrityError != got.IntegrityError || saved.DataState != "incomplete" || len(saved.Agents) != 1 {
		t.Fatalf("metadata repair lost cancellation, integrity or ownership: %+v", saved)
	}
}

func TestRememberRunAgentFailedMetadataKeepsCleanupOwnership(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	run := model.Experiment{ID: "owned", State: "running"}
	s.state.experiments[run.ID] = run
	if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
		t.Fatal(err)
	}
	agent := model.Agent{ID: "agent", URL: "http://agent", Capacity: 1}
	failure := errors.New("experiment metadata unavailable")
	if err := s.rememberRunAgentWithPersistence(run.ID, agent, func(model.Experiment) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("metadata failure was accepted: %v", err)
	}
	if got := s.state.experiments[run.ID]; len(got.Agents) != 0 {
		t.Fatalf("failed metadata write committed live ownership: %+v", got.Agents)
	}
	// The separate durable record precedes the metadata write, so an
	// interrupted create still has a cleanup target after Controller restart.
	restarted := New(s.config, nil)
	owners, err := restarted.cleanupParticipants(run.ID)
	if err != nil || len(owners) != 1 || owners[0].ID != agent.ID || owners[0].URL != agent.URL {
		t.Fatalf("metadata failure lost cleanup ownership: %+v, %v", owners, err)
	}
}
