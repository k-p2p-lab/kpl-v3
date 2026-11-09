package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const admissionResetRunA = "run-admission-reset-a"
const admissionResetRunB = "run-admission-reset-b"

func admissionResetFixture(t *testing.T) (*Server, model.Agent) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	run, _ := resultFixture(t, s, admissionResetRunA, "failed", time.Now().UTC())
	s.state.experiments[run.ID] = run
	agent := model.Agent{ID: "admission-agent", URL: "http://fake-agent.invalid", StartedAt: time.Now().UTC().Add(-time.Hour), LastSeen: time.Now().UTC(), State: model.AgentOnline, Capacity: 600, DefaultCapacity: 600, RunProcesses: true, Labels: map[string]string{"swarmNodeId": "admission-host"}}
	registered, err := s.state.registerAgent(agent)
	if err != nil {
		t.Fatal(err)
	}
	return s, registered
}

func TestAgentAdmissionRunResetReleasesOnlyItsSoftHintForNext500Reservations(t *testing.T) {
	s, agent := admissionResetFixture(t)
	s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: 30 * time.Second, reason: "previous Run reset is not complete"}, admissionResetRunA)
	if _, ok := s.tryReserveAgent(admissionResetRunB + "-before"); ok {
		t.Fatal("fixture cooldown did not block admission")
	}
	if err := s.retireRunRuntime(t.Context(), admissionResetRunA); err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	_, owner := s.state.agentAdmissionOwners[agent.ID]
	_, hint := s.state.agentAdmissionUntil[agent.ID]
	s.state.mu.RUnlock()
	if owner || hint {
		t.Fatal("verified reset retained its soft admission state")
	}
	for i := range 500 {
		if _, ok := s.tryReserveAgent(fmt.Sprintf("%s-%05d", admissionResetRunB, i)); !ok {
			t.Fatalf("next Run stalled at %d/500 reservations", i)
		}
	}
}

func TestAgentAdmissionRunResetKeepsOtherOwnersInstancesAndUnknownHints(t *testing.T) {
	for _, name := range []string{"other-run", "other-instance", "unknown-owner", "expired-own"} {
		t.Run(name, func(t *testing.T) {
			s, agent := admissionResetFixture(t)
			s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: 30 * time.Second, reason: "busy"}, admissionResetRunA)
			s.state.mu.Lock()
			switch name {
			case "other-run":
				s.state.agentAdmissionOwners[agent.ID] = agentAdmissionOwner{RunID: "another-run", StartedAt: agent.StartedAt}
			case "other-instance":
				current := s.state.agents[agent.ID]
				current.StartedAt = current.StartedAt.Add(time.Second)
				s.state.agents[agent.ID] = current
			case "unknown-owner":
				delete(s.state.agentAdmissionOwners, agent.ID)
			case "expired-own":
				s.state.agentAdmissionUntil[agent.ID] = time.Now().Add(-time.Second)
			}
			s.state.mu.Unlock()
			if err := s.retireRunRuntime(t.Context(), admissionResetRunA); err != nil {
				t.Fatal(err)
			}
			s.state.mu.RLock()
			_, hint := s.state.agentAdmissionUntil[agent.ID]
			_, owner := s.state.agentAdmissionOwners[agent.ID]
			s.state.mu.RUnlock()
			if name == "expired-own" {
				if hint || owner {
					t.Fatal("expired owned hint was retained")
				}
			} else if !hint {
				t.Fatal("reset cleared another or unverifiable admission hint")
			}
		})
	}
}

func TestAgentAdmissionOwnershipFollowsOnlyTheExtendedDeadlineAndAgentRestart(t *testing.T) {
	s, agent := admissionResetFixture(t)
	s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: 30 * time.Second, reason: "long"}, admissionResetRunA)
	s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: time.Second, reason: "short"}, admissionResetRunB)
	s.state.mu.RLock()
	owner := s.state.agentAdmissionOwners[agent.ID]
	s.state.mu.RUnlock()
	if owner.RunID != admissionResetRunA {
		t.Fatal("a shorter hint stole the maximum deadline")
	}
	s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: time.Minute, reason: "new long"}, admissionResetRunB)
	if err := s.retireRunRuntime(t.Context(), admissionResetRunA); err != nil {
		t.Fatal(err)
	}
	s.state.mu.Lock()
	owner = s.state.agentAdmissionOwners[agent.ID]
	current := s.state.agents[agent.ID]
	current.State = model.AgentOffline
	s.state.agents[agent.ID] = current
	s.state.mu.Unlock()
	if owner.RunID != admissionResetRunB {
		t.Fatal("reset cleared a newer Run's longer hint")
	}
	replacement := agent
	replacement.StartedAt = agent.StartedAt.Add(time.Minute)
	replacement.LastSeen = time.Now().UTC()
	if _, err := s.state.registerAgent(replacement); err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	_, hint := s.state.agentAdmissionUntil[agent.ID]
	_, owned := s.state.agentAdmissionOwners[agent.ID]
	s.state.mu.RUnlock()
	if hint || owned {
		t.Fatal("Agent replacement inherited its predecessor's hint")
	}
}

func TestAgentAdmissionFailedResetAndPhysicalHoldsRemainIntact(t *testing.T) {
	for _, name := range []string{"reservation", "uncertain", "physical-hold"} {
		t.Run(name, func(t *testing.T) {
			s, agent := admissionResetFixture(t)
			s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: 30 * time.Second, reason: "busy"}, admissionResetRunA)
			s.state.mu.Lock()
			switch name {
			case "reservation":
				s.state.reservations[admissionResetRunA+"-pending"] = agent.ID
			case "uncertain":
				s.state.uncertainCreates = map[string]uncertainCreate{"pending": {runID: admissionResetRunA, agentID: agent.ID, generation: 1}}
			case "physical-hold":
				s.state.hostRebootFences["admission-host"] = hostRebootFence{OperationID: "reboot-physical-hold", BootID: "boot", AgentStartedAt: agent.StartedAt, RequestedAt: time.Now()}
			}
			s.state.mu.Unlock()
			if name == "physical-hold" {
				if err := s.state.setHostDisabled("admission-host", true); err != nil {
					t.Fatal(err)
				}
			}
			err := s.retireRunRuntime(t.Context(), admissionResetRunA)
			s.state.mu.RLock()
			_, hint := s.state.agentAdmissionUntil[agent.ID]
			reservation := s.state.reservations[admissionResetRunA+"-pending"]
			uncertain := len(s.state.uncertainCreates)
			fences := len(s.state.hostRebootFences)
			disabled := s.state.agents[agent.ID].Disabled
			s.state.mu.RUnlock()
			if name == "physical-hold" {
				if err != nil || hint || fences != 1 || !disabled {
					t.Fatalf("physical fence/availability was changed: err=%v hint=%v fences=%d disabled=%v", err, hint, fences, disabled)
				}
			} else if err == nil || !hint || name == "reservation" && reservation == "" || name == "uncertain" && uncertain != 1 {
				t.Fatalf("failed reset erased evidence: %v hint=%v reservation=%q uncertain=%d", err, hint, reservation, uncertain)
			}
		})
	}
}

func TestAgentAdmissionAfterVerifiedResetStillRechecksActualBusyAgent(t *testing.T) {
	s, agent := admissionResetFixture(t)
	var attempts atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusServiceUnavailable, "previous Run reset is not complete")
	}))
	defer api.Close()
	agent.URL = api.URL
	s.state.mu.Lock()
	s.state.agents[agent.ID] = agent
	s.state.mu.Unlock()
	s.deferAgentAdmission(agent, &agentAdmissionDeferredError{after: 30 * time.Second, reason: "old reset"}, admissionResetRunA)
	if err := s.retireRunRuntime(t.Context(), admissionResetRunA); err != nil {
		t.Fatal(err)
	}
	run, _ := resultFixture(t, s, admissionResetRunB, "running", time.Now().UTC())
	s.state.mu.Lock()
	s.state.experiments[run.ID] = run
	s.state.mu.Unlock()
	request := model.CreateNodeRequest{ID: admissionResetRunB + "-peer", RunID: admissionResetRunB, Generation: 1}
	reserved, ok := s.tryReserveAgent(request.ID)
	if !ok {
		t.Fatal("next Run could not make its first admission request")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.createReservedNode(ctx, request, reserved, "", nil) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.state.mu.RLock()
		owner := s.state.agentAdmissionOwners[agent.ID]
		s.state.mu.RUnlock()
		if owner.RunID == admissionResetRunB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual busy response did not become a new Run-owned hint")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected canceled retry result: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("busy Agent was repeatedly called: %d attempts", attempts.Load())
	}
	if _, ok := s.tryReserveAgent(admissionResetRunB + "-again"); ok {
		t.Fatal("actual busy response was ignored")
	}
	if err := s.retireRunRuntime(t.Context(), admissionResetRunA); err != nil {
		t.Fatal(err)
	}
	s.state.mu.RLock()
	owner := s.state.agentAdmissionOwners[agent.ID]
	until := s.state.agentAdmissionUntil[agent.ID]
	s.state.mu.RUnlock()
	if owner.RunID != admissionResetRunB || !time.Now().Before(until) {
		t.Fatal("old Run cleanup cleared the new busy evidence")
	}
}
