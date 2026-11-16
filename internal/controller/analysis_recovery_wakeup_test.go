package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAnalysisQueuedBeforeRecoveryUsesRecoveredFrozenSlot(t *testing.T) {
	for _, mode := range []string{"enable", "heartbeat", "single-registration"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := isolatedAnalysisServer(t)
			s.config.ControlNodeID = "control-node"
			s.config.AnalysisAgentToken = remoteTestToken
			remote, commands := isolatedAnalysisServer(t)
			original := remote.workerCommand
			started := make(chan struct{}, 1)
			remote.workerCommand = func(kind string) *exec.Cmd { cmd := original(kind); started <- struct{}{}; return cmd }
			endpoint := httptest.NewServer(newRemoteAnalysisWorker(remoteTestToken, s.logger, remote.workerCommand))
			defer endpoint.Close()
			agent := analysisPoolAgent("registered-agent", "agent-host", endpoint.URL)
			registerAnalysisPoolAgent(t, s, agent)
			batchFixture(t, s, "batch", "run", "completed", 1, 1, 1)
			local := queueTestEnqueue(t, s, "analysis")
			queueTestAwait(t, local.entered, "Controller slot held while eligible remote snapshot is captured")
			if mode == "enable" {
				setEnabledTest(t, s.state, agent.ID, false)
			} else {
				s.state.mu.Lock()
				a := s.state.agents[agent.ID]
				a.State = model.AgentOffline
				s.state.agents[agent.ID] = a
				s.state.mu.Unlock()
			}
			if _, err := s.startAnalysisJob(t.Context(), "run", false); err != nil {
				t.Fatal(err)
			}
			control := httptest.NewServer(s.apiTestHandler(t.Context()))
			defer control.Close()
			method, path, body, expected := http.MethodPut, "/api/v1/agents/"+agent.ID+"/enabled", []byte(`{"enabled":true}`), http.StatusOK
			if mode != "enable" {
				agent.LastSeen = time.Now().UTC()
				method = http.MethodPost
				path = "/api/v1/agents/heartbeat"
				body, _ = json.Marshal(model.AgentHeartbeat{Agent: agent})
				expected = http.StatusNoContent
				if mode == "single-registration" {
					path = "/api/v1/agents/register"
					body, _ = json.Marshal(agent)
					expected = http.StatusCreated
				}
			}
			req, err := http.NewRequestWithContext(t.Context(), method, control.URL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := control.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != expected {
				t.Fatalf("reenable failed: %d", response.StatusCode)
			}
			woke := false
			select {
			case <-started:
				woke = true
			case <-time.After(time.Second):
			}
			local.once.Do(func() { close(local.release) })
			status := awaitAnalysisJob(t, s, "run")
			s.analysisWorkers.Wait()
			if !woke || len(commands()) != 1 || status.State != "completed" {
				t.Fatalf("reenabled frozen slot waited for unrelated Controller completion: woke=%t remoteChildren=%d state=%s", woke, len(commands()), status.State)
			}
			checkAnalysisAdmissionReleased(t, s)
		})
	}
}

func countBlockedAnalysisWakes(s *Server) int {
	stack := make([]byte, 1<<20)
	count := 0
	for _, routine := range strings.Split(string(stack[:runtime.Stack(stack, true)]), "\n\n") {
		if strings.Contains(routine, "controller.(*Server).wakeAnalysisAgent.func1(") && strings.Contains(routine, fmt.Sprintf("%p", &s.cancelMu)) {
			count++
		}
	}
	return count
}

func TestAgentHeartbeatCoalescesContendedAnalysisWakeWithoutBlockingResponse(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovery", true: "shutdown"}[shutdown], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), ControlNodeID: "control-node"}, nil)
			s.config.AnalysisAgentToken = remoteTestToken
			a := analysisPoolAgent("agent-a", "host-a", "http://agent-a:8090")
			b := analysisPoolAgent("agent-b", "host-b", "http://agent-b:8090")
			registerAnalysisPoolAgent(t, s, a)
			registerAnalysisPoolAgent(t, s, b)
			local := queueTestEnqueue(t, s, "analysis")
			queueTestAwait(t, local.entered, "Controller slot")
			setEnabledTest(t, s.state, a.ID, false)
			setEnabledTest(t, s.state, b.ID, false)
			first := queueTestEnqueue(t, s, "analysis")
			second := queueTestEnqueue(t, s, "analysis")
			endpoint := httptest.NewServer(s.apiTestHandler(context.Background()))
			defer endpoint.Close()
			s.cancelMu.Lock()
			locked := true
			defer func() {
				if locked {
					s.cancelMu.Unlock()
				}
			}()
			setEnabledTest(t, s.state, a.ID, true)
			setEnabledTest(t, s.state, b.ID, true)
			var requests sync.WaitGroup
			failures := make(chan error, 20)
			for i := 0; i < 20; i++ {
				requests.Add(1)
				go func(i int) {
					defer requests.Done()
					agent := a
					if i%2 != 0 {
						agent = b
					}
					agent.LastSeen = time.Now().UTC()
					raw, _ := json.Marshal(model.AgentHeartbeat{Agent: agent})
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					defer cancel()
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL+"/api/v1/agents/heartbeat", bytes.NewReader(raw))
					req.Header.Set("Content-Type", "application/json")
					response, err := endpoint.Client().Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, response.Body)
						_ = response.Body.Close()
						if response.StatusCode != 204 {
							err = fmt.Errorf("heartbeat status %d", response.StatusCode)
						}
					}
					if err != nil {
						failures <- err
					}
				}(i)
			}
			requests.Wait()
			close(failures)
			for err := range failures {
				if err != nil {
					t.Fatalf("heartbeat waited for unrelated scheduler ownership: %v", err)
				}
			}
			deadline := time.Now().Add(time.Second)
			for countBlockedAnalysisWakes(s) == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if count := countBlockedAnalysisWakes(s); count != 1 {
				t.Fatalf("contended heartbeat burst did not retain one coalesced wake: %d", count)
			}
			if shutdown {
				s.shuttingDown = true
			}
			s.cancelMu.Unlock()
			locked = false
			if shutdown {
				select {
				case <-first.entered:
					t.Fatal("coalesced wake admitted new work after shutdown")
				case <-second.entered:
					t.Fatal("coalesced wake admitted second work after shutdown")
				case <-time.After(50 * time.Millisecond):
				}
				first.slot.cancel()
				second.slot.cancel()
			} else {
				queueTestAwait(t, first.entered, "first recovered frozen Agent")
				queueTestAwait(t, second.entered, "second coalesced recovered Agent")
				if first.slot.analysisWorkerURL == second.slot.analysisWorkerURL || first.slot.analysisWorkerURL == "" || second.slot.analysisWorkerURL == "" {
					t.Fatal("coalesced recovery duplicated a host or Controller slot")
				}
			}
			for _, work := range []*queueTestWork{first, second, local} {
				work.once.Do(func() { close(work.release) })
			}
			for countBlockedAnalysisWakes(s) != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if count := countBlockedAnalysisWakes(s); count != 0 {
				t.Fatalf("completed wake retained callback goroutines: %d", count)
			}
		})
	}
}
