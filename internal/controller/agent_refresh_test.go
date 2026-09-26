package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func addRefreshTestAgent(t *testing.T, s *Server, id string, handler func(http.ResponseWriter, *http.Request, model.Agent)) {
	t.Helper()
	agent := model.Agent{ID: id, Name: id, Capacity: 10, StartedAt: time.Now().Add(-time.Hour)}
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/status" {
			t.Errorf("refresh made an unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		reported := agent
		reported.LastSeen = time.Now().UTC()
		handler(w, r, reported)
	}))
	t.Cleanup(endpoint.Close)
	agent.URL = endpoint.URL
	if _, err := s.state.registerAgent(agent); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRefreshUpdatesOfflineAgentsAndPreservesPartialFailures(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, id := range []string{"online", "offline", "unreachable", "wrong-identity"} {
		addRefreshTestAgent(t, s, id, func(w http.ResponseWriter, r *http.Request, agent model.Agent) {
			switch agent.ID {
			case "unreachable":
				http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
				return
			case "wrong-identity":
				agent.ID = "online"
			}
			agent.ActiveNodes = 1
			writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{{ID: agent.Name + "-peer", AgentID: agent.ID, State: model.NodeReady}}})
		})
		if id != "online" {
			agent := s.state.agents[id]
			agent.State, agent.LastSeen = model.AgentOffline, time.Now().Add(-time.Minute)
			s.state.agents[id] = agent
		}
	}
	response := s.refreshRegisteredAgents(context.Background())
	if response.Requested != 4 || response.Refreshed != 2 || len(response.Failures) != 2 || len(response.Agents) != 4 {
		t.Fatalf("incorrect partial refresh: %+v", response)
	}
	if s.state.agents["offline"].State != model.AgentOnline || s.state.agents["unreachable"].State != model.AgentOffline {
		t.Fatalf("offline recovery or failed Agent retention was lost: %+v", response.Agents)
	}
	if len(s.state.nodes) != 2 || s.state.nodes["offline-peer"].State != model.NodeReady || s.state.agents["online"].ActiveNodes != 1 {
		t.Fatalf("Peer inventory not refreshed, or wrong identity was accepted: %+v", s.state.nodes)
	}
	for _, failure := range response.Failures {
		if (failure.ID != "unreachable" && failure.ID != "wrong-identity") || failure.Error == "" {
			t.Fatalf("lost failure identity/reason: %+v", failure)
		}
	}
}

func TestAgentRefreshBoundsConcurrencyAndCancelsPendingChecks(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	started := make(chan struct{}, 8)
	var active, peak atomic.Int32
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		addRefreshTestAgent(t, s, id, func(w http.ResponseWriter, r *http.Request, agent model.Agent) {
			n := active.Add(1)
			defer active.Add(-1)
			for previous := peak.Load(); n > previous; previous = peak.Load() {
				if peak.CompareAndSwap(previous, n) {
					break
				}
			}
			started <- struct{}{}
			<-r.Context().Done()
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan agentRefreshResponse, 1)
	go func() { done <- s.refreshRegisteredAgents(ctx) }()
	for range 4 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not check Agents concurrently")
		}
	}
	cancel()
	select {
	case result := <-done:
		if result.Refreshed != 0 || len(result.Failures) != 6 || peak.Load() != 4 {
			t.Fatalf("cancellation or concurrency limit failed: %+v, peak=%d", result, peak.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh ignored request cancellation")
	}
}

func TestAgentRefreshRequiresDashboardSessionAndRejectsDuplicateRequests(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "refresh-test"}, nil)
	handler := s.Handler(context.Background())
	cookie := loginCookie(t, s)
	for _, tc := range []struct {
		name, method        string
		login, header, busy bool
		want                int
	}{
		{"anonymous", http.MethodPost, false, false, false, 401},
		{"missing dashboard header", http.MethodPost, true, false, false, 403},
		{"wrong method", http.MethodGet, true, true, false, 405},
		{"in progress", http.MethodPost, true, true, true, 409},
		{"empty inventory", http.MethodPost, true, true, false, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "/api/v1/agents/refresh", nil)
			if tc.login {
				r.AddCookie(cookie)
			}
			if tc.header {
				r.Header.Set("X-KPL-Request", "dashboard")
			}
			if tc.busy {
				s.agentRefreshMu.Lock()
				defer s.agentRefreshMu.Unlock()
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.want == http.StatusOK {
				var result agentRefreshResponse
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Requested != 0 || result.Agents == nil || result.Failures == nil {
					t.Fatalf("invalid empty response: %s (%v)", w.Body, err)
				}
			}
		})
	}
}
