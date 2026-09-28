package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type discoveryTransport func(*http.Request) (*http.Response, error)

func (f discoveryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Simulate task addresses while routing registration through the real protected
// Controller handler. No Docker daemon or live Swarm is used.
func discoveryFixture(t *testing.T, agents map[string]model.Agent) (*Server, *atomic.Int32) {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "discovery-test", AgentDiscoveryDNS: "tasks.lab_agent"}, nil)
	var posts atomic.Int32
	handler := s.Handler(context.Background())
	s.agentLookupIP = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		if host != "tasks.lab_agent" {
			t.Errorf("wrong task DNS name: %s", host)
		}
		var addresses []net.IPAddr
		for ip := range agents {
			addresses = append(addresses, net.IPAddr{IP: net.ParseIP(ip)}, net.IPAddr{IP: net.ParseIP(ip)})
		}
		return addresses, nil
	}
	s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		a, ok := agents[r.URL.Hostname()]
		if !ok {
			return nil, errors.New("old task address is unreachable")
		}
		if r.Header.Get("Authorization") != "Bearer "+s.config.Token {
			t.Error("missing internal credential")
		}
		if r.URL.Port() != "8090" {
			t.Errorf("wrong control port: %s", r.URL.Port())
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/registration/refresh":
			posts.Add(1)
			a.LastSeen = time.Now().UTC()
			body, _ := json.Marshal(a)
			registration := httptest.NewRequest(http.MethodPost, "/api/v1/agents/register", bytes.NewReader(body))
			registration.Header.Set("Authorization", r.Header.Get("Authorization"))
			registered := httptest.NewRecorder()
			handler.ServeHTTP(registered, registration.WithContext(r.Context()))
			if registered.Code != http.StatusCreated {
				return registered.Result(), nil
			}
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/status":
		default:
			t.Errorf("unexpected discovery operation: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return w.Result(), nil
		}
		a.LastSeen = time.Now().UTC()
		writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: a, Nodes: []model.Node{{ID: a.ID + "-peer", AgentID: a.ID, State: model.NodeReady}}})
		return w.Result(), nil
	})
	return s, &posts
}

func discoveryAgent(id, ip string) model.Agent {
	return model.Agent{ID: id, Name: id, URL: "http://" + net.JoinHostPort(ip, "8090"), Capacity: 20, DefaultCapacity: 20,
		StartedAt: time.Now().Add(-time.Hour), StartupReconciled: true}
}

func TestAgentDiscoveryRegistersMissingAgentsAndDoesNotReplayKnownAgents(t *testing.T) {
	s, posts := discoveryFixture(t, map[string]model.Agent{
		"192.0.2.1":   discoveryAgent("a", "192.0.2.1"),
		"2001:db8::2": discoveryAgent("b", "2001:db8::2"),
	})
	if old := s.refreshRegisteredAgents(context.Background()); old.Requested != 0 || old.Discovery != nil {
		t.Fatalf("ordinary refresh unexpectedly discovered unknown Agents: %+v", old)
	}
	result := s.refreshAgents(context.Background(), true)
	if result.Requested != 2 || result.Refreshed != 2 || len(result.Failures) != 0 || len(result.Agents) != 2 ||
		result.Discovery.Addresses != 2 || result.Discovery.Added != 2 || posts.Load() != 2 {
		t.Fatalf("missing Agent registration or duplicate DNS accounting: %+v discovery=%+v posts=%d", result, result.Discovery, posts.Load())
	}
	if len(s.state.nodes) != 2 || s.state.nodes["a-peer"].State != model.NodeReady {
		t.Fatalf("discovery lost Peer inventory: %+v", s.state.nodes)
	}
	result = s.refreshAgents(context.Background(), true)
	if result.Refreshed != 2 || result.Discovery.Added != 0 || posts.Load() != 2 {
		t.Fatalf("known Agents were re-registered or counted twice: %+v posts=%d", result, posts.Load())
	}
}

func TestAgentDiscoveryRecoversReplacementAddressAndKeepsSettings(t *testing.T) {
	current := discoveryAgent("a", "192.0.2.2")
	s, posts := discoveryFixture(t, map[string]model.Agent{"192.0.2.2": current})
	old := current
	old.URL, old.StartedAt = "http://192.0.2.1:8090", current.StartedAt.Add(-time.Hour)
	if _, err := s.state.registerAgent(old); err != nil {
		t.Fatal(err)
	}
	limit := 7
	capacityTestSet(t, s.state, old.ID, &limit)
	if _, _, err := s.state.setAgentEnabled(old.ID, false); err != nil {
		t.Fatal(err)
	}
	old = s.state.agents[old.ID]
	old.LastSeen, old.State = time.Now().Add(-time.Minute), model.AgentOffline
	s.state.agents[old.ID] = old
	result := s.refreshAgents(context.Background(), true)
	if result.Requested != 1 || result.Refreshed != 1 || len(result.Failures) != 0 || result.Discovery.Added != 0 || posts.Load() != 1 {
		t.Fatalf("failed old address obscured recovered instance: %+v", result)
	}
	a, _ := s.agent("a")
	if a.URL != current.URL || a.State != model.AgentOnline || !a.Disabled || a.CapacityOverride != 7 || !a.StartedAt.Equal(current.StartedAt) {
		t.Fatalf("replacement lost identity, URL, or settings: %+v", a)
	}
}

func TestAgentDiscoveryCannotReplaceLiveNewerInstance(t *testing.T) {
	candidate := discoveryAgent("a", "192.0.2.2")
	s, _ := discoveryFixture(t, map[string]model.Agent{"192.0.2.2": candidate})
	current := candidate
	current.URL, current.StartedAt = "http://192.0.2.1:8090", candidate.StartedAt.Add(time.Minute)
	if _, err := s.state.registerAgent(current); err != nil {
		t.Fatal(err)
	}
	result := s.refreshAgents(context.Background(), true)
	if result.Refreshed != 0 || len(result.Failures) != 1 || result.Discovery.Added != 0 {
		t.Fatalf("unexpected replacement success: %+v", result)
	}
	a, _ := s.agent("a")
	if a.URL != current.URL || !a.StartedAt.Equal(current.StartedAt) {
		t.Fatalf("discovery replaced a live newer Agent: %+v", a)
	}
}

func TestAgentDiscoveryRequiresRegistrationWithThisController(t *testing.T) {
	a := discoveryAgent("wrong-controller", "192.0.2.1")
	s, _ := discoveryFixture(t, map[string]model.Agent{"192.0.2.1": a})
	s.client.Transport = discoveryTransport(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: a})
		return w.Result(), nil
	})
	result := s.refreshAgents(context.Background(), true)
	if result.Refreshed != 0 || len(result.Agents) != 0 || len(result.Failures) != 1 || !strings.Contains(result.Failures[0].Error, "did not register with this Controller") {
		t.Fatalf("unauthenticated status was accepted as registration: %+v", result)
	}
}

func TestAgentDiscoveryDNSFailureStillRefreshesKnownAgents(t *testing.T) {
	a := discoveryAgent("a", "192.0.2.1")
	s, _ := discoveryFixture(t, map[string]model.Agent{"192.0.2.1": a})
	if _, err := s.state.registerAgent(a); err != nil {
		t.Fatal(err)
	}
	s.agentLookupIP = func(context.Context, string) ([]net.IPAddr, error) { return nil, errors.New("DNS unavailable") }
	result := s.refreshAgents(context.Background(), true)
	if result.Refreshed != 1 || len(result.Failures) != 0 || !strings.Contains(result.Discovery.Error, "DNS unavailable") {
		t.Fatalf("DNS failure suppressed known Agent refresh: %+v", result)
	}
	s.config.AgentDiscoveryDNS = ""
	result = s.refreshAgents(context.Background(), true)
	if result.Refreshed != 1 || result.Discovery.Enabled || result.Discovery.Error == "" {
		t.Fatalf("missing configuration was silently ignored: %+v", result)
	}
}

func TestAgentDiscoveryLimitsAddressesAndHonorsCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AgentDiscoveryDNS: "tasks.lab_agent"}, nil)
	s.agentLookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		ips := []net.IPAddr{{IP: net.IPv4zero}, {IP: net.ParseIP("ff02::1")}}
		for i := 0; i < agentDiscoveryLimit+10; i++ {
			ips = append(ips, net.IPAddr{IP: net.ParseIP(fmt.Sprintf("2001:db8::%x", i+1))})
		}
		return ips, nil
	}
	targets, result := s.discoverAgentTargets(context.Background())
	if len(targets) != agentDiscoveryLimit || result.Addresses != agentDiscoveryLimit+10 || result.Error == "" {
		t.Fatalf("unbounded or invalid discovery targets: %d %+v", len(targets), result)
	}
	s.agentLookupIP = func(ctx context.Context, _ string) ([]net.IPAddr, error) { <-ctx.Done(); return nil, ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	targets, result = s.discoverAgentTargets(ctx)
	if len(targets) != 0 || !strings.Contains(result.Error, "canceled") {
		t.Fatalf("discovery ignored cancellation: %+v", result)
	}
}

func TestAgentDiscoveryEndpointRequiresSessionAndValidOption(t *testing.T) {
	s, _ := discoveryFixture(t, nil)
	handler := s.Handler(context.Background())
	cookie := loginCookie(t, s)
	for _, tc := range []struct {
		query string
		login bool
		want  int
	}{
		{"true", false, 401}, {"invalid", true, 400}, {"true", true, 200},
	} {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/agents/refresh?discover="+tc.query, nil)
		if tc.login {
			r.AddCookie(cookie)
			r.Header.Set("X-KPL-Request", "dashboard")
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("query=%s login=%v: status=%d body=%s", tc.query, tc.login, w.Code, w.Body)
		}
		if tc.want == 200 && !strings.Contains(w.Body.String(), `"discovery":`) {
			t.Fatalf("missing discovery report: %s", w.Body)
		}
	}
}

func TestAgentDiscoveryRecoversTaskIPReassignedToAnotherAgent(t *testing.T) {
	current := discoveryAgent("new-host", "192.0.2.1")
	s, posts := discoveryFixture(t, map[string]model.Agent{"192.0.2.1": current})
	old := discoveryAgent("old-host", "192.0.2.1")
	if _, err := s.state.registerAgent(old); err != nil {
		t.Fatal(err)
	}
	old = s.state.agents[old.ID]
	old.LastSeen, old.State = time.Now().Add(-time.Minute), model.AgentOffline
	s.state.agents[old.ID] = old
	result := s.refreshAgents(context.Background(), true)
	if result.Refreshed != 1 || result.Discovery.Added != 1 || posts.Load() != 1 || len(result.Failures) != 0 {
		t.Fatalf("reassigned task address blocked discovery: %+v", result)
	}
	if previous, _ := s.agent(old.ID); previous.State != model.AgentOffline || !previous.LastSeen.Equal(old.LastSeen) {
		t.Fatalf("reassigned address revived the old identity: %+v", previous)
	}
}
