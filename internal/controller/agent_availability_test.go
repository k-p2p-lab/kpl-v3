package controller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func setEnabledTest(t *testing.T, s *state, id string, enabled bool) model.Agent {
	t.Helper()
	a, status, err := s.setAgentEnabled(id, enabled)
	if err != nil || status != 200 {
		t.Fatalf("availability %d %v", status, err)
	}
	return a
}
func TestAgentEnabledPersistsAcrossReportsAndKeepsExistingPeers(t *testing.T) {
	dir := t.TempDir()
	s := New(ServerConfig{DataDir: dir}, nil)
	a := capacityTestAgent(t, s.state, "a", 100)
	capacityTestAgent(t, s.state, "b", 100)
	s.state.nodes["old"] = model.Node{ID: "old", AgentID: "a", State: model.NodeReady}
	disabled := setEnabledTest(t, s.state, "a", false)
	if !disabled.Disabled || disabled.State != model.AgentOnline {
		t.Fatal("disabled Agent disconnected")
	}
	if got := capacityTestHeartbeat(t, s.state, a, 100, a.CapacityRevision); !got.Disabled {
		t.Fatal("heartbeat undid disabled state")
	}
	if s.state.nodes["old"].State != model.NodeReady {
		t.Fatal("disable stopped existing Peer")
	}
	if len(s.state.dashboardSnapshot().Nodes) != 0 || len(s.state.snapshot().Nodes) != 1 {
		t.Fatal("dashboard visibility affected retained inventory")
	}
	for _, random := range []*rand.Rand{nil, rand.New(rand.NewSource(42))} {
		a, ok := s.tryReserveAgentWithPlacement("new", "", random)
		if !ok || a.ID != "b" {
			t.Fatal("scheduled disabled Agent")
		}
		s.releaseReservation("new")
	}
	if _, ok := s.tryReserveAgentWithPlacement("explicit", "a", nil); ok {
		t.Fatal("explicit placement bypassed disable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if id, err := s.selectBatchAgent(ctx, rand.New(rand.NewSource(42))); err != nil || id != "b" {
		t.Fatalf("single-agent %s %v", id, err)
	}
	restored := newState(dir)
	if !capacityTestAgent(t, restored, "a", 200).Disabled {
		t.Fatal("restart lost disable")
	}
	setEnabledTest(t, s.state, "a", true)
	if a, ok := s.tryReserveAgentWithPlacement("explicit", "a", nil); !ok || a.ID != "a" {
		t.Fatal("reenabled Agent unavailable")
	}
	if len(s.state.dashboardSnapshot().Nodes) != 1 {
		t.Fatal("reenabling lost existing topology")
	}
}
func TestAgentDisabledExcludedFromHardwareAndPrometheusDiscovery(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	for _, id := range []string{"a", "b"} {
		_, err := s.state.registerAgent(model.Agent{ID: id, URL: "http://agent", MetricsURL: "http://10.0.0.1:9092/metrics", LastSeen: now, Resources: resourceFixture(now, 2)})
		if err != nil {
			t.Fatal(err)
		}
	}
	setEnabledTest(t, s.state, "a", false)
	total := sumAgentResources(s.state.agentInventory(), time.Now())
	if total.TotalAgents != 1 || total.MeasuredAgents != 1 || total.CPUCores != 2 || total.MemoryWorkingSetBytes != 3072 {
		t.Fatalf("disabled resources aggregated: %+v", total)
	}
	families, err := s.state.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "kpl_agent_cpu_") || strings.HasPrefix(family.GetName(), "kpl_agent_memory_") || strings.HasPrefix(family.GetName(), "kpl_agent_resource_") {
			for _, metric := range family.Metric {
				for _, label := range metric.Label {
					if label.GetName() == "agent_id" && label.GetValue() == "a" {
						t.Fatalf("disabled metric: %s", family.GetName())
					}
				}
			}
		}
	}
	response := resultRequest(s, "GET", "/api/v1/prometheus/agent-targets")
	if strings.Contains(response.Body.String(), `"agent_id":"a"`) || !strings.Contains(response.Body.String(), `"agent_id":"b"`) {
		t.Fatalf("discovery: %s", response.Body)
	}
	response = resultRequest(s, "GET", "/api/v1/agents/resources?format=csv")
	rows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row[0] == "a" && (row[3] != "disabled" || row[5] != "" || row[7] != "") {
			t.Fatalf("disabled CSV: %v", row)
		}
	}
}
func TestAgentEnabledPersistenceFailureAndCorruptStartupFailClosed(t *testing.T) {
	dir := t.TempDir()
	s := newState(dir)
	capacityTestAgent(t, s, "a", 100)
	if err := os.Mkdir(filepath.Join(dir, agentAvailabilityFile), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.setAgentEnabled("a", false); err == nil || s.agents["a"].Disabled {
		t.Fatal("failed save mutated availability")
	}
	if err := os.Remove(filepath.Join(dir, agentAvailabilityFile)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, agentAvailabilityFile), []byte(`null`), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := newState(dir)
	if !capacityTestAgent(t, restarted, "a", 100).Disabled {
		t.Fatal("unreadable disable list silently enabled Agent")
	}
	if _, status, _ := restarted.setAgentEnabled("a", true); status != 503 {
		t.Fatal("corrupt settings overwritten")
	}
}
func TestAgentEnabledAPIValidationAuthenticationAndCSRF(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "availability-test"}, nil)
	capacityTestAgent(t, s.state, "a", 100)
	path := "/api/v1/agents/a/enabled"
	send := func(body string, cookie *http.Cookie, csrf bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("PUT", path, strings.NewReader(body))
		if cookie != nil {
			req.AddCookie(cookie)
		}
		if csrf {
			req.Header.Set("X-KPL-Request", "dashboard")
		}
		out := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(out, req)
		return out
	}
	if r := send(`{"enabled":false}`, nil, true); r.Code != 401 {
		t.Fatal("unauthenticated edit")
	}
	cookie := loginCookie(t, s)
	if r := send(`{"enabled":false}`, cookie, false); r.Code != 403 {
		t.Fatal("missing CSRF accepted")
	}
	for _, body := range []string{`{}`, `{"enabled":null}`, `{"enabled":1}`, `{"enabled":false,"unknown":true}`, `{"enabled":false} {}`} {
		if r := send(body, cookie, true); r.Code != 400 {
			t.Fatalf("invalid body accepted %s: %d", body, r.Code)
		}
	}
	r := send(`{"enabled":false}`, cookie, true)
	var a model.Agent
	if err := json.Unmarshal(r.Body.Bytes(), &a); err != nil || r.Code != 200 || !a.Disabled {
		t.Fatalf("disable %d %s", r.Code, r.Body)
	}
}

func TestAgentDisabledAfterReservationReroutesOrCancelsPinnedJoin(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprint(pinned), func(t *testing.T) {
			var disabledCalls, enabledCalls atomic.Int32
			disabledEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				disabledCalls.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer disabledEndpoint.Close()
			enabledEndpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				enabledCalls.Add(1)
				writeJSON(w, http.StatusCreated, model.Node{ID: "node", State: model.NodeStarting})
			}))
			defer enabledEndpoint.Close()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			for id, endpoint := range map[string]string{"a": disabledEndpoint.URL, "b": enabledEndpoint.URL} {
				if _, err := s.state.registerAgent(model.Agent{ID: id, URL: endpoint, Capacity: 2}); err != nil {
					t.Fatal(err)
				}
			}
			a, ok := s.tryReserveAgentWithPlacement("node", "a", nil)
			if !ok {
				t.Fatal("initial reservation failed")
			}
			setEnabledTest(t, s.state, "a", false)
			target := ""
			timeout := 2 * time.Second
			if pinned {
				target, timeout = "a", 50*time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err := s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers"}, a, target, nil)
			if disabledCalls.Load() != 0 || s.state.agents["a"].ActiveNodes != 0 || s.state.reservations["node"] == "a" {
				t.Fatalf("disabled dispatch or leaked reservation: %v", err)
			}
			if pinned {
				if !errors.Is(err, context.DeadlineExceeded) || enabledCalls.Load() != 0 || len(s.state.reservations) != 0 {
					t.Fatalf("pinned join failed to wait for reenable/cancellation: %v", err)
				}
			} else if err != nil || enabledCalls.Load() != 1 || s.state.nodes["node"].AgentID != "b" || s.state.agents["b"].ActiveNodes != 1 || s.state.reservations["node"] != "b" {
				t.Fatalf("join did not reroute to enabled Agent: %v", err)
			}
		})
	}
}
