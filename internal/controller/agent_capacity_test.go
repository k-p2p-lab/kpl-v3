package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func capacityTestAgent(t *testing.T, s *state, id string, capacity int) model.Agent {
	t.Helper()
	agent, err := s.registerAgent(model.Agent{ID: id, URL: "http://agent", Capacity: capacity, DefaultCapacity: capacity})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}
func capacityTestSet(t *testing.T, s *state, id string, capacity *int) model.Agent {
	t.Helper()
	agent, status, err := s.setAgentCapacity(id, capacity)
	if err != nil || status != 200 {
		t.Fatalf("set capacity: status=%d error=%v", status, err)
	}
	return agent
}
func capacityTestHeartbeat(t *testing.T, s *state, agent model.Agent, capacity int, revision string) model.Agent {
	t.Helper()
	agent.Capacity, agent.CapacityRevision, agent.LastSeen = capacity, revision, time.Now().UTC()
	if err := s.heartbeat(model.AgentHeartbeat{Agent: agent, Partial: true}); err != nil {
		t.Fatal(err)
	}
	return s.agents[agent.ID]
}

func TestAgentCapacityPersistsByIDAndResetFollowsNewCLIDefault(t *testing.T) {
	dir := t.TempDir()
	s := newState(dir)
	capacityTestAgent(t, s, "a", 200)
	capacityTestAgent(t, s, "b", 200)
	limit := 100
	a := capacityTestSet(t, s, "a", &limit)
	if a.Capacity != 100 || a.CapacityOverride != 100 || !a.CapacityPending || s.agents["b"].Capacity != 200 {
		t.Fatalf("wrong per-Agent override: %+v", a)
	}
	a = capacityTestHeartbeat(t, s, a, 100, s.agentCapacityRevisions["a"])
	if a.CapacityPending || a.DefaultCapacity != 200 {
		t.Fatalf("not acknowledged: %+v", a)
	}
	// A fresh Controller and restarted Agent use the stored override even with a new CLI default.
	s = newState(dir)
	a = capacityTestAgent(t, s, "a", 300)
	if a.Capacity != 100 || a.DefaultCapacity != 300 || a.CapacityOverride != 100 {
		t.Fatalf("lost override: %+v", a)
	}
	a = capacityTestSet(t, s, "a", nil)
	if a.Capacity != 100 || a.CapacityOverride != 0 || !a.CapacityPending {
		t.Fatalf("reset raised capacity before ack: %+v", a)
	}
	a = capacityTestHeartbeat(t, s, a, 300, s.agentCapacityRevisions["a"])
	if a.Capacity != 300 || a.CapacityPending {
		t.Fatalf("reset did not restore default: %+v", a)
	}
	s = newState(dir)
	a = capacityTestAgent(t, s, "a", 400)
	if a.CapacityOverride != 0 || a.Capacity != 400 {
		t.Fatalf("reset not persisted: %+v", a)
	}
	info, err := os.Stat(filepath.Join(dir, agentCapacitiesFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("settings permissions: %v %v", info, err)
	}
}

func TestAgentCapacityEditsRequireCurrentAcknowledgmentAndKeepOccupiedPeers(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s := server.state
	a := capacityTestAgent(t, s, "a", 200)
	a.ActiveNodes = 150
	a = capacityTestHeartbeat(t, s, a, 200, s.agentCapacityRevisions["a"])
	old := a
	low, high := 100, 300
	a = capacityTestSet(t, s, "a", &low)
	lowRevision := s.agentCapacityRevisions["a"]
	if a.ActiveNodes != 150 {
		t.Fatal("capacity edit changed occupancy")
	}
	if _, ok := server.tryReserveAgent("too-many"); ok {
		t.Fatal("reduction allowed another Peer")
	}
	a = capacityTestSet(t, s, "a", &high)
	a = capacityTestHeartbeat(t, s, old, 200, old.CapacityRevision)
	if a.Capacity != 100 || !a.CapacityPending {
		t.Fatalf("old report raised admission: %+v", a)
	}
	a = capacityTestHeartbeat(t, s, old, 100, lowRevision)
	if a.Capacity != 100 || !a.CapacityPending {
		t.Fatalf("old settings acknowledged new edit: %+v", a)
	}
	a = capacityTestHeartbeat(t, s, old, 300, s.agentCapacityRevisions["a"])
	if a.Capacity != 300 || a.CapacityPending {
		t.Fatalf("latest settings not acknowledged: %+v", a)
	}
	if _, ok := server.tryReserveAgent("available"); !ok {
		t.Fatal("acknowledged increase not usable")
	}
}

func TestAgentCapacityConcurrentOfflineEditsAndFailedPersistence(t *testing.T) {
	s := newState(t.TempDir())
	for _, id := range []string{"a", "b"} {
		a := capacityTestAgent(t, s, id, 200)
		a.State = model.AgentOffline
		s.agents[id] = a
	}
	var workers sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		workers.Add(1)
		go func(id string) {
			defer workers.Done()
			limit := 100
			if _, _, err := s.setAgentCapacity(id, &limit); err != nil {
				t.Error(err)
			}
		}(id)
	}
	workers.Wait()
	restored := newState(s.dataDir)
	if len(restored.agentCapacityOverrides) != 2 {
		t.Fatal("concurrent save lost another Agent's override")
	}
	path := filepath.Join(s.dataDir, agentCapacitiesFile)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	limit := 50
	if _, status, err := s.setAgentCapacity("a", &limit); err == nil || status != 500 {
		t.Fatal("write failure reported success")
	}
	if s.agents["a"].Capacity != 100 || s.agentCapacityOverrides["a"] != 100 {
		t.Fatal("failed save changed live capacity")
	}
}

func TestInvalidSavedAgentCapacityFailsClosed(t *testing.T) {
	for _, data := range []string{`{`, `null`, `{"a":0}`, `{"a":-1}`, `{"":100}`} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, agentCapacitiesFile), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
		s := newState(dir)
		if s.agentSettingsErr == nil {
			t.Fatalf("accepted invalid saved settings %s", data)
		}
		if _, err := s.registerAgent(model.Agent{ID: "a", URL: "http://agent", Capacity: 200}); err == nil {
			t.Fatal("ignored corrupt saved settings")
		}
	}
}

func TestAgentCapacityAPIValidationAndSessionProtection(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	capacityTestAgent(t, server.state, "a", 200)
	h := server.Handler(context.Background())
	cookie := loginCookie(t, server)
	request := func(body string, loggedIn, marker bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("PUT", "/api/v1/agents/a/capacity", strings.NewReader(body))
		if loggedIn {
			req.AddCookie(cookie)
		} else {
			req.Header.Set("Authorization", "Bearer "+server.config.Token)
		}
		if marker {
			req.Header.Set("X-KPL-Request", "dashboard")
		}
		result := httptest.NewRecorder()
		h.ServeHTTP(result, req)
		return result
	}
	if got := request(`{"capacity":100}`, false, true); got.Code != 401 {
		t.Fatalf("internal token could edit settings: %d", got.Code)
	}
	if got := request(`{"capacity":100}`, true, false); got.Code != 403 {
		t.Fatalf("missing origin marker accepted: %d", got.Code)
	}
	for _, body := range []string{`{}`, `null`, `{"capacity":0}`, `{"capacity":-1}`, `{"capacity":1.5}`, `{"capacity":"100"}`, `{"capacity":100,"unknown":1}`, `{"capacity":100} {}`} {
		if got := request(body, true, true); got.Code != 400 {
			t.Fatalf("accepted %s: %d %s", body, got.Code, got.Body)
		}
	}
	for _, body := range []string{`{"capacity":100}`, `{"capacity":null}`} {
		if got := request(body, true, true); got.Code != 200 {
			t.Fatalf("valid edit failed: %d %s", got.Code, got.Body)
		}
	}
	limit := 100
	if _, status, _ := server.state.setAgentCapacity("missing", &limit); status != 404 {
		t.Fatal("missing Agent not rejected")
	}
	if _, err := server.state.registerAgent(model.Agent{ID: "old", URL: "http://old", Capacity: 200}); err != nil {
		t.Fatal(err)
	}
	if _, status, _ := server.state.setAgentCapacity("old", &limit); status != 409 {
		t.Fatal("unsupported Agent accepted an override")
	}
}

func TestAgentCapacityRejectionRetriesWithoutFailingTheJoin(t *testing.T) {
	for _, cancelAfterRejection := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelAfterRejection), func(t *testing.T) {
			var attempts atomic.Int32
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts.Add(1) == 1 {
					w.WriteHeader(http.StatusTooManyRequests)
					if cancelAfterRejection {
						cancel()
					}
					return
				}
				var request model.CreateNodeRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				writeJSON(w, 201, model.Node{ID: request.ID, State: model.NodeStarting})
			}))
			defer endpoint.Close()
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			if _, err := server.state.registerAgent(model.Agent{ID: "a", URL: endpoint.URL, Capacity: 2}); err != nil {
				t.Fatal(err)
			}
			agent, ok := server.tryReserveAgent("node")
			if !ok {
				t.Fatal("reservation failed")
			}
			err := server.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers"}, agent, "a", nil)
			if cancelAfterRejection {
				if err == nil || len(server.state.reservations) != 0 || server.state.agents["a"].ActiveNodes != 0 {
					t.Fatalf("cancel leaked reservation: %v", err)
				}
			} else if err != nil || attempts.Load() != 2 || server.state.nodes["node"].ID != "node" || server.state.agents["a"].ActiveNodes != 1 {
				t.Fatalf("join retry failed: %v attempts=%d", err, attempts.Load())
			}
		})
	}
}
