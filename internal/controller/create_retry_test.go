package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	agentserver "github.com/k-p2p-lab/kpl-v3/internal/agent"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func dropCreateResponse(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	_ = conn.Close()
}

func reserveReplayAgent(t *testing.T, s *Server, endpoint string, replay bool) model.Agent {
	t.Helper()
	_, err := s.state.registerAgent(model.Agent{ID: "a", URL: endpoint, Capacity: 1, StartedAt: time.Now().UTC(), CreateReplay: replay})
	if err != nil {
		t.Fatal(err)
	}
	agent, ok := s.tryReserveAgent("node")
	if !ok {
		t.Fatal("reservation failed")
	}
	return agent
}

func TestCreateRetryRecoversEOFAndTruncatedAcknowledgement(t *testing.T) {
	for _, mode := range []string{"eof", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			s := newLifecycleTestController(t)
			var calls atomic.Int32
			var first []byte
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if calls.Add(1) == 1 {
					first = body
					if mode == "eof" {
						dropCreateResponse(t, w)
					} else {
						w.Header().Set("Content-Length", "1000")
						w.WriteHeader(http.StatusCreated)
						_, _ = w.Write([]byte(`{"id":`))
					}
					return
				}
				if !bytes.Equal(first, body) {
					t.Error("recovery changed the create request")
				}
				s.state.mu.RLock()
				reserved, occupied := s.state.reservations["node"], s.state.agents["a"].ActiveNodes
				s.state.mu.RUnlock()
				if reserved != "a" || occupied != 1 {
					t.Error("uncertain creation released capacity during recovery")
				}
				var request model.CreateNodeRequest
				_ = json.Unmarshal(body, &request)
				if request.AgentStartedAt.IsZero() {
					t.Error("missing Agent instance fence")
				}
				writeJSON(w, http.StatusCreated, model.Node{ID: request.ID, RunID: request.RunID, Group: request.Group, Generation: request.Generation, AgentID: "a", State: model.NodeStarting})
			}))
			defer endpoint.Close()
			agent := reserveReplayAgent(t, s, endpoint.URL, true)
			err := s.createReservedNode(t.Context(), model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2, Seed: 33, Lifetime: "1s"}, agent, "a", nil)
			if err != nil || calls.Load() != 2 {
				t.Fatalf("calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

func TestCreateRetryFailureRetainsCapacityUntilConfirmedCleanup(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "supported"}[replay], func(t *testing.T) {
			s := newLifecycleTestController(t)
			var calls atomic.Int32
			var cleanup atomic.Bool
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					if !cleanup.Load() {
						http.Error(w, "cleanup pending", http.StatusConflict)
						return
					}
					w.WriteHeader(http.StatusAccepted)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				dropCreateResponse(t, w)
			}))
			defer endpoint.Close()
			agent := reserveReplayAgent(t, s, endpoint.URL, replay)
			err := s.createReservedNode(t.Context(), model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2}, agent, "a", nil)
			var uncertain *uncertainNodeCreateError
			wantCalls := int32(1)
			if replay {
				wantCalls = 3
			}
			if !errors.As(err, &uncertain) || calls.Load() != wantCalls || s.state.reservations["node"] != "a" {
				t.Fatalf("calls=%d reservations=%v error=%v", calls.Load(), s.state.reservations, err)
			}
			if _, ok := s.tryReserveAgent("other"); ok {
				t.Fatal("unconfirmed create slot was reused")
			}
			if err := s.state.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: agent.StartedAt}}); err != nil {
				t.Fatal(err)
			}
			if s.state.reservations["node"] != "a" || s.state.agents["a"].ActiveNodes != 1 {
				t.Fatal("empty inventory freed a potentially delayed create")
			}
			if err := s.stopRunGeneration(t.Context(), "run", 2); err == nil || len(s.state.uncertainCreates) != 1 {
				t.Fatalf("failed cleanup released uncertainty: %v", err)
			}
			cleanup.Store(true)
			if err := s.stopRunGeneration(t.Context(), "run", 1); err != nil || len(s.state.reservations) != 1 {
				t.Fatalf("older generation freed reservation: %v", err)
			}
			if err := s.stopRunGeneration(t.Context(), "run", 2); err != nil {
				t.Fatal(err)
			}
			if len(s.state.uncertainCreates) != 0 || len(s.state.reservations) != 0 || s.state.agents["a"].ActiveNodes != 0 {
				t.Fatal("confirmed cleanup leaked capacity")
			}
		})
	}
}

func TestCreateRetryStopsOnCancellationOrAgentRestart(t *testing.T) {
	for _, mode := range []string{"cancel", "restart", "conflict", "bad-identity", "invalid-json"} {
		t.Run(mode, func(t *testing.T) {
			s := newLifecycleTestController(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var calls atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				calls.Add(1)
				switch mode {
				case "cancel":
					cancel()
				case "restart":
					s.state.mu.Lock()
					a := s.state.agents["a"]
					a.StartedAt = a.StartedAt.Add(time.Second)
					s.state.agents["a"] = a
					s.state.mu.Unlock()
				case "conflict":
					http.Error(w, "invalid config", http.StatusConflict)
					return
				case "bad-identity":
					writeJSON(w, http.StatusCreated, model.Node{ID: "another"})
					return
				case "invalid-json":
					_, _ = w.Write([]byte(`invalid`))
					return
				}
				dropCreateResponse(t, w)
			}))
			defer endpoint.Close()
			agent := reserveReplayAgent(t, s, endpoint.URL, true)
			_, err := s.createOnAgent(ctx, agent, model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers"})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("calls=%d error=%v", calls.Load(), err)
			}
		})
	}
}

func TestCreateRetryWithAgentHandlerAcrossLostResponse(t *testing.T) {
	for _, admitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before-admission", true: "after-admission"}[admitted], func(t *testing.T) {
			s := newLifecycleTestController(t)
			// The executable always fails; this integration exercises the HTTP
			// admission boundary without contacting a Docker daemon or starting a Peer.
			backend, err := agentserver.New(agentserver.Config{ID: "a", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DockerBinary: "/bin/false", DockerImage: "test", DockerNetwork: "test", DataDir: t.TempDir()}, s.logger)
			if err != nil {
				t.Fatal(err)
			}
			handler := backend.Handler()
			t.Cleanup(func() {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/runs/run/nodes?generation=0", nil))
				_ = backend.Close()
			})
			status := httptest.NewRecorder()
			handler.ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/v1/status", nil))
			var report model.AgentHeartbeat
			if err := json.Unmarshal(status.Body.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			var original model.Node
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					if admitted {
						ack := httptest.NewRecorder()
						handler.ServeHTTP(ack, r)
						if ack.Code != http.StatusCreated {
							t.Errorf("admission: %d %s", ack.Code, ack.Body)
						}
						_ = json.Unmarshal(ack.Body.Bytes(), &original)
					} else {
						_, _ = io.Copy(io.Discard, r.Body)
					}
					dropCreateResponse(t, w)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer endpoint.Close()
			report.Agent.URL = endpoint.URL
			if _, err := s.state.registerAgent(report.Agent); err != nil {
				t.Fatal(err)
			}
			agent, ok := s.tryReserveAgent("node")
			if !ok {
				t.Fatal("reserve Agent")
			}
			err = s.createReservedNode(t.Context(), model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Lifetime: "1ms"}, agent, "a", nil)
			if err != nil || calls.Load() != 2 {
				t.Fatalf("calls=%d error=%v", calls.Load(), err)
			}
			node := s.state.nodes["node"]
			if admitted && !node.StartedAt.Equal(original.StartedAt) {
				t.Fatal("response recovery changed Peer admission time")
			}
		})
	}
}
