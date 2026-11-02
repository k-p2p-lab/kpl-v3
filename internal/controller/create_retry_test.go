package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentserver "github.com/k-p2p-lab/kpl-v3/internal/agent"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
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

func TestCreateServerFailureRetainsCapacityUntilConfirmedCleanup(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, 599} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("status-%d/replay-%t", status, replay), func(t *testing.T) {
				s := newLifecycleTestController(t)
				var creates atomic.Int32
				var cleaned atomic.Bool
				endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodDelete {
						if !cleaned.Load() {
							http.Error(w, "container removal pending", http.StatusConflict)
							return
						}
						w.WriteHeader(http.StatusAccepted)
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					creates.Add(1) // Admission may complete before an intermediary returns 5xx.
					w.Header().Set("Retry-After", "0")
					http.Error(w, "response failed after admission", status)
				}))
				defer endpoint.Close()
				agent := reserveReplayAgent(t, s, endpoint.URL, replay)
				err := s.createReservedNode(t.Context(), model.CreateNodeRequest{ID: "node", RunID: "run", Group: "workers", Generation: 2}, agent, "a", nil)
				var uncertain *uncertainNodeCreateError
				if !errors.As(err, &uncertain) || creates.Load() != 1 || s.state.reservations["node"] != "a" || len(s.state.uncertainCreates) != 1 {
					t.Fatalf("5xx lost uncertain admission: calls=%d reservations=%v uncertain=%v err=%v", creates.Load(), s.state.reservations, s.state.uncertainCreates, err)
				}
				if _, available := s.tryReserveAgent("another"); available {
					t.Fatal("5xx freed capacity before the admitted container was removed")
				}
				if err := s.state.heartbeat(model.AgentHeartbeat{Agent: model.Agent{ID: "a", StartedAt: agent.StartedAt}}); err != nil {
					t.Fatal(err)
				}
				if s.state.reservations["node"] != "a" || s.state.agents["a"].ActiveNodes != 1 {
					t.Fatal("empty inventory freed a potentially delayed admission")
				}
				if err := s.stopRunGeneration(t.Context(), "run", 2); err == nil || len(s.state.reservations) != 1 {
					t.Fatalf("failed cleanup released reservation: %v", err)
				}
				cleaned.Store(true)
				if err := s.stopRunGeneration(t.Context(), "run", 1); err != nil || len(s.state.reservations) != 1 {
					t.Fatalf("older generation released reservation: %v", err)
				}
				if err := s.stopRunGeneration(t.Context(), "run", 2); err != nil {
					t.Fatal(err)
				}
				if len(s.state.reservations) != 0 || len(s.state.uncertainCreates) != 0 || s.state.agents["a"].ActiveNodes != 0 {
					t.Fatal("confirmed cleanup did not release the uncertain reservation")
				}
				if _, available := s.tryReserveAgent("after-cleanup"); !available {
					t.Fatal("confirmed cleanup did not restore admission capacity")
				}
			})
		}
	}
}

func TestCanceledCreateLateServerFailureCannotAffectNextGenerationCapacity(t *testing.T) {
	s := newLifecycleTestController(t)
	started, canceled := make(chan struct{}), make(chan struct{})
	lateResponse, responseSent := make(chan struct{}), make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/nodes":
			_, _ = io.Copy(io.Discard, r.Body)
			close(started)
			<-r.Context().Done()
			close(canceled)
			<-lateResponse
			http.Error(w, "late failure after admitted container was removed", http.StatusServiceUnavailable)
			close(responseSent)
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
		case r.URL.Path == "/api/v1/status":
			writeJSON(w, http.StatusOK, model.AgentHeartbeat{Agent: model.Agent{ID: "a"}})
		default:
			t.Errorf("unexpected Agent request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer endpoint.Close()
	var release sync.Once
	allowLateResponse := func() { release.Do(func() { close(lateResponse) }) }
	defer allowLateResponse()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	agent := reserveReplayAgent(t, s, endpoint.URL, true)
	jobs := newPhaseJobs()
	if err := jobs.startContext(ctx, "admission", func(ctx context.Context) error {
		return s.createReservedNode(ctx, model.CreateNodeRequest{ID: "node", RunID: "run", Generation: 1}, agent, "a", nil)
	}, nil); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := s.runPhase(t.Context(), "run", 1, scenario.Phase{Action: "stop-all"}, nil, jobs, time.Second); err != nil {
		t.Fatal(err)
	}
	<-canceled
	if len(s.state.uncertainCreates) != 0 || len(s.state.reservations) != 0 || s.state.agents["a"].ActiveNodes != 0 {
		t.Fatal("stop-all did not settle the canceled creation")
	}
	if _, ok := s.tryReserveAgent("next-generation"); !ok {
		t.Fatal("completed cleanup did not admit the next generation")
	}
	// Releasing the old HTTP response after cleanup must not resurrect its
	// reservation or release the unrelated new generation's occupied slot.
	allowLateResponse()
	<-responseSent
	if len(s.state.uncertainCreates) != 0 || len(s.state.reservations) != 1 || s.state.reservations["next-generation"] != "a" || s.state.agents["a"].ActiveNodes != 1 {
		t.Fatal("late creation failure changed the next generation's reservation")
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
