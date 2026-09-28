package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRegistrationRefreshRecoversControllerInventoryWithoutStoppingPeers(t *testing.T) {
	dir := t.TempDir()
	first := authenticatedTestController(dir)
	firstHandler := first.Handler(context.Background())
	firstHTTP := httptest.NewServer(firstHandler)
	defer firstHTTP.Close()
	s, err := New(Config{ID: "worker", Capacity: 20, DockerImage: "kpl:test", DockerNetwork: "peers", Token: controllerTestToken,
		AdvertiseURL: "http://worker:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	s.config.ControllerURL = firstHTTP.URL
	s.startupReconciled = true
	canceled := false
	live := &process{node: model.Node{ID: "live", RunID: "run", State: model.NodeReady}, cancel: func() { canceled = true }}
	stopped := &process{node: model.Node{ID: "stopped", RunID: "run", State: model.NodeStopped}, exited: true}
	s.processes["live"], s.processes["stopped"] = live, stopped
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []struct{ path, body string }{
		{"/api/v1/agents/worker/capacity", `{"capacity":7}`},
		{"/api/v1/agents/worker/enabled", `{"enabled":false}`},
	} {
		r := controllerReadRequest(t, firstHandler, edit.path)
		r.Method, r.Body = http.MethodPut, io.NopCloser(strings.NewReader(edit.body))
		r.Header.Set("X-KPL-Request", "dashboard")
		w := httptest.NewRecorder()
		firstHandler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("settings: %d %s", w.Code, w.Body)
		}
	}

	// Restart only the Controller. Its Agent inventory is initially empty, but
	// operator settings survive. No automatic heartbeat loop runs in this test.
	second := authenticatedTestController(dir)
	secondHandler := second.Handler(context.Background())
	secondHTTP := httptest.NewServer(secondHandler)
	defer secondHTTP.Close()
	s.config.ControllerURL = secondHTTP.URL
	r := httptest.NewRequest(http.MethodPost, "/api/v1/registration/refresh", nil)
	r.Header.Set("Authorization", "Bearer "+controllerTestToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("refresh: %d %s", w.Code, w.Body)
	}
	var status model.AgentHeartbeat
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if len(status.Nodes) != 2 || status.Agent.Capacity != 7 || canceled || s.processes["live"] != live || stopped.heartbeatAcknowledged {
		t.Fatalf("refresh changed live Peers, lost settings or failed to reset replay: %+v", status)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	secondHandler.ServeHTTP(w, controllerReadRequest(t, secondHandler, "/api/v1/agents"))
	var agents []model.Agent
	if err := json.Unmarshal(w.Body.Bytes(), &agents); err != nil {
		t.Fatal(err)
	}
	if len(agents) != 1 || !agents[0].Disabled || agents[0].CapacityOverride != 7 {
		t.Fatalf("settings lost after recovery: %+v", agents)
	}
	w = httptest.NewRecorder()
	secondHandler.ServeHTTP(w, controllerReadRequest(t, secondHandler, "/api/v1/nodes"))
	var nodes []model.Node
	if err := json.Unmarshal(w.Body.Bytes(), &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 || canceled {
		t.Fatalf("Peer inventory not replayed: %+v", nodes)
	}
}

func TestRegistrationRefreshRequiresAuthenticationReadinessAndIdleHeartbeat(t *testing.T) {
	s := &Server{config: Config{ID: "worker", Token: controllerTestToken}, processes: make(map[string]*process)}
	handler := s.Handler()
	for _, tc := range []struct {
		name, method                string
		auth, ready, stopping, busy bool
		want                        int
	}{
		{"unauthenticated", http.MethodPost, false, true, false, false, 401},
		{"wrong method", http.MethodGet, true, true, false, false, 405},
		{"startup incomplete", http.MethodPost, true, false, false, false, 503},
		{"shutting down", http.MethodPost, true, true, true, false, 503},
		{"heartbeat busy", http.MethodPost, true, true, false, true, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.startupReconciled, s.shuttingDown = tc.ready, tc.stopping
			if tc.busy {
				s.heartbeatMu.Lock()
				defer s.heartbeatMu.Unlock()
			}
			r := httptest.NewRequest(tc.method, "/api/v1/registration/refresh", nil)
			if tc.auth {
				r.Header.Set("Authorization", "Bearer "+controllerTestToken)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
		})
	}
}

func TestRegistrationRefreshControllerFailureKeepsAcknowledgedHistory(t *testing.T) {
	s := &Server{config: Config{ID: "worker", ControllerURL: "http://controller", Token: controllerTestToken}, startupReconciled: true,
		processes: map[string]*process{"stopped": {node: model.Node{ID: "stopped", State: model.NodeStopped}, exited: true, heartbeatAcknowledged: true}},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 503, Status: "503 Service Unavailable", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("Controller unavailable"))}, nil
		})},
	}
	r := httptest.NewRequest(http.MethodPost, "/api/v1/registration/refresh", nil)
	r.Header.Set("Authorization", "Bearer "+controllerTestToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 502 || !s.processes["stopped"].heartbeatAcknowledged {
		t.Fatalf("failed registration reset history: %d %s", w.Code, w.Body)
	}
}

func TestRegistrationRetriesUntilControllerIsReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		registrations, heartbeats := 0, 0
		s.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			status := http.StatusNoContent
			if r.URL.Path == "/api/v1/agents/register" {
				registrations++
				if registrations <= 3 {
					status = http.StatusServiceUnavailable
				}
			} else if r.URL.Path == "/api/v1/agents/heartbeat" {
				heartbeats++
			} else {
				t.Fatalf("unexpected request %s", r.URL.Path)
			}
			return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); s.heartbeatLoop(ctx) }()
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		cancel()
		<-done
		if registrations != 4 || heartbeats != 2 {
			t.Fatalf("startup recovery: registrations=%d heartbeats=%d", registrations, heartbeats)
		}
	})
}
