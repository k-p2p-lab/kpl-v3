package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func TestWebCapacityOverrideReachesAgentAdmissionAndMetrics(t *testing.T) {
	dir := t.TempDir()
	controllerServer := authenticatedTestController(dir)
	handler := controllerServer.Handler(context.Background())
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	s := &Server{config: Config{ID: "worker", AdvertiseURL: "http://worker", ControllerURL: endpoint.URL, Token: controllerTestToken, Capacity: 200}, client: endpoint.Client(), startedAt: time.Now().UTC(), processes: make(map[string]*process)}
	for i := 0; i < 150; i++ {
		id := fmt.Sprintf("peer-%d", i)
		s.processes[id] = &process{node: model.Node{ID: id, RunID: "run", State: model.NodeReady}}
	}
	if err := s.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	edit := func(body string) model.Agent {
		t.Helper()
		req := controllerReadRequest(t, handler, endpoint.URL+"/api/v1/agents/worker/capacity")
		req.Method = http.MethodPut
		req.Body = io.NopCloser(strings.NewReader(body))
		req.ContentLength = int64(len(body))
		req.Header.Set("X-KPL-Request", "dashboard")
		response, err := endpoint.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var agent model.Agent
		if response.StatusCode != 200 {
			data, _ := io.ReadAll(response.Body)
			t.Fatalf("capacity edit failed: %d %s", response.StatusCode, data)
		}
		if err := json.NewDecoder(response.Body).Decode(&agent); err != nil {
			t.Fatal(err)
		}
		return agent
	}
	check := func(effective int, pending bool) {
		t.Helper()
		req := controllerReadRequest(t, handler, endpoint.URL+"/api/v1/agents")
		response, err := endpoint.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var agents []model.Agent
		if err := json.NewDecoder(response.Body).Decode(&agents); err != nil {
			t.Fatal(err)
		}
		if len(agents) != 1 || agents[0].Capacity != effective || agents[0].CapacityPending != pending || agents[0].DefaultCapacity != 200 {
			t.Fatalf("Controller capacity: %+v", agents)
		}
	}
	edit(`{"capacity":100}`)
	check(100, true)
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if status := s.snapshot().Agent; status.Capacity != 100 || status.DefaultCapacity != 200 || status.ActiveNodes != 150 {
		t.Fatalf("local override lost default or stopped Peers: %+v", status)
	}
	if _, err := s.createNode(context.Background(), model.CreateNodeRequest{ID: "extra", RunID: "run", Group: "workers"}); err != errCapacityReached {
		t.Fatalf("local admission ignored override: %v", err)
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(newLocalCollector(s))
	if value := localMetricGauge(t, registry, "kpl_local_capacity", map[string]string{"agent_id": "worker"}); value != 100 {
		t.Fatalf("local metric capacity=%v", value)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(100, false)
	edit(`{"capacity":300}`)
	check(100, true)
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.snapshotAgent().Capacity != 300 || s.config.Capacity != 200 {
		t.Fatal("override could not raise local admission or changed the CLI default")
	}
	check(100, true)
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(300, false)
	edit(`{"capacity":null}`)
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	check(200, false)
	edit(`{"capacity":100}`)
	// A new Controller process loads the saved ID-based setting; a fresh Agent
	// starts with its CLI value and receives that setting during registration.
	controllerServer = authenticatedTestController(dir)
	restarted := httptest.NewServer(controllerServer.Handler(context.Background()))
	defer restarted.Close()
	fresh := &Server{config: Config{ID: "worker", AdvertiseURL: "http://worker", ControllerURL: restarted.URL, Token: controllerTestToken, Capacity: 250}, client: restarted.Client(), startedAt: time.Now().UTC(), processes: make(map[string]*process)}
	if fresh.snapshotAgent().Capacity != 250 {
		t.Fatal("fresh Agent did not start at CLI default")
	}
	if err := fresh.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if agent := fresh.snapshotAgent(); agent.Capacity != 100 || agent.DefaultCapacity != 250 {
		t.Fatalf("restart lost override: %+v", agent)
	}
}

func TestCapacityHeaderOnlyAppliesToSuccessfulControlResponses(t *testing.T) {
	for _, tc := range []struct {
		path    string
		status  int
		value   string
		want    int
		failure bool
	}{
		{"/api/v1/agents/heartbeat", 204, "100", 100, false},
		{"/api/v1/agents/heartbeat", 204, "", 200, false},
		{"/api/v1/agents/heartbeat", 500, "100", 200, true},
		{"/api/v1/events/batch", 204, "100", 200, false},
		{"/api/v1/agents/register", 201, "-1", 200, true},
		{"/api/v1/agents/register", 201, "bad", 200, true},
	} {
		t.Run(fmt.Sprintf("%s-%d-%s", tc.path, tc.status, tc.value), func(t *testing.T) {
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-KPL-Agent-Capacity", tc.value)
				w.WriteHeader(tc.status)
			}))
			defer endpoint.Close()
			s := &Server{config: Config{Capacity: 200, ControllerURL: endpoint.URL}, client: endpoint.Client()}
			err := s.postData(context.Background(), tc.path, []byte(`{}`), nil)
			if (err != nil) != tc.failure || s.snapshotAgent().Capacity != tc.want {
				t.Fatalf("err=%v capacity=%d", err, s.snapshotAgent().Capacity)
			}
		})
	}
}
