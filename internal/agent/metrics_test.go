package agent

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func localMetricGauge(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if localMetricLabelsMatch(metric.Label, labels) {
				return metric.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}

func localMetricLabelsMatch(actual []*dto.LabelPair, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, pair := range actual {
		value, exists := expected[pair.GetName()]
		if !exists || value != pair.GetValue() {
			return false
		}
	}
	return true
}

func TestLocalMetricsReflectLifecycleCleanupAndQueue(t *testing.T) {
	s := &Server{config: Config{ID: "agent-a", Capacity: 12}, processes: map[string]*process{
		"ready-a":        {node: model.Node{State: model.NodeReady}},
		"ready-b":        {node: model.Node{State: model.NodeReady}},
		"starting":       {node: model.Node{State: model.NodeStarting}},
		"removing":       {node: model.Node{State: model.NodeStopping}},
		"cleanup-failed": {node: model.Node{State: model.NodeFailed}, exited: true, cleanupErr: errors.New("daemon unavailable")},
		"stopped":        {node: model.Node{State: model.NodeStopped}, exited: true},
	}, events: make([]model.TraceEvent, 7)}
	registry := prometheus.NewRegistry()
	registry.MustRegister(newLocalCollector(s))
	labels := map[string]string{"agent_id": "agent-a"}
	if value := localMetricGauge(t, registry, "kpl_local_capacity", labels); value != 12 {
		t.Fatalf("capacity=%v", value)
	}
	if value := localMetricGauge(t, registry, "kpl_local_cleanup_pending", labels); value != 2 {
		t.Fatalf("cleanup pending=%v", value)
	}
	if value := localMetricGauge(t, registry, "kpl_local_telemetry_queue_events", labels); value != 7 {
		t.Fatalf("queued=%v", value)
	}
	if value := localMetricGauge(t, registry, "kpl_local_nodes", map[string]string{"agent_id": "agent-a", "state": "ready", "runtime": "docker"}); value != 2 {
		t.Fatalf("Docker ready=%v", value)
	}
	if value := localMetricGauge(t, registry, "kpl_local_nodes", map[string]string{"agent_id": "agent-a", "state": "stopped", "runtime": "docker"}); value != 1 {
		t.Fatalf("retained stopped=%v", value)
	}
	s.mu.Lock()
	for _, id := range []string{"removing", "cleanup-failed"} {
		s.processes[id].node.State = model.NodeStopped
		s.processes[id].exited = true
		s.processes[id].cleanupErr = nil
	}
	s.mu.Unlock()
	s.eventsMu.Lock()
	s.events = s.events[:2]
	s.eventsMu.Unlock()
	if value := localMetricGauge(t, registry, "kpl_local_cleanup_pending", labels); value != 0 {
		t.Fatalf("cleanup gauge remained stale: %v", value)
	}
	if value := localMetricGauge(t, registry, "kpl_local_telemetry_queue_events", labels); value != 2 {
		t.Fatalf("queue gauge remained stale: %v", value)
	}
}

func TestAgentMetricsRegistriesAreIndependentAndExposeAgentRuntime(t *testing.T) {
	first := &Server{config: Config{ID: "first", Capacity: 3}, processes: map[string]*process{}}
	second := &Server{config: Config{ID: "second", Capacity: 9}, processes: map[string]*process{}}
	for _, s := range []*Server{first, second, first} {
		registry := newAgentMetricsRegistry(s)
		if value := localMetricGauge(t, registry, "kpl_local_capacity", map[string]string{"agent_id": s.config.ID}); value != float64(s.config.Capacity) {
			t.Fatal("registry leaked another Agent's capacity")
		}
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		foundGo := false
		for _, family := range families {
			if family.GetName() == "go_goroutines" {
				foundGo = true
			}
			if strings.HasPrefix(family.GetName(), "go_") || strings.HasPrefix(family.GetName(), "process_") {
				for _, metric := range family.Metric {
					foundAgent := false
					for _, label := range metric.Label {
						if label.GetName() == "agent_id" && label.GetValue() == s.config.ID {
							foundAgent = true
						}
					}
					if !foundAgent {
						t.Fatalf("process/runtime metric is missing Agent identity: %s", family.GetName())
					}
				}
			}
		}
		if !foundGo {
			t.Fatal("Go runtime collector missing")
		}
	}
}

func TestAgentMetricsRouteCanBeScrapedWithoutMutationAuthorization(t *testing.T) {
	s := &Server{config: Config{ID: "agent", Capacity: 4, Token: "secret"}, processes: map[string]*process{}}
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("scrape status=%d type=%s", response.Code, response.Header().Get("Content-Type"))
	}
	if !strings.Contains(response.Body.String(), `kpl_local_capacity{agent_id="agent"} 4`) {
		t.Fatalf("local metrics missing from route: %s", response.Body.String())
	}
}
