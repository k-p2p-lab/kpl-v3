package controller

import (
	"math"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func networkMetricFamilies(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	list, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	families := make(map[string]*dto.MetricFamily)
	for _, family := range list {
		families[family.GetName()] = family
	}
	return families
}

func networkMetricGauge(t *testing.T, families map[string]*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	for _, metric := range families[name].GetMetric() {
		if len(metric.Label) != len(labels) {
			continue
		}
		matches := true
		for _, pair := range metric.Label {
			value, exists := labels[pair.GetName()]
			if !exists || value != pair.GetValue() {
				matches = false
			}
		}
		if matches {
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s %v missing", name, labels)
	return 0
}

func networkMetricTestNode(id, state, raw string) model.Node {
	return model.Node{ID: id, RunID: "run", AgentID: "agent", Group: "workers", Profile: "wan", State: state, Metadata: map[string]string{"network": raw}}
}

func TestNetworkMetricsAggregateConcreteSettingsAndSeparateScopes(t *testing.T) {
	s := newState(t.TempDir())
	s.nodes = map[string]model.Node{
		"ready":    networkMetricTestNode("ready", model.NodeReady, `{"delay":"20ms","jitter":"2ms","lossPercent":2}`),
		"starting": networkMetricTestNode("starting", model.NodeStarting, `{"delay":"60ms","jitter":"6ms","lossPercent":10}`),
		"zero":     networkMetricTestNode("zero", model.NodeReady, `{"scope":"all"}`),
		"stopped":  networkMetricTestNode("stopped", model.NodeStopped, `{"delay":"10s","lossPercent":100}`),
	}
	other := networkMetricTestNode("other-run", model.NodeReady, `{"delay":"1s"}`)
	other.RunID = "other-run"
	s.nodes[other.ID] = other
	registry := prometheus.NewRegistry()
	registry.MustRegister(newNetworkCollector(s))
	families := networkMetricFamilies(t, registry)
	labels := map[string]string{"run_id": "run", "agent_id": "agent", "group": "workers", "profile": "wan", "scope": "p2p"}
	if value := networkMetricGauge(t, families, "kpl_network_configured_peers", labels); value != 2 {
		t.Fatalf("peers=%v", value)
	}
	for stat, want := range map[string]float64{"mean": 0.04, "min": 0.02, "max": 0.06} {
		labels["stat"] = stat
		if value := networkMetricGauge(t, families, "kpl_network_configured_delay_seconds", labels); math.Abs(value-want) > 1e-12 {
			t.Fatalf("delay %s=%v, want %v", stat, value, want)
		}
	}
	delete(labels, "stat")
	if value := networkMetricGauge(t, families, "kpl_network_configured_jitter_seconds", labels); math.Abs(value-0.004) > 1e-12 {
		t.Fatalf("jitter=%v", value)
	}
	if value := networkMetricGauge(t, families, "kpl_network_configured_loss_ratio", labels); math.Abs(value-0.06) > 1e-12 {
		t.Fatalf("loss ratio=%v", value)
	}
	labels["scope"] = "all"
	if value := networkMetricGauge(t, families, "kpl_network_configured_peers", labels); value != 1 {
		t.Fatalf("all-scope peers=%v", value)
	}
	labels["stat"] = "mean"
	if value := networkMetricGauge(t, families, "kpl_network_configured_delay_seconds", labels); value != 0 {
		t.Fatalf("explicit empty config should give configured zero delay: %v", value)
	}
}

func TestNetworkMetricsSkipMissingInvalidAndUnresolvedConfigurations(t *testing.T) {
	s := newState(t.TempDir())
	for id, raw := range []string{"", "null", "bad JSON", `{"unknown":1}`, `{"delay":"-1s"}`, `{"lossPercent":101}`, `{"delayDistribution":{"model":"fixed","value":"1s"}}`, `{} {}`} {
		node := networkMetricTestNode(string(rune('a'+id)), model.NodeReady, raw)
		s.nodes[node.ID] = node
	}
	s.nodes["stopping"] = networkMetricTestNode("stopping", model.NodeStopping, `{"delay":"1s"}`)
	s.nodes["failed"] = networkMetricTestNode("failed", model.NodeFailed, `{"delay":"1s"}`)
	registry := prometheus.NewRegistry()
	registry.MustRegister(newNetworkCollector(s))
	if families := networkMetricFamilies(t, registry); len(families) != 0 {
		t.Fatalf("missing/invalid settings fabricated metrics: %v", families)
	}
}

func TestNetworkMetricsDoNotRetainExitedPeerSeries(t *testing.T) {
	s := newState(t.TempDir())
	s.nodes["peer"] = networkMetricTestNode("peer", model.NodeReady, `{"delay":"25ms"}`)
	registry := prometheus.NewRegistry()
	registry.MustRegister(newNetworkCollector(s))
	if len(networkMetricFamilies(t, registry)) == 0 {
		t.Fatal("ready peer metrics missing")
	}
	s.mu.Lock()
	node := s.nodes["peer"]
	node.State = model.NodeStopped
	s.nodes["peer"] = node
	s.mu.Unlock()
	if families := networkMetricFamilies(t, registry); len(families) != 0 {
		t.Fatalf("stopped peer retained a current configuration series: %v", families)
	}
}
