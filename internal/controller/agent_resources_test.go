package controller

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func resourceFixture(at time.Time, cores float64) *model.AgentResources {
	usage, working := uint64(4096), uint64(3072)
	return &model.AgentResources{SampledAt: at, CPUCores: &cores, CPUCapacityCores: 8, MemoryUsageBytes: &usage, MemoryWorkingSetBytes: &working, Containers: 3, MeasuredContainers: 3, Complete: true}
}
func TestAgentResourcesFreshnessAggregationAndCSV(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	for _, a := range []model.Agent{
		{ID: "a", Name: "=unsafe", State: model.AgentOnline, LastSeen: now, Resources: resourceFixture(now, 1.5)},
		{ID: "b", State: model.AgentOnline, LastSeen: now, Resources: resourceFixture(now, 2)},
		{ID: "offline", State: model.AgentOffline, LastSeen: now, Resources: resourceFixture(now, 10)},
		{ID: "stale", State: model.AgentOnline, LastSeen: now, Resources: resourceFixture(now.Add(-time.Minute), 10)},
		{ID: "legacy", State: model.AgentOnline, LastSeen: now},
	} {
		s.state.agents[a.ID] = a
	}
	totals := sumAgentResources(s.state.agentInventory(), now)
	if totals.MeasuredAgents != 2 || totals.TotalAgents != 5 || totals.CPUCores != 3.5 || totals.MemoryWorkingSetBytes != 6144 {
		t.Fatalf("totals: %+v", totals)
	}
	response := resultRequest(s, http.MethodGet, "/api/v1/agents/resources?format=csv")
	rows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil || response.Code != 200 || len(rows) != 7 {
		t.Fatalf("CSV: %d %v %v", response.Code, rows, err)
	}
	if rows[1][1] != "'=unsafe" || rows[1][5] != "18.750000" || rows[6][5] != "21.875000" {
		t.Fatalf("CSV units/safety: %v", rows)
	}
	for _, row := range rows[1:6] {
		if row[0] == "stale" || row[0] == "offline" || row[0] == "legacy" {
			if row[5] != "" || row[7] != "" {
				t.Fatalf("missing sample rendered as measured: %v", row)
			}
		}
	}
	families, err := s.state.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "kpl_agent_cpu_usage_cores" {
			if len(family.Metric) != 2 {
				t.Fatalf("stale resource exported: %v", family)
			}
		}
	}
}
func TestAgentResourceClockNormalizationAndStaleHeartbeat(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	sourceNow := time.Now().UTC().Add(-time.Hour)
	agent := model.Agent{ID: "a", URL: "http://agent", LastSeen: sourceNow, Resources: resourceFixture(sourceNow.Add(-5*time.Second), 1)}
	registered, err := s.state.registerAgent(agent)
	if err != nil {
		t.Fatal(err)
	}
	if age := time.Since(registered.Resources.SampledAt); age < 4*time.Second || age > 6*time.Second {
		t.Fatalf("clock skew not normalized: %v", age)
	}
	agent.StartedAt = registered.StartedAt
	agent.LastSeen = sourceNow.Add(time.Second)
	agent.Resources = resourceFixture(sourceNow, 2)
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent}); err != nil {
		t.Fatal(err)
	}
	agent.LastSeen = sourceNow.Add(-time.Second)
	agent.Resources = resourceFixture(sourceNow.Add(-time.Second), 9)
	if err := s.state.heartbeat(model.AgentHeartbeat{Agent: agent, Partial: true}); err != nil {
		t.Fatal(err)
	}
	if *s.state.agents["a"].Resources.CPUCores != 2 {
		t.Fatal("old partial heartbeat overwrote resource sample")
	}
	invalid := resourceFixture(sourceNow, math.NaN())
	if normalizeAgentResources(invalid, sourceNow, time.Now()).CPUCores != nil {
		t.Fatal("non-finite value was accepted")
	}
	invalid = resourceFixture(sourceNow, 1)
	invalid.SampledAt = time.Time{}
	if normalizeAgentResources(invalid, sourceNow, time.Now()).Complete {
		t.Fatal("untimed sample marked fresh")
	}
}
func TestResourceHistoryPreservesPeaksGapsAndExportUnits(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.Method != http.MethodPost {
			t.Errorf("unexpected query %s %s", r.Method, r.URL.Path)
		}
		r.ParseForm()
		q := r.Form.Get("query")
		if !strings.Contains(q, `job="kpl-controller"`) || !strings.HasSuffix(q, "[3600s]") {
			t.Errorf("unbounded/non-fixed query: %s", q)
		}
		if r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "" {
			t.Error("forwarded credentials")
		}
		entry := func(name string, values [][2]any) any {
			return map[string]any{"metric": map[string]string{"agent_id": "=agent", "__name__": name}, "values": values}
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{
			entry("kpl_agent_cpu_usage_percent", [][2]any{{now.Add(-time.Minute).Unix(), "1"}, {now.Add(-30 * time.Second).Unix(), "9"}, {now.Add(-time.Second).Unix(), "NaN"}}),
			entry("kpl_agent_memory_working_set_bytes", [][2]any{{now.Add(-time.Minute).Unix(), "1024"}, {now.Add(-30 * time.Second).Unix(), "2048"}}),
		}}})
	}))
	defer prom.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	history, err := s.agentResourceHistory(context.Background(), time.Hour, now)
	if err != nil || len(history.Series) != 2 {
		t.Fatalf("history %+v %v", history, err)
	}
	cpu := history.Series[0]
	if len(cpu.Samples) != 2 || cpu.Mean != 5 || cpu.Max != 9 || cpu.Min != 1 || cpu.Unit != "percent_host" {
		t.Fatalf("wrong sampled mean/peak or gap interpolation: %+v", cpu)
	}
	response := resultRequest(s, http.MethodGet, "/api/v1/agents/resources/history?range=1h&kind=summary&format=csv")
	rows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil || response.Code != 200 || len(rows) != 3 || rows[1][0] != "'=agent" || rows[1][8] != "2" || rows[1][11] != "9.000000" {
		t.Fatalf("summary CSV %d %v %v", response.Code, rows, err)
	}
	response = resultRequest(s, http.MethodGet, "/api/v1/agents/resources/history?range=1h&format=csv")
	rows, err = csv.NewReader(response.Body).ReadAll()
	if err != nil || len(rows) != 5 {
		t.Fatalf("samples: %v %v", rows, err)
	}
	if response = resultRequest(s, http.MethodGet, "/api/v1/agents/resources/history?range=48h"); response.Code != 400 {
		t.Fatalf("unbounded history accepted: %d", response.Code)
	}
	for range cap(s.resourceHistorySlots) {
		s.resourceHistorySlots <- struct{}{}
	}
	if response = resultRequest(s, http.MethodGet, "/api/v1/agents/resources/history?range=1h"); response.Code != 503 {
		t.Fatal("unbounded history concurrency")
	}
}
func TestResourceEndpointsRequireLoginAndPrometheusFailuresAreExplicit(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "resource-test"}, nil)
	for _, path := range []string{"/api/v1/agents/resources?format=csv", "/api/v1/agents/resources/history?range=1h&format=csv"} {
		if response := resultRequest(s, http.MethodGet, path); response.Code != 401 {
			t.Fatalf("unauthorized export: %d", response.Code)
		}
	}
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer prom.Close()
	s = New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	if response := resultRequest(s, http.MethodGet, "/api/v1/agents/resources/history?range=1h&format=csv"); response.Code != 502 || strings.Contains(response.Header().Get("Content-Type"), "csv") {
		t.Fatalf("failed history produced CSV: %d %s", response.Code, response.Body)
	}
}

func TestAgentResourcePartialCoverageWeightedPercentAndLegacyCapacity(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	partial := resourceFixture(now, 4)
	partial.Complete = false
	partial.Containers = 47
	partial.MeasuredContainers = 46
	partial.CPUCapacityCores = 16
	full := resourceFixture(now, 2)
	full.CPUCapacityCores = 4
	legacy := resourceFixture(now, 100)
	legacy.CPUCapacityCores = 0
	empty := resourceFixture(now, 999)
	empty.Complete = false
	empty.MeasuredContainers = 0
	for id, r := range map[string]*model.AgentResources{"partial": partial, "full": full, "legacy": legacy, "empty": empty} {
		s.state.agents[id] = model.Agent{ID: id, LastSeen: now, State: model.AgentOnline, Resources: normalizeAgentResources(r, now, now)}
	}
	normalized := s.state.agents["partial"].Resources
	if !normalized.Valid() || normalized.Complete || normalized.CPUCapacityCores != 16 || *normalized.CPUCores != 4 {
		t.Fatalf("discarded partial: %+v", normalized)
	}
	totals := sumAgentResources(s.state.agentInventory(), now)
	// 4/16=25%, 2/4=50%; capacity-weighted total is 6/20=30%, not 37.5%.
	if totals.CPUPercent == nil || *totals.CPUPercent != 30 || totals.CPUCapacityCores != 20 || totals.CPUMeasuredAgents != 2 || totals.MeasuredAgents != 3 || totals.PartialAgents != 1 || totals.MeasuredContainers != 52 || totals.Containers != 53 {
		t.Fatalf("wrong weighted totals: %+v", totals)
	}
	response := resultRequest(s, "GET", "/api/v1/agents/resources?format=csv")
	rows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows[1:] {
		switch row[0] {
		case "partial":
			if row[3] != "partial" || row[5] != "25.000000" || row[6] != "16" || row[7] != "3072" || row[9] != "47" || row[10] != "46" {
				t.Fatalf("partial CSV: %v", row)
			}
		case "legacy":
			if row[5] != "" || row[7] != "3072" {
				t.Fatalf("guessed legacy denominator or lost memory: %v", row)
			}
		case "empty":
			if row[5] != "" || row[7] != "" {
				t.Fatalf("empty sample counted as zero: %v", row)
			}
		case "TOTAL":
			if row[5] != "30.000000" || row[6] != "20" || row[7] != "9216" {
				t.Fatalf("fleet CSV: %v", row)
			}
		}
	}
	families, err := s.state.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		for _, m := range family.Metric {
			id, kind := "", ""
			for _, label := range m.Label {
				if label.GetName() == "agent_id" {
					id = label.GetValue()
				}
				if label.GetName() == "kind" {
					kind = label.GetValue()
				}
			}
			values[family.GetName()+"/"+id+"/"+kind] = m.GetGauge().GetValue()
		}
	}
	for key, want := range map[string]float64{"kpl_agent_cpu_usage_percent/partial/": 25, "kpl_agent_cpu_capacity_cores/partial/": 16, "kpl_agent_resource_sample_complete/partial/": 0, "kpl_agent_resource_containers/partial/measured": 46, "kpl_agent_memory_working_set_bytes/partial/": 3072} {
		if got, ok := values[key]; !ok || got != want {
			t.Fatalf("metric %s: %v exists=%v", key, got, ok)
		}
	}
	if _, ok := values["kpl_agent_cpu_usage_percent/legacy/"]; ok {
		t.Fatal("legacy cores mislabeled as percent")
	}
	for _, at := range []time.Time{time.Time{}, now.Add(6 * time.Second)} {
		invalid := resourceFixture(at, 2)
		invalid.Complete = false
		invalid.MeasuredContainers = 2
		if got := normalizeAgentResources(invalid, now, now); got.Valid() || got.CPUCores != nil {
			t.Fatal("invalid partial sample timestamp accepted")
		}
	}
}

func TestResourceHistoryExportsContainerCoverageWithoutMixingLegacyCPU(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		query := r.Form.Get("query")
		if strings.Contains(query, "kpl_agent_cpu_usage_cores") || !strings.Contains(query, "kpl_agent_resource_containers") {
			t.Errorf("mixed units or missing coverage query: %s", query)
		}
		entries := []any{}
		for _, kind := range []string{"expected", "measured"} {
			value := "47"
			if kind == "measured" {
				value = "46"
			}
			entries = append(entries, map[string]any{"metric": map[string]string{"agent_id": "agent", "__name__": "kpl_agent_resource_containers", "kind": kind}, "values": [][2]any{{now.Add(-time.Second).Unix(), value}}})
		}
		entries = append(entries, map[string]any{"metric": map[string]string{"agent_id": "agent", "__name__": "kpl_agent_cpu_usage_cores"}, "values": [][2]any{{now.Add(-time.Second).Unix(), "4"}}})
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": entries}})
	}))
	defer prom.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	history, err := s.agentResourceHistory(context.Background(), time.Minute, now)
	if err != nil || len(history.Series) != 2 || history.Series[0].Metric != "containers_expected" || history.Series[0].Mean != 47 || history.Series[1].Metric != "containers_measured" || history.Series[1].Mean != 46 {
		t.Fatalf("history coverage: %+v %v", history, err)
	}
}
