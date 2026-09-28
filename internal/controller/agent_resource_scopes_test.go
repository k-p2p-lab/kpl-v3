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

func scopedResourceFixture(now time.Time, agentCPU, peersCPU float64, peers int) *model.AgentResources {
	aUsage, aWorking, pUsage, pWorking := uint64(1024), uint64(768), uint64(peers*1024), uint64(peers*768)
	totalCPU, totalUsage, totalWorking := agentCPU+peersCPU, aUsage+pUsage, aWorking+pWorking
	return &model.AgentResources{SampledAt: now, CPUCapacityCores: 8, CPUCores: &totalCPU, MemoryUsageBytes: &totalUsage, MemoryWorkingSetBytes: &totalWorking, Containers: peers + 1, MeasuredContainers: peers + 1, Complete: true,
		Agent: &model.ContainerResources{CPUCores: &agentCPU, MemoryUsageBytes: &aUsage, MemoryWorkingSetBytes: &aWorking, Containers: 1, MeasuredContainers: 1, Complete: true},
		Peers: &model.ContainerResources{CPUCores: &peersCPU, MemoryUsageBytes: &pUsage, MemoryWorkingSetBytes: &pWorking, Containers: peers, MeasuredContainers: peers, Complete: true}}
}

func TestResourceScopesPreserveTotalsAndExportIndependentCoverage(t *testing.T) {
	now := time.Now().UTC()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	a := scopedResourceFixture(now, 0.5, 1.5, 2)
	b := scopedResourceFixture(now, 1, 0, 0)
	b.CPUCapacityCores = 4
	for id, r := range map[string]*model.AgentResources{"a": a, "b": b, "disabled": a, "offline": a, "stale": scopedResourceFixture(now.Add(-time.Minute), 9, 9, 2)} {
		state := model.AgentOnline
		if id == "offline" {
			state = model.AgentOffline
		}
		s.state.agents[id] = model.Agent{ID: id, State: state, LastSeen: now, Disabled: id == "disabled", Resources: r}
	}
	for scope, want := range map[string]float64{"agent_and_peers": 25, "agent": 12.5, "peers": 12.5} {
		total := sumAgentResourceScope(s.state.agentInventory(), now, scope)
		if total.CPUPercent == nil || *total.CPUPercent != want || total.CPUCapacityCores != 12 || total.MeasuredAgents != 2 || total.TotalAgents != 4 {
			t.Fatalf("scope %s: %+v", scope, total)
		}
	}
	response := resultRequest(s, "GET", "/api/v1/agents/resources")
	var payload struct {
		Totals        agentResourceTotals
		TotalsByScope map[string]agentResourceTotals
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || len(payload.TotalsByScope) != 3 || *payload.Totals.CPUPercent != 25 {
		t.Fatalf("current JSON: %v %s", err, response.Body)
	}
	response = resultRequest(s, "GET", "/api/v1/agents/resources?format=csv")
	rows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	for _, row := range rows[1:] {
		found[row[0]+"/"+row[2]] = row
	}
	for key, want := range map[string]string{"a/agent_and_peers": "25.000000", "a/agent": "6.250000", "a/peers": "18.750000", "b/peers": "0.000000", "TOTAL/agent": "12.500000", "TOTAL/peers": "12.500000"} {
		row, ok := found[key]
		if !ok || row[5] != want {
			t.Fatalf("CSV %s = %v", key, row)
		}
	}
	for _, scope := range model.ResourceScopes {
		for _, id := range []string{"disabled", "offline", "stale"} {
			if row := found[id+"/"+scope]; len(row) == 0 || row[5] != "" || row[7] != "" {
				t.Fatalf("excluded CSV %s/%s %v", id, scope, row)
			}
		}
	}
	families, err := s.state.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	metrics := map[string]float64{}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "kpl_agent_component_") {
			continue
		}
		for _, m := range f.Metric {
			labels := map[string]string{}
			for _, l := range m.Label {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["agent_id"] == "disabled" {
				t.Fatal("disabled component exported")
			}
			metrics[f.GetName()+"/"+labels["agent_id"]+"/"+labels["component"]+"/"+labels["kind"]] = m.GetGauge().GetValue()
		}
	}
	for key, want := range map[string]float64{"cpu_usage_percent/a/agent/": 6.25, "cpu_usage_percent/a/peers/": 18.75, "cpu_usage_percent/b/peers/": 0, "resource_sample_complete/b/peers/": 1, "resource_containers/b/peers/expected": 0, "memory_working_set_bytes/a/agent/": 768, "memory_working_set_bytes/a/peers/": 1536} {
		if got, ok := metrics["kpl_agent_component_"+key]; !ok || got != want {
			t.Fatalf("metric %s = %v present %v", key, got, ok)
		}
	}
	for _, id := range []string{"offline", "stale"} {
		for _, scope := range []string{"agent", "peers"} {
			if _, ok := metrics["kpl_agent_component_cpu_usage_percent/"+id+"/"+scope+"/"]; ok {
				t.Fatal("unavailable CPU gauge present")
			}
		}
	}
}

func TestResourceScopesOwnCopiesAndNeverFabricateMissingOrEmptySamples(t *testing.T) {
	now := time.Now().UTC()
	source := scopedResourceFixture(now, 0.5, 1.5, 2)
	copy := normalizeAgentResources(source, now, now)
	*source.Agent.CPUCores = 7
	*source.Peers.MemoryWorkingSetBytes = 99
	if *copy.Agent.CPUCores != 0.5 || *copy.Peers.MemoryWorkingSetBytes != 1536 {
		t.Fatal("component pointers alias heartbeat input")
	}
	missing := scopedResourceFixture(now, 0, 2, 2)
	missing.Agent.CPUCores = nil
	missing.Agent.MeasuredContainers = 0
	missing.Agent.Complete = false
	missing.Complete = false
	missing.MeasuredContainers = 2
	missing = normalizeAgentResources(missing, now, now)
	if missing.ScopeValid("agent") || !missing.ScopeValid("peers") || !missing.Valid() {
		t.Fatal("component failure affected other scopes")
	}
	a := model.Agent{ID: "a", State: model.AgentOnline, LastSeen: now, Resources: missing}
	if sumAgentResourceScope([]model.Agent{a}, now, "agent").MeasuredAgents != 0 || sumAgentResourceScope([]model.Agent{a}, now, "peers").MeasuredAgents != 1 {
		t.Fatal("wrong per-scope denominator")
	}
	for _, scope := range []string{"agent", "peers"} {
		if resourceFixture(now, 3).ScopeValid(scope) {
			t.Fatal("legacy total became a component")
		}
	}
	empty := scopedResourceFixture(now, 1, 0, 0)
	if !empty.ScopeValid("peers") {
		t.Fatal("empty inventory rejected")
	}
	for _, value := range []float64{1, math.NaN(), math.Inf(1)} {
		invalid := scopedResourceFixture(now, 1, 0, 0)
		invalid.Peers.CPUCores = &value
		if normalizeAgentResources(invalid, now, now).Peers.CPUCores != nil {
			t.Fatal("invalid empty Peer sample accepted")
		}
	}
	empty.Peers.Complete = false
	if empty.ScopeValid("peers") {
		t.Fatal("incomplete inventory became measured zero")
	}
	for _, at := range []time.Time{{}, now.Add(10 * time.Second)} {
		invalid := scopedResourceFixture(at, 1, 0, 0)
		normalized := normalizeAgentResources(invalid, now, now)
		if normalized.ScopeValid("agent") || normalized.ScopeValid("peers") {
			t.Fatal("invalid shared clock accepted")
		}
	}
}

func TestResourceHistoryAndIntervalSeparateScopesAtTheSameTimestamp(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		query := r.Form.Get("query")
		if !strings.Contains(query, "kpl_agent_component_cpu_usage_percent") || strings.Contains(query, "cpu_usage_cores") {
			t.Errorf("wrong scope selector: %s", query)
		}
		entries := []any{}
		for _, scope := range []string{"agent_and_peers", "agent", "peers", "invalid"} {
			metric, value := "kpl_agent_component_cpu_usage_percent", "1"
			if scope == "agent_and_peers" {
				metric, value = "kpl_agent_cpu_usage_percent", "4"
			} else if scope == "peers" {
				value = "3"
			}
			entries = append(entries, map[string]any{"metric": map[string]string{"agent_id": "a", "component": scope, "__name__": metric}, "values": [][2]any{{now.Add(-30 * time.Second).Unix(), value}, {now.Add(-time.Second).Unix(), value}}})
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": entries}})
	}))
	defer prom.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	history, err := s.agentResourceHistory(context.Background(), time.Minute, now)
	if err != nil || len(history.Series) != 3 || history.Scope != "all" {
		t.Fatalf("history %+v %v", history, err)
	}
	for _, series := range history.Series {
		if series.Mean != map[string]float64{"agent_and_peers": 4, "agent": 1, "peers": 3}[series.Scope] || len(series.Samples) != 2 {
			t.Fatalf("merged different scopes: %+v", series)
		}
	}
	id := strings.Repeat("a", 32)
	s.resourceMeasurementsLoaded = true
	s.resourceMeasurements = []resourceMeasurement{{ID: id, StartedAt: now.Add(-time.Minute), EndedAt: now, EndReason: "stopped"}}
	for _, endpoint := range []string{"/api/v1/agents/resources/history?range=1m", "/api/v1/agents/resources/measurements/" + id + "/export?"} {
		for _, kind := range []string{"summary", "samples"} {
			response := resultRequest(s, "GET", endpoint+"&format=csv&kind="+kind)
			rows, err := csv.NewReader(response.Body).ReadAll()
			if err != nil || response.Code != 200 {
				t.Fatalf("CSV %d %v %s", response.Code, err, response.Body)
			}
			scopes := map[string]bool{}
			for _, row := range rows[1:] {
				scopes[row[1]] = true
			}
			if len(scopes) != 3 {
				t.Fatalf("CSV lost scopes: %v", rows)
			}
		}
	}
}
