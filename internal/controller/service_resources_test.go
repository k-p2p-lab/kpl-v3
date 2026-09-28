package controller

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func postServiceResources(s *Server, report model.ServiceResourceReport, token string) *httptest.ResponseRecorder {
	data, _ := json.Marshal(report)
	request := httptest.NewRequest("POST", "/api/v1/services/resources/report", bytes.NewReader(data))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	s.Handler(context.Background()).ServeHTTP(response, request)
	return response
}
func TestServiceResourcesAuthenticationCoverageAndStaleness(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), Token: "monitor-token"}, nil)
	now := time.Now().UTC()
	partial := resourceFixture(now, 2)
	partial.Complete = false
	partial.MeasuredContainers = 2
	report := model.ServiceResourceReport{NodeID: "control1", NodeName: "=manager", ReportedAt: now, Services: []model.ServiceResources{
		{Service: "controller", State: "running", Resources: resourceFixture(now, 1)},
		{Service: "prometheus", State: "running", Resources: partial},
		{Service: "grafana", State: "not_running"},
	}}
	for _, path := range []string{"/api/v1/services/resources", "/api/v1/services/resources/report"} {
		req := httptest.NewRequest("GET", path, nil)
		response := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(response, req)
		if response.Code != 401 {
			t.Fatalf("unauthenticated read accepted: %d", response.Code)
		}
	}
	if response := postServiceResources(s, report, "wrong"); response.Code != 401 {
		t.Fatalf("unauthenticated report accepted: %d", response.Code)
	}
	if response := postServiceResources(s, report, "monitor-token"); response.Code != 204 {
		t.Fatalf("report rejected: %d %s", response.Code, response.Body)
	}
	rows := s.serviceResourceSnapshot(time.Now())
	if len(rows) != 3 || rows[0].Service != "controller" {
		t.Fatalf("unexpected rows %+v", rows)
	}
	response := resultRequest(s, "GET", "/api/v1/agents/resources?format=csv")
	csvRows, err := csv.NewReader(response.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	byService := map[string][]string{}
	for _, r := range csvRows[1:] {
		if len(r) != 15 {
			t.Fatalf("CSV shape: %v", r)
		}
		if r[11] == "service" {
			byService[r[12]] = r
		}
	}
	if r := byService["controller"]; r[0] != "" || r[2] != "controller" || r[5] != "12.500000" || r[14] != "'=manager" {
		t.Fatalf("controller CSV %v", r)
	}
	if r := byService["prometheus"]; r[2] != "third_party" || r[3] != "partial" || r[5] != "25.000000" || r[10] != "2" {
		t.Fatalf("partial CSV %v", r)
	}
	if r := byService["grafana"]; r[3] != "not_running" || r[5] != "" || r[7] != "" {
		t.Fatalf("stopped service fabricated values %v", r)
	}
	// Disabled Agents do not turn off service gauges on the same node.
	s.state.agents["control1"] = model.Agent{ID: "control1", Disabled: true, Resources: resourceFixture(now, 99)}
	assertGauges := func(wantCPU int, wantRunning int) {
		t.Helper()
		families, err := s.state.metrics.registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		cpu, running := 0, 0
		for _, family := range families {
			switch family.GetName() {
			case "kpl_service_cpu_usage_percent":
				cpu = len(family.Metric)
			case "kpl_service_running":
				running = len(family.Metric)
			case "kpl_agent_cpu_usage_percent":
				t.Fatal("disabled Agent leaked into samples")
			}
		}
		if cpu != wantCPU || running != wantRunning {
			t.Fatalf("CPU %d / running %d", cpu, running)
		}
	}
	assertGauges(2, 3)
	older := report
	older.ReportedAt = now.Add(-time.Second)
	older.Services = nil
	if postServiceResources(s, older, "monitor-token").Code != 204 || len(s.serviceResourceSnapshot(now)) != 3 {
		t.Fatal("old report replaced current inventory")
	}
	invalid := report
	invalid.Services = append(append([]model.ServiceResources{}, report.Services...), report.Services[0])
	invalid.ReportedAt = now.Add(time.Second)
	if postServiceResources(s, invalid, "monitor-token").Code != 400 {
		t.Fatal("duplicate service identity accepted")
	}
	s.serviceResources.mu.Lock()
	entry := s.serviceResources.nodes["control1"]
	entry.received = now.Add(-time.Minute)
	s.serviceResources.nodes["control1"] = entry
	s.serviceResources.mu.Unlock()
	assertGauges(0, 0)
	for _, row := range s.serviceResourceSnapshot(now) {
		if row.State != "stale" {
			t.Fatal("collector loss not visible")
		}
		r := serviceResourceCSVRow(row, now)
		if r[5] != "" || r[7] != "" {
			t.Fatal("stale service values exported")
		}
	}
	// Expire historical hosts from live memory, while retaining explicit placeholders.
	report.NodeID = "control2"
	report.ReportedAt = time.Now().UTC()
	s.serviceResources.mu.Lock()
	entry.received = now.Add(-11 * time.Minute)
	s.serviceResources.nodes["control1"] = entry
	s.serviceResources.mu.Unlock()
	if postServiceResources(s, report, "monitor-token").Code != 204 {
		t.Fatal("replacement host rejected")
	}
	if len(s.serviceResources.nodes) != 1 {
		t.Fatal("expired host cache retained")
	}
}

func TestServiceResourceHistoryAndIntervalKeepServiceAndHostIdentity(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		query := r.Form.Get("query")
		if !strings.Contains(query, "kpl_service_cpu_usage_percent") || !strings.Contains(query, "kpl_service_running") || !strings.Contains(query, "kpl_agent_component_cpu_usage_percent") {
			t.Errorf("missing service or Agent history: %s", query)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("service credentials forwarded to Prometheus")
		}
		series := []any{}
		for _, entity := range []struct{ service, node, metric, value string }{{"controller", "host1", "cpu_usage_percent", "12.5"}, {"prometheus", "host1", "cpu_usage_percent", "25"}, {"grafana", "host2", "cpu_usage_percent", "50"}, {"grafana", "host1", "cpu_usage_percent", "6.25"}, {"grafana", "host1", "running", "0"}, {"controller", "host1", "resource_report_fresh", "1"}} {
			series = append(series, map[string]any{"metric": map[string]string{"__name__": "kpl_service_" + entity.metric, "service": entity.service, "node_id": entity.node}, "values": [][2]any{{now.Add(-30 * time.Second).Unix(), entity.value}, {now.Add(-time.Second).Unix(), entity.value}}})
		}
		series = append(series, map[string]any{"metric": map[string]string{"__name__": "kpl_agent_cpu_usage_percent", "agent_id": "a"}, "values": [][2]any{{now.Add(-time.Second).Unix(), "75"}}})
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": series}})
	}))
	defer prom.Close()
	s := New(ServerConfig{DataDir: t.TempDir(), PrometheusURL: prom.URL}, nil)
	history, err := s.agentResourceHistory(context.Background(), time.Minute, now)
	if err != nil || len(history.Series) != 7 {
		t.Fatalf("history %+v %v", history, err)
	}
	seen := map[string]float64{}
	for _, series := range history.Series {
		if series.EntityType == "service" {
			if series.AgentID != "" {
				t.Fatal("service assigned to Agent")
			}
			seen[series.Service+"/"+series.NodeID+"/"+series.Metric] = series.Mean
		}
	}
	if seen["grafana/host1/cpu_percent"] != 6.25 || seen["grafana/host2/cpu_percent"] != 50 || seen["controller/host1/cpu_percent"] != 12.5 {
		t.Fatalf("services merged by scope: %v", seen)
	}
	id := strings.Repeat("a", 32)
	s.resourceMeasurementsLoaded = true
	s.resourceMeasurements = []resourceMeasurement{{ID: id, StartedAt: now.Add(-time.Minute), EndedAt: now, EndReason: "stopped"}}
	for _, kind := range []string{"samples", "summary"} {
		response := resultRequest(s, "GET", measurementsPath+"/"+id+"/export?format=csv&kind="+kind)
		rows, err := csv.NewReader(response.Body).ReadAll()
		if err != nil || response.Code != 200 {
			t.Fatalf("interval %d %v", response.Code, err)
		}
		services := map[string]bool{}
		for _, row := range rows[1:] {
			offset := len(row) - 4
			if row[offset] == "service" {
				services[row[offset+1]+"/"+row[offset+2]] = true
			}
		}
		if len(services) != 4 {
			t.Fatalf("interval lost services %s %v", kind, services)
		}
	}
}

func TestServiceResourcesConcurrentReportsAndScrapes(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), Token: "test"}, nil)
	var wg sync.WaitGroup
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				now := time.Now().UTC()
				if i%2 == 0 {
					r := postServiceResources(s, model.ServiceResourceReport{NodeID: "node", ReportedAt: now, Services: []model.ServiceResources{{Service: "controller", State: "running", Resources: resourceFixture(now, 1)}}}, "test")
					if r.Code != 204 {
						t.Errorf("report %d", r.Code)
					}
				} else {
					s.serviceResourceSnapshot(now)
					if _, err := s.state.metrics.registry.Gather(); err != nil {
						t.Error(err)
					}
				}
			}
		}()
	}
	wg.Wait()
}
