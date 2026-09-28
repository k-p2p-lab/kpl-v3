package controller

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/auth"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

type serviceResourceEntry struct {
	report   model.ServiceResourceReport
	received time.Time
}
type serviceResourceStore struct {
	mu    sync.RWMutex
	nodes map[string]serviceResourceEntry
}
type serviceResourceView struct {
	model.ServiceResources
	Scope      string    `json:"scope"`
	NodeID     string    `json:"nodeId"`
	NodeName   string    `json:"nodeName"`
	ReportedAt time.Time `json:"reportedAt,omitempty"`
	Error      string    `json:"error,omitempty"`
}

var serviceResourceID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func (s *Server) handleServiceResourceReport(w http.ResponseWriter, r *http.Request) {
	// Reports are machine telemetry, never a browser mutation.
	if s.config.Token == "" || !auth.Equal(r.Header.Get("Authorization"), "Bearer "+s.config.Token) {
		writeError(w, 401, "service authentication required")
		return
	}
	var report model.ServiceResourceReport
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&report) != nil || decoder.Decode(new(any)) != io.EOF || !serviceResourceID.MatchString(report.NodeID) || len(report.NodeName) > 128 || report.ReportedAt.IsZero() || len(report.Services) > 64 || len(report.Error) > 256 {
		writeError(w, 400, "invalid service resource report")
		return
	}
	now := time.Now().UTC()
	seen := map[string]bool{}
	for i := range report.Services {
		item := &report.Services[i]
		if !serviceResourceID.MatchString(item.Service) || item.Service == "agent" || seen[item.Service] || (item.State != "running" && item.State != "not_running" && item.State != "unknown") {
			writeError(w, 400, "invalid service resource identity or state")
			return
		}
		seen[item.Service] = true
		if r := item.Resources; r != nil {
			if r.Containers < 0 || r.Containers > 512 || r.MeasuredContainers < 0 || r.MeasuredContainers > r.Containers || r.CPUCapacityCores < 0 || r.CPUCapacityCores > 1048576 {
				writeError(w, 400, "invalid service resource coverage")
				return
			}
			item.Resources = normalizeAgentResources(r, report.ReportedAt, now)
			item.Resources.Agent = nil
			item.Resources.Peers = nil
		}
		if item.State != "running" || report.Error != "" {
			item.Resources = nil
		}
	}
	cache := s.serviceResources
	cache.mu.Lock()
	for id, entry := range cache.nodes {
		if now.Sub(entry.received) > 10*time.Minute {
			delete(cache.nodes, id)
		}
	}
	previous, exists := cache.nodes[report.NodeID]
	if exists && !report.ReportedAt.After(previous.report.ReportedAt) {
		cache.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !exists && len(cache.nodes) >= 512 {
		cache.mu.Unlock()
		writeError(w, 503, "service resource inventory is full")
		return
	}
	cache.nodes[report.NodeID] = serviceResourceEntry{report: report, received: now}
	cache.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) serviceResourceSnapshot(now time.Time) []serviceResourceView {
	rows := []serviceResourceView{}
	found := map[string]bool{}
	s.serviceResources.mu.RLock()
	for _, entry := range s.serviceResources.nodes {
		if now.Sub(entry.received) > 10*time.Minute {
			continue
		}
		for _, item := range entry.report.Services {
			row := serviceResourceView{ServiceResources: item, Scope: model.ServiceResourceScope(item.Service), NodeID: entry.report.NodeID, NodeName: entry.report.NodeName, ReportedAt: entry.received, Error: entry.report.Error}
			if entry.report.Error != "" {
				row.State = "unknown"
			}
			if now.Sub(entry.received) > 30*time.Second {
				row.State = "stale"
			}
			rows = append(rows, row)
			found[row.Service] = true
		}
	}
	s.serviceResources.mu.RUnlock()
	for _, name := range []string{"controller", "prometheus", "grafana"} {
		if !found[name] {
			rows = append(rows, serviceResourceView{ServiceResources: model.ServiceResources{Service: name, State: "awaiting"}, Scope: model.ServiceResourceScope(name)})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if (a.Service == "controller") != (b.Service == "controller") {
			return a.Service == "controller"
		}
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		return a.NodeID < b.NodeID
	})
	return rows
}
func (s *Server) handleServiceResources(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, struct {
		GeneratedAt             time.Time             `json:"generatedAt"`
		ControllerUptimeSeconds float64               `json:"controllerUptimeSeconds"`
		Services                []serviceResourceView `json:"services"`
	}{now, now.Sub(s.startedAt).Seconds(), s.serviceResourceSnapshot(now)})
}

type serviceResourceCollector struct {
	server         *Server
	resources      resourceMetricDescriptors
	running, fresh *prometheus.Desc
}

func newServiceResourceCollector(s *Server) *serviceResourceCollector {
	labels := []string{"service", "node_id", "scope"}
	return &serviceResourceCollector{server: s, resources: resourceDescriptors("kpl_service_", labels),
		running: prometheus.NewDesc("kpl_service_running", "1 if service containers were observed running, 0 if absent; omitted for unknown or stale inventory.", labels, nil),
		fresh:   prometheus.NewDesc("kpl_service_resource_report_fresh", "1 if the service inventory report is fresh and successful; otherwise 0.", labels, nil)}
}
func (c *serviceResourceCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.resources.all() {
		ch <- d
	}
	ch <- c.running
	ch <- c.fresh
}
func (c *serviceResourceCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	d := c.resources
	for _, row := range c.server.serviceResourceSnapshot(now) {
		if row.NodeID == "" {
			continue
		}
		labels := []string{row.Service, row.NodeID, row.Scope}
		gauge := func(desc *prometheus.Desc, value float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
		}
		fresh := row.State == "running" || row.State == "not_running"
		flag := 0.
		if fresh {
			flag = 1
		}
		gauge(c.fresh, flag)
		if fresh {
			flag = 0
			if row.State == "running" {
				flag = 1
			}
			gauge(c.running, flag)
		}
		r := row.Resources
		valid := row.State == "running" && r.Fresh(now)
		flag = 0
		if valid && r.Complete {
			flag = 1
		}
		gauge(d.complete, flag)
		expected, measured := 0, 0
		if r != nil {
			expected = r.Containers
			if valid {
				measured = r.MeasuredContainers
			}
			if !r.SampledAt.IsZero() {
				gauge(d.timestamp, float64(r.SampledAt.UnixNano())/1e9)
			}
		}
		ch <- prometheus.MustNewConstMetric(d.containers, prometheus.GaugeValue, float64(expected), append(labels, "expected")...)
		ch <- prometheus.MustNewConstMetric(d.containers, prometheus.GaugeValue, float64(measured), append(labels, "measured")...)
		if !valid {
			continue
		}
		gauge(d.cpu, *r.CPUCores)
		gauge(d.memory, float64(*r.MemoryUsageBytes))
		gauge(d.working, float64(*r.MemoryWorkingSetBytes))
		if percent, ok := r.CPUPercent(); ok {
			gauge(d.cpuPercent, percent)
			gauge(d.cpuCapacity, float64(r.CPUCapacityCores))
		}
	}
}

// Existing Agent columns stay in place; explicit entity columns distinguish
// service rows without putting service identities into agent_id.
var resourceEntityHeaders = []string{"entity_type", "service", "node_id", "node_name"}

func serviceResourceCSVRow(row serviceResourceView, now time.Time) []string {
	values := []string{"", "", row.Scope, row.State, "", "", "", "", "", "", "", "service", resourceCSVCell(row.Service), resourceCSVCell(row.NodeID), resourceCSVCell(row.NodeName)}
	if r := row.Resources; r != nil {
		values[4] = r.SampledAt.Format(time.RFC3339Nano)
		values[9] = strconv.Itoa(r.Containers)
		values[10] = "0"
		if row.State == "running" && r.Fresh(now) {
			values[3] = "complete"
			if !r.Complete {
				values[3] = "partial"
			}
			if percent, ok := r.CPUPercent(); ok {
				values[5] = csvFloat(percent)
				values[6] = strconv.Itoa(r.CPUCapacityCores)
			}
			values[7] = strconv.FormatUint(*r.MemoryWorkingSetBytes, 10)
			values[8] = strconv.FormatUint(*r.MemoryUsageBytes, 10)
			values[10] = strconv.Itoa(r.MeasuredContainers)
		} else if row.State == "running" {
			values[3] = "unavailable"
		}
	}
	return values
}
func (s *Server) writeServiceResourceCSV(out *csv.Writer, now time.Time) {
	for _, row := range s.serviceResourceSnapshot(now) {
		if err := out.Write(serviceResourceCSVRow(row, now)); err != nil {
			return
		}
	}
}

func resourceHistoryEntity(metric map[string]string) (kind, service, node, scope string) {
	if !strings.HasPrefix(metric["__name__"], "kpl_service_") {
		return "agent", "", "", model.ResourceScopeTotal
	}
	service, node = metric["service"], metric["node_id"]
	if !serviceResourceID.MatchString(service) || !serviceResourceID.MatchString(node) {
		return "", "", "", ""
	}
	return "service", service, node, model.ServiceResourceScope(service)
}
