package controller

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/prometheus/client_golang/prometheus"
)

func normalizeAgentResources(value *model.AgentResources, reportedAt, receivedAt time.Time) *model.AgentResources {
	if value == nil {
		return nil
	}
	copy := *value
	if value.SampledAt.IsZero() || reportedAt.IsZero() || value.SampledAt.After(reportedAt.Add(5*time.Second)) {
		copy.Complete = false
		copy.SampledAt = time.Time{}
	} else {
		copy.SampledAt = topologyReportTime(receivedAt, reportedAt, value.SampledAt)
	}
	if !copy.Valid() {
		copy.Complete = false
		copy.CPUCores, copy.MemoryUsageBytes, copy.MemoryWorkingSetBytes = nil, nil, nil
	}
	if copy.Valid() {
		cpu, usage, working := *copy.CPUCores, *copy.MemoryUsageBytes, *copy.MemoryWorkingSetBytes
		copy.CPUCores, copy.MemoryUsageBytes, copy.MemoryWorkingSetBytes = &cpu, &usage, &working
	}
	copy.Agent, copy.Peers = value.Agent.Clone(), value.Peers.Clone()
	for _, scope := range []string{model.ResourceScopeAgent, model.ResourceScopePeers} {
		if u := copy.Usage(scope); u != nil && !copy.ScopeValid(scope) {
			u.Complete = false
			u.CPUCores, u.MemoryUsageBytes, u.MemoryWorkingSetBytes = nil, nil, nil
		}
	}
	if len(copy.Error) > 256 {
		copy.Error = copy.Error[:256]
	}
	return &copy
}

type resourceMetricDescriptors struct {
	cpu, cpuPercent, cpuCapacity, memory, working, timestamp, complete, containers *prometheus.Desc
}
type agentResourceCollector struct {
	state            *state
	total, component resourceMetricDescriptors
}

func resourceDescriptors(prefix string, labels []string) resourceMetricDescriptors {
	return resourceMetricDescriptors{
		cpu:         prometheus.NewDesc(prefix+"cpu_usage_cores", "Raw CPU cores used by measured containers in this scope. Fresh partial samples are included.", labels, nil),
		cpuPercent:  prometheus.NewDesc(prefix+"cpu_usage_percent", "Measured container CPU as a percentage of the whole Docker host (100 percent).", labels, nil),
		cpuCapacity: prometheus.NewDesc(prefix+"cpu_capacity_cores", "Docker host logical CPU count for the corresponding fresh scoped CPU percentage.", labels, nil),
		memory:      prometheus.NewDesc(prefix+"memory_usage_bytes", "Measured container cgroup memory usage including cache.", labels, nil),
		working:     prometheus.NewDesc(prefix+"memory_working_set_bytes", "Measured container memory minus inactive file cache, matching Linux docker stats.", labels, nil),
		timestamp:   prometheus.NewDesc(prefix+"resource_sample_timestamp_seconds", "Resource sample time normalized to the Controller clock.", labels, nil),
		complete:    prometheus.NewDesc(prefix+"resource_sample_complete", "1 for a complete, fresh online sample in this scope; otherwise 0.", labels, nil),
		containers:  prometheus.NewDesc(prefix+"resource_containers", "Expected or successfully measured containers in this scope.", append(append([]string{}, labels...), "kind"), nil),
	}
}
func newAgentResourceCollector(s *state) *agentResourceCollector {
	return &agentResourceCollector{state: s,
		total:     resourceDescriptors("kpl_agent_", []string{"agent_id"}),
		component: resourceDescriptors("kpl_agent_component_", []string{"agent_id", "component"}),
	}
}
func (d resourceMetricDescriptors) all() []*prometheus.Desc {
	return []*prometheus.Desc{d.cpu, d.cpuPercent, d.cpuCapacity, d.memory, d.working, d.timestamp, d.complete, d.containers}
}
func (c *agentResourceCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range append(c.total.all(), c.component.all()...) {
		ch <- d
	}
}
func (c *agentResourceCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	for _, agent := range c.state.agentInventory() {
		if agent.Disabled {
			continue
		}
		for _, scope := range model.ResourceScopes {
			d, labels := c.total, []string{agent.ID}
			if scope != model.ResourceScopeTotal {
				d, labels = c.component, append(labels, scope)
			}
			r := agent.Resources
			u := r.Usage(scope)
			// Legacy Agents have no component samples; never invent zeros.
			if scope != model.ResourceScopeTotal && u == nil {
				continue
			}
			fresh := agentIsOnline(agent, now) && r.ScopeFresh(now, scope)
			flag := 0.
			if fresh && u.Complete {
				flag = 1
			}
			ch <- prometheus.MustNewConstMetric(d.complete, prometheus.GaugeValue, flag, labels...)
			if r == nil || u == nil {
				continue
			}
			if !r.SampledAt.IsZero() {
				ch <- prometheus.MustNewConstMetric(d.timestamp, prometheus.GaugeValue, float64(r.SampledAt.UnixNano())/1e9, labels...)
			}
			ch <- prometheus.MustNewConstMetric(d.containers, prometheus.GaugeValue, float64(u.Containers), append(labels, "expected")...)
			measured := 0
			if fresh {
				measured = u.MeasuredContainers
			}
			ch <- prometheus.MustNewConstMetric(d.containers, prometheus.GaugeValue, float64(measured), append(labels, "measured")...)
			if !fresh {
				continue
			}
			ch <- prometheus.MustNewConstMetric(d.cpu, prometheus.GaugeValue, *u.CPUCores, labels...)
			if percent, ok := r.ScopeCPUPercent(scope); ok {
				ch <- prometheus.MustNewConstMetric(d.cpuPercent, prometheus.GaugeValue, percent, labels...)
				ch <- prometheus.MustNewConstMetric(d.cpuCapacity, prometheus.GaugeValue, float64(r.CPUCapacityCores), labels...)
			}
			ch <- prometheus.MustNewConstMetric(d.memory, prometheus.GaugeValue, float64(*u.MemoryUsageBytes), labels...)
			ch <- prometheus.MustNewConstMetric(d.working, prometheus.GaugeValue, float64(*u.MemoryWorkingSetBytes), labels...)
		}
	}
}

type agentResourceTotals struct {
	CPUPercent            *float64 `json:"cpuPercent,omitempty"`
	CPUCapacityCores      int      `json:"cpuCapacityCores"`
	CPUMeasuredAgents     int      `json:"cpuMeasuredAgents"`
	Containers            int      `json:"containers"`
	MeasuredContainers    int      `json:"measuredContainers"`
	PartialAgents         int      `json:"partialAgents"`
	CPUCores              float64  `json:"cpuCores"`
	MemoryUsageBytes      uint64   `json:"memoryUsageBytes"`
	MemoryWorkingSetBytes uint64   `json:"memoryWorkingSetBytes"`
	MeasuredAgents        int      `json:"measuredAgents"`
	TotalAgents           int      `json:"totalAgents"`
}

func sumAgentResources(agents []model.Agent, now time.Time) agentResourceTotals {
	return sumAgentResourceScope(agents, now, model.ResourceScopeTotal)
}
func sumAgentResourceScope(agents []model.Agent, now time.Time, scope string) agentResourceTotals {
	result := agentResourceTotals{}
	var normalizedCores float64
	for _, a := range agents {
		if a.Disabled {
			continue
		}
		result.TotalAgents++
		if !agentIsOnline(a, now) || !a.Resources.ScopeFresh(now, scope) {
			continue
		}
		u := a.Resources.Usage(scope)
		result.CPUCores += *u.CPUCores
		result.MemoryUsageBytes += *u.MemoryUsageBytes
		result.MemoryWorkingSetBytes += *u.MemoryWorkingSetBytes
		result.MeasuredAgents++
		result.Containers += u.Containers
		result.MeasuredContainers += u.MeasuredContainers
		if !u.Complete {
			result.PartialAgents++
		}
		if _, ok := a.Resources.ScopeCPUPercent(scope); ok {
			normalizedCores += *u.CPUCores
			result.CPUCapacityCores += a.Resources.CPUCapacityCores
			result.CPUMeasuredAgents++
		}
	}
	if result.CPUCapacityCores > 0 {
		percent := normalizedCores / float64(result.CPUCapacityCores) * 100
		result.CPUPercent = &percent
	}
	return result
}

// CSV cells with user-controlled identifiers must not become spreadsheet formulas.
func resourceCSVCell(value string) string {
	if trimmed := strings.TrimLeft(value, " \t\r\n"); trimmed != "" && strings.ContainsRune("=+-@", rune(trimmed[0])) {
		return "'" + value
	}
	return value
}
func csvFloat(value float64) string { return strconv.FormatFloat(value, 'f', 6, 64) }
func resourceCSVHeaders(w http.ResponseWriter, name string) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.csv"`, name))
	w.Header().Set("Cache-Control", "no-store")
}
func resourceCurrentCSVRow(a model.Agent, scope string, now time.Time) []string {
	row := []string{resourceCSVCell(a.ID), resourceCSVCell(a.Name), scope, "unavailable", "", "", "", "", "", "", ""}
	if a.Disabled {
		row[3] = "disabled"
		return row
	}
	r, u := a.Resources, a.Resources.Usage(scope)
	if r == nil || u == nil {
		return row
	}
	row[4], row[9], row[10] = r.SampledAt.Format(time.RFC3339Nano), strconv.Itoa(u.Containers), strconv.Itoa(u.MeasuredContainers)
	if agentIsOnline(a, now) && r.ScopeFresh(now, scope) {
		row[3] = "complete"
		if !u.Complete {
			row[3] = "partial"
		}
		if percent, ok := r.ScopeCPUPercent(scope); ok {
			row[5], row[6] = csvFloat(percent), strconv.Itoa(r.CPUCapacityCores)
		}
		row[7], row[8] = strconv.FormatUint(*u.MemoryWorkingSetBytes, 10), strconv.FormatUint(*u.MemoryUsageBytes, 10)
	} else if !agentIsOnline(a, now) {
		row[3] = "offline"
	} else if r.ScopeValid(scope) {
		row[3] = "stale"
	} else {
		row[3] = "incomplete"
	}
	return row
}
func (s *Server) handleAgentResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	now, agents := time.Now().UTC(), s.state.agentInventory()
	byScope := make(map[string]agentResourceTotals, len(model.ResourceScopes))
	for _, scope := range model.ResourceScopes {
		byScope[scope] = sumAgentResourceScope(agents, now, scope)
	}
	if r.URL.Query().Get("format") != "csv" {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, struct {
			GeneratedAt   time.Time                      `json:"generatedAt"`
			Agents        []model.Agent                  `json:"agents"`
			Totals        agentResourceTotals            `json:"totals"`
			TotalsByScope map[string]agentResourceTotals `json:"totalsByScope"`
		}{now, agents, byScope[model.ResourceScopeTotal], byScope})
		return
	}
	resourceCSVHeaders(w, "kpl-agent-resources-current")
	out := csv.NewWriter(w)
	defer out.Flush()
	_ = out.Write([]string{"agent_id", "agent_name", "scope", "status", "sampled_at_utc", "cpu_percent_host", "cpu_capacity_cores", "memory_working_set_bytes", "memory_usage_bytes", "containers", "measured_containers"})
	for _, a := range agents {
		for _, scope := range model.ResourceScopes {
			if scope != model.ResourceScopeTotal && a.Resources.Usage(scope) == nil {
				continue
			}
			if err := out.Write(resourceCurrentCSVRow(a, scope, now)); err != nil {
				return
			}
		}
	}
	for _, scope := range model.ResourceScopes {
		totals := byScope[scope]
		if totals.MeasuredAgents == 0 {
			continue
		}
		percent, capacity := "", ""
		if totals.CPUPercent != nil {
			percent, capacity = csvFloat(*totals.CPUPercent), strconv.Itoa(totals.CPUCapacityCores)
		}
		if err := out.Write([]string{"TOTAL", "", scope, fmt.Sprintf("%d/%d agents; %d partial; CPU %d/%d agents", totals.MeasuredAgents, totals.TotalAgents, totals.PartialAgents, totals.CPUMeasuredAgents, totals.TotalAgents), now.Format(time.RFC3339Nano), percent, capacity, strconv.FormatUint(totals.MemoryWorkingSetBytes, 10), strconv.FormatUint(totals.MemoryUsageBytes, 10), strconv.Itoa(totals.Containers), strconv.Itoa(totals.MeasuredContainers)}); err != nil {
			return
		}
	}
}
