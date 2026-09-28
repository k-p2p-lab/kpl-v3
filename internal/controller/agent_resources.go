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
	if len(copy.Error) > 256 {
		copy.Error = copy.Error[:256]
	}
	return &copy
}

type agentResourceCollector struct {
	state                                                                          *state
	cpu, cpuPercent, cpuCapacity, memory, working, timestamp, complete, containers *prometheus.Desc
}

func newAgentResourceCollector(s *state) *agentResourceCollector {
	labels := []string{"agent_id"}
	return &agentResourceCollector{state: s,
		cpu:         prometheus.NewDesc("kpl_agent_cpu_usage_cores", "Raw CPU cores used by measured Agent and managed Peer containers. Fresh partial samples are included.", labels, nil),
		cpuPercent:  prometheus.NewDesc("kpl_agent_cpu_usage_percent", "CPU used by measured KPL containers as a percentage of Docker host logical CPU capacity (whole host = 100 percent).", labels, nil),
		cpuCapacity: prometheus.NewDesc("kpl_agent_cpu_capacity_cores", "Docker host logical CPU count for the corresponding fresh KPL CPU percentage.", labels, nil),
		memory:      prometheus.NewDesc("kpl_agent_memory_usage_bytes", "Total cgroup memory usage of the Agent container plus its running managed Peer containers, including cache.", labels, nil),
		working:     prometheus.NewDesc("kpl_agent_memory_working_set_bytes", "Agent plus managed Peer cgroup memory minus inactive file cache, matching Linux docker stats memory usage.", labels, nil),
		timestamp:   prometheus.NewDesc("kpl_agent_resource_sample_timestamp_seconds", "Latest KPL resource sample time normalized to the Controller clock.", labels, nil),
		complete:    prometheus.NewDesc("kpl_agent_resource_sample_complete", "1 if the Agent is online and its latest KPL resource sample is complete and fresh; otherwise 0.", labels, nil),
		containers:  prometheus.NewDesc("kpl_agent_resource_containers", "Containers in the resource inventory or successfully measured in a fresh online sample, including the Agent itself.", []string{"agent_id", "kind"}, nil),
	}
}
func (c *agentResourceCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.cpu, c.cpuPercent, c.cpuCapacity, c.memory, c.working, c.timestamp, c.complete, c.containers} {
		ch <- d
	}
}
func (c *agentResourceCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()
	for _, agent := range c.state.agentInventory() {
		if agent.Disabled {
			continue
		}
		r := agent.Resources
		fresh := agentIsOnline(agent, now) && r.Fresh(now)
		flag := 0.
		if fresh && r.Complete {
			flag = 1
		}
		ch <- prometheus.MustNewConstMetric(c.complete, prometheus.GaugeValue, flag, agent.ID)
		if r == nil {
			continue
		}
		if !r.SampledAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.timestamp, prometheus.GaugeValue, float64(r.SampledAt.UnixNano())/1e9, agent.ID)
		}
		ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(r.Containers), agent.ID, "expected")
		measured := 0
		if fresh {
			measured = r.MeasuredContainers
		}
		ch <- prometheus.MustNewConstMetric(c.containers, prometheus.GaugeValue, float64(measured), agent.ID, "measured")
		if !fresh {
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.cpu, prometheus.GaugeValue, *r.CPUCores, agent.ID)
		if percent, ok := r.CPUPercent(); ok {
			ch <- prometheus.MustNewConstMetric(c.cpuPercent, prometheus.GaugeValue, percent, agent.ID)
			ch <- prometheus.MustNewConstMetric(c.cpuCapacity, prometheus.GaugeValue, float64(r.CPUCapacityCores), agent.ID)
		}
		ch <- prometheus.MustNewConstMetric(c.memory, prometheus.GaugeValue, float64(*r.MemoryUsageBytes), agent.ID)
		ch <- prometheus.MustNewConstMetric(c.working, prometheus.GaugeValue, float64(*r.MemoryWorkingSetBytes), agent.ID)
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
	result := agentResourceTotals{}
	var normalizedCores float64
	for _, a := range agents {
		if a.Disabled {
			continue
		}
		result.TotalAgents++
		if !agentIsOnline(a, now) || !a.Resources.Fresh(now) {
			continue
		}
		result.CPUCores += *a.Resources.CPUCores
		result.MemoryUsageBytes += *a.Resources.MemoryUsageBytes
		result.MemoryWorkingSetBytes += *a.Resources.MemoryWorkingSetBytes
		result.MeasuredAgents++
		result.Containers += a.Resources.Containers
		result.MeasuredContainers += a.Resources.MeasuredContainers
		if !a.Resources.Complete {
			result.PartialAgents++
		}
		if _, ok := a.Resources.CPUPercent(); ok {
			normalizedCores += *a.Resources.CPUCores
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
func (s *Server) handleAgentResources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	now, agents := time.Now().UTC(), s.state.agentInventory()
	totals := sumAgentResources(agents, now)
	if r.URL.Query().Get("format") != "csv" {
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, struct {
			GeneratedAt time.Time           `json:"generatedAt"`
			Agents      []model.Agent       `json:"agents"`
			Totals      agentResourceTotals `json:"totals"`
		}{now, agents, totals})
		return
	}
	resourceCSVHeaders(w, "kpl-agent-resources-current")
	out := csv.NewWriter(w)
	defer out.Flush()
	_ = out.Write([]string{"agent_id", "agent_name", "scope", "status", "sampled_at_utc", "cpu_percent_host", "cpu_capacity_cores", "memory_working_set_bytes", "memory_usage_bytes", "containers", "measured_containers"})
	for _, a := range agents {
		row := []string{resourceCSVCell(a.ID), resourceCSVCell(a.Name), "agent_and_peers", "unavailable", "", "", "", "", "", "", ""}
		if a.Disabled {
			row[3] = "disabled"
			if err := out.Write(row); err != nil {
				return
			}
			continue
		}
		if a.Resources != nil {
			x := a.Resources
			row[4], row[9], row[10] = x.SampledAt.Format(time.RFC3339Nano), strconv.Itoa(x.Containers), strconv.Itoa(x.MeasuredContainers)
			if agentIsOnline(a, now) && x.Fresh(now) {
				row[3] = "complete"
				if !x.Complete {
					row[3] = "partial"
				}
				if percent, ok := x.CPUPercent(); ok {
					row[5], row[6] = csvFloat(percent), strconv.Itoa(x.CPUCapacityCores)
				}
				row[7], row[8] = strconv.FormatUint(*x.MemoryWorkingSetBytes, 10), strconv.FormatUint(*x.MemoryUsageBytes, 10)
			} else if !agentIsOnline(a, now) {
				row[3] = "offline"
			} else if x.Valid() {
				row[3] = "stale"
			} else {
				row[3] = "incomplete"
			}
		}
		if err := out.Write(row); err != nil {
			return
		}
	}
	if totals.MeasuredAgents > 0 {
		percent, capacity := "", ""
		if totals.CPUPercent != nil {
			percent, capacity = csvFloat(*totals.CPUPercent), strconv.Itoa(totals.CPUCapacityCores)
		}
		_ = out.Write([]string{"TOTAL", "", "agent_and_peers", fmt.Sprintf("%d/%d agents; %d partial; CPU %d/%d agents", totals.MeasuredAgents, totals.TotalAgents, totals.PartialAgents, totals.CPUMeasuredAgents, totals.TotalAgents), now.Format(time.RFC3339Nano), percent, capacity, strconv.FormatUint(totals.MemoryWorkingSetBytes, 10), strconv.FormatUint(totals.MemoryUsageBytes, 10), strconv.Itoa(totals.Containers), strconv.Itoa(totals.MeasuredContainers)})
	}
}
