package model

import (
	"math"
	"time"
)

// AgentResources covers the Agent container and its running managed Peers.
// CPUCores retains the raw occupied cores; CPUCapacityCores is the Docker host
// logical CPU count used for host-normalized percentages. Memory is in bytes.
// Partial samples sum only measured containers and retain their coverage.
type AgentResources struct {
	SampledAt             time.Time `json:"sampledAt"`
	CPUCapacityCores      int       `json:"cpuCapacityCores,omitempty"`
	CPUCores              *float64  `json:"cpuCores,omitempty"`
	MemoryUsageBytes      *uint64   `json:"memoryUsageBytes,omitempty"`
	MemoryWorkingSetBytes *uint64   `json:"memoryWorkingSetBytes,omitempty"`
	Containers            int       `json:"containers"`
	MeasuredContainers    int       `json:"measuredContainers"`
	Complete              bool      `json:"complete"`
	Error                 string    `json:"error,omitempty"`
}

const AgentResourceMaxAge = 30 * time.Second

func (r *AgentResources) Valid() bool {
	return r != nil && !r.SampledAt.IsZero() && r.Containers > 0 && r.MeasuredContainers > 0 && r.MeasuredContainers <= r.Containers &&
		(!r.Complete || r.MeasuredContainers == r.Containers) &&
		r.CPUCores != nil && !math.IsNaN(*r.CPUCores) && !math.IsInf(*r.CPUCores, 0) && *r.CPUCores >= 0 &&
		r.MemoryUsageBytes != nil && r.MemoryWorkingSetBytes != nil && *r.MemoryWorkingSetBytes <= *r.MemoryUsageBytes
}

func (r *AgentResources) Fresh(now time.Time) bool {
	return r.Valid() && now.Sub(r.SampledAt) >= -5*time.Second && now.Sub(r.SampledAt) <= AgentResourceMaxAge
}

// CPUPercent is KPL container usage as a percentage of the whole Docker host.
// A missing denominator (including older Agents) never implies a one-core host.
func (r *AgentResources) CPUPercent() (float64, bool) {
	if !r.Valid() || r.CPUCapacityCores <= 0 {
		return 0, false
	}
	percent := *r.CPUCores / float64(r.CPUCapacityCores) * 100
	if math.IsNaN(percent) || math.IsInf(percent, 0) {
		return 0, false
	}
	return percent, true
}
