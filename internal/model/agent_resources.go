package model

import (
	"math"
	"time"
)

const (
	ResourceScopeTotal = "agent_and_peers"
	ResourceScopeAgent = "agent"
	ResourceScopePeers = "peers"
)

var ResourceScopes = [...]string{ResourceScopeTotal, ResourceScopeAgent, ResourceScopePeers}

// ContainerResources is one part of a sample, sharing its timestamp and host
// CPU capacity. Missing values are unavailable, not zero usage.
type ContainerResources struct {
	CPUCores              *float64 `json:"cpuCores,omitempty"`
	MemoryUsageBytes      *uint64  `json:"memoryUsageBytes,omitempty"`
	MemoryWorkingSetBytes *uint64  `json:"memoryWorkingSetBytes,omitempty"`
	Containers            int      `json:"containers"`
	MeasuredContainers    int      `json:"measuredContainers"`
	Complete              bool     `json:"complete"`
}

func (r *ContainerResources) Valid(allowEmpty bool) bool {
	if r == nil || r.Containers < 0 || r.MeasuredContainers < 0 || r.MeasuredContainers > r.Containers ||
		(r.Complete && r.MeasuredContainers != r.Containers) ||
		r.CPUCores == nil || math.IsNaN(*r.CPUCores) || math.IsInf(*r.CPUCores, 0) || *r.CPUCores < 0 ||
		r.MemoryUsageBytes == nil || r.MemoryWorkingSetBytes == nil || *r.MemoryWorkingSetBytes > *r.MemoryUsageBytes {
		return false
	}
	if r.Containers == 0 {
		return allowEmpty && r.Complete && *r.CPUCores == 0 && *r.MemoryUsageBytes == 0 && *r.MemoryWorkingSetBytes == 0
	}
	return r.MeasuredContainers > 0
}

func (r *ContainerResources) Clone() *ContainerResources {
	if r == nil {
		return nil
	}
	out := *r
	if r.CPUCores != nil {
		value := *r.CPUCores
		out.CPUCores = &value
	}
	if r.MemoryUsageBytes != nil {
		value := *r.MemoryUsageBytes
		out.MemoryUsageBytes = &value
	}
	if r.MemoryWorkingSetBytes != nil {
		value := *r.MemoryWorkingSetBytes
		out.MemoryWorkingSetBytes = &value
	}
	return &out
}

// AgentResources covers the Agent container and its running managed Peers.
// CPUCores retains the raw occupied cores; CPUCapacityCores is the Docker host
// logical CPU count used for host-normalized percentages. Memory is in bytes.
// Partial samples sum only measured containers and retain their coverage.
type AgentResources struct {
	Agent                 *ContainerResources `json:"agent,omitempty"`
	Peers                 *ContainerResources `json:"peers,omitempty"`
	SampledAt             time.Time           `json:"sampledAt"`
	CPUCapacityCores      int                 `json:"cpuCapacityCores,omitempty"`
	CPUCores              *float64            `json:"cpuCores,omitempty"`
	MemoryUsageBytes      *uint64             `json:"memoryUsageBytes,omitempty"`
	MemoryWorkingSetBytes *uint64             `json:"memoryWorkingSetBytes,omitempty"`
	Containers            int                 `json:"containers"`
	MeasuredContainers    int                 `json:"measuredContainers"`
	Complete              bool                `json:"complete"`
	Error                 string              `json:"error,omitempty"`
}

const AgentResourceMaxAge = 30 * time.Second

// Usage preserves the existing total fields and exposes optional breakdowns.
func (r *AgentResources) Usage(scope string) *ContainerResources {
	if r == nil {
		return nil
	}
	switch scope {
	case ResourceScopeTotal:
		return &ContainerResources{CPUCores: r.CPUCores, MemoryUsageBytes: r.MemoryUsageBytes, MemoryWorkingSetBytes: r.MemoryWorkingSetBytes, Containers: r.Containers, MeasuredContainers: r.MeasuredContainers, Complete: r.Complete}
	case ResourceScopeAgent:
		return r.Agent
	case ResourceScopePeers:
		return r.Peers
	default:
		return nil
	}
}
func (r *AgentResources) ScopeValid(scope string) bool {
	u := r.Usage(scope)
	return r != nil && !r.SampledAt.IsZero() && u.Valid(scope == ResourceScopePeers) && (scope != ResourceScopeAgent || u.Containers == 1)
}
func (r *AgentResources) Valid() bool { return r.ScopeValid(ResourceScopeTotal) }
func (r *AgentResources) ScopeFresh(now time.Time, scope string) bool {
	return r.ScopeValid(scope) && now.Sub(r.SampledAt) >= -5*time.Second && now.Sub(r.SampledAt) <= AgentResourceMaxAge
}
func (r *AgentResources) Fresh(now time.Time) bool { return r.ScopeFresh(now, ResourceScopeTotal) }

// Each scope uses the same whole-host denominator. Unknown CPU capacity never
// implies a one-core host, even when only one Agent container is measured.
func (r *AgentResources) ScopeCPUPercent(scope string) (float64, bool) {
	if !r.ScopeValid(scope) || r.CPUCapacityCores <= 0 {
		return 0, false
	}
	percent := *r.Usage(scope).CPUCores / float64(r.CPUCapacityCores) * 100
	if math.IsNaN(percent) || math.IsInf(percent, 0) {
		return 0, false
	}
	return percent, true
}
func (r *AgentResources) CPUPercent() (float64, bool) { return r.ScopeCPUPercent(ResourceScopeTotal) }
