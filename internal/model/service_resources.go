package model

import "time"

// Service resources are separate from Agent/Peer totals. IDs identify a Swarm
// service on one Docker host, so replicas on different hosts never share a CPU
// denominator or get merged into one time series.
type ServiceResources struct {
	Service   string          `json:"service"`
	State     string          `json:"state"`
	Resources *AgentResources `json:"resources,omitempty"`
}

type ServiceResourceReport struct {
	NodeID     string             `json:"nodeId"`
	NodeName   string             `json:"nodeName"`
	ReportedAt time.Time          `json:"reportedAt"`
	Services   []ServiceResources `json:"services"`
	Error      string             `json:"error,omitempty"`
}

func ServiceResourceScope(service string) string {
	if service == "controller" {
		return "controller"
	}
	if service == "resource-monitor" {
		return "monitor"
	}
	return "third_party"
}
