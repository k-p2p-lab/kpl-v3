package model

import "time"

const HostRebootAuthHeader = "X-KPL-Host-Reboot-Auth"

// AgentHost identifies the Docker host, separately from the Agent container.
// RebootSupported is a checked capability, not proof of a completed reboot.
type AgentHost struct {
	NodeID          string `json:"nodeId"`
	Hostname        string `json:"hostname"`
	BootID          string `json:"bootId"`
	RebootEnabled   bool   `json:"rebootEnabled"`
	RebootSupported bool   `json:"rebootSupported"`
	RebootReason    string `json:"rebootReason,omitempty"`
	RebootPending   bool   `json:"rebootPending,omitempty"`
}

type HostRebootRequest struct {
	OperationID    string    `json:"operationId"`
	NodeID         string    `json:"nodeId"`
	BootID         string    `json:"bootId"`
	AgentStartedAt time.Time `json:"agentStartedAt"`
}

// Requested acknowledges the command, while unknown never authorizes a retry.
// A different boot ID and a ready Agent are required to observe completion.
type HostRebootResult struct {
	OperationID string `json:"operationId"`
	NodeID      string `json:"nodeId"`
	BootID      string `json:"bootId"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
	Signature   string `json:"signature,omitempty"`
}

type HostRebootProofRequest struct {
	Nonce string `json:"nonce"`
}

type HostRebootProof struct {
	Nonce             string    `json:"nonce"`
	AgentID           string    `json:"agentId"`
	NodeID            string    `json:"nodeId"`
	BootID            string    `json:"bootId"`
	AgentStartedAt    time.Time `json:"agentStartedAt"`
	StartupReconciled bool      `json:"startupReconciled"`
	RebootPending     bool      `json:"rebootPending"`
	RebootSupported   bool      `json:"rebootSupported"`
	Signature         string    `json:"signature,omitempty"`
}
