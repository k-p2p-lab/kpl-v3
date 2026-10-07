package model

import "time"

// Network preparation is outside the experiment clock. Epoch identifies one
// start attempt; NetworkID fences delayed admissions against a replaced overlay.
type ExperimentNetworkRequest struct {
	RunID       string            `json:"runId"`
	Epoch       string            `json:"epoch"`
	RequestedAt time.Time         `json:"requestedAt"`
	Agents      map[string]string `json:"agents,omitempty"`
}

type ExperimentNetwork struct {
	RunID          string `json:"runId"`
	Epoch          string `json:"epoch"`
	NetworkID      string `json:"networkId"`
	NetworkName    string `json:"networkName"`
	GatewayURL     string `json:"gatewayUrl"`
	PeerGatewayURL string `json:"peerGatewayUrl"`
}
