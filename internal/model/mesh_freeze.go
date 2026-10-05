package model

// MeshFreezeRequest is scoped to the exact Peer incarnation. A delayed control
// request must not affect another run or a replacement container.
type MeshFreezeRequest struct {
	RunID      string `json:"runId"`
	Generation uint64 `json:"generation"`
}

// MeshFreezeResponse acknowledges that the router has applied the freeze.
type MeshFreezeResponse struct {
	NodeID string `json:"nodeId"`
	PeerID string `json:"peerId"`
	Frozen bool   `json:"frozen"`
}
