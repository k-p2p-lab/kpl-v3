package model

import "time"

// WorkQueueItem describes live work in the shared execution queue. Consecutive
// analyses can run concurrently on distinct workers when they reach the head.
type WorkQueueItem struct {
	ID            string     `json:"id"`
	Kind          string     `json:"kind"`
	Worker        string     `json:"worker,omitempty"`
	WorkerAgentID string     `json:"workerAgentId,omitempty"`
	Name          string     `json:"name"`
	RunID         string     `json:"runId,omitempty"`
	BatchID       string     `json:"batchId,omitempty"`
	OperationID   string     `json:"operationId,omitempty"`
	State         string     `json:"state"`
	Position      int        `json:"position"`
	CreatedAt     time.Time  `json:"createdAt"`
	StartedAt     *time.Time `json:"startedAt,omitempty"`
	Phase         string     `json:"phase,omitempty"`
	Progress      float64    `json:"progress,omitempty"`
}
