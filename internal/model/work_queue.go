package model

import "time"

// WorkQueueItem describes live Controller work. Experiment repetitions share
// one slot; single-Run and batch analyses occupy separate slots in the same FIFO.
type WorkQueueItem struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Name      string     `json:"name"`
	RunID     string     `json:"runId,omitempty"`
	BatchID   string     `json:"batchId,omitempty"`
	State     string     `json:"state"`
	Position  int        `json:"position"`
	CreatedAt time.Time  `json:"createdAt"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	Phase     string     `json:"phase,omitempty"`
	Progress  float64    `json:"progress,omitempty"`
}
