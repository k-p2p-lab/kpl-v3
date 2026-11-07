package model

import "time"

const DefaultRunDrainTimeout = 30 * time.Second
const MaxRunDrainTimeout = 2 * time.Minute
const RunTelemetryPendingCode = "run_telemetry_pending"

// A pending drain is never a delivery acknowledgement. Older Controllers keep
// seeing the original 503/error contract; newer ones can retry the typed result.
type RunDrainError struct {
	Error           string `json:"error"`
	Code            string `json:"code"`
	RemainingEvents int    `json:"remainingEvents"`
}
