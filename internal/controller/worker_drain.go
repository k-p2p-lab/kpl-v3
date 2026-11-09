package controller

import (
	"encoding/json"
	"errors"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type agentRunWorkerPendingError struct{ message string }

func (e *agentRunWorkerPendingError) Error() string { return e.message }
func hasPendingRunWorker(err error) bool {
	var pending *agentRunWorkerPendingError
	return errors.As(err, &pending)
}
func parseRunWorkerPending(data []byte) error {
	var result model.RunDrainError
	if json.Unmarshal(data, &result) == nil && result.Code == model.RunWorkerPendingCode && result.WorkerPending {
		return &agentRunWorkerPendingError{message: result.Error}
	}
	return nil
}
