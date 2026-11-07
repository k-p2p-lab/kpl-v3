package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

var errAnalysisAttemptChanged = errors.New("Analysis attempt changed; refresh its status before retrying.")

type analysisExpectation struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func decodeAnalysisExpectation(w http.ResponseWriter, r *http.Request) (*analysisExpectation, error) {
	if r.Body == nil {
		return nil, nil
	}
	var request *struct {
		Expected json.RawMessage `json:"expectedAnalysis"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("invalid analysis request: %w", err)
	}
	if request == nil {
		return nil, errors.New("analysis request must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("analysis request must contain one JSON object")
	}
	if len(request.Expected) == 0 {
		return nil, nil
	}
	var expected *struct {
		ID    *string `json:"id"`
		State *string `json:"state"`
	}
	decoder = json.NewDecoder(bytes.NewReader(request.Expected))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&expected); err != nil || expected == nil || expected.ID == nil || expected.State == nil {
		return nil, errors.New("expectedAnalysis must contain an id and state")
	}
	id, state := *expected.ID, *expected.State
	if state == "idle" {
		if id != "" && !validResultID(id) {
			return nil, errors.New("expectedAnalysis must contain a valid analysis id")
		}
	} else {
		if !validResultID(id) {
			return nil, errors.New("expectedAnalysis must contain a valid analysis id")
		}
		switch state {
		case "queued", "running", "completed", "failed", "interrupted", "canceled":
		default:
			return nil, errors.New("expectedAnalysis has an unsupported state")
		}
	}
	return &analysisExpectation{ID: id, State: state}, nil
}

func (expected *analysisExpectation) matches(status analysisJobStatus) bool {
	return expected == nil || expected.ID == status.ID && expected.State == status.State
}

func (expected *analysisExpectation) matchesBatch(status analysisJobStatus) bool {
	// Batch GET exposes stale completed attempts as idle, retaining the old
	// ID for saved artifacts. Match that exact projection only after the
	// caller has ruled out live-job and valid-cache reuse.
	return expected.matches(status) || expected != nil && expected.State == "idle" && expected.ID != "" && expected.ID == status.ID && status.State == "completed"
}

func writeAnalysisAttemptChanged(w http.ResponseWriter) {
	writeJSON(w, http.StatusConflict, map[string]string{"error": errAnalysisAttemptChanged.Error(), "code": "analysis-attempt-changed"})
}
