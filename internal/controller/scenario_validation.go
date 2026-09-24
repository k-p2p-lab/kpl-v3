package controller

import (
	"net/http"
	"strings"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

// handleScenarioValidation checks editor input without creating storage, jobs,
// or peers. It uses the same parser as saving and running a scenario.
func (s *Server) handleScenarioValidation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	raw, err := readLimitedRequestBody(w, r, scenarioYAMLLimit)
	if err != nil {
		writeScenarioRequestError(w, err)
		return
	}
	if strings.TrimSpace(string(raw)) == "" {
		writeError(w, http.StatusBadRequest, "scenario YAML is required")
		return
	}
	var parsed scenario.Scenario
	err = validateScenarioSource(string(raw), func(data []byte) error {
		var parseErr error
		parsed, parseErr = scenario.Parse(data)
		return parseErr
	})
	if err != nil {
		writeScenarioRequestError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Valid  bool   `json:"valid"`
		Name   string `json:"name"`
		Phases int    `json:"phases"`
	}{Valid: true, Name: parsed.Name, Phases: len(parsed.Phases)})
}
