package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestScenarioValidationRequiresSessionAndHasNoSideEffects(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	// Exercise profiles, aliases, and explicit scoring through the actual API.
	raw, err := os.ReadFile("../../examples/swarm-churn-prysm-block.yaml")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := scenario.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios/validate", strings.NewReader(string(raw)))
	request.Header.Set("Content-Type", "application/yaml")
	authenticateRequest(t, server, request)
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	var result struct {
		Valid  bool   `json:"valid"`
		Name   string `json:"name"`
		Phases int    `json:"phases"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.Valid || result.Name != parsed.Name || result.Phases != len(parsed.Phases) {
		t.Fatalf("validation status=%d body=%s", response.Code, response.Body)
	}
	snapshot := server.state.snapshot()
	if len(snapshot.Experiments) != 0 || len(snapshot.Nodes) != 0 || len(snapshot.Agents) != 0 {
		t.Fatalf("validation changed runtime state: %+v", snapshot)
	}
	for _, dir := range []string{"scenarios", "runs"} {
		if _, err := os.Stat(filepath.Join(server.config.DataDir, dir)); !os.IsNotExist(err) {
			t.Fatalf("validation created %s storage: %v", dir, err)
		}
	}
	for _, target := range []string{"/api/v1/scenarios", "/api/v1/experiments", "/api/v1/scenarios/validate/"} {
		if response := scenarioAPIRequest(t, server, http.MethodPost, target, nil, false); response.Code != http.StatusUnauthorized {
			t.Fatalf("validation exemption leaked to %s: %d", target, response.Code)
		}
	}
}

func TestScenarioValidationReturnsParserDiagnostics(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, test := range []struct {
		name, yaml, diagnostic string
	}{
		{"empty", " \n", "scenario YAML is required"},
		{"syntax", "name: [broken\n", "line"},
		{"unknown field", "name: broken\nunexpected: true\nphases: [{action: stop-all}]", "field unexpected"},
		{"phase setting", "name: broken\nphases: [{name: warm-up, action: wait, duration: invalid}]", "warm-up"},
		{"protocol setting", "name: broken\nphases: [{action: join, group: workers, count: 1, node: {gossipsub: {params: {d: 6, dLow: 9}}}}]", "dLow"},
		{"extra document", validSavedScenarioYAML("first") + "---\nname: ignored\nphases: [{action: stop-all}]", "exactly one YAML document"},
		{"invalid UTF-8", string([]byte{0xff}), "UTF-8"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios/validate", strings.NewReader(test.yaml))
			response := httptest.NewRecorder()
			server.apiTestHandler(context.Background()).ServeHTTP(response, request)
			var result struct {
				Error string `json:"error"`
			}
			if response.Code != http.StatusBadRequest || json.Unmarshal(response.Body.Bytes(), &result) != nil || !strings.Contains(result.Error, test.diagnostic) {
				t.Fatalf("validation status=%d body=%s, want %q", response.Code, response.Body, test.diagnostic)
			}
		})
	}
}

func TestScenarioValidationLimitsAndMethods(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	base := validSavedScenarioYAML("limit") + "#"
	exact := base + strings.Repeat("x", scenarioYAMLLimit-len(base))
	for _, test := range []struct {
		method, body string
		status       int
	}{
		{http.MethodPost, exact, http.StatusOK},
		{http.MethodPost, exact + "x", http.StatusRequestEntityTooLarge},
		{http.MethodGet, "", http.StatusMethodNotAllowed},
		{http.MethodPut, "", http.StatusMethodNotAllowed},
	} {
		request := httptest.NewRequest(test.method, "/api/v1/scenarios/validate", strings.NewReader(test.body))
		response := httptest.NewRecorder()
		server.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("%s (%d bytes) status=%d body=%s, want %d", test.method, len(test.body), response.Code, response.Body, test.status)
		}
	}
}
