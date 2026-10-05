package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestScenarioValidationRejectsTopologyWithoutMeshFreezeCapability(t *testing.T) {
	for _, test := range []struct {
		name, profiles, joinConfig string
		want                       int
	}{
		{"inline omitted", "", "node: {gossipsub: {topics: [blocks]}}", http.StatusBadRequest},
		{"inline false", "", "node: {gossipsub: {topics: [blocks], meshFreeze: false}}", http.StatusBadRequest},
		{"inline true", "", "node: {gossipsub: {topics: [blocks], meshFreeze: true}}", http.StatusOK},
		{"profile omitted", "profiles: {workers: {gossipsub: {topics: [blocks]}}}\n", "profile: workers", http.StatusBadRequest},
		{"profile false", "profiles: {workers: {gossipsub: {topics: [blocks], meshFreeze: false}}}\n", "profile: workers", http.StatusBadRequest},
		{"profile true", "profiles: {workers: {gossipsub: {topics: [blocks], meshFreeze: true}}}\n", "profile: workers", http.StatusOK},
		{"inline disables inherited capability", "profiles: {workers: {gossipsub: {topics: [blocks], meshFreeze: true}}}\n", "profile: workers, node: {gossipsub: {meshFreeze: false}}", http.StatusBadRequest},
		{"inline enables inherited capability", "profiles: {workers: {gossipsub: {topics: [blocks], meshFreeze: false}}}\n", "profile: workers, node: {gossipsub: {meshFreeze: true}}", http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "test-password"}, nil)
			yaml := fmt.Sprintf("version: 3\nname: topology-capability\n%sphases:\n"+
				"  - {name: create-workers, action: join, group: workers, type: full, count: 4, %s}\n"+
				"  - {name: form-workers, action: topology, group: workers, topic: blocks, topology: {model: er, p: 0.5}}\n", test.profiles, test.joinConfig)
			request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios/validate", strings.NewReader(yaml))
			request.Header.Set("Content-Type", "application/yaml")
			authenticateRequest(t, server, request)
			response := httptest.NewRecorder()
			server.apiTestHandler(context.Background()).ServeHTTP(response, request)
			var result struct {
				Valid bool   `json:"valid"`
				Error string `json:"error"`
			}
			if response.Code != test.want || json.Unmarshal(response.Body.Bytes(), &result) != nil {
				t.Fatalf("validation status=%d body=%s, want %d", response.Code, response.Body, test.want)
			}
			if test.want == http.StatusOK {
				if !result.Valid {
					t.Fatalf("enabled topology scenario was not valid: %s", response.Body)
				}
			} else if result.Valid || !strings.Contains(result.Error, "meshFreeze") {
				t.Fatalf("missing mesh-freeze capability diagnostic: %s", response.Body)
			}
			snapshot := server.state.snapshot()
			if len(snapshot.Experiments) != 0 || len(snapshot.Nodes) != 0 || len(snapshot.Agents) != 0 {
				t.Fatalf("scenario validation started runtime work: %+v", snapshot)
			}
		})
	}
}

func TestScenarioValidationTopologyCapabilityAppliesOnlyToSelectedGroup(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	yaml := `version: 3
name: topology-selected-group
phases:
  - {action: join, group: unrelated, type: full, count: 1}
  - {action: join, group: workers, type: full, count: 4, node: {gossipsub: {topics: [blocks], meshFreeze: true}}}
  - {action: topology, group: workers, topic: blocks, topology: {model: er, p: 0.5}}
`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios/validate", strings.NewReader(yaml))
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unselected group blocked valid topology: status=%d body=%s", response.Code, response.Body)
	}
}
