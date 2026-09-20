package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/scenario"
)

func validSavedScenarioYAML(name string) string {
	return "version: 2\nname: " + name + "\nphases:\n  - action: wait\n    duration: 1ms\n"
}

func scenarioAPIRequest(t *testing.T, server *Server, method, target string, body any, authenticated bool) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, target, bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	if authenticated {
		authenticateRequest(t, server, request)
	}
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	return response
}

func TestSavedScenarioCRUDPersistsAcrossControllerRestart(t *testing.T) {
	dataDir := t.TempDir()
	server := New(ServerConfig{DataDir: dataDir, User: "admin", Password: "secret"}, nil)
	originalYAML := validSavedScenarioYAML("original")
	createBody := scenarioSubmission{Name: "  Baseline experiment  ", YAML: originalYAML}
	if response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", createBody, false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated create status=%d body=%s", response.Code, response.Body)
	}
	response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", createBody, true)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}
	var created savedScenario
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !validScenarioID(created.ID) || created.Name != "Baseline experiment" || created.YAML != originalYAML || created.CreatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("unexpected created scenario: %+v", created)
	}

	// A fresh Controller has no in-memory dependency on the creating process.
	restarted := New(ServerConfig{DataDir: dataDir, User: "admin", Password: "secret"}, nil)
	response = scenarioAPIRequest(t, restarted, http.MethodGet, "/api/v1/scenarios", nil, true)
	if response.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", response.Code, response.Body)
	}
	var summaries []savedScenarioSummary
	if err := json.Unmarshal(response.Body.Bytes(), &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].ID != created.ID || summaries[0].Name != created.Name {
		t.Fatalf("unexpected scenario list: %+v", summaries)
	}
	var listed map[string]any
	var rawList []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &rawList); err != nil {
		t.Fatal(err)
	}
	if len(rawList) != 1 {
		t.Fatalf("unexpected raw list: %+v", rawList)
	}
	listed = rawList[0]
	if _, includesYAML := listed["yaml"]; includesYAML {
		t.Fatal("scenario list included YAML instead of lightweight metadata")
	}

	response = scenarioAPIRequest(t, restarted, http.MethodGet, "/api/v1/scenarios/"+created.ID, nil, true)
	var loaded savedScenario
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &loaded) != nil || loaded != created {
		t.Fatalf("load status=%d body=%s loaded=%+v", response.Code, response.Body, loaded)
	}

	updatedYAML := validSavedScenarioYAML("updated")
	response = scenarioAPIRequest(t, restarted, http.MethodPut, "/api/v1/scenarios/"+created.ID, scenarioSubmission{Name: "Renamed", YAML: updatedYAML}, true)
	if response.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", response.Code, response.Body)
	}
	var updated savedScenario
	if err := json.Unmarshal(response.Body.Bytes(), &updated); err != nil {
		t.Fatal(err)
	}
	if updated.ID != created.ID || updated.Name != "Renamed" || updated.YAML != updatedYAML || !updated.CreatedAt.Equal(created.CreatedAt) || !updated.UpdatedAt.After(created.UpdatedAt) {
		t.Fatalf("unexpected updated scenario: %+v", updated)
	}

	if response := scenarioAPIRequest(t, restarted, http.MethodDelete, "/api/v1/scenarios/"+created.ID, nil, false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated delete status=%d", response.Code)
	}
	if response := scenarioAPIRequest(t, restarted, http.MethodDelete, "/api/v1/scenarios/"+created.ID, nil, true); response.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", response.Code, response.Body)
	}
	if response := scenarioAPIRequest(t, restarted, http.MethodGet, "/api/v1/scenarios/"+created.ID, nil, true); response.Code != http.StatusNotFound {
		t.Fatalf("deleted scenario status=%d body=%s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "scenarios", created.ID+".json")); !os.IsNotExist(err) {
		t.Fatalf("deleted scenario file remains: %v", err)
	}
}

func TestSavedScenarioValidationAndMethods(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	validYAML := validSavedScenarioYAML("valid")
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "invalid JSON", body: `{`, want: http.StatusBadRequest},
		{name: "unknown JSON field", body: `{"name":"valid","yaml":"` + strings.ReplaceAll(validYAML, "\n", `\n`) + `","extra":true}`, want: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"name":"valid","yaml":"` + strings.ReplaceAll(validYAML, "\n", `\n`) + `"}{}`, want: http.StatusBadRequest},
		{name: "empty name", body: `{"name":" ","yaml":"` + strings.ReplaceAll(validYAML, "\n", `\n`) + `"}`, want: http.StatusBadRequest},
		{name: "control in name", body: `{"name":"bad\nname","yaml":"` + strings.ReplaceAll(validYAML, "\n", `\n`) + `"}`, want: http.StatusBadRequest},
		{name: "long name", body: string(mustJSON(t, scenarioSubmission{Name: strings.Repeat("a", scenarioNameRuneLimit+1), YAML: validYAML})), want: http.StatusBadRequest},
		{name: "empty YAML", body: `{"name":"valid","yaml":""}`, want: http.StatusBadRequest},
		{name: "invalid YAML", body: `{"name":"valid","yaml":"name: broken"}`, want: http.StatusBadRequest},
		{name: "unknown YAML field", body: `{"name":"valid","yaml":"name: broken\\nunknown: true\\nphases: [{action: wait, duration: 1ms}]"}`, want: http.StatusBadRequest},
		{name: "large YAML", body: string(mustJSON(t, scenarioSubmission{Name: "valid", YAML: validYAML + "#" + strings.Repeat("x", scenarioYAMLLimit)})), want: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			server.apiTestHandler(context.Background()).ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%s, want %d", response.Code, response.Body, test.want)
			}
		})
	}
	oversized := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios", strings.NewReader(strings.Repeat(" ", scenarioJSONRequestBodyLimit+1)))
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, oversized)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized request status=%d body=%s", response.Code, response.Body)
	}
	for _, method := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete} {
		if response := scenarioAPIRequest(t, server, method, "/api/v1/scenarios", nil, false); response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("collection %s status=%d", method, response.Code)
		}
	}
	for _, target := range []string{"/api/v1/scenarios/not-hex", "/api/v1/scenarios/" + strings.Repeat("a", 32) + "/extra", `/api/v1/scenarios/..%2Foutside`} {
		if response := scenarioAPIRequest(t, server, http.MethodGet, target, nil, true); response.Code != http.StatusNotFound {
			t.Fatalf("unsafe target %q status=%d body=%s", target, response.Code, response.Body)
		}
	}
}

func TestSavedScenarioJSONEnvelopeAcceptsEscapedOneMiBAndRejectsLargerYAML(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	base := validSavedScenarioYAML("limit")
	exact := base + strings.Repeat("\n", scenarioYAMLLimit-len(base))
	encoded := mustJSON(t, scenarioSubmission{Name: "exact limit", YAML: exact})
	if len(exact) != scenarioYAMLLimit || len(encoded) <= scenarioYAMLLimit || len(encoded) > scenarioJSONRequestBodyLimit {
		t.Fatalf("test input does not exercise JSON escaping: yaml=%d json=%d limit=%d", len(exact), len(encoded), scenarioJSONRequestBodyLimit)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios", bytes.NewReader(encoded))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("escaped 1 MiB scenario status=%d body=%s", response.Code, response.Body)
	}
	var created savedScenario
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	restarted := New(ServerConfig{DataDir: server.config.DataDir}, nil)
	if response := scenarioAPIRequest(t, restarted, http.MethodGet, "/api/v1/scenarios/"+created.ID, nil, true); response.Code != http.StatusOK {
		t.Fatalf("stored escaped 1 MiB scenario status=%d body=%s", response.Code, response.Body)
	}

	tooLarge := mustJSON(t, scenarioSubmission{Name: "over limit", YAML: exact + "\n"})
	request = httptest.NewRequest(http.MethodPost, "/api/v1/scenarios", bytes.NewReader(tooLarge))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "cannot exceed") {
		t.Fatalf("decoded YAML over limit status=%d body=%s", response.Code, response.Body)
	}
}

func TestSavedScenarioRequestRejectsInvalidUTF8BeforeJSONDecode(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	raw := append([]byte(`{"name":"`), 0xff)
	raw = append(raw, []byte(`","yaml":"version: 2\nname: valid\nphases: [{action: wait, duration: 1ms}]"}`)...)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/scenarios", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "valid UTF-8") {
		t.Fatalf("invalid UTF-8 status=%d body=%s", response.Code, response.Body)
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "scenarios")); !os.IsNotExist(err) {
		t.Fatalf("invalid UTF-8 request created storage: %v", err)
	}
}

func TestSavedScenarioSummaryCacheRevalidatesChangedRecords(t *testing.T) {
	dataDir := t.TempDir()
	creator := New(ServerConfig{DataDir: dataDir}, nil)
	response := scenarioAPIRequest(t, creator, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "initial", YAML: validSavedScenarioYAML("initial")}, false)
	var item savedScenario
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}

	server := New(ServerConfig{DataDir: dataDir}, nil)
	var validations atomic.Int64
	server.scenarioRecordCheck = func(data []byte) error {
		validations.Add(1)
		_, err := scenario.Parse(data)
		return err
	}
	list := func() *httptest.ResponseRecorder {
		return scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios", nil, true)
	}
	if response := list(); response.Code != http.StatusOK {
		t.Fatalf("cold list status=%d body=%s", response.Code, response.Body)
	}
	if got := validations.Load(); got != 1 {
		t.Fatalf("cold list validations=%d, want 1", got)
	}
	for range 5 {
		if response := list(); response.Code != http.StatusOK {
			t.Fatalf("cached list status=%d body=%s", response.Code, response.Body)
		}
	}
	if got := validations.Load(); got != 1 {
		t.Fatalf("unchanged lists reparsed YAML: validations=%d", got)
	}

	// A write through another Controller replaces the file atomically. Its new
	// identity invalidates this Controller's summary without shared memory.
	other := New(ServerConfig{DataDir: dataDir}, nil)
	response = scenarioAPIRequest(t, other, http.MethodPut, "/api/v1/scenarios/"+item.ID, scenarioSubmission{Name: "external update", YAML: validSavedScenarioYAML("external")}, false)
	if response.Code != http.StatusOK {
		t.Fatalf("external update status=%d body=%s", response.Code, response.Body)
	}
	response = list()
	var summaries []savedScenarioSummary
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &summaries) != nil || len(summaries) != 1 || summaries[0].Name != "external update" {
		t.Fatalf("changed list status=%d body=%s", response.Code, response.Body)
	}
	if got := validations.Load(); got != 2 {
		t.Fatalf("changed record validations=%d, want 2", got)
	}

	recordPath := filepath.Join(dataDir, "scenarios", item.ID+".json")
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var corrupted savedScenario
	if err := json.Unmarshal(raw, &corrupted); err != nil {
		t.Fatal(err)
	}
	corrupted.YAML = "name: invalid"
	if err := writeFileAtomic(recordPath, append(mustJSON(t, corrupted), '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if response := list(); response.Code != http.StatusInternalServerError {
		t.Fatalf("corrupted cached record status=%d body=%s", response.Code, response.Body)
	}
	if got := validations.Load(); got != 3 {
		t.Fatalf("corrupted record was not revalidated: validations=%d", got)
	}
	restarted := New(ServerConfig{DataDir: dataDir}, nil)
	if response := scenarioAPIRequest(t, restarted, http.MethodGet, "/api/v1/scenarios", nil, true); response.Code != http.StatusInternalServerError {
		t.Fatalf("restart trusted corrupted record: status=%d body=%s", response.Code, response.Body)
	}
	if response := scenarioAPIRequest(t, restarted, http.MethodDelete, "/api/v1/scenarios/"+item.ID, nil, false); response.Code != http.StatusNoContent {
		t.Fatalf("cannot delete corrupted regular record: status=%d body=%s", response.Code, response.Body)
	}
}

func TestConcurrentColdScenarioListsShareOneLargeYAMLValidation(t *testing.T) {
	dataDir := t.TempDir()
	creator := New(ServerConfig{DataDir: dataDir}, nil)
	base := validSavedScenarioYAML("large-cold-record")
	largeYAML := base + strings.Repeat("\n", (scenarioYAMLLimit/2)-len(base))
	response := scenarioAPIRequest(t, creator, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "large", YAML: largeYAML}, false)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}

	server := New(ServerConfig{DataDir: dataDir}, nil)
	var validations atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	server.scenarioRecordCheck = func(data []byte) error {
		validations.Add(1)
		enteredOnce.Do(func() { close(entered) })
		<-release
		_, err := scenario.Parse(data)
		return err
	}
	const requests = 24
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, requests)
	var group sync.WaitGroup
	group.Add(requests)
	for range requests {
		go func() {
			defer group.Done()
			<-start
			responses <- scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios", nil, true)
		}()
	}
	close(start)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cold list validation did not start")
	}
	close(release)
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent cold lists did not complete")
	}
	close(responses)
	for response := range responses {
		if response.Code != http.StatusOK {
			t.Fatalf("concurrent list status=%d body=%s", response.Code, response.Body)
		}
	}
	if got := validations.Load(); got != 1 {
		t.Fatalf("concurrent cold lists validated YAML %d times, want 1", got)
	}
	server.scenarioCacheMu.Lock()
	flights := len(server.scenarioSummaryFlights)
	server.scenarioCacheMu.Unlock()
	if flights != 0 {
		t.Fatalf("completed summary flights remain: %d", flights)
	}
}

func TestScenarioSummaryFlightsSeparateChangedFileIdentities(t *testing.T) {
	dataDir := t.TempDir()
	creator := New(ServerConfig{DataDir: dataDir}, nil)
	response := scenarioAPIRequest(t, creator, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "old", YAML: validSavedScenarioYAML("old")}, false)
	var item savedScenario
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}

	server := New(ServerConfig{DataDir: dataDir}, nil)
	var validations atomic.Int64
	entered := make(chan int64, 2)
	release := make(chan struct{})
	server.scenarioRecordCheck = func(data []byte) error {
		count := validations.Add(1)
		entered <- count
		<-release
		_, err := scenario.Parse(data)
		return err
	}
	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() { responses <- scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios", nil, true) }()
	select {
	case count := <-entered:
		if count != 1 {
			t.Fatalf("first validation count=%d", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first identity validation did not start")
	}

	// Replace the record atomically through an independent Controller while
	// the old identity's validation is still running.
	other := New(ServerConfig{DataDir: dataDir}, nil)
	response = scenarioAPIRequest(t, other, http.MethodPut, "/api/v1/scenarios/"+item.ID, scenarioSubmission{Name: "new", YAML: validSavedScenarioYAML("new")}, false)
	if response.Code != http.StatusOK {
		t.Fatalf("replace status=%d body=%s", response.Code, response.Body)
	}
	go func() { responses <- scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios", nil, true) }()
	select {
	case count := <-entered:
		if count != 2 {
			t.Fatalf("new identity validation count=%d", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new file identity incorrectly joined the old validation flight")
	}
	close(release)
	for range 2 {
		select {
		case response := <-responses:
			if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"new"`) {
				t.Fatalf("identity-aware list status=%d body=%s", response.Code, response.Body)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("identity-aware list did not complete")
		}
	}
	if got := validations.Load(); got != 2 {
		t.Fatalf("identity validations=%d, want 2", got)
	}
	if response := scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios", nil, true); response.Code != http.StatusOK {
		t.Fatalf("cached new identity status=%d body=%s", response.Code, response.Body)
	}
	if got := validations.Load(); got != 2 {
		t.Fatalf("current identity was cold after flight completion: validations=%d", got)
	}
}

func TestScenarioSummaryFlightReleasesWaitersOnErrorAndPanic(t *testing.T) {
	for _, test := range []struct {
		name  string
		check func([]byte) error
	}{
		{name: "validation error", check: func([]byte) error { return errors.New("invalid") }},
		{name: "validation panic", check: func([]byte) error { panic("invalid") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dataDir := t.TempDir()
			creator := New(ServerConfig{DataDir: dataDir}, nil)
			response := scenarioAPIRequest(t, creator, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "flight", YAML: validSavedScenarioYAML("flight")}, false)
			var item savedScenario
			if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
				t.Fatalf("create status=%d body=%s", response.Code, response.Body)
			}
			server := New(ServerConfig{DataDir: dataDir}, nil)
			server.scenarioRecordCheck = test.check
			root, err := server.openScenarioDirectory(false)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			info, err := root.Lstat(item.ID + ".json")
			if err != nil {
				t.Fatal(err)
			}
			_, leaderFlight, leader := server.acquireScenarioSummaryFlight(item.ID, info)
			if !leader || leaderFlight == nil {
				t.Fatal("cold record did not create a validation flight")
			}
			_, waiterFlight, waiterLeader := server.acquireScenarioSummaryFlight(item.ID, info)
			if waiterLeader || waiterFlight != leaderFlight {
				t.Fatal("same identity did not join its existing validation flight")
			}
			leaderDone := make(chan error, 1)
			go func() {
				_, err := server.runScenarioSummaryFlight(root, item.ID, leaderFlight)
				leaderDone <- err
			}()
			select {
			case <-waiterFlight.done:
				if waiterFlight.err == nil {
					t.Fatal("waiter did not receive the validation failure")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("validation waiter was not released")
			}
			select {
			case err := <-leaderDone:
				if err == nil {
					t.Fatal("leader did not receive the validation failure")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("validation leader did not finish")
			}
			server.scenarioCacheMu.Lock()
			flights := len(server.scenarioSummaryFlights)
			server.scenarioCacheMu.Unlock()
			if flights != 0 {
				t.Fatalf("failed flight remained registered: %d", flights)
			}
		})
	}
}

func TestSavedScenarioDiskRecordRejectsInvalidUTF8BeforeJSONDecode(t *testing.T) {
	dataDir := t.TempDir()
	server := New(ServerConfig{DataDir: dataDir}, nil)
	response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "valid", YAML: validSavedScenarioYAML("valid")}, false)
	var item savedScenario
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}
	recordPath := filepath.Join(dataDir, "scenarios", item.ID+".json")
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	marker := bytes.Index(raw, []byte(`"name":"valid"`))
	if marker < 0 {
		t.Fatalf("name marker missing from record: %q", raw)
	}
	raw[marker+len(`"name":"`)] = 0xff
	if err := writeFileAtomic(recordPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, current := range []*Server{server, New(ServerConfig{DataDir: dataDir}, nil)} {
		response := scenarioAPIRequest(t, current, http.MethodGet, "/api/v1/scenarios/"+item.ID, nil, true)
		if response.Code != http.StatusInternalServerError || !strings.Contains(response.Body.String(), "cannot load") {
			t.Fatalf("invalid UTF-8 record status=%d body=%s", response.Code, response.Body)
		}
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSavedScenarioStorageRejectsSymlinks(t *testing.T) {
	dataDir := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "outside.json")
	if err := os.WriteFile(outsideFile, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dataDir, "scenarios")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	server := New(ServerConfig{DataDir: dataDir}, nil)
	response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "safe", YAML: validSavedScenarioYAML("safe")}, false)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("symlinked directory create status=%d body=%s", response.Code, response.Body)
	}
	if data, err := os.ReadFile(outsideFile); err != nil || string(data) != "preserve" {
		t.Fatalf("scenario storage escaped data directory: %q %v", data, err)
	}
}

func TestSavedScenarioRecordSymlinkIsNeverReadUpdatedOrDeleted(t *testing.T) {
	dataDir := t.TempDir()
	server := New(ServerConfig{DataDir: dataDir}, nil)
	response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "safe", YAML: validSavedScenarioYAML("safe")}, false)
	var item savedScenario
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}
	outsideFile := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outsideFile, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dataDir, "scenarios", item.ID+".json")
	if err := os.Remove(record); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, record); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	for _, operation := range []struct {
		method string
		body   any
	}{
		{method: http.MethodGet},
		{method: http.MethodPut, body: scenarioSubmission{Name: "changed", YAML: validSavedScenarioYAML("changed")}},
		{method: http.MethodDelete},
	} {
		response := scenarioAPIRequest(t, server, operation.method, "/api/v1/scenarios/"+item.ID, operation.body, false)
		if response.Code != http.StatusInternalServerError {
			t.Fatalf("%s symlinked record status=%d body=%s", operation.method, response.Code, response.Body)
		}
	}
	if data, err := os.ReadFile(outsideFile); err != nil || string(data) != "preserve" {
		t.Fatalf("scenario record operation followed symlink: %q %v", data, err)
	}
}

func TestConcurrentSavedScenarioUpdatesRemainAtomic(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	response := scenarioAPIRequest(t, server, http.MethodPost, "/api/v1/scenarios", scenarioSubmission{Name: "initial", YAML: validSavedScenarioYAML("initial")}, false)
	var item savedScenario
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &item) != nil {
		t.Fatalf("create status=%d body=%s", response.Code, response.Body)
	}
	const writers = 16
	var group sync.WaitGroup
	group.Add(writers)
	for index := 0; index < writers; index++ {
		index := index
		go func() {
			defer group.Done()
			name := "version-" + time.Unix(int64(index), 0).UTC().Format("150405")
			response := scenarioAPIRequest(t, server, http.MethodPut, "/api/v1/scenarios/"+item.ID, scenarioSubmission{Name: name, YAML: validSavedScenarioYAML(name)}, false)
			if response.Code != http.StatusOK {
				t.Errorf("update %d status=%d body=%s", index, response.Code, response.Body)
			}
		}()
	}
	group.Wait()
	response = scenarioAPIRequest(t, server, http.MethodGet, "/api/v1/scenarios/"+item.ID, nil, true)
	var loaded savedScenario
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &loaded) != nil {
		t.Fatalf("load status=%d body=%s", response.Code, response.Body)
	}
	if loaded.YAML != validSavedScenarioYAML(loaded.Name) {
		t.Fatalf("observed a mixed or partial update: %+v", loaded)
	}
	entries, err := os.ReadDir(filepath.Join(server.config.DataDir, "scenarios"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != item.ID+".json" {
		t.Fatalf("temporary scenario records remain: %+v", entries)
	}
}
