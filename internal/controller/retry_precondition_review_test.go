package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func retryPreconditionPOST(t *testing.T, server *Server, batchID string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/result-batches/"+batchID+"/retry", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	authenticateRequest(t, server, request)
	response := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(response, request)
	return response
}

func retryPreconditionBody(t *testing.T, ids []string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"expectedRunIds": ids})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestRetryPreconditionRejectsBrowserReplayAfterFastFailure(t *testing.T) {
	f := newResumeFixture(t)
	f.fail.Store(false)
	f.failAt.Store(2)
	if _, err := f.server.StartScenarioRepeated(context.Background(), []byte(resumeScenario), 3); err != nil {
		t.Fatal(err)
	}
	runs := waitRepetitions(t, f.server)
	if runs[0].State != "completed" || runs[1].State != "failed" || runs[2].State != "canceled" {
		t.Fatalf("initial states: %+v", runs)
	}
	completedPath := filepath.Join(f.server.config.DataDir, currentRunsDirectory, runs[0].ID, "experiment.json")
	completedBefore, err := os.ReadFile(completedPath)
	if err != nil {
		t.Fatal(err)
	}
	f.fail.Store(true) // The replacement attempt can fail before a browser replay.
	body := retryPreconditionBody(t, []string{runs[2].ID, runs[1].ID})
	response := retryPreconditionPOST(t, f.server, runs[0].BatchID, body)
	if response.Code != http.StatusAccepted {
		t.Fatalf("initial guarded retry: %d %s", response.Code, response.Body)
	}
	var first model.Experiment
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.ID == runs[1].ID || first.Iteration != 2 {
		t.Fatalf("retry did not create the expected new attempt: %+v", first)
	}
	current := retryMembers(t, f.server, first.BatchID)
	if current[0].ID != runs[0].ID || current[1].State != "failed" {
		t.Fatalf("fast failure did not preserve completed iteration: %+v", current)
	}
	before, err := f.server.allBatchMembers(context.Background(), first.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	for replay := 0; replay < 3; replay++ {
		response = retryPreconditionPOST(t, f.server, first.BatchID, body)
		var failure struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(response.Body.Bytes(), &failure)
		if response.Code != http.StatusConflict || failure.Code != "batch-attempt-changed" {
			t.Fatalf("replayed source attempt created another retry: %d %s", response.Code, response.Body)
		}
	}
	after, err := f.server.allBatchMembers(context.Background(), first.BatchID)
	if err != nil || len(after) != len(before) {
		t.Fatalf("replay created more attempts: %d -> %d (%v)", len(before), len(after), err)
	}
	f.server.cancelMu.Lock()
	slots := len(f.server.workSlots)
	f.server.cancelMu.Unlock()
	if slots != 0 {
		t.Fatal("replayed retry reserved new work")
	}
	completedAfter, err := os.ReadFile(completedPath)
	if err != nil || !bytes.Equal(completedBefore, completedAfter) {
		t.Fatal("guarded retry changed completed results")
	}
}

func TestRetryPreconditionRejectsInvalidBodiesBeforeAdmission(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 2)
	ids := []string{runs[0].ID, runs[1].ID}
	tooMany := make([]string, maxScenarioRepetitions+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("foreign-%d", i)
	}
	cases := map[string][]byte{
		"duplicate":      retryPreconditionBody(t, []string{ids[0], ids[0]}),
		"invalid-id":     retryPreconditionBody(t, []string{"../outside"}),
		"empty-list":     []byte(`{"expectedRunIds":[]}`),
		"null-list":      []byte(`{"expectedRunIds":null}`),
		"null-request":   []byte(`null`),
		"wrong-type":     []byte(`{"expectedRunIds":"run"}`),
		"too-many":       retryPreconditionBody(t, tooMany),
		"unknown-field":  []byte(`{"unexpected":true}`),
		"trailing-value": append(retryPreconditionBody(t, ids), []byte(` {}`)...),
		"oversized":      []byte(`{"expectedRunIds":["` + strings.Repeat("x", 32<<10) + `"]}`),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			response := retryPreconditionPOST(t, f.server, runs[0].BatchID, body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid precondition was admitted: %d %s", response.Code, response.Body)
			}
		})
	}
	after, err := f.server.allBatchMembers(context.Background(), runs[0].BatchID)
	if err != nil || len(after) != len(runs) {
		t.Fatalf("invalid body changed result membership: %d (%v)", len(after), err)
	}
}

func TestRetryPreconditionRejectsChangedAndForeignUnfinishedSets(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			f := newResumeFixture(t)
			runs := f.failedBatch(t, 2)
			expected := []string{runs[0].ID, runs[1].ID}
			if changed {
				f.server.updateExperiment(runs[0].ID, func(run *model.Experiment) { run.State = "completed" })
			} else {
				expected[0] = "foreign-attempt"
			}
			response := retryPreconditionPOST(t, f.server, runs[0].BatchID, retryPreconditionBody(t, expected))
			var failure struct {
				Code string `json:"code"`
			}
			_ = json.Unmarshal(response.Body.Bytes(), &failure)
			if response.Code != http.StatusConflict || failure.Code != "batch-attempt-changed" {
				t.Fatalf("changed source attempts were admitted: %d %s", response.Code, response.Body)
			}
			after, err := f.server.allBatchMembers(context.Background(), runs[0].BatchID)
			if err != nil || len(after) != len(runs) {
				t.Fatal("precondition mismatch reserved new attempts")
			}
		})
	}
}

func TestRetryPreconditionSurvivesControllerRestart(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 2)
	body := retryPreconditionBody(t, []string{runs[0].ID, runs[1].ID})
	response := retryPreconditionPOST(t, f.server, runs[0].BatchID, body)
	if response.Code != http.StatusAccepted {
		t.Fatalf("initial guarded retry: %d %s", response.Code, response.Body)
	}
	waitRepetitions(t, f.server)
	restarted := New(f.server.config, f.server.logger)
	f.register(t, restarted)
	response = retryPreconditionPOST(t, restarted, runs[0].BatchID, body)
	var failure struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(response.Body.Bytes(), &failure)
	if response.Code != http.StatusConflict || failure.Code != "batch-attempt-changed" {
		t.Fatalf("restart allowed an already-replaced source attempt: %d %s", response.Code, response.Body)
	}
	members, err := restarted.allBatchMembers(context.Background(), runs[0].BatchID)
	if err != nil || len(members) != 4 {
		t.Fatalf("restart replay admitted another attempt: %d %v", len(members), err)
	}
}

func TestRetryPreconditionBodySupportsMaximumRunCount(t *testing.T) {
	ids := make([]string, maxScenarioRepetitions)
	for i := range ids {
		ids[i] = fmt.Sprintf("%03d-%s", i, strings.Repeat("x", 124))
	}
	request := httptest.NewRequest(http.MethodPost, "/retry", bytes.NewReader(retryPreconditionBody(t, ids)))
	decoded, err := decodeRetryExpectedRuns(httptest.NewRecorder(), request)
	if err != nil || len(decoded) != maxScenarioRepetitions {
		t.Fatalf("valid maximum-size precondition was rejected: %d %v", len(decoded), err)
	}
}
