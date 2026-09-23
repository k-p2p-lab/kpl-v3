package controller

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestSavedResultsOrderQueuedIterationsNumerically(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	now := time.Now().UTC()
	for _, run := range []model.Experiment{
		{ID: "a-ten", BatchID: "batch-a", Iteration: 10, Repetitions: 12, State: "queued"},
		{ID: "z-two", BatchID: "batch-a", Iteration: 2, Repetitions: 12, State: "queued"},
		{ID: "m-three", BatchID: "batch-a", Iteration: 3, Repetitions: 12, State: "queued"},
		{ID: "b-ten", BatchID: "batch-b", Iteration: 10, Repetitions: 12, State: "queued"},
		{ID: "y-two", BatchID: "batch-b", Iteration: 2, Repetitions: 12, State: "queued"},
		{ID: "running", BatchID: "batch-a", Iteration: 1, Repetitions: 12, State: "running", StartedAt: now},
		{ID: "completed", State: "completed", StartedAt: now.Add(-time.Hour)},
	} {
		if err := s.persistManifest(run, []byte("name: order\nphases: [{action: wait, duration: 1s}]")); err != nil {
			t.Fatal(err)
		}
		s.state.experiments[run.ID] = run
	}
	response := resultRequest(s, http.MethodGet, "/api/v1/results")
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body)
	}
	var results []savedResult
	if err := json.Unmarshal(response.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, run := range results {
		ids = append(ids, run.ID)
	}
	want := []string{"running", "completed", "z-two", "m-three", "a-ten", "y-two", "b-ten"}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("order=%v want=%v", ids, want)
	}
}
