package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func groupedScenarioFixture(t *testing.T, s *Server, yaml string) (savedScenario, libraryGroupsSnapshot) {
	t.Helper()
	item, err := s.createSavedScenario(scenarioSubmission{Name: "Source scenario", YAML: yaml})
	if err != nil {
		t.Fatal(err)
	}
	groups := newTestLibraryGroup(t, s, "Source group", "0")
	groups = moveTestLibraryItems(t, s, groups.Groups[0].ID, []string{"scenario:" + item.ID}, groups.Revision)
	return item, groups
}

func submitLibraryScenarioRequest(t *testing.T, s *Server, sourceID, raw string, count int, key string) *httptest.ResponseRecorder {
	t.Helper()
	body := map[string]any{"scenario": raw, "repetitions": count}
	if sourceID != "" {
		body["scenarioId"] = sourceID
	}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	authenticateRequest(t, s, request)
	response := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(response, request)
	return response
}

func acceptedLibraryRun(t *testing.T, response *httptest.ResponseRecorder) model.Experiment {
	t.Helper()
	var run model.Experiment
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &run) != nil {
		t.Fatalf("submission: %d %s", response.Code, response.Body)
	}
	return run
}

func TestLibraryScenarioSubmissionAssignsSingleAndRepeatedResultsBeforeExecution(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			source, before := groupedScenarioFixture(t, s, validSavedScenarioYAML("saved"))
			edited := validSavedScenarioYAML("edited-without-saving")
			first := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, s, source.ID, edited, count, ""))
			groups := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
			if first.BatchID != first.ID || groups.Memberships["batch:"+first.BatchID] != before.Groups[0].ID || groups.Memberships["run:"+first.ID] != "" || groups.Revision == before.Revision {
				t.Fatalf("new result was not assigned as one batch: %+v", groups)
			}
			runs := waitRepetitions(t, s)
			if len(runs) != count {
				t.Fatalf("got %d runs, want %d", len(runs), count)
			}
			for _, run := range runs {
				data, err := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, run.ID, "scenario.yaml"))
				if err != nil || string(data) != edited || run.BatchID != first.BatchID || run.State != "completed" {
					t.Fatalf("editor input or batch identity changed: %+v %v", run, err)
				}
			}
			restarted := New(s.config, nil)
			groups = librarySnapshot(t, libraryRequest(t, restarted, "GET", "", nil), http.StatusOK)
			if groups.Memberships["batch:"+first.BatchID] != before.Groups[0].ID || len(groups.Memberships) != 2 {
				t.Fatalf("group was lost on restart or assigned per run: %+v", groups)
			}
			groups = newTestLibraryGroup(t, restarted, "Next group", groups.Revision)
			groups = moveTestLibraryItems(t, restarted, groups.Groups[1].ID, []string{"scenario:" + source.ID}, groups.Revision)
			next := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, restarted, source.ID, edited, 1, ""))
			waitRepetitions(t, restarted)
			groups = librarySnapshot(t, libraryRequest(t, restarted, "GET", "", nil), http.StatusOK)
			if groups.Memberships["batch:"+first.BatchID] != before.Groups[0].ID || groups.Memberships["batch:"+next.BatchID] != groups.Groups[1].ID {
				t.Fatalf("source move changed old results or failed to affect new results: %+v", groups)
			}
		})
	}
}

func TestLibraryScenarioIdempotentReplayPreservesManualGroupAndDeletedSource(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	yaml := validSavedScenarioYAML("same-yaml")
	source, _ := groupedScenarioFixture(t, s, yaml)
	other, err := s.createSavedScenario(scenarioSubmission{Name: "Other source", YAML: yaml})
	if err != nil {
		t.Fatal(err)
	}
	first := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, s, source.ID, yaml, 1, "one-submission"))
	waitRepetitions(t, s)
	if response := submitLibraryScenarioRequest(t, s, other.ID, yaml, 1, "one-submission"); response.Code != http.StatusConflict {
		t.Fatalf("different source reused the submission: %d %s", response.Code, response.Body)
	}
	groups := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
	moveTestLibraryItems(t, s, "", []string{"batch:" + first.BatchID}, groups.Revision)
	if err := s.deleteSavedScenario(source.ID); err != nil {
		t.Fatal(err)
	}
	restarted := New(s.config, nil)
	replayed := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, restarted, source.ID, yaml, 1, "one-submission"))
	if replayed.ID != first.ID {
		t.Fatal("response retry created a second experiment")
	}
	groups = librarySnapshot(t, libraryRequest(t, restarted, "GET", "", nil), http.StatusOK)
	if groups.Memberships["batch:"+first.BatchID] != "" {
		t.Fatal("response retry overwrote a manual move to Ungrouped")
	}
	if response := submitLibraryScenarioRequest(t, restarted, source.ID, yaml, 1, "new-submission"); response.Code != http.StatusNotFound {
		t.Fatalf("deleted source started a new experiment: %d %s", response.Code, response.Body)
	}
}

func TestLibraryScenarioUngroupedAndUnselectedInputsRemainUngrouped(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("grouped"))
	for _, sourceID := range []string{"", source.ID} {
		if sourceID != "" {
			groups = moveTestLibraryItems(t, s, "", []string{"scenario:" + sourceID}, groups.Revision)
		}
		first := acceptedLibraryRun(t, submitLibraryScenarioRequest(t, s, sourceID, source.YAML, 1, ""))
		waitRepetitions(t, s)
		groups = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
		if groups.Memberships["batch:"+first.BatchID] != "" {
			t.Fatal("an unselected or ungrouped source inherited another group's membership")
		}
	}
}

func TestLibraryScenarioGroupErrorsDoNotStartExperiments(t *testing.T) {
	for _, kind := range []string{"invalid-id", "missing-id", "metadata", "limit"} {
		t.Run(kind, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("blocked"))
			id, want := source.ID, http.StatusBadRequest
			switch kind {
			case "invalid-id":
				id = "../scenario"
			case "missing-id":
				id, want = strings.Repeat("0", 32), http.StatusNotFound
			case "metadata":
				want = http.StatusInternalServerError
				if err := os.WriteFile(filepath.Join(s.config.DataDir, libraryGroupsFile), []byte("invalid JSON"), 0600); err != nil {
					t.Fatal(err)
				}
			case "limit":
				for len(groups.Memberships) < libraryMembershipLimit {
					groups.Memberships[fmt.Sprintf("run:existing-%d", len(groups.Memberships))] = groups.Groups[0].ID
				}
				if err := s.writeLibraryGroups(groups); err != nil {
					t.Fatal(err)
				}
			}
			response := submitLibraryScenarioRequest(t, s, id, source.YAML, 1, "")
			if response.Code != want || len(s.state.snapshot().Experiments) != 0 {
				t.Fatalf("failed group assignment admitted a run: %d %s", response.Code, response.Body)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed group assignment reserved result files: %v", err)
			}
		})
	}
}

func TestLibraryScenarioAdmissionFailureRestoresMembershipUnlessCommitted(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			source, groups := groupedScenarioFixture(t, s, validSavedScenarioYAML("rollback"))
			failure := errors.New("admission storage failure")
			s.cancelMu.Lock()
			s.state.persistMu.Lock()
			err := s.admitScenarioResultGroup(context.Background(), source.ID, "new-batch", func() error {
				if committed {
					root, err := s.resultGroupDirectory("new-batch", true)
					if err != nil {
						return err
					}
					defer root.Close()
					if err := writeBatchRecord(root, batchExtension{Version: 1, BatchID: "new-batch", Repetitions: 1, Current: map[int]string{1: "new-batch"}}); err != nil {
						return err
					}
				}
				return failure
			})
			s.state.persistMu.Unlock()
			s.cancelMu.Unlock()
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			got := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
			if (got.Memberships["batch:new-batch"] == groups.Groups[0].ID) != committed || got.Memberships["scenario:"+source.ID] != groups.Groups[0].ID {
				t.Fatalf("failed admission changed the wrong memberships: %+v", got)
			}
		})
	}
}

func TestLibraryScenarioRetryResumeAndAppendKeepResultGroup(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			fixture := newResumeFixture(t)
			s := fixture.server
			source, groups := groupedScenarioFixture(t, s, resumeScenario)
			first, err := s.submitScenarioFromLibrary(context.Background(), []byte(source.YAML), 2, "", source.ID)
			if err != nil {
				t.Fatal(err)
			}
			waitRepetitions(t, s)
			groups = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
			groups = newTestLibraryGroup(t, s, "Results moved here", groups.Revision)
			group := groups.Groups[1].ID
			moveTestLibraryItems(t, s, group, []string{"batch:" + first.BatchID}, groups.Revision)
			fixture.fail.Store(false)
			if retry {
				_, err = s.RetryScenarioBatch(context.Background(), first.BatchID)
			} else {
				_, err = s.ResumeScenarioBatch(context.Background(), first.BatchID)
			}
			if err != nil {
				t.Fatal(err)
			}
			waitRepetitions(t, s)
			groups = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
			if groups.Memberships["batch:"+first.BatchID] != group {
				t.Fatal("continuation re-applied the source scenario group")
			}
			if retry {
				if _, err := s.AppendScenarioBatch(context.Background(), first.BatchID, 1, 2); err != nil {
					t.Fatal(err)
				}
				waitRepetitions(t, s)
				groups = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), http.StatusOK)
				if groups.Memberships["batch:"+first.BatchID] != group {
					t.Fatal("appended run changed the existing result group")
				}
			}
		})
	}
}
