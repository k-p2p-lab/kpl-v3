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
	"sync"
	"testing"
	"time"
)

func libraryRequest(t *testing.T, s *Server, method, suffix string, body any) *httptest.ResponseRecorder {
	t.Helper()
	return scenarioAPIRequest(t, s, method, "/api/v1/library-groups"+suffix, body, true)
}

func librarySnapshot(t *testing.T, response *httptest.ResponseRecorder, status int) libraryGroupsSnapshot {
	t.Helper()
	if response.Code != status {
		t.Fatalf("groups status=%d want=%d body=%s", response.Code, status, response.Body)
	}
	var snapshot libraryGroupsSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Groups == nil || snapshot.Memberships == nil || snapshot.Revision == "" {
		t.Fatalf("invalid snapshot: %+v", snapshot)
	}
	return snapshot
}

func newTestLibraryGroup(t *testing.T, s *Server, name, revision string) libraryGroupsSnapshot {
	t.Helper()
	return librarySnapshot(t, libraryRequest(t, s, http.MethodPost, "", map[string]any{"name": name, "revision": revision}), http.StatusCreated)
}

func moveTestLibraryItems(t *testing.T, s *Server, group string, keys []string, revision string) libraryGroupsSnapshot {
	t.Helper()
	return librarySnapshot(t, libraryRequest(t, s, http.MethodPut, "/members", map[string]any{"groupId": group, "keys": keys, "revision": revision}), http.StatusOK)
}

func TestLibraryGroupsSharedCRUDAndRestartPreserveOriginals(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	scenario, err := s.createSavedScenario(scenarioSubmission{Name: "Scenario", YAML: validSavedScenarioYAML("source")})
	if err != nil {
		t.Fatal(err)
	}
	_, originalRun := resultFixture(t, s, "single", "completed", time.Now())
	batchFixture(t, s, "batch", "batch-first", "completed", 1, 2, 1)
	scenarioPath := filepath.Join(s.config.DataDir, "scenarios", scenario.ID+".json")
	originalScenario, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if snapshot.Revision != "0" || len(snapshot.Groups) != 0 {
		t.Fatal("unexpected initial state")
	}
	snapshot = newTestLibraryGroup(t, s, "  연구 A  ", snapshot.Revision)
	groupID := snapshot.Groups[0].ID
	if snapshot.Groups[0].Name != "연구 A" {
		t.Fatal("group name was not normalized")
	}
	keys := []string{"scenario:" + scenario.ID, "run:single", "batch:batch"}
	snapshot = moveTestLibraryItems(t, s, groupID, keys, snapshot.Revision)
	for _, key := range keys {
		if snapshot.Memberships[key] != groupID {
			t.Fatalf("missing membership %q", key)
		}
	}
	// Appended runs and retries use the same batch identity, with no per-run
	// membership copies and no edits to either original manifest.
	batchFixture(t, s, "batch", "batch-appended", "completed", 2, 2, 1)
	if len(snapshot.Memberships) != 3 {
		t.Fatal("batch membership was expanded per run")
	}
	restarted := New(ServerConfig{DataDir: s.config.DataDir}, nil)
	loaded := librarySnapshot(t, libraryRequest(t, restarted, "GET", "", nil), 200)
	if loaded.Revision != snapshot.Revision || loaded.Memberships["batch:batch"] != groupID {
		t.Fatal("restart lost memberships")
	}
	renamed := librarySnapshot(t, libraryRequest(t, restarted, "PATCH", "/"+groupID, map[string]string{"name": "Renamed", "revision": loaded.Revision}), 200)
	unchanged := librarySnapshot(t, libraryRequest(t, restarted, "PATCH", "/"+groupID, map[string]string{"name": "Renamed", "revision": renamed.Revision}), 200)
	if unchanged.Revision != renamed.Revision {
		t.Fatal("no-op rename changed revision")
	}
	cleared := librarySnapshot(t, libraryRequest(t, restarted, "DELETE", "/"+groupID, map[string]string{"revision": unchanged.Revision}), 200)
	if len(cleared.Groups) != 0 || len(cleared.Memberships) != 0 {
		t.Fatal("group deletion retained memberships")
	}
	gotScenario, _ := os.ReadFile(scenarioPath)
	gotRun, _ := os.ReadFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "single", "experiment.json"))
	if !bytes.Equal(gotScenario, originalScenario) || !bytes.Equal(gotRun, originalRun) {
		t.Fatal("group changes modified source records")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, "batch-first", "experiment.json")); err != nil {
		t.Fatal("group deletion removed result")
	}
}

func TestLibraryGroupsMoveUngroupAndRejectStaleRequests(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "first", "completed", time.Now())
	resultFixture(t, s, "second", "completed", time.Now())
	snapshot := newTestLibraryGroup(t, s, "First", "0")
	first := snapshot.Groups[0].ID
	snapshot = newTestLibraryGroup(t, s, "Second", snapshot.Revision)
	second := snapshot.Groups[1].ID
	snapshot = moveTestLibraryItems(t, s, first, []string{"run:first", "run:second"}, snapshot.Revision)
	oldRevision := snapshot.Revision
	snapshot = moveTestLibraryItems(t, s, second, []string{"run:first", "run:first"}, snapshot.Revision)
	if snapshot.Memberships["run:first"] != second || snapshot.Memberships["run:second"] != first {
		t.Fatal("move changed unselected membership")
	}
	for _, operation := range []struct {
		method, suffix string
		body           any
	}{
		{"POST", "", map[string]string{"name": "Stale", "revision": oldRevision}},
		{"PATCH", "/" + first, map[string]string{"name": "Stale", "revision": oldRevision}},
		{"DELETE", "/" + first, map[string]string{"revision": oldRevision}},
		{"PUT", "/members", map[string]any{"groupId": "", "keys": []string{"run:second"}, "revision": oldRevision}},
	} {
		if got := libraryRequest(t, s, operation.method, operation.suffix, operation.body); got.Code != 409 {
			t.Fatalf("stale %s accepted: %s", operation.method, got.Body)
		}
	}
	snapshot = moveTestLibraryItems(t, s, "", []string{"run:first"}, snapshot.Revision)
	if _, ok := snapshot.Memberships["run:first"]; ok {
		t.Fatal("ungroup failed")
	}
	noChange := moveTestLibraryItems(t, s, "", []string{"run:absent"}, snapshot.Revision)
	if noChange.Revision != snapshot.Revision {
		t.Fatal("ungrouping absent key should be a no-op")
	}
	if got := libraryRequest(t, s, "POST", "", map[string]string{"name": "fIRSt", "revision": snapshot.Revision}); got.Code != 409 {
		t.Fatal("duplicate name accepted")
	}
}

func TestLibraryGroupsMoveValidatesAllItemsBeforeSaving(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "exists", "completed", time.Now())
	batchFixture(t, s, "batch", "batched", "completed", 1, 2, 1)
	snapshot := newTestLibraryGroup(t, s, "Group", "0")
	for _, keys := range [][]string{{"run:exists", "run:missing"}, {"run:batched"}, {"batch:missing"}, {"scenario:" + strings.Repeat("a", 32)}} {
		got := libraryRequest(t, s, "PUT", "/members", map[string]any{"groupId": snapshot.Groups[0].ID, "keys": keys, "revision": snapshot.Revision})
		if got.Code != 404 {
			t.Fatalf("missing or wrong-scope item accepted: %d %s", got.Code, got.Body)
		}
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if loaded.Revision != snapshot.Revision || len(loaded.Memberships) != 0 {
		t.Fatal("failed move partially applied")
	}
}

func TestLibraryGroupsRemoveDeletedItemsWithoutDeletingGroup(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	scenario, err := s.createSavedScenario(scenarioSubmission{Name: "Scenario", YAML: validSavedScenarioYAML("s")})
	if err != nil {
		t.Fatal(err)
	}
	resultFixture(t, s, "single", "completed", time.Now())
	batchFixture(t, s, "batch", "batched", "completed", 1, 2, 1)
	snapshot := newTestLibraryGroup(t, s, "Keep group", "0")
	snapshot = moveTestLibraryItems(t, s, snapshot.Groups[0].ID, []string{"scenario:" + scenario.ID, "run:single", "batch:batch"}, snapshot.Revision)
	if err := s.deleteSavedScenario(scenario.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.deleteSavedResult("single"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deleteSavedBatch(context.Background(), "batch"); err != nil {
		t.Fatal(err)
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if len(loaded.Groups) != 1 || len(loaded.Memberships) != 0 || loaded.Revision == snapshot.Revision {
		t.Fatalf("source deletion cleanup: %+v", loaded)
	}
}

func TestLibraryGroupsConcurrentEditsHaveOneWinner(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	snapshot := newTestLibraryGroup(t, s, "Initial", "0")
	start := make(chan struct{})
	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for _, name := range []string{"Alpha", "Beta"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			codes <- libraryRequest(t, s, "PATCH", "/"+snapshot.Groups[0].ID, map[string]string{"name": name, "revision": snapshot.Revision}).Code
		}(name)
	}
	close(start)
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("concurrent edits: %v", counts)
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if loaded.Groups[0].Name != "Alpha" && loaded.Groups[0].Name != "Beta" {
		t.Fatal("invalid concurrent result")
	}
}

func TestLibraryGroupsValidationAndAuthentication(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	for _, operation := range []struct{ method, suffix string }{{"GET", ""}, {"POST", ""}, {"PUT", "/members"}, {"PATCH", "/" + strings.Repeat("a", 32)}, {"DELETE", "/" + strings.Repeat("a", 32)}} {
		got := scenarioAPIRequest(t, s, operation.method, "/api/v1/library-groups"+operation.suffix, map[string]string{"name": "Group", "revision": "0"}, false)
		if got.Code != 401 {
			t.Fatalf("unauthenticated %s: %d", operation.method, got.Code)
		}
	}
	for _, name := range []string{"", " ", "a\nb", strings.Repeat("가", 129)} {
		if got := libraryRequest(t, s, "POST", "", map[string]string{"name": name, "revision": "0"}); got.Code != 400 {
			t.Fatalf("bad name accepted: %d", got.Code)
		}
	}
	for _, body := range []string{`{"name":"x"}`, `{"name":"x","revision":"0","unknown":true}`, `{"name":"x","revision":"0"} {}`, "{\"name\":\"\xff\",\"revision\":\"0\"}"} {
		request := httptest.NewRequest("POST", "/api/v1/library-groups", strings.NewReader(body))
		authenticateRequest(t, s, request)
		response := httptest.NewRecorder()
		s.apiTestHandler(context.Background()).ServeHTTP(response, request)
		if response.Code != 400 {
			t.Fatalf("bad JSON accepted: %d %s", response.Code, response.Body)
		}
	}
	snapshot := newTestLibraryGroup(t, s, "Valid", "0")
	for _, keys := range [][]string{{}, {"run:../outside"}, {"scenario:wrong"}, {"other:run"}, make([]string, libraryMoveLimit+1)} {
		got := libraryRequest(t, s, "PUT", "/members", map[string]any{"groupId": snapshot.Groups[0].ID, "keys": keys, "revision": snapshot.Revision})
		if got.Code != 400 {
			t.Fatalf("invalid keys accepted: %d", got.Code)
		}
	}
	request := httptest.NewRequest("POST", "/api/v1/library-groups", strings.NewReader(strings.Repeat(" ", libraryGroupsRequestLimit+1)))
	authenticateRequest(t, s, request)
	response := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(response, request)
	if response.Code != 413 {
		t.Fatalf("oversized body: %d", response.Code)
	}
	for _, suffix := range []string{"/scenarios", "/../bad", "/" + strings.Repeat("a", 32) + "/extra"} {
		if got := libraryRequest(t, s, "GET", suffix, nil); got.Code == 200 {
			t.Fatalf("invalid path accepted: %q", suffix)
		}
	}
}

func TestLibraryGroupsRejectUnsafeStorageWithoutReplacement(t *testing.T) {
	for _, mode := range []string{"symlink", "directory", "invalid", "oversized", "invalid-member", "invalid-utf8"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			path := filepath.Join(s.config.DataDir, libraryGroupsFile)
			outside := filepath.Join(t.TempDir(), "private.json")
			if err := os.WriteFile(outside, []byte("private-marker"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mode {
			case "symlink":
				err = os.Symlink(outside, path)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "invalid":
				err = os.WriteFile(path, []byte("invalid private-marker"), 0600)
			case "oversized":
				err = os.WriteFile(path, bytes.Repeat([]byte("x"), libraryGroupsFileLimit+1), 0600)
			case "invalid-member":
				err = os.WriteFile(path, []byte(`{"version":1,"revision":"x","groups":[],"memberships":{"run:single":"missing"}}`), 0600)
			case "invalid-utf8":
				err = os.WriteFile(path, []byte("\xff"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"GET", "POST"} {
				got := libraryRequest(t, s, method, "", map[string]string{"name": "replace", "revision": "0"})
				if got.Code != 500 || strings.Contains(got.Body.String(), "private-marker") {
					t.Fatalf("unsafe storage accepted/leaked: %d %s", got.Code, got.Body)
				}
			}
			data, err := os.ReadFile(outside)
			if err != nil || string(data) != "private-marker" {
				t.Fatal("external target changed")
			}
		})
	}
}

func TestLibraryGroupsLimitsAndNoExperimentLockDependency(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	snapshot := emptyLibraryGroups()
	snapshot.Revision = "seed"
	for i := 0; i < libraryGroupLimit; i++ {
		snapshot.Groups = append(snapshot.Groups, libraryGroup{ID: fmt.Sprintf("%032x", i), Name: fmt.Sprintf("Group %d", i)})
	}
	if err := s.writeLibraryGroups(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := libraryRequest(t, s, "POST", "", map[string]string{"name": "Overflow", "revision": "seed"}); got.Code != 400 {
		t.Fatalf("group limit: %d", got.Code)
	}
	// Group editing must complete while the runtime's main locks are occupied.
	s.cancelMu.Lock()
	s.state.persistMu.Lock()
	s.scenarioMu.Lock()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- libraryRequest(t, s, "PATCH", "/"+snapshot.Groups[0].ID, map[string]string{"name": "Updated", "revision": "seed"})
	}()
	select {
	case response := <-done:
		librarySnapshot(t, response, 200)
	case <-time.After(2 * time.Second):
		t.Error("group changes waited for experiment locks")
	}
	s.scenarioMu.Unlock()
	s.state.persistMu.Unlock()
	s.cancelMu.Unlock()
}

func TestLibraryGroupsLegacyImportedResultsWorkWithoutNAS(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "legacy", "completed", time.Now())
	if err := os.Rename(filepath.Join(s.config.DataDir, currentRunsDirectory), filepath.Join(s.config.DataDir, archivedRunsDirectory)); err != nil {
		t.Fatal(err)
	}
	if err := s.importArchivedRuns(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(s.config.DataDir, archivedRunsDirectory), filepath.Join(s.config.DataDir, "offline-archive")); err != nil {
		t.Fatal(err)
	}
	snapshot := newTestLibraryGroup(t, s, "Archived", "0")
	snapshot = moveTestLibraryItems(t, s, snapshot.Groups[0].ID, []string{"run:legacy"}, snapshot.Revision)
	if len(snapshot.Memberships) != 1 {
		t.Fatal("legacy archive membership missing")
	}
}

func TestLibraryGroupsUnreadableResultsAndDeletionMarkers(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "unreadable", "completed", time.Now())
	if err := os.WriteFile(filepath.Join(s.config.DataDir, currentRunsDirectory, "unreadable", "experiment.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	resultFixture(t, s, "deleting", "completed", time.Now())
	s.state.persistMu.Lock()
	err := s.markResultDeletedLocked("deleting")
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := newTestLibraryGroup(t, s, "Group", "0")
	snapshot = moveTestLibraryItems(t, s, snapshot.Groups[0].ID, []string{"run:unreadable"}, snapshot.Revision)
	response := libraryRequest(t, s, "PUT", "/members", map[string]any{"groupId": snapshot.Groups[0].ID, "keys": []string{"run:deleting"}, "revision": snapshot.Revision})
	if response.Code != 404 {
		t.Fatalf("deleted result accepted: %d %s", response.Code, response.Body)
	}
}

func TestLibraryGroupsDeletingLastBatchMemberRemovesMembership(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "batch", "first", "completed", 1, 2, 1)
	batchFixture(t, s, "batch", "second", "completed", 2, 2, 1)
	snapshot := newTestLibraryGroup(t, s, "Group", "0")
	snapshot = moveTestLibraryItems(t, s, snapshot.Groups[0].ID, []string{"batch:batch"}, snapshot.Revision)
	if err := s.deleteSavedResult("first"); err != nil {
		t.Fatal(err)
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if loaded.Memberships["batch:batch"] == "" || loaded.Revision != snapshot.Revision {
		t.Fatal("deleting one member cleared the remaining batch")
	}
	if err := s.deleteSavedResult("second"); err != nil {
		t.Fatal(err)
	}
	loaded = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if len(loaded.Memberships) != 0 || loaded.Revision == snapshot.Revision {
		t.Fatal("last batch deletion left its membership")
	}
}

func TestLibraryGroupsConcurrentMoveAndSourceDeletion(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	snapshot := newTestLibraryGroup(t, s, "Group", "0")
	group := snapshot.Groups[0].ID
	for i := 0; i < 12; i++ {
		item, err := s.createSavedScenario(scenarioSubmission{Name: "Race", YAML: validSavedScenarioYAML("race")})
		if err != nil {
			t.Fatal(err)
		}
		key := "scenario:" + item.ID
		start := make(chan struct{})
		moved := make(chan int, 1)
		deleted := make(chan error, 1)
		go func(revision string) {
			<-start
			moved <- libraryRequest(t, s, "PUT", "/members", map[string]any{"groupId": group, "keys": []string{key}, "revision": revision}).Code
		}(snapshot.Revision)
		go func() { <-start; deleted <- s.deleteSavedScenario(item.ID) }()
		close(start)
		if err := <-deleted; err != nil {
			t.Fatal(err)
		}
		code := <-moved
		if code != 200 && code != 404 && code != 409 {
			t.Fatalf("unexpected move/delete race result: %d", code)
		}
		snapshot = librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
		if _, exists := snapshot.Memberships[key]; exists {
			t.Fatal("delete race left a membership")
		}
		if _, err := os.Stat(filepath.Join(s.config.DataDir, "scenarios", item.ID+".json")); !os.IsNotExist(err) {
			t.Fatal("move recreated deleted scenario")
		}
	}
}

func TestLibraryGroupsMembershipLimitDoesNotPartiallyMove(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "extra", "completed", time.Now())
	snapshot := newTestLibraryGroup(t, s, "Full", "0")
	for i := 0; i < libraryMembershipLimit; i++ {
		snapshot.Memberships[fmt.Sprintf("run:existing-%d", i)] = snapshot.Groups[0].ID
	}
	if err := s.writeLibraryGroups(snapshot); err != nil {
		t.Fatal(err)
	}
	got := libraryRequest(t, s, "PUT", "/members", map[string]any{"groupId": snapshot.Groups[0].ID, "keys": []string{"run:extra"}, "revision": snapshot.Revision})
	if got.Code != 400 {
		t.Fatalf("membership limit not enforced: %d %s", got.Code, got.Body)
	}
	loaded := librarySnapshot(t, libraryRequest(t, s, "GET", "", nil), 200)
	if loaded.Revision != snapshot.Revision || loaded.Memberships["run:extra"] != "" || len(loaded.Memberships) != libraryMembershipLimit {
		t.Fatal("limit failure partially changed memberships")
	}
}
