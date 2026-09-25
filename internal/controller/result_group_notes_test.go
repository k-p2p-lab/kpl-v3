package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func groupNoteRequest(s *Server, method, id, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(r, httptest.NewRequest(method, "/api/v1/result-batches/"+id+"/note", strings.NewReader(body)))
	return r
}
func putGroupNote(t *testing.T, s *Server, id, text, revision string) resultNote {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"text": text, "revision": revision})
	request := httptest.NewRequest("PUT", "/api/v1/result-batches/"+id+"/note", strings.NewReader(string(raw)))
	if s.config.User != "" {
		authenticateRequest(t, s, request)
	}
	r := httptest.NewRecorder()
	s.apiTestHandler(context.Background()).ServeHTTP(r, request)
	if r.Code != 200 {
		t.Fatalf("group note: %d %s", r.Code, r.Body)
	}
	var note resultNote
	if err := json.Unmarshal(r.Body.Bytes(), &note); err != nil {
		t.Fatal(err)
	}
	return note
}

func TestGroupNotesPersistOfflineAndRemainSeparateFromRuns(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for i, id := range []string{"first", "second"} {
		batchFixture(t, s, "group", id, "completed", i+1, 2, 1)
	}
	batchFixture(t, s, "other", "unrelated", "completed", 1, 2, 1)
	if r := groupNoteRequest(s, "GET", "group", ""); r.Code != 200 || !strings.Contains(r.Body.String(), `"batchId":"group"`) || !strings.Contains(r.Body.String(), `"revision":"0"`) {
		t.Fatalf("empty group note: %d %s", r.Code, r.Body)
	}
	text := "Group 관측\n<script>plain text</script>" + strings.Repeat("메모", 120)
	note := putGroupNote(t, s, "group", text, "0")
	putNote(t, s, "first", "Run only", "0")
	if note.BatchID != "group" || note.RunID != "" {
		t.Fatalf("wrong scope: %+v", note)
	}
	archiveTestRun(t, s, "first")
	archiveTestRun(t, s, "second")
	restarted := New(s.config, nil)
	restarted.archiveIOCheck = func() { t.Fatal("group metadata attempted NAS access") }
	if err := os.Rename(filepath.Join(s.config.DataDir, "runs"), filepath.Join(s.config.DataDir, "offline")); err != nil {
		t.Fatal(err)
	}
	loaded := groupNoteRequest(restarted, "GET", "group", "")
	var got resultNote
	if loaded.Code != 200 || json.Unmarshal(loaded.Body.Bytes(), &got) != nil || got.Text != text || got.Revision != note.Revision {
		t.Fatalf("restart lost group note: %s", loaded.Body)
	}
	var list []savedResult
	r := resultRequest(restarted, "GET", "/api/v1/results")
	if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &list) != nil {
		t.Fatalf("offline list: %s", r.Body)
	}
	count := 0
	for _, run := range list {
		if run.GroupNote != nil {
			count++
			if run.BatchID != "group" || len([]rune(run.GroupNote.Preview)) != 161 {
				t.Fatalf("preview: %+v", run.GroupNote)
			}
		}
		if (run.Note != nil) != (run.ID == "first") {
			t.Fatal("run notes and group notes were mixed")
		}
	}
	if count != 1 || strings.Contains(r.Body.String(), strings.Repeat("메모", 120)) {
		t.Fatal("list repeated the group note or leaked full text")
	}
	if err := restarted.deleteSavedResult("first"); err != nil {
		t.Fatal(err)
	}
	if r := groupNoteRequest(restarted, "GET", "group", ""); r.Code != 200 {
		t.Fatalf("deleting first run lost group note: %d", r.Code)
	}
	if r := resultRequest(restarted, "GET", "/api/v1/results"); !strings.Contains(r.Body.String(), `"groupNote"`) {
		t.Fatal("preview disappeared with its original carrier run")
	}
	if _, err := restarted.deleteSavedBatch(context.Background(), "group"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, resultGroupsDirectory, "group")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("group deletion retained its note")
	}
	if r := groupNoteRequest(restarted, "PUT", "group", `{"text":"stale","revision":"0"}`); r.Code != 404 {
		t.Fatalf("deleted group recreated: %d", r.Code)
	}
	if r := groupNoteRequest(restarted, "GET", "other", ""); r.Code != 200 {
		t.Fatal("group deletion changed another group")
	}
}

func TestGroupNotesConflictClearAndDeleteRace(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	batchFixture(t, s, "group", "member", "completed", 1, 2, 1)
	initial := putGroupNote(t, s, "group", "first", "0")
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, text := range []string{"editor-a", "editor-b"} {
		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]string{"text": text, "revision": initial.Revision})
			codes <- groupNoteRequest(s, "PUT", "group", string(raw)).Code
		}(text)
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("concurrent edits: %v", counts)
	}
	var latest resultNote
	if err := json.Unmarshal(groupNoteRequest(s, "GET", "group", "").Body.Bytes(), &latest); err != nil {
		t.Fatal(err)
	}
	cleared := putGroupNote(t, s, "group", " \n\t", latest.Revision)
	if cleared.Text != "" || cleared.Revision == latest.Revision {
		t.Fatal("clear lost revision")
	}
	raw, _ := json.Marshal(map[string]string{"text": "stale", "revision": latest.Revision})
	if r := groupNoteRequest(s, "PUT", "group", string(raw)); r.Code != 409 {
		t.Fatal("stale edit restored cleared text")
	}
	raw, _ = json.Marshal(map[string]string{"text": "racing", "revision": cleared.Revision})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- groupNoteRequest(s, "PUT", "group", string(raw)) }()
	if _, err := s.deleteSavedBatch(context.Background(), "group"); err != nil {
		t.Fatal(err)
	}
	if r := <-done; r.Code != 200 && r.Code != 404 {
		t.Fatalf("race: %d %s", r.Code, r.Body)
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, resultGroupsDirectory, "group")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("concurrent editor recreated deleted group")
	}
}

func TestGroupNoteSurvivesRetryAndCanBeDeletedWithoutMembers(t *testing.T) {
	f := newResumeFixture(t)
	runs := f.failedBatch(t, 2)
	note := putGroupNote(t, f.server, runs[0].BatchID, "Shared across attempts", "0")
	f.fail.Store(false)
	if _, err := f.server.RetryScenarioBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatal(err)
	}
	waitRepetitions(t, f.server)
	same := putGroupNote(t, f.server, runs[0].BatchID, note.Text, note.Revision)
	if same.Revision != note.Revision {
		t.Fatal("retry replaced group note")
	}
	members, err := f.server.allBatchMembers(context.Background(), runs[0].BatchID)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range members {
		if err := f.server.deleteSavedResult(run.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.server.deleteSavedBatch(context.Background(), runs[0].BatchID); err != nil {
		t.Fatalf("orphan note deletion: %v", err)
	}
}

func TestGroupNoteValidationAuthenticationAndUnsafeStorage(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	batchFixture(t, s, "group", "member", "completed", 1, 2, 1)
	for _, method := range []string{"GET", "PUT"} {
		if r := groupNoteRequest(s, method, "group", `{"text":"x","revision":"0"}`); r.Code != 401 {
			t.Fatalf("unauthenticated note: %d", r.Code)
		}
	}
	cookie := loginCookie(t, s)
	for _, header := range []bool{false, true} {
		request := httptest.NewRequest("PUT", "/api/v1/result-batches/group/note", strings.NewReader(`{"text":"x","revision":"0"}`))
		request.AddCookie(cookie)
		if header {
			request.Header.Set("X-KPL-Request", "dashboard")
		}
		response := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(response, request)
		want := 403
		if header {
			want = 200
		}
		if response.Code != want {
			t.Fatalf("CSRF: %d", response.Code)
		}
	}
	plain := New(ServerConfig{DataDir: s.config.DataDir}, nil)
	for _, body := range []string{`{}`, `{"text":"x"}`, `{"text":"x","revision":"0","extra":1}`} {
		if r := groupNoteRequest(plain, "PUT", "group", body); r.Code != 400 {
			t.Fatalf("invalid payload: %d", r.Code)
		}
	}
	oversized, _ := json.Marshal(map[string]string{"text": strings.Repeat("가", resultNoteTextLimit/3+1), "revision": "0"})
	if r := groupNoteRequest(plain, "PUT", "group", string(oversized)); r.Code != 400 {
		t.Fatal("group note bypassed UTF-8 limit")
	}
	if r := groupNoteRequest(plain, "GET", "missing", ""); r.Code != 404 {
		t.Fatal("unknown group accepted")
	}
	notePath := filepath.Join(s.config.DataDir, resultGroupsDirectory, "group", resultNoteFile)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(notePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, notePath); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"GET", "PUT"} {
		if r := groupNoteRequest(plain, method, "group", `{"text":"replace","revision":"0"}`); r.Code != 500 {
			t.Fatalf("unsafe note accepted: %d", r.Code)
		}
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "untouched" {
		t.Fatal("modified symlink target")
	}
}
