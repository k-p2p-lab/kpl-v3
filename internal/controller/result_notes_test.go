package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func noteRequest(server *Server, method, id, body string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	server.apiTestHandler(context.Background()).ServeHTTP(r, httptest.NewRequest(method, "/api/v1/results/"+id+"/note", strings.NewReader(body)))
	return r
}

func putNote(t *testing.T, server *Server, id, text, revision string) resultNote {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"text": text, "revision": revision})
	r := noteRequest(server, http.MethodPut, id, string(body))
	if r.Code != http.StatusOK {
		t.Fatalf("save note: %d %s", r.Code, r.Body)
	}
	var note resultNote
	if err := json.Unmarshal(r.Body.Bytes(), &note); err != nil {
		t.Fatal(err)
	}
	return note
}

func TestResultNotesPersistIndependentlyAndExport(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	experiment, original := resultFixture(t, server, "run-note", "running", time.Now())
	empty := noteRequest(server, "GET", experiment.ID, "")
	if empty.Code != 200 || !strings.Contains(empty.Body.String(), `"revision":"0"`) || empty.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("empty note: %d %s", empty.Code, empty.Body)
	}
	text := "한글 메모\n<script>alert('plain text')</script>\n" + strings.Repeat("관측", 120)
	note := putNote(t, server, experiment.ID, text, "0")
	if note.Text != text || note.Revision == "0" || note.UpdatedAt == nil {
		t.Fatalf("saved note: %+v", note)
	}
	unchanged, _ := os.ReadFile(filepath.Join(server.config.DataDir, "runs", experiment.ID, "experiment.json"))
	if !bytes.Equal(original, unchanged) {
		t.Fatal("note changed experiment metadata")
	}
	experiment.State = "completed"
	if err := server.persistManifest(experiment, []byte("original scenario")); err != nil {
		t.Fatal(err)
	}
	restarted := New(server.config, nil)
	if got := noteRequest(restarted, "GET", experiment.ID, ""); got.Code != 200 || got.Body.String() == "" {
		t.Fatalf("restarted note: %s", got.Body)
	} else {
		var loaded resultNote
		_ = json.Unmarshal(got.Body.Bytes(), &loaded)
		if loaded.Text != text || loaded.Revision != note.Revision {
			t.Fatal("note was lost after finalizing or restarting")
		}
	}
	var results []savedResult
	list := resultRequest(restarted, "GET", "/api/v1/results")
	if err := json.Unmarshal(list.Body.Bytes(), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Note == nil || len([]rune(results[0].Note.Preview)) != 161 || strings.Contains(list.Body.String(), strings.Repeat("관측", 120)) {
		t.Fatalf("list should contain a bounded preview: %s", list.Body)
	}
	if results[0].Note.UpdatedAt == nil || results[0].SourceBytes == nil {
		t.Fatal("missing note timestamp or source size")
	}
	download := resultRequest(restarted, "GET", "/api/v1/experiments/"+experiment.ID+"/download")
	if download.Code != 200 {
		t.Fatalf("download: %d %s", download.Code, download.Body)
	}
	files := decodeResultZIP(t, download.Body.Bytes())
	var exported resultNote
	if err := json.Unmarshal(files[resultNoteFile], &exported); err != nil || exported.Text != text {
		t.Fatalf("exported note: %s, %v", files[resultNoteFile], err)
	}
	var total int64
	for _, name := range resultSourceFiles {
		total += int64(len(files[name]))
	}
	if total != *results[0].SourceBytes {
		t.Fatalf("source size does not include note: got %d want %d", *results[0].SourceBytes, total)
	}
	// An unreadable experiment must not hide its independently saved note.
	if err := os.WriteFile(filepath.Join(server.config.DataDir, "runs", experiment.ID, "experiment.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	list = resultRequest(restarted, "GET", "/api/v1/results")
	if err := json.Unmarshal(list.Body.Bytes(), &results); err != nil || len(results) != 1 || results[0].State != "unreadable" || results[0].Note == nil {
		t.Fatalf("note hidden by corrupt experiment metadata: %s", list.Body)
	}
}

func TestResultNotesConflictClearAndRunIsolation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, id := range []string{"attempt-old", "attempt-new"} {
		resultFixture(t, server, id, "failed", time.Now())
	}
	first := putNote(t, server, "attempt-old", "first note", "0")
	stale := noteRequest(server, "PUT", "attempt-old", `{"text":"stale edit","revision":"0"}`)
	if stale.Code != 409 {
		t.Fatalf("stale save: %d", stale.Code)
	}
	second := putNote(t, server, "attempt-new", "retry note", "0")
	cleared := putNote(t, server, "attempt-old", " \n\t", first.Revision)
	if cleared.Text != "" || cleared.Revision == first.Revision || cleared.Revision == "0" {
		t.Fatalf("cleared note revision lost: %+v", cleared)
	}
	body, _ := json.Marshal(map[string]string{"text": "resurrected", "revision": first.Revision})
	if got := noteRequest(server, "PUT", "attempt-old", string(body)); got.Code != 409 {
		t.Fatal("stale save resurrected cleared note")
	}
	unchanged := putNote(t, server, "attempt-new", second.Text, second.Revision)
	if unchanged.Revision != second.Revision {
		t.Fatal("no-op save changed another run's note")
	}
	var results []savedResult
	_ = json.Unmarshal(resultRequest(server, "GET", "/api/v1/results").Body.Bytes(), &results)
	for _, run := range results {
		if (run.Note != nil) != (run.ID == "attempt-new") {
			t.Fatalf("notes leaked between attempts: %+v", run)
		}
	}
}

func TestResultNoteConcurrentEditorsAndSnapshot(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run-concurrent", "completed", time.Now())
	old := putNote(t, server, "run-concurrent", "old note", "0")
	snapshot, err := server.captureResult("run-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	// Populate the exact ZIP-size cache before replacing the note.
	if _, err := server.prepareResultArchiveInfo(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, text := range []string{"editor A", "editor B"} {
		wg.Add(1)
		go func(text string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]string{"text": text, "revision": old.Revision})
			codes <- noteRequest(server, "PUT", "run-concurrent", string(body)).Code
		}(text)
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("concurrent updates: %v", counts)
	}
	for _, file := range snapshot.files {
		if file.name == resultNoteFile {
			data, _ := io.ReadAll(io.NewSectionReader(file.file, 0, file.size))
			var note resultNote
			_ = json.Unmarshal(data, &note)
			if note.Text != old.Text {
				t.Fatal("note changed inside an already captured ZIP snapshot")
			}
		}
	}
	fresh, err := server.captureResultFiles("run-concurrent", false)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.close()
	if _, usable := server.cachedResultArchiveInfo(fresh, time.Now()); usable {
		t.Fatal("note edit reused stale ZIP metadata")
	}
}

func TestResultNoteValidationAndDeletedResult(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, server, "run-validation", "completed", time.Now())
	for _, body := range []string{`{}`, `null`, `{"text":null,"revision":"0"}`, `{"text":"x"}`, `{"text":3,"revision":"0"}`, `{"text":"x","revision":"0","extra":1}`, `{"text":"x","revision":"0"} {}`} {
		if got := noteRequest(server, "PUT", "run-validation", body); got.Code != 400 {
			t.Fatalf("invalid body accepted: %s: %d", body, got.Code)
		}
	}
	body, _ := json.Marshal(map[string]string{"text": strings.Repeat("가", resultNoteTextLimit/3+1), "revision": "0"})
	if got := noteRequest(server, "PUT", "run-validation", string(body)); got.Code != 400 {
		t.Fatalf("UTF-8 byte limit: %d", got.Code)
	}
	if got := noteRequest(server, "PUT", "run-validation", strings.Repeat(" ", resultNoteFileLimit+1)); got.Code != 413 {
		t.Fatalf("envelope limit: %d", got.Code)
	}
	// Worst-case escaping is allowed for text that still fits the decoded limit.
	note := putNote(t, server, "run-validation", strings.Repeat("\x00", resultNoteTextLimit), "0")
	if len(note.Text) != resultNoteTextLimit {
		t.Fatal("valid escaped note was truncated")
	}
	for _, method := range []string{"POST", "DELETE", "PATCH"} {
		if got := noteRequest(server, method, "run-validation", ""); got.Code != 405 {
			t.Fatalf("unsupported method: %s %d", method, got.Code)
		}
	}
	if err := server.deleteSavedResult("run-validation"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"run-validation", "missing", "bad/id"} {
		for _, method := range []string{"GET", "PUT"} {
			if got := noteRequest(server, method, id, `{"text":"revive","revision":"0"}`); got.Code != 404 {
				t.Fatalf("missing result %s %s: %d", method, id, got.Code)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(server.config.DataDir, "runs", "run-validation")); !os.IsNotExist(err) {
		t.Fatal("note recreated deleted result")
	}
}

func TestResultNoteRejectsUnsafeOrCorruptStorage(t *testing.T) {
	for _, kind := range []string{"symlink", "directory", "corrupt", "wrong-run"} {
		t.Run(kind, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			resultFixture(t, server, "run-storage", "completed", time.Now())
			notePath := filepath.Join(server.config.DataDir, "runs", "run-storage", resultNoteFile)
			outside := filepath.Join(t.TempDir(), "outside.json")
			if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(outside, notePath)
			case "directory":
				err = os.Mkdir(notePath, 0700)
			case "corrupt":
				err = os.WriteFile(notePath, []byte("invalid"), 0600)
			case "wrong-run":
				err = os.WriteFile(notePath, []byte(`{"runId":"another","text":"private","revision":"1","updatedAt":"2026-09-25T00:00:00Z"}`), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"GET", "PUT"} {
				if got := noteRequest(server, method, "run-storage", `{"text":"replacement","revision":"0"}`); got.Code != 500 || strings.Contains(got.Body.String(), "private") {
					t.Fatalf("unsafe note: %d %s", got.Code, got.Body)
				}
			}
			data, _ := os.ReadFile(outside)
			if string(data) != "untouched" {
				t.Fatal("symlink target modified")
			}
			list := resultRequest(server, "GET", "/api/v1/results")
			if list.Code != 200 || !strings.Contains(list.Body.String(), `"state":"completed"`) {
				t.Fatal("corrupt note hid valid experiment metadata")
			}
		})
	}
}

func TestResultNotesRequireSessionAndCSRFHeader(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	resultFixture(t, server, "run-auth", "completed", time.Now())
	for _, method := range []string{"GET", "PUT"} {
		if got := noteRequest(server, method, "run-auth", `{"text":"x","revision":"0"}`); got.Code != 401 {
			t.Fatalf("unauthenticated %s accepted: %d", method, got.Code)
		}
	}
	cookie := loginCookie(t, server)
	for _, header := range []bool{false, true} {
		r := httptest.NewRequest("PUT", "/api/v1/results/run-auth/note", strings.NewReader(`{"text":"x","revision":"0"}`))
		r.AddCookie(cookie)
		if header {
			r.Header.Set("X-KPL-Request", "dashboard")
		}
		w := httptest.NewRecorder()
		server.Handler(context.Background()).ServeHTTP(w, r)
		want := 403
		if header {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("CSRF header=%v: %d %s", header, w.Code, w.Body)
		}
	}
}
