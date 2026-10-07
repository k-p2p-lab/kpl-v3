package controller

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func libraryTicket(t *testing.T, s *Server, kind string, ids ...string) string {
	t.Helper()
	response := scenarioAPIRequest(t, s, http.MethodPost, "/api/v1/downloads", libraryDownload{Kind: kind, IDs: ids}, true)
	if response.Code != http.StatusCreated {
		t.Fatalf("ticket status=%d body=%s", response.Code, response.Body)
	}
	var ticket struct {
		URL   string `json:"url"`
		Count int    `json:"count"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.Count == 0 || !strings.HasPrefix(ticket.URL, "/api/v1/downloads/") {
		t.Fatalf("ticket=%+v", ticket)
	}
	return ticket.URL
}

func TestLibraryScenarioDownloadPreservesYAMLAndDisambiguatesNames(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "test-password"}, nil)
	first, err := s.createSavedScenario(scenarioSubmission{Name: "한글/name", YAML: validSavedScenarioYAML("first")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.createSavedScenario(scenarioSubmission{Name: "한글\\name", YAML: validSavedScenarioYAML("second")})
	if err != nil {
		t.Fatal(err)
	}
	path := libraryTicket(t, s, "scenarios", first.ID, second.ID, first.ID)
	// A download URL is not an authentication credential.
	if response := scenarioAPIRequest(t, s, http.MethodGet, path, nil, false); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated download=%d", response.Code)
	}
	response := scenarioAPIRequest(t, s, http.MethodGet, path, nil, true)
	if response.Code != 200 || response.Header().Get("Content-Type") != "application/zip" {
		t.Fatalf("download: %d %s", response.Code, response.Body)
	}
	files := decodeResultZIP(t, response.Body.Bytes())
	if len(files) != 2 || string(files["한글_name.yaml"]) != first.YAML || string(files["한글_name (2).yaml"]) != second.YAML {
		t.Fatalf("files=%v", files)
	}
	if response := scenarioAPIRequest(t, s, http.MethodGet, path, nil, true); response.Code != http.StatusGone {
		t.Fatalf("reused ticket=%d", response.Code)
	}
	if len(s.libraryDownloads) != 0 || len(s.libraryDownloadSlots) != 0 {
		t.Fatal("download state retained")
	}
}

func TestLibraryResultDownloadIncludesBatchMembersAndCompleteRunZIPs(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, id := range []string{"one", "two"} {
		experiment, _ := resultFixture(t, s, id, "completed", time.Now().UTC())
		experiment.Name = "실험 / ER"
		experiment.BatchID = "batch"
		if err := s.persistManifest(experiment, []byte(validSavedScenarioYAML(id))); err != nil {
			t.Fatal(err)
		}
	}
	path := libraryTicket(t, s, "results", "one", "two", "one")
	response := resultRequest(s, http.MethodGet, path)
	if response.Code != 200 {
		t.Fatalf("download=%d %s", response.Code, response.Body)
	}
	outer := decodeResultZIP(t, response.Body.Bytes())
	if len(outer) != 2 {
		t.Fatalf("entries=%d", len(outer))
	}
	for _, id := range []string{"one", "two"} {
		inner := decodeResultZIP(t, outer["[실험 _ ER]-"+id+".zip"])
		for _, name := range []string{"scenario.yaml", "experiment.json", "events.jsonl", "metrics.json", "export.json"} {
			if _, ok := inner[name]; !ok {
				t.Errorf("%s missing %s", id, name)
			}
		}
		var metadata map[string]any
		if err := json.Unmarshal(inner["experiment.json"], &metadata); err != nil || metadata["id"] != id {
			t.Fatalf("wrong Run metadata: %v %v", metadata, err)
		}
	}
	if len(s.resultDownloads) != 0 || len(s.resultArchiveSlots) != 0 || len(s.libraryDownloadSlots) != 0 {
		t.Fatal("export lease/slot leak")
	}
}

func TestLibraryDownloadRejectsBadSelectionsAndCSRF(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "test-password"}, nil)
	for _, body := range []any{
		libraryDownload{Kind: "unknown", IDs: []string{"run"}},
		libraryDownload{Kind: "results"},
		libraryDownload{Kind: "results", IDs: []string{"../secret"}},
		libraryDownload{Kind: "results", IDs: make([]string, libraryDownloadLimit+1)},
		map[string]any{"kind": "results", "ids": []string{"run"}, "unexpected": true},
	} {
		if response := scenarioAPIRequest(t, s, http.MethodPost, "/api/v1/downloads", body, true); response.Code != 400 {
			t.Fatalf("selection accepted: %d %s", response.Code, response.Body)
		}
	}
	if response := scenarioAPIRequest(t, s, http.MethodPost, "/api/v1/downloads", libraryDownload{Kind: "results", IDs: []string{"missing"}}, true); response.Code != 404 {
		t.Fatalf("missing=%d", response.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/downloads", strings.NewReader(`{"kind":"results","ids":["run"]}`))
	req.AddCookie(loginCookie(t, s))
	response := httptest.NewRecorder()
	s.Handler(context.Background()).ServeHTTP(response, req)
	if response.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF header=%d", response.Code)
	}
	if len(s.libraryDownloads) != 0 {
		t.Fatal("invalid request created ticket")
	}
}

func TestLibraryDownloadTicketExpiryAndBounds(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Now().UTC())
	path := libraryTicket(t, s, "results", "run")
	id := strings.TrimPrefix(path, "/api/v1/downloads/")
	request := s.libraryDownloads[id]
	request.ExpiresAt = time.Now().Add(-time.Second)
	s.libraryDownloads[id] = request
	if response := resultRequest(s, http.MethodGet, path); response.Code != http.StatusGone {
		t.Fatalf("expired=%d", response.Code)
	}
	for i := 0; i < libraryDownloadTicketLimit; i++ {
		libraryTicket(t, s, "results", "run")
	}
	response := scenarioAPIRequest(t, s, http.MethodPost, "/api/v1/downloads", libraryDownload{Kind: "results", IDs: []string{"run"}}, true)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("ticket cap=%d", response.Code)
	}
	for id := range s.libraryDownloads {
		path = "/api/v1/downloads/" + id
		break
	}
	s.libraryDownloadSlots <- struct{}{}
	s.libraryDownloadSlots <- struct{}{}
	if response := resultRequest(s, http.MethodGet, path); response.Code != http.StatusTooManyRequests {
		t.Fatalf("stream cap=%d", response.Code)
	}
	<-s.libraryDownloadSlots
	<-s.libraryDownloadSlots
	if response := resultRequest(s, http.MethodGet, path); response.Code != http.StatusOK {
		t.Fatalf("busy consumed ticket: %d", response.Code)
	}
}

func TestLibraryDownloadCancellationReleasesRunAndRejectsPartialArchive(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "first", "completed", time.Now().UTC())
	resultFixture(t, s, "second", "completed", time.Now().UTC())
	path := libraryTicket(t, s, "results", "first", "second")
	// Losing a later selected Run must abort, not finish a ZIP missing that Run.
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, currentRunsDirectory, "second")); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	func() {
		defer func() {
			if got := recover(); got != http.ErrAbortHandler {
				t.Errorf("partial export panic=%v", got)
			}
		}()
		s.handleLibraryDownload(response, httptest.NewRequest(http.MethodGet, path, nil))
	}()
	if _, err := zip.NewReader(bytes.NewReader(response.Body.Bytes()), int64(response.Body.Len())); err == nil {
		t.Fatal("partial export was a valid ZIP")
	}
	if len(s.resultDownloads) != 0 || len(s.libraryDownloadSlots) != 0 || len(s.resultArchiveSlots) != 0 {
		t.Fatal("failure leaked lease/slot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	err := s.writeLibraryDownloadItem(ctx, "results", "first", map[string]bool{}, func() *zip.Writer { called = true; return nil })
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("canceled export=%v started=%v", err, called)
	}
}

func TestDownloadFilePartsArePortableAndBounded(t *testing.T) {
	for _, tc := range []struct{ name, want string }{{" ../a\\b:*?\"<>|\n", "_a_b________"}, {"...", "fallback"}, {"CON", "_CON"}, {"한글 이름", "한글 이름"}} {
		if got := downloadFilePart(tc.name, "fallback"); got != tc.want {
			t.Errorf("%q -> %q, want %q", tc.name, got, tc.want)
		}
	}
	got := downloadFilePart(strings.Repeat("한", 10000), "fallback")
	if len(got) > 96 || !utf8.ValidString(got) {
		t.Fatalf("invalid/truncated file part %q", got)
	}
}

func TestLibraryDownloadTicketCanOnlyBeClaimedOnceConcurrently(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "run", "completed", time.Now().UTC())
	path := libraryTicket(t, s, "results", "run")
	start, statuses := make(chan struct{}), make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			statuses <- resultRequest(s, http.MethodGet, path).Code
		}()
	}
	close(start)
	first, second := <-statuses, <-statuses
	if !((first == 200 && second == 410) || (first == 410 && second == 200)) {
		t.Fatalf("concurrent claims=%d,%d", first, second)
	}
}

func TestLibraryDownloadMissingFirstRunReturnsErrorBeforeZIP(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	resultFixture(t, s, "first", "completed", time.Now().UTC())
	path := libraryTicket(t, s, "results", "first")
	if err := os.RemoveAll(filepath.Join(s.config.DataDir, currentRunsDirectory, "first")); err != nil {
		t.Fatal(err)
	}
	response := resultRequest(s, http.MethodGet, path)
	if response.Code != 404 || response.Header().Get("Content-Disposition") != "" {
		t.Fatalf("missing first run: %d %v", response.Code, response.Header())
	}
	if len(s.resultDownloads) != 0 || len(s.libraryDownloadSlots) != 0 {
		t.Fatal("failed first item retained resources")
	}
}
