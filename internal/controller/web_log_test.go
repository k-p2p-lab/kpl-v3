package controller

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func readWebLog(t *testing.T, dataDir, name string) []webLogRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataDir, "logs", name))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	records := []webLogRecord{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		var record webLogRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("invalid JSON log line: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestWebLogsRecordVisitsLoginAndLogoutWithoutCredentials(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "private-password-sentinel"}, nil)
	handler := s.Handler(context.Background())
	request := func(method, path, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.RemoteAddr = "198.51.100.7:4567"
		r.Header.Set("User-Agent", "audit-test")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-KPL-Request", "dashboard")
		r.Header.Set("Authorization", "Bearer hidden-authorization-sentinel")
		r.Header.Set("Referer", "http://example.org/?secret=hidden-referer-sentinel")
		r.Header.Set("X-Forwarded-For", "203.0.113.99")
		r.Header.Set("Forwarded", "for=203.0.113.99")
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if got := request("GET", "/?token=hidden-query-sentinel", "", nil); got.Code != 303 {
		t.Fatalf("redirect: %d", got.Code)
	}
	if got := request("GET", "/login", "", nil); got.Code != 200 {
		t.Fatalf("login page: %d", got.Code)
	}
	if got := request("POST", "/api/v1/auth/login", `{"user":"wrong-user","password":"wrong-password-sentinel"}`, nil); got.Code != 401 {
		t.Fatalf("invalid credentials: %d", got.Code)
	}
	body, _ := json.Marshal(map[string]string{"user": s.config.User, "password": s.config.Password})
	loggedIn := request("POST", "/api/v1/auth/login", string(body), nil)
	if loggedIn.Code != 200 {
		t.Fatalf("login: %d %s", loggedIn.Code, loggedIn.Body)
	}
	cookie := loggedIn.Result().Cookies()[0]
	if got := request("GET", "/", "", cookie); got.Code != 200 {
		t.Fatalf("dashboard: %d", got.Code)
	}
	if got := request("POST", "/api/v1/auth/logout", "", cookie); got.Code != 204 {
		t.Fatalf("logout: %d", got.Code)
	}
	if got := request("GET", "/api/v1/results", "", cookie); got.Code != 401 {
		t.Fatalf("revoked session: %d", got.Code)
	}
	access := readWebLog(t, s.config.DataDir, "access.jsonl")
	auth := readWebLog(t, s.config.DataDir, "auth.jsonl")
	if len(access) != 7 || len(auth) != 5 {
		t.Fatalf("access/auth counts: %d/%d", len(access), len(auth))
	}
	for i, record := range access {
		if record.RequestID == "" || record.Timestamp.IsZero() || record.RemoteIP != "198.51.100.7" || record.UserAgent != "audit-test" || record.DurationMS < 0 {
			t.Fatalf("incomplete request %d: %+v", i, record)
		}
	}
	if access[0].Path != "/" || access[4].Authentication != "session" || access[4].User != "admin" || access[4].Bytes <= 0 {
		t.Fatalf("incorrect visit data: %+v", access)
	}
	if auth[1].Event != "login" || auth[1].User != "wrong-user" || auth[1].Reason != "invalid_credentials" || auth[1].Outcome != "failure" {
		t.Fatalf("missing login failure: %+v", auth[1])
	}
	if auth[2].Event != "login" || auth[2].Outcome != "success" || auth[2].RequestID != access[3].RequestID || auth[2].Reason != "" {
		t.Fatalf("missing login success: %+v", auth[2])
	}
	if auth[3].Event != "logout" || auth[3].User != "admin" || auth[3].Outcome != "success" {
		t.Fatalf("missing logout: %+v", auth[3])
	}
	if auth[4].Event != "access_denied" || auth[4].Reason != "login_required" {
		t.Fatalf("missing expired/revoked session denial: %+v", auth[4])
	}
	for _, name := range []string{"access.jsonl", "auth.jsonl"} {
		raw, err := os.ReadFile(filepath.Join(s.config.DataDir, "logs", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{s.config.Password, s.config.Token, cookie.Value, "wrong-password-sentinel", "hidden-query-sentinel", "hidden-authorization-sentinel", "hidden-referer-sentinel"} {
			if bytes.Contains(raw, []byte(secret)) {
				t.Fatalf("credential or request secret in %s", name)
			}
		}
		info, err := os.Stat(filepath.Join(s.config.DataDir, "logs", name))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("log permissions: %v %v", info, err)
		}
	}
	info, err := os.Stat(filepath.Join(s.config.DataDir, "logs"))
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("directory permissions: %v %v", info, err)
	}
}

func TestWebAuthLogsExplainRejectedLoginAttempts(t *testing.T) {
	for _, tc := range []struct {
		name, body, media, origin, reason string
		status                            int
	}{
		{"malformed", `{"password":"hidden-body-sentinel"`, "application/json", "", "invalid_request", 400},
		{"trailing", `{"user":"admin","password":"secret"} {}`, "application/json", "", "invalid_request", 400},
		{"media", `{"user":"admin","password":"secret"}`, "text/plain", "", "unsupported_media_type", 415},
		{"origin", `{"user":"admin","password":"secret"}`, "application/json", "http://other.example", "origin_denied", 403},
		{"rate-limit", `{"user":"admin","password":"secret"}`, "application/json", "", "rate_limited", 429},
		{"capacity", `{"user":"admin","password":"secret"}`, "application/json", "", "session_capacity", 503},
		{"not-configured", `{"user":"admin","password":"secret"}`, "application/json", "", "not_configured", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
			if tc.name == "not-configured" {
				s.config.Password = ""
			}
			if tc.name == "rate-limit" {
				s.auth.failures["198.51.100.8"] = loginAttempts{count: 10, expires: time.Now().Add(time.Minute)}
			}
			if tc.name == "capacity" {
				for i := 0; i < sessionLimit; i++ {
					key := [32]byte{byte(i), byte(i >> 8)}
					s.auth.sessions[key] = &browserSession{expires: time.Now().Add(time.Hour), done: make(chan struct{})}
				}
			}
			r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(tc.body))
			r.RemoteAddr = "198.51.100.8:10"
			r.Header.Set("Content-Type", tc.media)
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			s.Handler(context.Background()).ServeHTTP(w, r)
			records := readWebLog(t, s.config.DataDir, "auth.jsonl")
			if w.Code != tc.status || len(records) != 1 || records[0].Reason != tc.reason || records[0].Event != "login" || records[0].Outcome != "failure" {
				t.Fatalf("rejection: status=%d logs=%+v", w.Code, records)
			}
		})
	}
}

func TestWebLogsRecordCSRFAndDeniedLogout(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	cookie := loginCookie(t, s)
	for _, authenticated := range []bool{true, false} {
		r := httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
		if authenticated {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(w, r)
	}
	records := readWebLog(t, s.config.DataDir, "auth.jsonl")
	if len(records) != 3 || records[1].Event != "logout" || records[1].Reason != "origin_denied" || records[1].Status != 403 || records[2].Reason != "login_required" || records[2].Status != 401 {
		t.Fatalf("denied logout: %+v", records)
	}
}

func TestWebLogsOmitRoutineInternalTrafficButKeepErrorsAndProbes(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	h := s.Handler(context.Background())
	for _, path := range []string{"/api/v1/health", "/metrics", "/api/v1/prometheus/agent-targets", "/api/v1/prometheus/controller-targets", "/api/v1/bootstrap?runId=run", "/api/v1/discovery?runId=run&topic=t&requesterNodeId=node"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+s.config.Token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("routine %s: %d", path, w.Code)
		}
	}
	if records := readWebLog(t, s.config.DataDir, "access.jsonl"); len(records) != 0 {
		t.Fatalf("routine traffic logged: %+v", records)
	}
	for _, token := range []string{s.config.Token, "wrong-token"} {
		r := httptest.NewRequest("GET", "/api/v1/bootstrap", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
	}
	records := readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(records) != 2 || records[0].Status != 400 || records[0].Authentication != "internal" || records[1].Status != 401 {
		t.Fatalf("internal errors/probes lost: %+v", records)
	}
}

func TestWebLogRotationSurvivesRestartAndBoundsRetention(t *testing.T) {
	dataDir := t.TempDir()
	writer := newWebLogs(dataDir).access
	record := webLogRecord{Timestamp: time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC), Event: "http_access", RequestID: "0", Path: "/", Status: 200}
	raw, _ := json.Marshal(record)
	writer.maxBytes = int64(len(raw) + 1)
	for i := 0; i < 9; i++ {
		record.RequestID = fmt.Sprint(i)
		if err := writer.append(record); err != nil {
			t.Fatal(err)
		}
	}
	restarted := newWebLogs(dataDir).access
	restarted.maxBytes = writer.maxBytes
	if err := restarted.prepare(); err != nil {
		t.Fatal(err)
	}
	if current := readWebLog(t, dataDir, "access.jsonl"); len(current) != 1 || current[0].RequestID != "8" {
		t.Fatalf("startup truncated log: %+v", current)
	}
	record.RequestID = "9"
	if err := restarted.append(record); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dataDir, "logs"))
	if err != nil || len(entries) != webLogBackups+1 {
		t.Fatalf("retention: %v %v", entries, err)
	}
	for i := 0; i <= webLogBackups; i++ {
		name := "access.jsonl"
		if i > 0 {
			name = fmt.Sprintf("access.jsonl.%d", i)
		}
		records := readWebLog(t, dataDir, name)
		if len(records) != 1 || records[0].RequestID != fmt.Sprint(9-i) {
			t.Fatalf("rotation order %s: %+v", name, records)
		}
		info, err := os.Stat(filepath.Join(dataDir, "logs", name))
		if err != nil || info.Size() > writer.maxBytes || info.Mode().Perm() != 0600 {
			t.Fatalf("rotation size/permissions: %v %v", info, err)
		}
	}
}

func TestWebLogConcurrentRequestsKeepCompleteIndependentJSONRecords(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	handler := s.withWebLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201); _, _ = io.WriteString(w, "yes") }))
	var group sync.WaitGroup
	for i := 0; i < 80; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", fmt.Sprintf("/page/%d", i), nil))
		}(i)
	}
	group.Wait()
	records := readWebLog(t, s.config.DataDir, "access.jsonl")
	seen := map[string]bool{}
	if len(records) != 80 {
		t.Fatalf("lost concurrent requests: %d", len(records))
	}
	for _, record := range records {
		if seen[record.RequestID] || record.Status != 201 || record.Bytes != 3 {
			t.Fatalf("mixed requests: %+v", record)
		}
		seen[record.RequestID] = true
	}
}

func TestWebLogsKeepSSEStreamingAndRecordOpenBeforeDisconnect(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	cookie := loginCookie(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler := s.Handler(ctx)
	done := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		close(done)
	}))
	defer api.Close()
	request, err := http.NewRequest("GET", api.URL+"/api/v1/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.AddCookie(cookie)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	first, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || response.StatusCode != 200 || first != "event: snapshot\n" {
		t.Fatalf("stream not flushed: %q %v", first, err)
	}
	records := readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(records) != 2 || records[1].Event != "sse_open" || records[1].User != "admin" {
		t.Fatalf("missing immediate stream open: %+v", records)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not close")
	}
	records = readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(records) != 3 || records[2].Event != "sse_close" || records[2].RequestID != records[1].RequestID || records[2].Bytes == 0 {
		t.Fatalf("missing stream close: %+v", records)
	}
}

type webDeadlineWriter struct {
	http.ResponseWriter
	deadline time.Time
}

func (w *webDeadlineWriter) SetReadDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestWebLogWriterPreservesDeadlineControlAndFlusherAvailability(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	deadline := time.Now().Add(time.Minute)
	writer := &webDeadlineWriter{ResponseWriter: httptest.NewRecorder()}
	handler := s.withWebLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := w.(http.Flusher); ok {
			t.Error("advertised unsupported Flusher")
		}
		if err := http.NewResponseController(w).SetReadDeadline(deadline); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	handler.ServeHTTP(writer, httptest.NewRequest("GET", "/", nil))
	if !writer.deadline.Equal(deadline) {
		t.Fatal("ResponseController cannot unwrap logger")
	}
	records := readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(records) != 1 || records[0].Status != 204 || records[0].Bytes != 0 {
		t.Fatalf("no-body response: %+v", records)
	}
}

func TestWebLogFailureIsReportedWithoutBreakingRequestsAndCanRecover(t *testing.T) {
	var output bytes.Buffer
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, slog.New(slog.NewJSONHandler(&output, nil)))
	if err := s.webLogs.access.prepare(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, "logs", "access.jsonl")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler(context.Background()).ServeHTTP(w, httptest.NewRequest("GET", "/login", nil))
	if w.Code != 200 || !strings.Contains(output.String(), "write web audit log") {
		t.Fatalf("unreported log failure or changed response: %d %s", w.Code, output.String())
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.Handler(context.Background()).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/login", nil))
	if records := readWebLog(t, s.config.DataDir, "access.jsonl"); len(records) != 1 {
		t.Fatalf("logging did not recover: %+v", records)
	}
}

func TestWebLogStartupRejectsInvalidOrSymlinkedLogPaths(t *testing.T) {
	for _, mode := range []string{"directory-is-file", "directory-symlink", "file-symlink"} {
		t.Run(mode, func(t *testing.T) {
			dataDir := t.TempDir()
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			if err := os.WriteFile(sentinel, []byte("unchanged"), 0644); err != nil {
				t.Fatal(err)
			}
			logs := filepath.Join(dataDir, "logs")
			switch mode {
			case "directory-is-file":
				if err := os.WriteFile(logs, []byte("unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory-symlink":
				if err := os.Symlink(outside, logs); err != nil {
					t.Fatal(err)
				}
			case "file-symlink":
				if err := os.Mkdir(logs, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(sentinel, filepath.Join(logs, "access.jsonl")); err != nil {
					t.Fatal(err)
				}
			}
			server := New(ServerConfig{Listen: "127.0.0.1:0", DataDir: dataDir, User: "admin", Password: "secret"}, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := server.Run(ctx); err == nil || !strings.Contains(err.Error(), "prepare web audit log") {
				t.Fatalf("unusable log path accepted: %v", err)
			}
			raw, err := os.ReadFile(sentinel)
			if err != nil || string(raw) != "unchanged" {
				t.Fatalf("followed log symlink: %s %v", raw, err)
			}
		})
	}
}

func TestWebLogsEncodeUntrustedTextAsBoundedSingleLines(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	body, _ := json.Marshal(map[string]string{"user": "injected\n{\"event\":\"success\"}" + strings.Repeat("x", 200), "password": "wrong"})
	r := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("User-Agent", "test\r\nspoof"+strings.Repeat("a", 900))
	r.RemoteAddr = "[2001:db8::1]:5000"
	s.Handler(context.Background()).ServeHTTP(httptest.NewRecorder(), r)
	auth := readWebLog(t, s.config.DataDir, "auth.jsonl")
	if len(auth) != 1 || len(auth[0].User) != 128 || len(auth[0].UserAgent) != 512 || auth[0].RemoteIP != "2001:db8::1" || auth[0].Outcome != "failure" {
		t.Fatalf("unsafe field encoding: %+v", auth)
	}
	r = httptest.NewRequest("GET", "/"+strings.Repeat("x", 3000)+"?secret=not-recorded", nil)
	s.Handler(context.Background()).ServeHTTP(httptest.NewRecorder(), r)
	access := readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(access) != 2 || len(access[1].Path) != 2048 || strings.Contains(access[1].Path, "not-recorded") {
		t.Fatalf("unbounded/request query path: %+v", access)
	}
}

func TestWebLogsRecordAbortedHandlersWithoutSwallowingPanics(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	handler := s.withWebLogging(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { panic("original panic") }))
	func() {
		defer func() {
			if got := recover(); got != "original panic" {
				t.Fatalf("changed panic: %v", got)
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}()
	records := readWebLog(t, s.config.DataDir, "access.jsonl")
	if len(records) != 1 || records[0].Status != 500 || records[0].Outcome != "aborted" || records[0].Reason != "handler_aborted" {
		t.Fatalf("aborted request not recorded: %+v", records)
	}
}

func TestWebLogFilesAreNotServedAsDashboardAssets(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	cookie := loginCookie(t, s)
	for _, path := range []string{"/logs/auth.jsonl", "/logs/access.jsonl", "/logs/auth.jsonl.1"} {
		r := httptest.NewRequest("GET", path, nil)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(w, r)
		if w.Code != 404 {
			t.Fatalf("audit log served at %s: %d", path, w.Code)
		}
	}
}
