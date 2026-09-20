package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func loginCookie(t *testing.T, s *Server) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"user": s.config.User, "password": s.config.Password})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	s.Handler(context.Background()).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("login failed: %d %s", response.Code, response.Body)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected a session cookie, got %d", len(cookies))
	}
	return cookies[0]
}

func authenticateRequest(t *testing.T, s *Server, r *http.Request) {
	t.Helper()
	if s.config.User == "" {
		return
	}
	r.AddCookie(loginCookie(t, s))
	r.Header.Set("X-KPL-Request", "dashboard")
}

func TestLoginProtectsDashboardReadMutationDownloadAndStream(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "private-password"}, nil)
	h := s.Handler(context.Background())
	request := func(method, path string, cookie *http.Cookie, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if cookie != nil {
			req.AddCookie(cookie)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		result := httptest.NewRecorder()
		h.ServeHTTP(result, req)
		return result
	}
	for _, path := range []string{"/", "/index.html"} {
		r := request("GET", path, nil, nil)
		if r.Code != http.StatusSeeOther || r.Header().Get("Location") != "/login" {
			t.Fatalf("%s was not gated: %d", path, r.Code)
		}
	}
	for _, path := range []string{"/login", "/login.js", "/styles.css", "/kpl-logo.jpg", "/api/v1/health", "/metrics", "/api/v1/prometheus/agent-targets"} {
		if r := request("GET", path, nil, nil); r.Code != http.StatusOK {
			t.Fatalf("public path %s: %d", path, r.Code)
		}
	}
	for _, method := range []string{"GET", "HEAD", "POST", "DELETE"} {
		for _, path := range []string{"/api/v1/snapshot", "/api/v1/stream", "/api/v1/results", "/api/v1/scenarios/validate", "/api/v1/experiments/missing/download", "/api/v1/analysis-jobs/missing/result"} {
			if r := request(method, path, nil, nil); r.Code != http.StatusUnauthorized {
				t.Fatalf("unprotected %s %s: %d", method, path, r.Code)
			}
		}
	}
	cookie := loginCookie(t, s)
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.MaxAge != 43200 || cookie.Value == s.config.Password {
		t.Fatalf("unsafe session cookie attributes")
	}
	for _, path := range []string{"/", "/api/v1/snapshot", "/api/v1/results", "/api/v1/auth/session"} {
		r := request("GET", path, cookie, nil)
		if r.Code != http.StatusOK || r.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("session %s: %d", path, r.Code)
		}
		if strings.Contains(r.Body.String(), s.config.Password) || strings.Contains(r.Body.String(), s.config.Token) {
			t.Fatal("credential exposed in response")
		}
	}
	for _, headers := range []map[string]string{
		nil, {"X-KPL-Request": "dashboard", "Origin": "http://attacker.example"},
		{"X-KPL-Request": "dashboard", "Sec-Fetch-Site": "cross-site"},
	} {
		if r := request("POST", "/api/v1/auth/logout", cookie, headers); r.Code != http.StatusForbidden {
			t.Fatalf("CSRF accepted: %d", r.Code)
		}
	}
	if r := request("POST", "/api/v1/auth/logout", cookie, map[string]string{"X-KPL-Request": "dashboard"}); r.Code != http.StatusNoContent {
		t.Fatalf("logout: %d", r.Code)
	}
	if r := request("GET", "/api/v1/snapshot", cookie, nil); r.Code != http.StatusUnauthorized {
		t.Fatal("logout session still valid")
	}
}

func TestInternalCredentialCannotReplaceBrowserLogin(t *testing.T) {
	s := New(ServerConfig{User: "admin", Password: "secret", DataDir: t.TempDir()}, nil)
	h := s.Handler(context.Background())
	for _, path := range []string{"/api/v1/bootstrap", "/api/v1/discovery", "/api/v1/results", "/api/v1/snapshot", "/api/v1/auth/session"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+s.config.Token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		internal := path == "/api/v1/bootstrap" || path == "/api/v1/discovery"
		if internal && w.Code == http.StatusUnauthorized {
			t.Fatalf("internal lookup blocked: %s", path)
		}
		if !internal && w.Code != http.StatusUnauthorized {
			t.Fatalf("internal key opened browser API: %s", path)
		}
	}
	for _, path := range []string{"/api/v1/agents/register", "/api/v1/agents/heartbeat", "/api/v1/events", "/api/v1/events/batch", "/api/v1/experiments"} {
		r := httptest.NewRequest("POST", path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer "+s.config.Token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if path == "/api/v1/experiments" {
			if w.Code != 401 {
				t.Fatal("internal key started user experiment")
			}
		} else if w.Code == 401 {
			t.Fatalf("internal ingestion blocked: %s", path)
		}
	}
}

func TestLoginRejectsBadCredentialsCrossOriginAndLimitsAttempts(t *testing.T) {
	s := New(ServerConfig{User: "admin", Password: "secret", DataDir: t.TempDir()}, nil)
	h := s.Handler(context.Background())
	for _, tc := range []struct {
		content, origin, body string
		status                int
	}{
		{"application/json", "http://attacker.example", `{"user":"admin","password":"secret"}`, 403},
		{"text/plain", "", `{"user":"admin","password":"secret"}`, 415},
		{"application/json", "", `{"user":"admin","password":"secret","extra":1}`, 400},
		{"application/json", "", `{"user":"admin","password":"secret"} {}`, 400},
		{"application/json", "", "{}", 401},
	} {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", tc.content)
		r.Header.Set("Origin", tc.origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("login status %d want %d", w.Code, tc.status)
		}
	}
	for i := 1; i <= 10; i++ {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"user":"admin","password":"wrong"}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := 401
		if i == 10 {
			want = 429
		}
		if w.Code != want {
			t.Fatalf("attempt %d: %d want %d", i, w.Code, want)
		}
	}
}

func TestSessionsExpireRotateAndDoNotSurviveRestart(t *testing.T) {
	config := ServerConfig{User: "admin", Password: "secret", DataDir: t.TempDir()}
	s := New(config, nil)
	cookie := loginCookie(t, s)
	req := httptest.NewRequest("GET", "/api/v1/auth/session", nil)
	req.AddCookie(cookie)
	session := s.auth.session(req)
	session.expires = time.Now().Add(-time.Second)
	if s.auth.session(req) != nil {
		t.Fatal("expired session accepted")
	}
	select {
	case <-session.done:
	default:
		t.Fatal("expired session streams not closed")
	}
	cookie = loginCookie(t, s)
	req = httptest.NewRequest("GET", "/api/v1/auth/session", nil)
	req.AddCookie(cookie)
	if New(config, nil).auth.session(req) != nil {
		t.Fatal("session survived restart")
	}
	old := s.auth.session(req)
	body, _ := json.Marshal(map[string]string{"user": config.User, "password": config.Password})
	login := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	login.AddCookie(cookie)
	login.Header.Set("Content-Type", "application/json")
	login.Header.Set("X-Forwarded-Proto", "https")
	response := httptest.NewRecorder()
	s.Handler(context.Background()).ServeHTTP(response, login)
	if response.Code != 200 || !response.Result().Cookies()[0].Secure {
		t.Fatal("HTTPS login or secure cookie failed")
	}
	if s.auth.sessions[sha256.Sum256([]byte(cookie.Value))] != nil {
		t.Fatal("old session retained after rotation")
	}
	select {
	case <-old.done:
	default:
		t.Fatal("rotating session did not revoke streams")
	}
}

func TestLogoutClosesAnExistingEventStream(t *testing.T) {
	s := New(ServerConfig{User: "admin", Password: "secret", DataDir: t.TempDir()}, nil)
	cookie := loginCookie(t, s)
	server := httptest.NewServer(s.Handler(context.Background()))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/v1/stream", nil)
	req.AddCookie(cookie)
	stream, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	if stream.StatusCode != 200 {
		t.Fatalf("stream: %d", stream.StatusCode)
	}
	logout, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/api/v1/auth/logout", nil)
	logout.AddCookie(cookie)
	logout.Header.Set("X-KPL-Request", "dashboard")
	response, err := server.Client().Do(logout)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatalf("logout: %d", response.StatusCode)
	}
	if _, err := io.Copy(io.Discard, stream.Body); err != nil {
		t.Fatalf("stream did not close cleanly: %v", err)
	}
}

func TestControllerCannotListenWithoutLoginCredentials(t *testing.T) {
	for _, config := range []ServerConfig{{}, {User: "admin"}, {Password: "secret"}} {
		config.Listen = "127.0.0.1:0"
		config.DataDir = t.TempDir()
		if err := New(config, nil).Run(context.Background()); err == nil || !strings.Contains(err.Error(), "KPL_USER and KPL_PASSWORD") {
			t.Fatalf("missing credentials: %v", err)
		}
	}
}

// Functional route tests isolate their subject from authentication. Authentication
// tests use Handler directly, and explicitly configured route tests do so as well.
func (s *Server) apiTestHandler(ctx context.Context) http.Handler {
	if s.config.User != "" || s.config.Password != "" {
		return s.Handler(ctx)
	}
	return s.withMiddleware(s.routes(ctx))
}

func TestUnconfiguredHandlerStillRequiresLogin(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, path := range []string{"/api/v1/snapshot", "/api/v1/results", "/api/v1/scenarios/validate"} {
		request := httptest.NewRequest("GET", path, nil)
		response := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("unconfigured handler opened %s: %d", path, response.Code)
		}
	}
}
