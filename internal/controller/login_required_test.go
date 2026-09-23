package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLoginRequiredPagePreservesAPIAuthentication(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "admin", Password: "secret"}, nil)
	h := s.Handler(context.Background())
	for _, tc := range []struct {
		name, method, path, accept, mode string
		html                             bool
	}{
		{"error page", "GET", "/login-required", "", "", true},
		{"error page file", "GET", "/login-required.html", "", "", true},
		{"download navigation", "GET", "/api/v1/experiments/private-run/download", "text/html,application/xhtml+xml,*/*;q=0.8", "navigate", true},
		{"browser without fetch metadata", "GET", "/api/v1/results", "text/html", "", true},
		{"HTML HEAD", "HEAD", "/api/v1/results", "text/html", "", true},
		{"API default", "GET", "/api/v1/results", "", "", false},
		{"API JSON", "GET", "/api/v1/results", "application/json", "", false},
		{"API prefers JSON", "GET", "/api/v1/results", "text/html;q=0.5,application/json", "", false},
		{"declined HTML", "GET", "/api/v1/results", "text/html;q=0.0,*/*", "", false},
		{"stream", "GET", "/api/v1/stream", "text/event-stream", "", false},
		{"mutation", "POST", "/api/v1/experiments", "text/html", "navigate", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			r.Header.Set("Accept", tc.accept)
			r.Header.Set("Sec-Fetch-Mode", tc.mode)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status/cache=%d/%q", w.Code, w.Header().Get("Cache-Control"))
			}
			if tc.html {
				if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") {
					t.Fatalf("expected HTML: %s", w.Body)
				}
				if tc.method == http.MethodHead {
					if w.Body.Len() != 0 {
						t.Fatal("HEAD returned a body")
					}
				} else if !strings.Contains(w.Body.String(), "Login required") || !strings.Contains(w.Body.String(), `href="/login"`) {
					t.Fatal("missing login explanation or action")
				}
				if strings.Contains(w.Body.String(), "private-run") {
					t.Fatal("page reflected a protected resource ID")
				}
			} else {
				var result map[string]string
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result["error"] != "login required" {
					t.Fatalf("JSON authentication contract changed: %s", w.Body)
				}
			}
		})
	}
	// A stale error-page tab must not expose data even if a session now exists.
	r := httptest.NewRequest(http.MethodGet, "/login-required", nil)
	r.AddCookie(loginCookie(t, s))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), "Login required") {
		t.Fatalf("error page was not routable with a session: %d", w.Code)
	}
}
