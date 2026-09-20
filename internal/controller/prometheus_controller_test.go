package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusControllerTargetsUseAdvertisedEndpoint(t *testing.T) {
	for _, tc := range []struct{ url, target, scheme string }{
		{"http://10.20.0.7:18080/metrics", "10.20.0.7:18080", "http"},
		{"https://[2001:db8::7]/metrics", "[2001:db8::7]:443", "https"},
		{"https://control.example:8443/metrics", "control.example:8443", "https"},
		{"", "", ""},
		{"http://localhost:8080/metrics", "", ""},
		{"http://user:secret@control.example/metrics", "", ""},
	} {
		t.Run(tc.url, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir(), Token: "test-token", MetricsURL: tc.url}, nil)
			handler := server.apiTestHandler(context.Background())
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/prometheus/controller-targets", nil))
			if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d headers=%v body=%s", recorder.Code, recorder.Header(), recorder.Body.String())
			}
			var groups []prometheusTargetGroup
			if err := json.Unmarshal(recorder.Body.Bytes(), &groups); err != nil {
				t.Fatal(err)
			}
			if tc.target == "" {
				if groups == nil || len(groups) != 0 {
					t.Fatalf("unconfigured or invalid endpoint must return [], got %s", recorder.Body.String())
				}
			} else if len(groups) != 1 || len(groups[0].Targets) != 1 || groups[0].Targets[0] != tc.target || groups[0].Labels["__scheme__"] != tc.scheme {
				t.Fatalf("unexpected targets: %+v", groups)
			}
			request := httptest.NewRequest(http.MethodPost, "/api/v1/prometheus/controller-targets", nil)
			request.Header.Set("Authorization", "Bearer test-token")
			recorder = httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusMethodNotAllowed {
				t.Fatalf("POST status=%d, want 405", recorder.Code)
			}
		})
	}
}

func TestControllerRejectsInvalidMetricsURLBeforeListening(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1:8080/metrics", "http://[::]:8080/metrics",
		"http://control.example:65536/metrics", "http://control.example/status",
		"http://control.example/metrics?token=secret", "http://control.example/metrics#fragment",
	} {
		t.Run(raw, func(t *testing.T) {
			server := New(ServerConfig{DataDir: t.TempDir(), MetricsURL: raw, User: "admin", Password: "secret"}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := server.Run(ctx); err == nil || !strings.Contains(err.Error(), "Controller metrics URL must be") {
				t.Fatalf("expected metrics URL validation before listening, got %v", err)
			}
		})
	}
}
