package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVersionRequiresSessionAndReportsDeploymentImage(t *testing.T) {
	image := "registry.example:5000/kpl@sha256:" + strings.Repeat("a", 64)
	s := New(ServerConfig{RuntimeImage: image, DataDir: t.TempDir(), User: "admin", Password: "private-password"}, nil)
	request := func(method string, authenticated bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/v1/version", nil)
		if authenticated {
			authenticateRequest(t, s, r)
		}
		w := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(w, r)
		return w
	}
	if w := request(http.MethodGet, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous version status=%d", w.Code)
	}
	w := request(http.MethodGet, true)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("version status=%d cache=%q", w.Code, w.Header().Get("Cache-Control"))
	}
	var version runtimeVersion
	if err := json.Unmarshal(w.Body.Bytes(), &version); err != nil {
		t.Fatal(err)
	}
	if version.Image != image || version.ImageDigest != "sha256:"+strings.Repeat("a", 64) || version.ImageSource != "deployment" {
		t.Fatalf("image identity=%+v", version)
	}
	if version.Build.GoVersion == "" || version.Build.BinaryHash == "" {
		t.Fatalf("missing actual build evidence: %+v", version.Build)
	}
	if strings.Contains(w.Body.String(), s.config.Password) || strings.Contains(w.Body.String(), s.config.Token) {
		t.Fatal("version exposes credentials")
	}
	if w := request(http.MethodPost, true); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST version status=%d", w.Code)
	}
}

func TestVersionUnknownImageDoesNotInventDigest(t *testing.T) {
	for _, image := range []string{"", "registry.example/kpl:latest", "sha256:" + strings.Repeat("a", 64), "registry.example/kpl@sha256:short"} {
		t.Run(image, func(t *testing.T) {
			s := New(ServerConfig{RuntimeImage: image, DataDir: t.TempDir()}, nil)
			w := httptest.NewRecorder()
			s.handleVersion(w, httptest.NewRequest(http.MethodGet, "/api/v1/version", nil))
			var version runtimeVersion
			if err := json.Unmarshal(w.Body.Bytes(), &version); err != nil {
				t.Fatal(err)
			}
			if version.ImageDigest != "" || image == "" && version.ImageSource != "unknown" {
				t.Fatalf("invented image observation: %+v", version)
			}
		})
	}
}
