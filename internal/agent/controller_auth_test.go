package agent

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/k-p2p-lab/v3/internal/auth"
	"github.com/k-p2p-lab/v3/internal/controller"
)

var controllerTestToken = auth.InternalToken("agent-integration", "test-password")

func authenticatedTestController(dataDir string) *controller.Server {
	return controller.New(controller.ServerConfig{DataDir: dataDir, User: "agent-integration", Password: "test-password"}, nil)
}

// Real Controller handlers stay protected in integration tests: Agents use the
// derived service credential and operator reads use a freshly logged-in session.
func controllerReadRequest(t *testing.T, handler http.Handler, path string) *http.Request {
	t.Helper()
	login := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewBufferString(`{"user":"agent-integration","password":"test-password"}`))
	login.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, login.WithContext(context.Background()))
	if response.Code != http.StatusOK || len(response.Result().Cookies()) != 1 {
		t.Fatalf("integration controller login: %d", response.Code)
	}
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.AddCookie(response.Result().Cookies()[0])
	request.RequestURI = ""
	return request
}
