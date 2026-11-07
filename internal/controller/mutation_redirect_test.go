package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAgentMutationsNeverAcknowledgeRedirectDestinations(t *testing.T) {
	for _, status := range []int{302, 303, 307, 308} {
		for _, action := range []struct{ method, path string }{
			{http.MethodDelete, "/api/v1/runs/run/nodes?generation=1"},
			{http.MethodPost, "/api/v1/runs/run/drain?timeout=1s"},
			{http.MethodPost, "/api/v1/network/activate"},
			{http.MethodPost, "/api/v1/nodes/peer/mesh-freeze"},
		} {
			var redirected atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/login" {
					redirected.Add(1)
					_, _ = w.Write([]byte("successful login page"))
					return
				}
				w.Header().Set("Location", "/login")
				w.WriteHeader(status)
			}))
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			err := s.callAgent(ctx, api.URL, action.method, action.path, nil, nil)
			cancel()
			api.Close()
			if err == nil || redirected.Load() != 0 {
				t.Errorf("mutation %s %s accepted redirect %d: err=%v followed=%d", action.method, action.path, status, err, redirected.Load())
			}
			if s.client.CheckRedirect != nil {
				t.Fatal("shared client was modified by a request")
			}
		}
	}
	// A direct acknowledgement still releases normal cleanup/drain callers.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer api.Close()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if err := s.callAgent(context.Background(), api.URL, http.MethodPost, "/api/v1/runs/run/drain", nil, nil); err != nil {
		t.Fatal(err)
	}
}
