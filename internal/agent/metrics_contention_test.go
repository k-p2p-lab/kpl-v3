package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsScrapesBoundStalledCollectorsAndKeepAgentResponsive(t *testing.T) {
	s := historyTestServer(t)
	s.startupReconciled = true
	handler := s.Handler()
	var active atomic.Int64
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			active.Add(1)
			defer active.Add(-1)
		}
		handler.ServeHTTP(w, r)
	}))
	s.eventsMu.Lock() // A durable telemetry write can hold this during slow I/O.
	locked := true
	defer func() {
		if locked {
			s.eventsMu.Unlock()
		}
		endpoint.Close()
	}()
	client := endpoint.Client()
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{}, 2)
	for range 2 {
		go func() {
			defer func() { done <- struct{}{} }()
			r, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.URL+"/metrics", nil)
			if response, err := client.Do(r); err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				response.Body.Close()
			}
		}()
	}
	request := func(method, path string, timeout time.Duration) (int, error) {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()
		r, _ := http.NewRequestWithContext(ctx, method, endpoint.URL+path, nil)
		response, err := client.Do(r)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		_, err = io.Copy(io.Discard, response.Body)
		return response.StatusCode, err
	}
	// Request scheduling need not admit the first two clients in order. Once
	// both collection slots are occupied, another scrape must fail promptly.
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, err := request(http.MethodGet, "/metrics", 100*time.Millisecond)
		if err == nil && status == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stalled collections were unbounded: active=%d status=%d error=%v", active.Load(), status, err)
		}
	}
	if got := active.Load(); got != 2 {
		t.Fatalf("retained %d collections, want two", got)
	}
	cancel()
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("scrape client did not cancel")
		}
	}
	// Canceling a client cannot cancel Collector.Collect, so its limiter slot
	// must remain held until the actual collector finishes.
	if status, err := request(http.MethodGet, "/metrics", time.Second); err != nil || status != http.StatusServiceUnavailable {
		t.Fatalf("client cancellation released a blocked collection slot: %d %v", status, err)
	}
	for _, path := range []string{"/api/v1/health", "/api/v1/status", "/api/v1/nodes", "/api/v1/registration/refresh"} {
		method := http.MethodGet
		if path == "/api/v1/registration/refresh" {
			method = http.MethodPost
		}
		if status, err := request(method, path, time.Second); err != nil || status != http.StatusOK {
			t.Fatalf("stalled metrics blocked %s: %d %v", path, status, err)
		}
	}
	s.eventsMu.Unlock()
	locked = false
	deadline = time.Now().Add(3 * time.Second)
	for {
		status, err := request(http.MethodGet, "/metrics", time.Second)
		if err == nil && status == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics did not recover after telemetry I/O: %d %v", status, err)
		}
		time.Sleep(time.Millisecond)
	}
}
