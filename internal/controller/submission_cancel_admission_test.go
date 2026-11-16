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
	"runtime"
	"strings"
	"testing"
	"time"
)

func waitSubmissionAdmissionBoundary(t *testing.T) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	stack := make([]byte, 1<<20)
	for {
		for _, routine := range strings.Split(string(stack[:runtime.Stack(stack, true)]), "\n\n") {
			if strings.Contains(routine, "controller.(*Server).submitScenarioFromLibrary(") && (strings.Contains(routine, "contextlock.Lock") || strings.Contains(routine, "contextlock.(*Mutex).") || strings.Contains(routine, "sync.(*Mutex).Lock")) {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatal("HTTP submission never reached its serialized admission boundary")
		case <-ticker.C:
		}
	}
}

func TestCanceledSubmissionReleasesAdmissionAndPreservesPublishedReceipt(t *testing.T) {
	for _, boundary := range []string{"before-receipt", "after-receipt", "before-recovery"} {
		t.Run(boundary, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			endpoint := httptest.NewServer(s.apiTestHandler(ctx))
			defer endpoint.Close()
			var mu interface {
				Lock()
				Unlock()
			} = &s.submissionMu
			if boundary == "after-receipt" {
				mu = &s.cancelMu
			}
			if boundary == "before-recovery" {
				mu = &s.state.persistMu
			}
			mu.Lock()
			locked := true
			defer func() {
				if locked {
					mu.Unlock()
				}
			}()
			body, _ := json.Marshal(map[string]any{"scenario": queueShortScenario, "repetitions": 1})
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint.URL+"/api/v1/experiments", bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", "canceled-before-execution")
			finished := make(chan error, 1)
			go func() {
				response, err := endpoint.Client().Do(request)
				if err == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				}
				finished <- err
			}()
			waitSubmissionAdmissionBoundary(t)
			// Cancel the Controller lifecycle passed to the handler, while retaining
			// the actual HTTP connection and its independent request context.
			cancel()
			timedOut := false
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(150 * time.Millisecond):
				timedOut = true
			}
			mu.Unlock()
			locked = false
			if timedOut {
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(filepath.Join(s.config.DataDir, "run-submissions"))
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			expected := 1
			if boundary == "before-receipt" {
				expected = 0
			}
			s.state.mu.RLock()
			runs := len(s.state.experiments)
			s.state.mu.RUnlock()
			if timedOut || len(entries) != expected || runs != 0 {
				t.Fatalf("canceled submission crossed admission boundary: blocked=%t receipts=%d want=%d runs=%d", timedOut, len(entries), expected, runs)
			}
			// An already published receipt stays bound and the original input can
			// complete under a fresh lifecycle; cancellation never silently rekeys it.
			accepted, err := s.submitScenario(t.Context(), []byte(queueShortScenario), 1, "canceled-before-execution")
			if err != nil {
				t.Fatal(err)
			}
			waitWorkerRuns(t, s)
			if !strings.HasPrefix(accepted.ID, "run-request-") {
				t.Fatalf("recovery changed submission identity: %s", accepted.ID)
			}
		})
	}
}
