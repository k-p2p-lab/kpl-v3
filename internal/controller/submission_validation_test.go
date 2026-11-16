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
	"testing"
)

func TestRejectedSubmissionDoesNotConsumeIdempotencyKey(t *testing.T) {
	for _, kind := range []string{"invalid-scenario", "invalid-count", "malformed-envelope"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			t.Cleanup(func() { cancel(); s.runs.Wait() })
			endpoint := httptest.NewServer(s.apiTestHandler(ctx))
			defer endpoint.Close()
			post := func(body []byte) int {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint.URL+"/api/v1/experiments", bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Idempotency-Key", "rejected-request")
				response, err := endpoint.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				return response.StatusCode
			}
			input := map[string]any{"scenario": appendScenario, "repetitions": 1}
			if kind == "invalid-scenario" {
				input["scenario"] = "invalid"
			}
			if kind == "invalid-count" {
				input["repetitions"] = 0
			}
			body, _ := json.Marshal(input)
			if kind == "malformed-envelope" {
				body = []byte("{")
			}
			if code := post(body); code != http.StatusBadRequest {
				t.Fatalf("first request status=%d", code)
			}
			entries, _ := os.ReadDir(filepath.Join(s.config.DataDir, "run-submissions"))
			body, _ = json.Marshal(map[string]any{"scenario": appendScenario, "repetitions": 1})
			if code := post(body); code != http.StatusAccepted {
				t.Fatalf("a definitely rejected request consumed the key: correctedStatus=%d receiptsAfterRejection=%d", code, len(entries))
			}
		})
	}
}

func TestSubmissionStorageFailureKeepsOriginalKeyBinding(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	t.Cleanup(func() { cancel(); s.runs.Wait() })
	endpoint := httptest.NewServer(s.apiTestHandler(ctx))
	defer endpoint.Close()
	post := func(raw string) int {
		data, err := json.Marshal(map[string]any{"scenario": raw, "repetitions": 1})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint.URL+"/api/v1/experiments", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "storage-failure")
		response, err := endpoint.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode
	}
	blocked := filepath.Join(s.config.DataDir, currentRunsDirectory)
	if err := os.WriteFile(blocked, []byte("fixture admission block"), 0600); err != nil {
		t.Fatal(err)
	}
	if status := post(appendScenario); status < 400 {
		t.Fatalf("fixture did not interrupt admission after input validation: %d", status)
	}
	entries, err := os.ReadDir(filepath.Join(s.config.DataDir, "run-submissions"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("valid admission failure lost its uncertain key binding: receipts=%d err=%v", len(entries), err)
	}
	if status := post(appendScenario + "# different request\n"); status != http.StatusConflict {
		t.Fatalf("storage failure allowed a different request to reuse its key: %d", status)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if status := post(appendScenario); status != http.StatusAccepted {
		t.Fatalf("original bound request could not recover after storage repair: %d", status)
	}
	if status := post(appendScenario + "# changed after acceptance\n"); status != http.StatusConflict {
		t.Fatalf("accepted request lost its immutable key binding: %d", status)
	}
}
