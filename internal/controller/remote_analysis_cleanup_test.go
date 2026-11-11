package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestAnalysisWorkerCleanupSurvivesRestartAndCanceledWait(t *testing.T) {
	const unused = "http://unused-analysis-worker:8093"
	config := ServerConfig{DataDir: t.TempDir(), AnalysisWorkerURLs: []string{unused}, AnalysisWorkerToken: strings.Repeat("x", 32)}
	first := New(config, nil)
	worker := newRemoteAnalysisWorker(config.AnalysisWorkerToken, first.logger, nil)
	worker.slot <- struct{}{}
	probed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		worker.ServeHTTP(w, r)
		select {
		case probed <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	slot := &workSlot{id: "next-experiment"}
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	if err := first.waitAnalysisWorkerCleanup(ctx, slot); err != nil {
		t.Fatalf("unused worker affected the queue: %v", err)
	}
	if err := first.trackAnalysisWorker(ctx, server.URL); err != nil {
		t.Fatal(err)
	}
	restarted := New(config, nil)
	if err := restarted.trackAnalysisWorker(ctx, server.URL); !errors.Is(err, errRemoteAnalysisCleanupUnconfirmed) {
		t.Fatalf("worker reused while cleanup was pending: %v", err)
	}
	wait, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	done := make(chan error, 1)
	go func() { done <- restarted.waitAnalysisWorkerCleanup(wait, slot) }()
	select {
	case <-probed:
	case <-ctx.Done():
		t.Fatal("restart did not verify the recorded worker")
	}
	select {
	case err := <-done:
		t.Fatalf("busy worker did not block the next experiment: %v", err)
	default:
	}
	cancelWait()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cleanup wait was not canceled: %v", err)
	}
	if pending, err := restarted.pendingAnalysisWorkerCleanup(); err != nil || !slices.Equal(pending, []string{server.URL}) {
		t.Fatalf("canceled wait lost its durable record: %v, %v", pending, err)
	}
	<-worker.slot
	if err := restarted.waitAnalysisWorkerCleanup(ctx, slot); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(config.DataDir, analysisWorkerCleanupFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified idle worker left a cleanup record: %v", err)
	}
}

func TestAnalysisWorkerCleanupRetainsConcurrentEndpoints(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	endpoints := []string{"http://analysis-a:8093", "http://analysis-b:8093"}
	done := make(chan error, len(endpoints))
	for _, endpoint := range endpoints {
		go func() { done <- s.trackAnalysisWorker(t.Context(), endpoint) }()
	}
	for range endpoints {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || !slices.Equal(pending, endpoints) {
		t.Fatalf("parallel admissions lost a cleanup record: %v, %v", pending, err)
	}
	if err := s.clearAnalysisWorker(endpoints[0]); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.pendingAnalysisWorkerCleanup(); err != nil || !slices.Equal(pending, endpoints[1:]) {
		t.Fatalf("clearing one worker changed another record: %v, %v", pending, err)
	}
}

func TestAnalysisWorkerCleanupRejectsInvalidLedger(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	path := filepath.Join(s.config.DataDir, analysisWorkerCleanupFile)
	if err := os.WriteFile(path, []byte(`["http://valid:8093","https://unsafe:secret@example.com"]`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.waitAnalysisWorkerCleanup(t.Context(), &workSlot{id: "experiment"}); err == nil || !strings.Contains(err.Error(), "invalid worker endpoint") {
		t.Fatalf("experiment passed a corrupt cleanup ledger: %v", err)
	}
}
