package controller

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestAnalysisCleanupLedgerLockHonorsCancellation(t *testing.T) {
	for _, boundary := range []string{"snapshot", "after idle proof"} {
		for _, mode := range []string{"cancel", "deadline"} {
			t.Run(boundary+"/"+mode, func(t *testing.T) {
				s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true, AnalysisAgentToken: remoteTestToken}, nil)
				worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, nil)
				entered := make(chan struct{})
				var once sync.Once
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if boundary == "after idle proof" {
						once.Do(func() { s.analysisWorkerCleanupMu.Lock(); close(entered) })
					}
					worker.ServeHTTP(w, r)
				}))
				defer api.Close()
				owner := analysisPoolAgent("analysis-cleanup-owner", "analysis-cleanup-host", api.URL)
				registerAnalysisPoolAgent(t, s, owner)
				if err := s.trackAnalysisWorker(t.Context(), api.URL, owner); err != nil {
					t.Fatal(err)
				}
				ledgerPath := filepath.Join(s.config.DataDir, analysisWorkerCleanupFile)
				ledger, err := os.ReadFile(ledgerPath)
				if err != nil {
					t.Fatal(err)
				}
				if boundary == "snapshot" {
					s.analysisWorkerCleanupMu.Lock()
				}
				ctx, cancel := context.WithCancel(t.Context())
				want := context.Canceled
				if mode == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 500*time.Millisecond)
					want = context.DeadlineExceeded
				}
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- s.waitAnalysisWorkerCleanup(ctx, &workSlot{id: "next-experiment"}) }()
				if boundary == "after idle proof" {
					select {
					case <-entered:
					case err := <-done:
						t.Fatalf("cleanup exited before the real idle proof: %v", err)
					case <-time.After(3 * time.Second):
						t.Fatal("cleanup did not request its owner's idle proof")
					}
				}
				locked := true
				defer func() {
					if locked {
						s.analysisWorkerCleanupMu.Unlock()
					}
				}()
				if boundary == "after idle proof" {
					retirementReviewWait(t, "idle proof did not reach the ledger completion lock", func() bool {
						stacks := make([]byte, 2<<20)
						n := runtime.Stack(stacks, true)
						return bytes.Contains(stacks[:n], []byte("(*Server).clearAnalysisWorker"))
					})
				} else {
					select {
					case err := <-done:
						t.Fatalf("cleanup bypassed its ledger snapshot lock: %v", err)
					case <-time.After(20 * time.Millisecond):
					}
				}
				if mode == "cancel" {
					cancel()
				}
				<-ctx.Done()
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("ledger wait lost its cancellation: %v", err)
					}
				case <-time.After(300 * time.Millisecond):
					s.analysisWorkerCleanupMu.Unlock()
					locked = false
					lateErr := <-done
					t.Fatalf("analysis cleanup ignored cancellation behind its %s ledger lock: %v", boundary, lateErr)
				}
				current, err := os.ReadFile(ledgerPath)
				if err != nil || !bytes.Equal(current, ledger) {
					t.Fatalf("canceled ledger wait changed durable ownership: %v", err)
				}
				s.analysisWorkerCleanupMu.Unlock()
				locked = false
				if err := s.waitAnalysisWorkerCleanup(t.Context(), &workSlot{id: "next-experiment"}); err != nil {
					t.Fatalf("retry did not verify and clear its old analysis worker: %v", err)
				}
				if _, err := os.Stat(ledgerPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("successful idle-proof retry retained its ledger: %v", err)
				}
			})
		}
	}
}
