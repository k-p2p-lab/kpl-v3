package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestObservationPersistenceLockHonorsCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			const id = "observation-storage-deadline"
			run := model.Experiment{ID: id, State: "running", DataState: "collecting", StartedAt: time.Now().Add(-time.Minute)}
			s.state.experiments[id] = run
			if err := s.persistManifest(run, []byte(appendScenario)); err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "experiment.json")
			manifest, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			revision := s.state.resultsRevision.Load()
			notified, unsubscribe := s.state.subscribe()
			defer unsubscribe()
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			s.state.persistMu.Lock()
			locked := true
			defer func() {
				if locked {
					s.state.persistMu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- s.state.recordAnalysisObservation(id, time.Now().UTC(), ctx) }()
			select {
			case err := <-done:
				t.Fatalf("observation bypassed its held persistence mutex: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if mode == "cancel" {
				cancel()
			}
			<-ctx.Done()
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("observation discarded its context error: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				s.state.persistMu.Unlock()
				locked = false
				lateErr := <-done
				_, appendErr := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "observations.jsonl"))
				t.Fatalf("observation ignored cancellation behind persistence (late error=%v, append=%v)", lateErr, appendErr)
			}
			s.state.persistMu.Unlock()
			locked = false
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id, "observations.jsonl")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("canceled observation appended after its persistence wait: %v", err)
			}
			s.state.mu.RLock()
			result := s.state.experiments[id]
			s.state.mu.RUnlock()
			if result.IntegrityError != "" || result.DataState != "collecting" {
				t.Fatalf("canceled pre-write observation invented storage damage: %+v", result)
			}
			if got := s.state.resultsRevision.Load(); got != revision {
				t.Fatalf("canceled pre-write observation changed result revision: %d to %d", revision, got)
			}
			select {
			case <-notified:
				t.Fatal("canceled pre-write observation notified state watchers")
			default:
			}
			current, err := os.ReadFile(manifestPath)
			if err != nil || !bytes.Equal(current, manifest) {
				t.Fatalf("canceled pre-write observation changed durable metadata: %v", err)
			}
		})
	}
}
