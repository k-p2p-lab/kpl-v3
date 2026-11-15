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

func TestRetirementStageCleanupHonorsPersistenceDeadline(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s, id, _ := summaryFreshnessFixture(t, time.Time{})
			s.state.nodes[id+"-peer"] = model.Node{ID: id + "-peer", RunID: id, State: model.NodeStopped, Metadata: map[string]string{"cleanupComplete": "true"}}
			s.queueCleanup = []string{id}
			if err := s.writePendingWorkCleanup(id); err != nil {
				t.Fatal(err)
			}
			stagePath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, ".observation-0123456789abcdef0123456789abcdef.tmp")
			stage := []byte("uncommitted observation retained until cleanup owns persistence\n")
			if err := os.WriteFile(stagePath, stage, 0600); err != nil {
				t.Fatal(err)
			}
			summaryPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, workerSummaryFile)
			summary, err := os.ReadFile(summaryPath)
			if err != nil {
				t.Fatal(err)
			}
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
			go func() { done <- s.retireRunRuntime(ctx, id) }()
			select {
			case err := <-done:
				t.Fatalf("retirement bypassed its held persistence mutex: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if mode == "cancel" {
				cancel()
			}
			<-ctx.Done()
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("retirement discarded its context error: %v", err)
				}
			case <-time.After(300 * time.Millisecond):
				s.state.persistMu.Unlock()
				locked = false
				lateErr := <-done
				t.Fatalf("retirement ignored its deadline behind stage-cleanup persistence: %v", lateErr)
			}
			current, err := os.ReadFile(stagePath)
			if err != nil || !bytes.Equal(current, stage) {
				t.Fatalf("canceled retirement altered its pending stage: %v", err)
			}
			if retired, err := s.isRuntimeRetired(id); err != nil || retired {
				t.Fatalf("canceled stage cleanup published a retirement fence: %t %v", retired, err)
			}
			current, err = os.ReadFile(summaryPath)
			if err != nil || !bytes.Equal(current, summary) {
				t.Fatalf("canceled retirement altered the scientific summary: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); err != nil || len(s.queueCleanup) != 1 {
				t.Fatalf("canceled retirement lost its cleanup obligation: marker=%v queue=%v", err, s.queueCleanup)
			}
			s.state.mu.RLock()
			_, retained := s.state.nodes[id+"-peer"]
			s.state.mu.RUnlock()
			if !retained {
				t.Fatal("canceled retirement discarded the terminal Peer evidence")
			}
			s.state.persistMu.Unlock()
			locked = false
			if err := s.retireRunRuntime(t.Context(), id); err != nil {
				t.Fatalf("retry did not reclaim the pending stage: %v", err)
			}
			if _, err := os.Stat(stagePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("successful retry retained an abandoned stage: %v", err)
			}
			if retired, err := s.isRuntimeRetired(id); err != nil || !retired {
				t.Fatalf("successful retry did not publish its retirement fence: %t %v", retired, err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, pendingWorkCleanupDirectory, id)); !errors.Is(err, os.ErrNotExist) || len(s.queueCleanup) != 0 {
				t.Fatalf("successful retirement retained its cleanup obligation: marker=%v queue=%v", err, s.queueCleanup)
			}
		})
	}
}
