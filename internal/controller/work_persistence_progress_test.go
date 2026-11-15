package controller

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCleanupOwnershipCommitUsesPersistenceQueueTurnUnderMetadataWrites(t *testing.T) {
	for _, network := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered-cleanup", true: "network-prepare-owner"}[network], func(t *testing.T) {
			previous := runtime.GOMAXPROCS(1)
			defer runtime.GOMAXPROCS(previous)
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			const active = "pair-active-metadata"
			const target = "pair-cleanup-target"
			telemetryCancellationRun(t, s, active)
			restored := telemetryCancellationRun(t, s, target)
			restored.State = "interrupted"
			commit := func(ctx context.Context) error {
				if network {
					return s.rememberRunNetworkAgents(ctx, target, []model.Agent{{ID: "cleanup-only-agent", URL: "http://agent.invalid", RunProcesses: true, RunDrain: true}})
				}
				return s.mergeRecoveredWorkCleanup(ctx, map[string]bool{target: true}, map[string]model.Experiment{target: restored}, nil)
			}
			var writes atomic.Int32
			stop := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(stop) }) }
			s.state.persistMu.Lock()
			locked := true
			var workers sync.WaitGroup
			defer func() {
				finish()
				if locked {
					s.state.persistMu.Unlock()
				}
				workers.Wait()
			}()
			const parallel = 32
			started := make(chan struct{}, parallel)
			for range parallel {
				workers.Add(1)
				go func() {
					defer workers.Done()
					started <- struct{}{}
					for {
						select {
						case <-stop:
							return
						default:
							s.updateExperiment(active, func(run *model.Experiment) { run.Phase++; writes.Add(1) })
						}
					}
				}()
			}
			for range parallel {
				<-started
			}
			runtime.Gosched()
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- commit(ctx) }()
			runtime.Gosched()
			began := time.Now()
			s.state.persistMu.Unlock()
			locked = false
			err := <-done
			elapsed := time.Since(began)
			during := writes.Load()
			finish()
			workers.Wait()
			if err2 := commit(t.Context()); err2 != nil {
				t.Fatalf("fresh ownership commit did not recover: %v", err2)
			}
			ids, err2 := s.pendingWorkCleanupIDs()
			if err2 != nil || len(ids) != 1 || ids[0] != target {
				t.Fatalf("cleanup marker lost after writer stop: ids=%v error=%v", ids, err2)
			}
			if network {
				owners, err2 := s.readRunCleanupOwnership(target)
				if err2 != nil || len(owners) != 1 || owners[0].ID != "cleanup-only-agent" {
					t.Fatalf("cleanup owners lost: %v %v", owners, err2)
				}
			}
			t.Logf("actual ownership commit network=%t result=%v elapsed=%s concurrent metadata writes=%d", network, err, elapsed, during)
			if err != nil {
				t.Fatalf("ownership commit repeatedly relinquished its FIFO persistence turn while %d actual metadata writes made progress: %v", during, err)
			}
		})
	}
}
