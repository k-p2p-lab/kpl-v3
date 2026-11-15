package controller

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	bolt "go.etcd.io/bbolt"
)

func TestJournalReconstructionPersistenceWaitHonorsCancellation(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	const id = "journal-storage-deadline"
	journalReviewLegacyRun(t, s, id)
	eventsPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")
	before, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.rebuildJournalIndex(ctx, id) }()
	<-ctx.Done()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("journal reconstruction lost deadline: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		s.state.persistMu.Unlock()
		locked = false
		late := <-done
		t.Fatalf("journal reconstruction retained canceled persistence waiter: %v", late)
	}
	s.state.persistMu.Unlock()
	locked = false
	after, err := os.ReadFile(eventsPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("canceled journal waiter changed scientific source: %v", err)
	}
	if err := s.rebuildJournalIndex(t.Context(), id); err != nil {
		t.Fatalf("later journal reconstruction did not recover: %v", err)
	}
	journalReviewAssertReleased(t, s, id)
	journalReviewAssertNoStaging(t, s, id, true)
}

func TestIdleJournalRecoveryYieldsToActualNewWork(t *testing.T) {
	s, _ := isolatedAnalysisServer(t)
	const id = "idle-journal-storage-cancel"
	journalReviewLegacyRun(t, s, id)
	if err := s.writePendingWorkCleanup(id); err != nil {
		t.Fatal(err)
	}
	s.journalBuildMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.journalBuildMu.Unlock()
		}
	}()
	idleDone := make(chan bool, 1)
	go func() { idleDone <- s.tryRefreshIdleRunMetrics(t.Context(), id) }()
	retirementReviewWait(t, "idle maintenance did not reach actual journal cleanup recovery", func() bool {
		stacks := make([]byte, 2<<20)
		n := runtime.Stack(stacks, true)
		return bytes.Contains(stacks[:n], []byte("(*Server).rebuildJournalIndex"))
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s.cancelMu.Lock()
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", Name: "next experiment"}, cancel)
	s.cancelMu.Unlock()
	defer s.finishWork(slot)
	select {
	case claimed := <-idleDone:
		if !claimed {
			t.Fatal("idle journal maintenance did not claim its admission slot")
		}
	case <-time.After(300 * time.Millisecond):
		s.journalBuildMu.Unlock()
		locked = false
		<-idleDone
		t.Fatal("idle journal recovery ignored actual enqueue cancellation while waiting for another reconstruction")
	}
	select {
	case <-slot.ready:
	default:
		t.Fatal("canceled journal recovery retained the next FIFO admission slot")
	}
	s.cancelMu.Lock()
	active := s.summaryRefreshActive || s.summaryRefreshIdleCancel != nil
	pending := len(s.queueCleanup) != 0
	s.cancelMu.Unlock()
	if active || !pending {
		t.Fatalf("canceled maintenance lost ownership boundary: active=%t cleanupPending=%t", active, pending)
	}
	ids, err := s.pendingWorkCleanupIDs()
	if err != nil || len(ids) != 1 || ids[0] != id {
		t.Fatalf("canceled journal maintenance discarded durable cleanup marker: %v %v", ids, err)
	}
	s.journalBuildMu.Unlock()
	locked = false
	if err := s.rebuildJournalIndex(t.Context(), id); err != nil {
		t.Fatalf("pending journal index did not recover after lock release: %v", err)
	}
	journalReviewAssertReleased(t, s, id)
	journalReviewAssertNoStaging(t, s, id, true)
}

func TestIdleJournalCallbackStorageWaitYieldsToNewWork(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	const id = "idle-journal-callback-cancel"
	journalReviewLegacyRun(t, s, id)
	if err := s.writePendingWorkCleanup(id); err != nil {
		t.Fatal(err)
	}
	eventsPath := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")
	before, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	original := s.workerCommand
	held := make(chan struct{})
	var holdOnce sync.Once
	s.workerCommand = func(kind string) *exec.Cmd {
		if kind == "controller-journal" {
			// Stage creation has closed its database. Hold a different owner's
			// persistence mutex before the actual child enters journal.keys.
			holdOnce.Do(func() {
				s.state.persistMu.Lock()
				close(held)
			})
		}
		return original(kind)
	}
	idleDone := make(chan bool, 1)
	go func() { idleDone <- s.tryRefreshIdleRunMetrics(t.Context(), id) }()
	select {
	case <-held:
	case <-time.After(3 * time.Second):
		t.Fatal("journal reconstruction did not reach its post-stage child boundary")
	}
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
	}()
	retirementReviewWait(t, "actual journal callback did not reach its persistence wait", func() bool {
		buf := make([]byte, 2<<20)
		n := runtime.Stack(buf, true)
		for _, stack := range bytes.Split(buf[:n], []byte("\n\n")) {
			if bytes.Contains(stack, []byte("(*Server).rebuildJournalIndex.func")) &&
				(bytes.Contains(stack, []byte("contextlock.Lock")) || bytes.Contains(stack, []byte("sync.(*Mutex).Lock"))) {
				return true
			}
		}
		return false
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s.cancelMu.Lock()
	slot := s.enqueueWorkLocked(ctx, model.WorkQueueItem{Kind: "experiment", Name: "next experiment"}, cancel)
	s.cancelMu.Unlock()
	defer s.finishWork(slot)
	select {
	case claimed := <-idleDone:
		if !claimed {
			t.Fatal("idle journal callback maintenance did not claim its work slot")
		}
	case <-time.After(2 * time.Second):
		s.state.persistMu.Unlock()
		locked = false
		<-idleDone
		t.Fatal("journal callback/abort/snapshot release ignored actual enqueue cancellation behind unrelated persistence")
	}
	select {
	case <-slot.ready:
	default:
		t.Fatal("quiescent canceled journal callback retained the next FIFO slot")
	}
	// The callback never acquired persistence. Only an inactive, empty bucket
	// can remain; it must not authorize receipts or replace the source index.
	root, db, err := openJournalIndex(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	if journalIndexReady(db) {
		t.Error("canceled reconstruction promoted its incomplete index")
	}
	stages := 0
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			if strings.HasPrefix(string(name), "rebuild-") {
				stages++
				if bucket.Stats().KeyN != 0 {
					t.Error("canceled callback wrote receipt identities after its persistence wait")
				}
			}
			return nil
		})
	})
	err = errors.Join(err, db.Close(), root.Close())
	if err != nil || stages != 1 {
		t.Fatalf("canceled reconstruction did not preserve its one inactive stage: stages=%d error=%v", stages, err)
	}
	s.resultDownloadMu.Lock()
	downloads := s.resultDownloads[id]
	s.resultDownloadMu.Unlock()
	s.workerMu.Lock()
	unreaped, snapshots := len(s.unreapedWorkers[id]), len(s.workerSnapshots)
	s.workerMu.Unlock()
	if downloads != 0 || unreaped != 0 || snapshots != 0 {
		t.Fatalf("quiescent callback retained resources: downloads=%d workers=%d snapshots=%d", downloads, unreaped, snapshots)
	}
	started := commands()
	if len(started) != 1 || started[0].ProcessState == nil {
		t.Fatal("journal maintenance released FIFO before collecting its actual child")
	}
	s.state.persistMu.Unlock()
	locked = false
	after, err := os.ReadFile(eventsPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("canceled journal callback changed scientific source: %v", err)
	}
	if err := s.waitQueueCleanup(ctx, "", scenario.Scenario{JobShutdownTimeout: "2s"}); err != nil {
		t.Fatalf("next FIFO owner could not repair the pending journal cleanup: %v", err)
	}
	journalReviewAssertReleased(t, s, id)
	journalReviewAssertNoStaging(t, s, id, true)
}
