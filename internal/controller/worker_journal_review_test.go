package controller

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func journalReviewLegacyRun(t *testing.T, s *Server, id string) {
	t.Helper()
	s.state.requestJournalRecovery = nil
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute).UTC())
	if err := s.state.persistEvents(id, []model.TraceEvent{journalTestEvent(id, 1)}); err != nil {
		t.Fatal(err)
	}
}

func journalReviewAssertReleased(t *testing.T, s *Server, id string) {
	t.Helper()
	s.state.persistMu.Lock()
	defer s.state.persistMu.Unlock()
	if s.resultDownloads[id] != 0 || s.resultReadPins[id] != 0 || len(s.archiveReadSlots) != 0 {
		t.Fatalf("journal reconstruction retained source ownership: downloads=%d pins=%d slots=%d", s.resultDownloads[id], s.resultReadPins[id], len(s.archiveReadSlots))
	}
}

func journalReviewAssertNoStaging(t *testing.T, s *Server, id string, ready bool) {
	t.Helper()
	root, db, err := openJournalIndex(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	defer db.Close()
	if journalIndexReady(db) != ready {
		t.Fatalf("journal index ready=%t; want %t", journalIndexReady(db), ready)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		var active []byte
		if meta := tx.Bucket([]byte("meta")); meta != nil {
			active = meta.Get([]byte("active"))
		}
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if strings.HasPrefix(string(name), "rebuild-") && string(active) != string(name) {
				t.Errorf("failed reconstruction left a staging bucket %q", name)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestJournalReconstructionHoldsDeletionLeaseAndReleasesCanceledStage(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "cancel"}[canceled], func(t *testing.T) {
			s, commands := isolatedAnalysisServer(t)
			const id = "journal-lease"
			journalReviewLegacyRun(t, s, id)
			originalCommand := s.workerCommand
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			s.workerCommand = func(kind string) *exec.Cmd {
				close(entered)
				<-release
				return originalCommand(kind)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.rebuildJournalIndex(ctx, id) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("reconstruction did not reach its child startup boundary")
			}
			if err := s.deleteSavedResult(id); !errors.Is(err, errResultBusy) {
				t.Fatalf("active reconstruction lost its deletion lease: %v", err)
			}
			if canceled {
				cancel()
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				if canceled && !errors.Is(err, context.Canceled) || !canceled && err != nil {
					t.Fatalf("unexpected reconstruction result: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("reconstruction failed to release its child and source")
			}
			journalReviewAssertReleased(t, s, id)
			journalReviewAssertNoStaging(t, s, id, !canceled)
			started := commands()
			if len(started) != 1 || started[0].ProcessState == nil {
				t.Fatal("reconstruction returned before collecting its actual child")
			}
			if canceled {
				s.workerCommand = originalCommand
				if err := s.rebuildJournalIndex(t.Context(), id); err != nil {
					t.Fatalf("canceled staging prevented a later reconstruction: %v", err)
				}
				journalReviewAssertNoStaging(t, s, id, true)
			}
			if err := s.deleteSavedResult(id); err != nil {
				t.Fatalf("reaped reconstruction kept its deletion lease: %v", err)
			}
			if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted reconstructed Run was recreated: %v", err)
			}
		})
	}
}

func TestJournalReconstructionRespectsTombstoneDuringStagedWorker(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	const id = "journal-tombstone"
	journalReviewLegacyRun(t, s, id)
	originalCommand := s.workerCommand
	s.workerCommand = func(kind string) *exec.Cmd {
		// Simulate a durable recovery/deletion fence crossing an older worker
		// lease. API deletion is separately prevented by the source read pin.
		s.state.persistMu.Lock()
		err := s.markResultDeletedLocked(id)
		if err == nil {
			var runs *os.Root
			runs, err = s.openResultRuns()
			if err == nil {
				err = removeResultDirectory(runs, id)
				runs.Close()
			}
		}
		s.state.persistMu.Unlock()
		if err != nil {
			t.Error(err)
		}
		return originalCommand(kind)
	}
	if err := s.rebuildJournalIndex(t.Context(), id); err == nil {
		t.Fatal("stale worker published a reconstructed index after deletion")
	}
	if _, err := os.Stat(filepath.Join(s.config.DataDir, currentRunsDirectory, id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("broker or staging cleanup recreated a tombstoned Run: %v", err)
	}
	journalReviewAssertReleased(t, s, id)
	if started := commands(); len(started) != 1 || started[0].ProcessState == nil {
		t.Fatal("stale reconstruction did not collect its actual worker")
	}
}

func TestJournalReconstructionBlockedArchiveKeepsOwnershipWithoutHoldingIngressLock(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	const id = "journal-archive"
	journalReviewLegacyRun(t, s, id)
	archiveTestRun(t, s, id)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s.archiveIOCheck = func() { enteredOnce.Do(func() { close(entered); <-release }) }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.rebuildJournalIndex(ctx, id) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reconstruction did not reach archived source")
	}
	if err := s.deleteSavedResult(id); !errors.Is(err, errResultBusy) {
		t.Fatalf("blocked archive read lost deletion ownership: %v", err)
	}
	appendDone := make(chan error, 1)
	go func() {
		appendDone <- s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("unrelated", 1)}})
	}()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked NAS reconstruction held the ingress persistence lock")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("claimed cancellation while the simulated NAS syscall still owned a source: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("archive cancellation was lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("released archive read did not honor cancellation")
	}
	journalReviewAssertReleased(t, s, id)
	if len(commands()) != 0 {
		t.Fatal("canceled source resolution launched an experiment process")
	}
	s.archiveIOCheck = nil
	if err := s.rebuildJournalIndex(t.Context(), id); err != nil {
		t.Fatalf("archive reconstruction did not recover after cancellation: %v", err)
	}
	journalReviewAssertReleased(t, s, id)
	journalReviewAssertNoStaging(t, s, id, true)
	if started := commands(); len(started) != 1 || started[0].ProcessState == nil {
		t.Fatal("restored archive reconstruction did not reap its worker")
	}
}
