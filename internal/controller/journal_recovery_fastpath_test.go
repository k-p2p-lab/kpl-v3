package controller

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func TestJournalRecoveryReadyIndexDoesNotTakeWriterLock(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
	before, err := os.ReadFile(filepath.Join(directory, journalIndexFile))
	if err != nil {
		t.Fatal(err)
	}
	// A ready index with no outstanding transaction needs only a shared read
	// lock. Holding another reader makes an unnecessary exclusive open fail.
	root, db, err := openJournalIndexMode(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	if err := s.rebuildJournalIndex(ctx, "run"); err != nil {
		t.Fatalf("ready journal cleanup required an exclusive database writer: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(directory, journalIndexFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("read-only cleanup changed committed identity evidence: %v", err)
	}
}

func TestJournalRecoveryFastPathPreservesPendingTransactions(t *testing.T) {
	for _, mode := range []string{"before-write", "partial-write", "after-fsync", "legacy-intent"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestJournalWALCrashHelper$", "--", "journal-wal-crash", directory, mode)
			if err := command.Run(); err == nil || command.ProcessState.ExitCode() != 23 {
				t.Fatalf("crash fixture did not retain its intent: %v", err)
			}
			s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
			if err := s.rebuildJournalIndex(t.Context(), "run"); err != nil {
				t.Fatal(err)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
				t.Fatal(err)
			}
			if events := readJournalEvents(t, directory, "run"); len(events) != 1 || events[0].Sequence != 1 {
				t.Fatalf("cleanup skipped or duplicated committed pending evidence: %+v", events)
			}
		})
	}
}

func TestJournalRecoveryReadyIndexHonorsCancellation(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.rebuildJournalIndex(ctx, "run"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ready-index recovery reported success: %v", err)
	}
}

func TestJournalRecoveryFastPathRejectsDamagedEvidence(t *testing.T) {
	for _, damage := range []string{"database", "intent"} {
		t.Run(damage, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
			before, err := os.ReadFile(filepath.Join(directory, "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			name := journalIndexFile
			if damage == "intent" {
				name = journalPendingFile
			}
			if err := os.WriteFile(filepath.Join(directory, name), []byte("damaged"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := s.rebuildJournalIndex(t.Context(), "run"); err == nil {
				t.Fatal("damaged evidence was reported as a clean ready index")
			}
			after, err := os.ReadFile(filepath.Join(directory, "events.jsonl"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("failed recovery changed canonical evidence: %v", err)
			}
		})
	}
}

func TestJournalRecoveryFastPathPreservesDeletionFences(t *testing.T) {
	for _, fence := range []string{"deleted", "lost"} {
		t.Run(fence, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
				t.Fatal(err)
			}
			s.state.persistMu.Lock()
			var err error
			if fence == "deleted" {
				err = s.markResultDeletedLocked("run")
			} else {
				err = s.markResultWriteFencedLocked("run")
			}
			s.state.persistMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
			if err := os.RemoveAll(directory); err != nil {
				t.Fatal(err)
			}
			if err := s.rebuildJournalIndex(t.Context(), "run"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery recreated a fenced Run: %v", err)
			}
		})
	}
}

func BenchmarkJournalReadyRecovery(b *testing.B) {
	for _, legacy := range []bool{false, true} {
		b.Run(fmt.Sprintf("legacy-unsynced-freelist=%t", legacy), func(b *testing.B) {
			s := New(ServerConfig{DataDir: b.TempDir(), RunWorkers: true}, nil)
			directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
			root, db, err := openJournalIndex(directory)
			if err != nil {
				b.Fatal(err)
			}
			if err := initializeEmptyJournalIndex(root, db, "run"); err != nil {
				b.Fatal(err)
			}
			db.NoFreelistSync = legacy
			err = db.Update(func(tx *bolt.Tx) error {
				bucket := journalActiveBucket(tx)
				for index := range 200000 {
					var key [32]byte
					binary.BigEndian.PutUint64(key[:8], uint64(index))
					if err := bucket.Put(key[:], []byte{1}); err != nil {
						return err
					}
				}
				return nil
			})
			if err = errors.Join(err, db.Close(), root.Close()); err != nil {
				b.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(directory, journalIndexFile))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := s.rebuildJournalIndex(b.Context(), "run"); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(info.Size()), "index-bytes")
		})
	}
}
