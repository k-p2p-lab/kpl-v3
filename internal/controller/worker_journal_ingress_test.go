package controller

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func TestJournalCleanCursorRecoveryAcceptsReadOnlyIndex(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
	indexPath := filepath.Join(directory, journalIndexFile)
	before, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(indexPath, 0400); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(indexPath, 0600)
	s.state.persistMu.Lock()
	err = s.state.recoverPendingJournalLocked("run")
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatalf("clean cursor required a writable database: %v", err)
	}
	root, db, err := openJournalIndexMode(directory, true)
	if err != nil {
		t.Fatal(err)
	}
	if !db.IsReadOnly() || db.PreLoadFreelist {
		t.Fatal("read probe loaded a writable freelist")
	}
	db.Close()
	root.Close()
	after, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("read probe changed the identity index: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, journalPendingFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read probe created an intent: %v", err)
	}
}

func TestJournalCursorProbeRecoversLegacyDatabaseIntent(t *testing.T) {
	directory := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestJournalWALCrashHelper$", "--", "journal-wal-crash", directory, "legacy-intent")
	if err := command.Run(); err == nil || command.ProcessState.ExitCode() != 23 {
		t.Fatalf("crash fixture did not leave a legacy intent: %v", err)
	}
	s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
	s.state.persistMu.Lock()
	err := s.state.recoverPendingJournalLocked("run")
	s.state.persistMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if events := readJournalEvents(t, directory, "run"); len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("cursor lost legacy committed bytes: %+v", events)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
		t.Fatal(err)
	}
	if events := readJournalEvents(t, directory, "run"); len(events) != 1 {
		t.Fatal("legacy cursor recovery admitted a duplicate")
	}
}

func TestJournalFastCommitKeepsIntentOnRawWriteFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses the read-only source write failure")
	}
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	directory := filepath.Join(s.config.DataDir, currentRunsDirectory, "run")
	root, db, err := openJournalIndex(directory)
	if err != nil {
		t.Fatal(err)
	}
	err = initializeEmptyJournalIndex(root, db, "run")
	db.Close()
	root.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "events.jsonl"), nil, 0400); err != nil {
		t.Fatal(err)
	}
	event := journalTestEvent("run", 1)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err == nil {
		t.Fatal("failed raw write was acknowledged")
	}
	encoded, err := os.ReadFile(filepath.Join(directory, journalPendingFile))
	if err != nil {
		t.Fatalf("failed raw write lost its durable intent: %v", err)
	}
	var pending pendingJournalAppend
	if err := json.Unmarshal(encoded, &pending); err != nil || pending.SHA256 != journalDataChecksum(pending.Data) {
		t.Fatalf("invalid retained intent: %v", err)
	}
	if len(s.state.recentEvents()) != 0 {
		t.Fatal("uncommitted events entered the public recent history")
	}
	if err := os.Chmod(filepath.Join(directory, "events.jsonl"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	if events := readJournalEvents(t, s.config.DataDir, "run"); len(events) != 1 || events[0].Sequence != 1 {
		t.Fatalf("retry lost or duplicated failed commit: %+v", events)
	}
	if len(s.state.recentEvents()) != 1 {
		t.Fatal("recovered event was published more than once")
	}
}

// Both variants perform the same durable file/index/intent commit. Replay is
// retained here as the previous ingress path, including its redundant intent
// encode/read/decode, so allocations and throughput can be compared directly.
func BenchmarkJournalDurableCommit(b *testing.B) {
	for _, replay := range []bool{true, false} {
		name := "known-batch"
		if replay {
			name = "replay-batch"
		}
		b.Run(name, func(b *testing.B) {
			root, db, err := openJournalIndex(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer root.Close()
			defer db.Close()
			if err := initializeEmptyJournalIndex(root, db, "run"); err != nil {
				b.Fatal(err)
			}
			var raw bytes.Buffer
			var keys [][]byte
			for i := 1; i <= 128; i++ {
				event := journalTestEvent("run", uint64(i))
				event.Fields = map[string]any{"payload": strings.Repeat("x", 8192)}
				if err := json.NewEncoder(&raw).Encode(event); err != nil {
					b.Fatal(err)
				}
				keys = append(keys, journalEventKey(event))
			}
			pending := pendingJournalAppend{Data: raw.Bytes(), Keys: keys, SHA256: journalDataChecksum(raw.Bytes())}
			b.ReportAllocs()
			b.SetBytes(int64(raw.Len()))
			b.ResetTimer()
			for range b.N {
				encoded, err := json.Marshal(pending)
				if err != nil {
					b.Fatal(err)
				}
				if replay {
					if err := writeAnalysisJSON(root, journalPendingFile, pending); err != nil {
						b.Fatal(err)
					}
					if _, err := recoverJournalAppend(root, db); err != nil {
						b.Fatal(err)
					}
				} else {
					if err := writeJournalIntent(root, encoded); err != nil {
						b.Fatal(err)
					}
					if err := commitJournalAppend(root, db, pending, true); err != nil {
						b.Fatal(err)
					}
				}
				pending.Offset += int64(len(pending.Data))
			}
		})
	}
}

func BenchmarkJournalCursorProbe(b *testing.B) {
	for _, readOnly := range []bool{false, true} {
		name := "writable-probe"
		if readOnly {
			name = "read-only-probe"
		}
		b.Run(name, func(b *testing.B) {
			directory := b.TempDir()
			s := newState(directory)
			path := filepath.Join(directory, currentRunsDirectory, "run")
			root, db, err := openJournalIndex(path)
			if err != nil {
				b.Fatal(err)
			}
			if err := initializeEmptyJournalIndex(root, db, "run"); err != nil {
				b.Fatal(err)
			}
			for first := 0; first < 100000; first += 1000 {
				if err := db.Update(func(tx *bolt.Tx) error {
					bucket := journalActiveBucket(tx)
					for i := first; i < first+1000; i++ {
						if err := bucket.Put(journalEventKey(journalTestEvent("run", uint64(i+1))), []byte{1}); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			db.Close()
			root.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if readOnly {
					if err := s.recoverPendingJournalLocked("run"); err != nil {
						b.Fatal(err)
					}
				} else {
					root, db, err := openJournalIndex(path)
					if err != nil {
						b.Fatal(err)
					}
					_, err = recoverJournalAppend(root, db)
					err = errors.Join(err, db.Close(), root.Close())
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
