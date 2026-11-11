package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestRemoteAnalysisSourceKeepsPinnedLength(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	snapshot := &resultSnapshot{files: []resultFile{
		{name: "events.jsonl", size: 3, readAt: strings.NewReader("abclate")},
		{name: "observations.jsonl", size: 3, readAt: strings.NewReader("ab")},
	}}
	read := s.remoteAnalysisSnapshotSource([]*resultSnapshot{snapshot})
	value, err := read(t.Context(), "source.read", json.RawMessage(`{"file":0,"offset":2,"length":8}`))
	if err != nil {
		t.Fatal(err)
	}
	chunk := value.(workerSourceBytes)
	if string(chunk.Data) != "c" || !chunk.EOF {
		t.Fatalf("remote read crossed captured boundary: %+v", chunk)
	}
	if _, err := read(t.Context(), "source.read", json.RawMessage(`{"file":1,"length":8}`)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated captured prefix was accepted: %v", err)
	}
}

func TestRemoteAnalysisSourceSharesTransferBudget(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), AnalysisTransferMiBPerSecond: 1}, nil)
	data := bytes.Repeat([]byte("x"), workerSourceChunk)
	snapshot := &resultSnapshot{files: []resultFile{{name: "events.jsonl", size: int64(len(data)), readAt: bytes.NewReader(data)}}}
	read := s.remoteAnalysisSnapshotSource([]*resultSnapshot{snapshot})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := time.Now()
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := read(ctx, "source.read", json.RawMessage(`{"length":65536}`))
			done <- err
		}()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(started); elapsed < 125*time.Millisecond {
		t.Fatalf("two reads bypassed the shared transfer budget in %v", elapsed)
	}
	paced, stopPacing := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stopPacing()
	if _, err := read(paced, "source.read", json.RawMessage(`{"length":1048576}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pacing ignored cancellation: %v", err)
	}
	if _, err := read(ctx, "source.read", json.RawMessage(`{"length":1}`)); err != nil {
		t.Fatalf("canceled read held the gate: %v", err)
	}
}

func TestRemoteAnalysisCaptureLeavesJournalRecoveryToRun(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "intent file"
		if legacy {
			name = "legacy database intent"
		}
		t.Run(name, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			const id = "completed-run"
			resultFixture(t, s, id, "completed", time.Now().UTC())
			path := filepath.Join(s.config.DataDir, currentRunsDirectory, id)
			intent := []byte(`{"offset":0,"data":"YWJj"}`)
			if legacy {
				root, db, err := openJournalIndex(path)
				if err != nil {
					t.Fatal(err)
				}
				err = db.Update(func(tx *bolt.Tx) error {
					meta, err := tx.CreateBucketIfNotExists([]byte("meta"))
					if err != nil {
						return err
					}
					return meta.Put([]byte("pending"), intent)
				})
				if err := errors.Join(err, db.Close(), root.Close()); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(path, journalPendingFile), intent, 0600); err != nil {
				t.Fatal(err)
			}
			snapshot, err := s.captureRemoteAnalysisResultFilesContext(t.Context(), id)
			if snapshot != nil {
				snapshot.close()
			}
			if snapshot != nil || !errors.Is(err, errResultBusy) {
				t.Fatalf("remote analysis did not reject a pending append: %v", err)
			}
			if legacy {
				pending, err := hasLegacyJournalIntent(path)
				if err != nil || !pending {
					t.Fatalf("remote analysis changed the legacy intent: %t, %v", pending, err)
				}
			} else {
				data, err := os.ReadFile(filepath.Join(path, journalPendingFile))
				if err != nil || !bytes.Equal(data, intent) {
					t.Fatalf("remote analysis changed the journal intent: %q, %v", data, err)
				}
			}
		})
	}
}
