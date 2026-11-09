package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	bolt "go.etcd.io/bbolt"
)

func journalTestEvent(id string, sequence uint64) model.TraceEvent {
	return model.TraceEvent{RunID: id, NodeID: "peer", SessionID: "session", Sequence: sequence, EventID: "event", Type: "publish", Topic: "topic", MessageID: "message", Timestamp: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)}
}

func readJournalEvents(t *testing.T, directory, id string) []model.TraceEvent {
	t.Helper()
	file, err := os.Open(filepath.Join(directory, currentRunsDirectory, id, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var events []model.TraceEvent
	decoder := json.NewDecoder(file)
	for {
		var event model.TraceEvent
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func TestJournalDiskIdentitySurvivesRestartAndSequenceHoles(t *testing.T) {
	directory := t.TempDir()
	for range 3 {
		s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
		for _, sequence := range []uint64{4, 1, 4, 2, 1} {
			event := journalTestEvent("run", sequence)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event, event}}); err != nil {
				t.Fatal(err)
			}
		}
		if len(s.state.journalIndices) != 0 || len(s.state.runMetrics) != 0 {
			t.Fatal("persistent service retained a per-Run raw identity or scientific index")
		}
	}
	if events := readJournalEvents(t, directory, "run"); len(events) != 3 {
		t.Fatalf("restart admitted duplicate identities or discarded sequence holes: %d", len(events))
	}
}

func TestJournalWALCrashHelper(t *testing.T) {
	if len(os.Args) < 4 || os.Args[len(os.Args)-3] != "journal-wal-crash" {
		return
	}
	directory, mode := os.Args[len(os.Args)-2], os.Args[len(os.Args)-1]
	root, db, err := openJournalIndex(filepath.Join(directory, currentRunsDirectory, "run"))
	if err != nil || initializeEmptyJournalIndex(root, db, "run") != nil {
		os.Exit(2)
	}
	event := journalTestEvent("run", 1)
	var data bytes.Buffer
	_ = json.NewEncoder(&data).Encode(event)
	pending := pendingJournalAppend{Data: data.Bytes(), Keys: [][]byte{journalEventKey(event)}, SHA256: journalDataChecksum(data.Bytes())}
	var intentErr error
	if mode == "legacy-intent" {
		encoded, _ := json.Marshal(pending)
		intentErr = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("meta")).Put([]byte("pending"), encoded) })
	} else {
		intentErr = writeAnalysisJSON(root, journalPendingFile, pending)
	}
	if intentErr != nil {
		os.Exit(2)
	}
	if mode != "before-write" {
		length := data.Len()
		if mode == "partial-write" {
			length /= 2
		}
		file, err := root.OpenFile("events.jsonl", os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			os.Exit(2)
		}
		if _, err = file.Write(data.Bytes()[:length]); err != nil || file.Sync() != nil || syncRunDirectory(root) != nil {
			os.Exit(2)
		}
	}
	// Simulate process loss without closing the database or clearing its intent.
	os.Exit(23)
}

func TestJournalWALRecoversProcessLossAtEveryAppendBoundary(t *testing.T) {
	for _, mode := range []string{"before-write", "partial-write", "after-fsync", "legacy-intent"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestJournalWALCrashHelper$", "--", "journal-wal-crash", directory, mode)
			if err := command.Run(); err == nil || command.ProcessState.ExitCode() != 23 {
				t.Fatalf("fixture did not terminate at intended crash boundary: %v", err)
			}
			s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err != nil {
				t.Fatal(err)
			}
			if events := readJournalEvents(t, directory, "run"); len(events) != 1 || events[0].Sequence != 1 {
				t.Fatalf("WAL lost or duplicated acknowledged source: %+v", events)
			}
			if len(s.state.journalIndices) != 0 || len(s.state.runMetrics) != 0 {
				t.Fatal("crash recovery reconstructed a main-process raw index")
			}
			if _, err := os.Stat(filepath.Join(directory, currentRunsDirectory, "run", journalPendingFile)); !os.IsNotExist(err) {
				t.Fatalf("acknowledged append retained its write-ahead batch: %v", err)
			}
		})
	}
}

func TestJournalLargeAcknowledgedBatchDoesNotLeaveWALBlobInIdentityDatabase(t *testing.T) {
	directory := t.TempDir()
	s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
	event := journalTestEvent("run", 1)
	event.Fields = map[string]any{"payload": strings.Repeat("x", 4<<20)}
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	index, err := os.Stat(filepath.Join(directory, currentRunsDirectory, "run", journalIndexFile))
	if err != nil || index.Size() >= 1<<20 {
		t.Fatalf("large batch payload remains in the persistent identity database: %v %+v", err, index)
	}
	if _, err := os.Stat(filepath.Join(directory, currentRunsDirectory, "run", journalPendingFile)); !os.IsNotExist(err) {
		t.Fatalf("acknowledged batch left a pending WAL file: %v", err)
	}
}

func TestJournalDamagedIntentCannotCreateOrOverwriteCanonicalEvidence(t *testing.T) {
	for _, mode := range []string{"empty", "checksum"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestJournalWALCrashHelper$", "--", "journal-wal-crash", directory, "before-write")
			if err := command.Run(); err == nil || command.ProcessState.ExitCode() != 23 {
				t.Fatalf("crash fixture: %v", err)
			}
			path := filepath.Join(directory, currentRunsDirectory, "run", journalPendingFile)
			var damaged []byte
			if mode == "checksum" {
				encoded, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var pending pendingJournalAppend
				if err := json.Unmarshal(encoded, &pending); err != nil {
					t.Fatal(err)
				}
				pending.Data[0] = '!'
				damaged, _ = json.Marshal(pending)
			}
			if err := os.WriteFile(path, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err == nil {
				t.Fatal("damaged intent was acknowledged or replaced")
			}
			if _, err := os.Stat(filepath.Join(directory, currentRunsDirectory, "run", "events.jsonl")); !os.IsNotExist(err) {
				t.Fatalf("damaged intent created canonical source bytes: %v", err)
			}
			retained, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(retained, damaged) {
				t.Fatalf("recovery overwrote damaged intent evidence: %v", err)
			}
		})
	}
}

func TestJournalPendingPrefixMismatchPreservesEvidenceAndRefusesACK(t *testing.T) {
	directory := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestJournalWALCrashHelper$", "--", "journal-wal-crash", directory, "after-fsync")
	if err := command.Run(); err == nil || command.ProcessState.ExitCode() != 23 {
		t.Fatalf("crash fixture: %v", err)
	}
	path := filepath.Join(directory, currentRunsDirectory, "run", "events.jsonl")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before[0] = '!'
	if err := os.WriteFile(path, before, 0644); err != nil {
		t.Fatal(err)
	}
	s := New(ServerConfig{DataDir: directory, RunWorkers: true}, nil)
	if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{journalTestEvent("run", 1)}}); err == nil || !strings.Contains(err.Error(), "prefix differs") {
		t.Fatalf("mismatched durable source was acknowledged: %v", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("recovery overwrote differing source evidence")
	}
}

func TestLegacyJournalIndexRebuildRunsOutsideMainAndPreservesArchivedSource(t *testing.T) {
	for _, archive := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "archive"}[archive], func(t *testing.T) {
			s, commands := isolatedAnalysisServer(t)
			s.state.requestJournalRecovery = nil
			resultFixture(t, s, "legacy-run", "completed", time.Now().Add(-time.Minute).UTC())
			first := journalTestEvent("legacy-run", 1)
			if err := s.state.persistEvents("legacy-run", []model.TraceEvent{first}); err != nil {
				t.Fatal(err)
			}
			if archive {
				archiveTestRun(t, s, "legacy-run")
			}
			root, db, err := openJournalIndex(filepath.Join(s.config.DataDir, currentRunsDirectory, "legacy-run"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				bucket, err := tx.CreateBucket([]byte("rebuild-abandoned-before-parent-loss"))
				if err != nil {
					return err
				}
				return bucket.Put([]byte("unfinished"), []byte("never published"))
			})
			db.Close()
			root.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{first}}); !errors.Is(err, errJournalIndexRequired) {
				t.Fatalf("unindexed legacy source accepted replay: %v", err)
			}
			if err := s.rebuildJournalIndex(context.Background(), "legacy-run"); err != nil {
				t.Fatal(err)
			}
			root, db, err = openJournalIndex(filepath.Join(s.config.DataDir, currentRunsDirectory, "legacy-run"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.View(func(tx *bolt.Tx) error {
				if tx.Bucket([]byte("rebuild-abandoned-before-parent-loss")) != nil {
					return errors.New("reconstruction retained an abandoned staging bucket")
				}
				return nil
			})
			db.Close()
			root.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.state.appendEvents(model.EventBatch{Events: []model.TraceEvent{first, journalTestEvent("legacy-run", 2)}}); err != nil {
				t.Fatal(err)
			}
			snapshot, err := s.captureResultFilesContext(t.Context(), "legacy-run", false)
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.close()
			count := 0
			for _, file := range snapshot.files {
				if file.name == "events.jsonl" {
					err = scanAnalysisFile(t.Context(), file, func([]byte) error { count++; return nil })
				}
			}
			if err != nil || count != 2 {
				t.Fatalf("legacy reconstruction changed source identities: %d %v", count, err)
			}
			started := commands()
			if len(started) != 1 || started[0].ProcessState == nil || len(s.state.runMetrics) != 0 || len(s.state.journalIndices) != 0 {
				t.Fatal("legacy reconstruction survived in main or advanced before worker Wait")
			}
		})
	}
}
