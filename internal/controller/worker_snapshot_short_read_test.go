package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestWorkerSnapshotSourcePreservesPinnedReadLength(t *testing.T) {
	for _, test := range []struct {
		name     string
		offset   int64
		length   int
		truncate bool
		keep     int64
		want     string
		eof      bool
		wantErr  error
	}{
		{name: "whole source", length: 99, want: "first\nsecond\n", eof: true},
		{name: "part before end", length: 6, want: "first\n"},
		{name: "normal final read", offset: 6, length: 99, want: "second\n", eof: true},
		{name: "at pinned end", offset: 13, length: 99, eof: true},
		{name: "beyond pinned end", offset: 20, length: 99, eof: true},
		{name: "zero length", offset: 3, length: 0},
		{name: "available prefix after truncation", length: 6, truncate: true, keep: 6, want: "first\n"},
		{name: "missing tail", length: 99, truncate: true, keep: 6, wantErr: io.ErrUnexpectedEOF},
		{name: "missing final read", offset: 6, length: 99, truncate: true, keep: 6, wantErr: io.ErrUnexpectedEOF},
		{name: "entire source lost", length: 99, truncate: true, wantErr: io.ErrUnexpectedEOF},
		{name: "pinned end after truncation", offset: 13, length: 99, truncate: true, keep: 6, eof: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			file := hashTestFile(t, "events.jsonl", "first\nsecond\n")
			if test.truncate {
				if err := os.Truncate(file.file.Name(), test.keep); err != nil {
					t.Fatal(err)
				}
			}
			source := workerSourceHandler([]*resultSnapshot{{files: []resultFile{file}}})
			raw, err := json.Marshal(workerSourceRead{File: 0, Offset: test.offset, Length: test.length})
			if err != nil {
				t.Fatal(err)
			}
			value, err := source(t.Context(), "source.read", raw)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("read error=%v want=%v response=%+v", err, test.wantErr, value)
			}
			if err == nil {
				block := value.(workerSourceBytes)
				if string(block.Data) != test.want || block.EOF != test.eof {
					t.Fatalf("source boundary changed: data=%q eof=%t", block.Data, block.EOF)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := source(ctx, "source.read", raw); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled read error=%v", err)
			}
			if err := file.file.Close(); err != nil {
				t.Fatal(err)
			}
			raw, _ = json.Marshal(workerSourceRead{Length: 1})
			if _, err := source(t.Context(), "source.read", raw); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("actual I/O error was lost: %v", err)
			}
		})
	}
}

func TestExportWorkerRejectsTruncatedSnapshotAndRetriesHealthySource(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	const id = "truncated-worker-source"
	resultFixture(t, s, id, "completed", time.Now().Add(-time.Minute))
	events := []model.TraceEvent{
		{RunID: id, EventID: "first", NodeID: "sender", Type: "publish", Topic: "topic", MessageID: "first", Timestamp: time.Now().Add(-time.Minute)},
		{RunID: id, EventID: "second", NodeID: "sender", Type: "publish", Topic: "topic", MessageID: "second", Timestamp: time.Now().Add(-time.Minute)},
	}
	if err := s.state.persistEvents(id, events); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.config.DataDir, currentRunsDirectory, id, "events.jsonl")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.captureResultFilesContext(t.Context(), id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	if err := os.Truncate(path, int64(bytes.IndexByte(original, '\n')+1)); err != nil {
		t.Fatal(err)
	}
	metrics, failedErr := snapshot.metricsCache.get(t.Context())
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if failedErr == nil {
		var archive bytes.Buffer
		if err := snapshot.writeZIP(t.Context(), &archive); err != nil {
			t.Fatal(err)
		}
		files := decodeResultZIP(t, archive.Bytes())
		t.Fatalf("short snapshot became successful cached metrics: published=%d completeEvents=%d", metrics.Published, len(storedEventIDs(t, files["events.jsonl"])))
	}
	metrics, err = snapshot.metricsCache.get(t.Context())
	if err != nil || metrics.Published != 2 {
		t.Fatalf("healthy source retry failed: published=%d error=%v", metrics.Published, err)
	}
	var archive bytes.Buffer
	if err := snapshot.writeZIP(t.Context(), &archive); err != nil {
		t.Fatal(err)
	}
	files := decodeResultZIP(t, archive.Bytes())
	var exported model.Metrics
	if err := json.Unmarshal(files["metrics.json"], &exported); err != nil || exported.Published != 2 || len(storedEventIDs(t, files["events.jsonl"])) != 2 {
		t.Fatalf("repaired export mismatch: %+v error=%v", exported, err)
	}
	started := commands()
	if len(started) != 2 || started[0].ProcessState == nil || started[1].ProcessState == nil || len(s.archiveReadSlots) != 0 {
		t.Fatal("failed/successful source reads retained workers or storage slots")
	}
}
