package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetryDiskBacklogAndRecoveryKeepPayloadMemoryBounded(t *testing.T) {
	runtime.GC()
	runtime.GC()
	var before, queued, recovered runtime.MemStats
	runtime.ReadMemStats(&before)
	dir := t.TempDir()
	spool, _, err := openTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{spool: spool, config: Config{ID: "agent"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	const count = 1024
	for start := 0; start < count; start += 32 {
		events := make([]model.TraceEvent, 32)
		for i := range events {
			index := start + i
			events[i] = model.TraceEvent{EventID: fmt.Sprintf("event-%04d", index), RunID: "run", Fields: map[string]any{"payload": fmt.Sprintf("%04d:%s", index, strings.Repeat("x", 32<<10))}}
		}
		if !s.enqueueEvents(model.EventBatch{Events: events}) {
			t.Fatal("durable admission failed")
		}
	}
	if len(s.events) != 0 || len(spool.records) != count || spool.bytes < 32<<20 {
		t.Fatalf("backlog retained payloads or lost events: memory=%d records=%d bytes=%d", len(s.events), len(spool.records), spool.bytes)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&queued)
	restored, window, err := openTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.records) != count || len(window) >= count || window[0].EventID != "event-0000" {
		t.Fatal("restart did not restore a bounded, ordered window")
	}
	size, err := telemetryEncodedSize(window)
	if err != nil || size > telemetryWindowBytes {
		t.Fatalf("oversized recovery window: %d %v", size, err)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&recovered)
	// Test-only GC measures reachable data, rather than timing of the scavenger.
	for name, mem := range map[string]runtime.MemStats{"queued": queued, "recovered": recovered} {
		if mem.HeapAlloc > before.HeapAlloc+8<<20 {
			t.Errorf("%s retained full payload backlog: before=%d after=%d", name, before.HeapAlloc, mem.HeapAlloc)
		}
	}
	t.Logf("32 MiB payload backlog: heap baseline %.2f MiB, queued %.2f MiB, recovered %.2f MiB (%d loaded events)", float64(before.HeapAlloc)/(1<<20), float64(queued.HeapAlloc)/(1<<20), float64(recovered.HeapAlloc)/(1<<20), len(window))
	runtime.KeepAlive(s)
	next := 0
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.EventBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		for _, event := range batch.Events {
			if event.EventID != fmt.Sprintf("event-%04d", next) {
				t.Errorf("order changed at %d: %s", next, event.EventID)
			}
			next++
		}
		w.WriteHeader(204)
	}))
	defer controller.Close()
	s = &Server{spool: restored, events: window, config: Config{ID: "agent", ControllerURL: controller.URL}, client: controller.Client(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := s.drainRunEvents(ctx, "run"); err != nil {
		t.Fatal(err)
	}
	if next != count || len(restored.records) != 0 || restored.bytes != 0 {
		t.Fatalf("incomplete drain: received=%d records=%d bytes=%d", next, len(restored.records), restored.bytes)
	}
	again, pending, err := openTelemetrySpool(dir)
	if err != nil || len(pending) != 0 || len(again.records) != 0 {
		t.Fatalf("delivered data replayed: %v", err)
	}
}

func TestTelemetryRecoveryAcceptsLegacyWhitespaceAndPartialAck(t *testing.T) {
	dir := t.TempDir()
	spool, _, err := openTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	events := []model.TraceEvent{{EventID: "acked", RunID: "old"}, {EventID: "pending", RunID: "run", Fields: map[string]any{"nested": map[string]any{"x": "y"}}}}
	data, _ := json.MarshalIndent(events, "", "  ")
	name := "00000000000000000001-legacy.json"
	if err := writeTelemetryFile(filepath.Join(spool.directory, name), data); err != nil {
		t.Fatal(err)
	}
	if err := writeTelemetryFile(filepath.Join(spool.directory, name+".ack"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	restored, window, err := openTelemetrySpool(dir)
	if err != nil || len(window) != 1 || window[0].EventID != "pending" || restored.records[0].runID != "run" {
		t.Fatalf("legacy recovery: %+v %v", window, err)
	}
}

func TestTelemetryPartialAckFailureCannotAcknowledgeUnsentEvents(t *testing.T) {
	dir := t.TempDir()
	spool, _, err := openTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "unsent"} {
		if err := spool.append([]model.TraceEvent{{EventID: id}}); err != nil {
			t.Fatal(err)
		}
	}
	blocked := filepath.Join(spool.directory, spool.records[1].segment.name+".ack")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	n, err := spool.acknowledge(2)
	if err == nil || n != 1 || len(spool.records) != 2 {
		t.Fatalf("partial ack progress: %d %v records=%d", n, err, len(spool.records))
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	_, pending, err := openTelemetrySpool(dir)
	if err != nil || len(pending) != 2 || pending[0].EventID != "two" || pending[1].EventID != "unsent" {
		t.Fatalf("partial ack lost/reordered data: %+v %v", pending, err)
	}
}

func TestTelemetryDecoderAdmissionRejectsBeforeReadingBody(t *testing.T) {
	s := &Server{}
	readers := make([]*io.PipeReader, 2)
	writers := make([]*io.PipeWriter, 2)
	done := make(chan int, 2)
	for i := range readers {
		readers[i], writers[i] = io.Pipe()
		t.Cleanup(func() { _ = readers[i].Close(); _ = writers[i].Close() })
		go func(i int) {
			response := httptest.NewRecorder()
			s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", readers[i]))
			done <- response.Code
		}(i)
	}
	deadline := time.Now().Add(time.Second)
	for s.telemetryDecoders.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.telemetryDecoders.Load() != 2 {
		t.Fatal("decoders did not reserve slots")
	}
	response := httptest.NewRecorder()
	body := &unreadTelemetryBody{t: t}
	s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", body))
	if response.Code != 503 || response.Header().Get("Retry-After") == "" || s.telemetryDecodersRejected.Load() != 1 {
		t.Fatal("busy request was acknowledged")
	}
	for _, writer := range writers {
		_, _ = io.WriteString(writer, `{"events":[]}`)
		_ = writer.Close()
	}
	for range readers {
		select {
		case status := <-done:
			if status != 204 {
				t.Fatalf("reserved request: %d", status)
			}
		case <-time.After(time.Second):
			t.Fatal("decoder did not finish")
		}
	}
	if s.telemetryDecoders.Load() != 0 {
		t.Fatal("decoder slots leaked")
	}
}

type unreadTelemetryBody struct{ t *testing.T }

func (b *unreadTelemetryBody) Read([]byte) (int, error) {
	b.t.Error("busy admission read request body")
	return 0, io.EOF
}

func TestTelemetryByteBackpressureKeepsDurableEvidence(t *testing.T) {
	spool, _, err := openTelemetrySpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]model.TraceEvent{{EventID: "retained"}}); err != nil {
		t.Fatal(err)
	}
	// Simulate the byte high-water mark without writing 256 MiB in every test run.
	spool.bytes = telemetryBacklogBytes
	s := &Server{spool: spool}
	response := httptest.NewRecorder()
	s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", strings.NewReader(`{"events":[{"eventId":"retry"}]}`)))
	if response.Code != 503 || len(spool.records) != 1 {
		t.Fatal("byte limit acknowledged or discarded evidence")
	}
	pending, err := spool.loadPrefix()
	if err != nil || len(pending) != 1 || pending[0].EventID != "retained" {
		t.Fatal("backpressure changed durable evidence")
	}
}

func TestTelemetryAcknowledgedSegmentCleanupRetriesWithoutRedelivery(t *testing.T) {
	spool, _, err := openTelemetrySpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]model.TraceEvent{{EventID: "delivered"}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(spool.directory, spool.records[0].segment.name)
	// Make unlink fail after a durable acknowledgment, without relying on UID.
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "blocked"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	n, err := spool.acknowledge(1)
	if err == nil || n != 1 || len(spool.records) != 0 || spool.cleanup == nil {
		t.Fatalf("cleanup failure lost committed state: %d %v", n, err)
	}
	if _, err := spool.loadPrefix(); err == nil {
		t.Fatal("cleanup failure hidden")
	}
	if err := os.Remove(filepath.Join(path, "blocked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	s := &Server{spool: spool, spoolError: errors.New("previous cleanup failure")}
	s.flushEvents(context.Background())
	if s.spoolError != nil || spool.cleanup != nil || len(s.events) != 0 {
		t.Fatal("successful cleanup kept a stale error or redelivered acknowledged data")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged segment retained: %v", err)
	}
}
