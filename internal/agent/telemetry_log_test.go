package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetryLogReusesPayloadAndAcknowledgmentInodes(t *testing.T) {
	dir := t.TempDir()
	spool, err := readTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	var payloadInfo, ackInfo os.FileInfo
	for i := range 256 {
		if err := spool.append([]model.TraceEvent{{RunID: "run", EventID: fmt.Sprint(i)}}); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(spool.directory, spool.active.name)
		if n, err := spool.acknowledge(1); n != 1 || err != nil {
			t.Fatalf("ack: %d %v", n, err)
		}
		payload, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := os.Stat(path + ".ack")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			payloadInfo, ackInfo = payload, ack
		} else if !os.SameFile(payloadInfo, payload) || !os.SameFile(ackInfo, ack) {
			t.Fatal("small batch created/replaced an inode")
		}
	}
	files, err := os.ReadDir(spool.directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 || len(spool.records) != 0 || spool.bytes != 0 {
		t.Fatalf("idle spool grew: files=%d records=%d bytes=%d", len(files), len(spool.records), spool.bytes)
	}
	if err := spool.append([]model.TraceEvent{{RunID: "run", EventID: "pending"}}); err != nil {
		t.Fatal(err)
	}
	restored, pending, err := openTelemetrySpool(dir)
	if err != nil || len(pending) != 1 || pending[0].EventID != "pending" {
		t.Fatalf("reused segment replayed acknowledged records: %+v %v", pending, err)
	}
	if _, err := restored.acknowledge(1); err != nil {
		t.Fatal(err)
	}
}

func TestTelemetryRawWindowFillsByteBudgetAndPreservesJSON(t *testing.T) {
	spool, err := readTelemetrySpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	events := make([]model.TraceEvent, 1500)
	for i := range events {
		events[i] = model.TraceEvent{EventID: fmt.Sprint(i), RunID: "run", Sequence: uint64(i + 1), Fields: map[string]any{"ids": []string{"abc", "def"}, "number": json.Number("9007199254740993")}}
	}
	if err := spool.append(events); err != nil {
		t.Fatal(err)
	}
	data, count, err := spool.marshalPrefix("agent")
	if err != nil || count != len(events) || len(data) > telemetryWindowBytes {
		t.Fatalf("small-event throughput still capped: count=%d bytes=%d err=%v", count, len(data), err)
	}
	if !strings.Contains(string(data), `"number":9007199254740993`) {
		t.Fatal("raw forwarding changed a stored value")
	}
	var batch model.EventBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		t.Fatal(err)
	}
	if batch.AgentID != "agent" || len(batch.Events) != len(events) {
		t.Fatal("invalid batch envelope")
	}
	for i, event := range batch.Events {
		if event.EventID != fmt.Sprint(i) || event.Sequence != uint64(i+1) {
			t.Fatal("event identity/order changed")
		}
	}
}

func TestTelemetryLogRecoversInterruptedPayloadAndAckTails(t *testing.T) {
	for _, tail := range []string{"payload", "partial-ack", "torn-ack"} {
		t.Run(tail, func(t *testing.T) {
			dir := t.TempDir()
			spool, err := readTelemetrySpool(dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := spool.append([]model.TraceEvent{{EventID: "one"}, {EventID: "two"}, {EventID: "three"}}); err != nil {
				t.Fatal(err)
			}
			if _, err := spool.acknowledge(1); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(spool.directory, spool.active.name)
			data := []byte(`{"eventId":"unfinished`)
			if tail != "payload" {
				path += ".ack"
				data = make([]byte, 7)
				if tail == "torn-ack" {
					data = make([]byte, telemetryAckFrameBytes)
				}
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(data); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			restored, pending, err := openTelemetrySpool(dir)
			if err != nil || len(pending) != 2 || pending[0].EventID != "two" || pending[1].EventID != "three" {
				t.Fatalf("recovery lost evidence: %+v %v", pending, err)
			}
			if _, err := restored.acknowledge(2); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTelemetryLogRepairsAnUnacknowledgedAppendBeforeRetry(t *testing.T) {
	dir := t.TempDir()
	spool, err := readTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]model.TraceEvent{{EventID: "accepted"}}); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(spool.directory, spool.active.name), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("partial failed write")
	_ = f.Close()
	if err := spool.append([]model.TraceEvent{{EventID: "retry"}}); err != nil {
		t.Fatal(err)
	}
	_, pending, err := openTelemetrySpool(dir)
	if err != nil || len(pending) != 2 || pending[0].EventID != "accepted" || pending[1].EventID != "retry" {
		t.Fatalf("append retry lost prefix: %+v %v", pending, err)
	}
}

func TestTelemetryAdmissionWaitsWithoutDecodingAndCancels(t *testing.T) {
	s := &Server{}
	for range 2 {
		if !s.acquireTelemetryDecoder(context.Background()) {
			t.Fatal("initial decoder slot")
		}
	}
	defer s.releaseTelemetryDecoder()
	defer s.releaseTelemetryDecoder()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan bool, 1)
	go func() { started <- s.acquireTelemetryDecoder(ctx) }()
	deadline := time.Now().Add(time.Second)
	for s.telemetryWaiters.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.telemetryWaiters.Load() != 1 || s.telemetryDecoders.Load() != 2 {
		t.Fatal("waiter consumed a decoder or failed to queue")
	}
	cancel()
	select {
	case admitted := <-started:
		if admitted {
			t.Fatal("canceled waiter admitted")
		}
	case <-time.After(time.Second):
		t.Fatal("admission ignored cancellation")
	}
	if s.telemetryWaiters.Load() != 0 {
		t.Fatal("waiter leaked")
	}
}

func BenchmarkTelemetryForwardingWindow(b *testing.B) {
	spool, err := readTelemetrySpool(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	events := make([]model.TraceEvent, 2000)
	for i := range events {
		events[i] = model.TraceEvent{RunID: "run", EventID: fmt.Sprint(i), Fields: map[string]any{"ids": []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}, "counter": i}}
	}
	if err := spool.append(events); err != nil {
		b.Fatal(err)
	}
	b.Run("decoded", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			window, err := spool.loadPrefix()
			if err != nil {
				b.Fatal(err)
			}
			if _, _, err := model.MarshalEventBatchPrefix("agent", window); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, _, err := spool.marshalPrefix("agent"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestTelemetryFormatGuardPreventsSilentDowngradeAndClearsAfterDrain(t *testing.T) {
	dir := t.TempDir()
	spool, err := readTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]model.TraceEvent{{EventID: "accepted"}}); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(spool.directory, telemetryFormatMarker)
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var legacy []model.TraceEvent
	if json.Unmarshal(data, &legacy) == nil {
		t.Fatal("old Agent could silently ignore new-format events")
	}
	if err := spool.releaseIdleLog(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("pending evidence lost downgrade guard")
	}
	if _, err := spool.acknowledge(1); err != nil {
		t.Fatal(err)
	}
	if err := spool.releaseIdleLog(); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(spool.directory)
	if err != nil || len(files) != 0 {
		t.Fatalf("drained spool cannot roll back: %d files %v", len(files), err)
	}
}

func TestAgentMarksOnlyPreAdmissionBackpressureAsSafeToRetry(t *testing.T) {
	s := historyTestServer(t)
	s.spool.records = make([]telemetryRecord, telemetryBacklogEvents)
	response := httptest.NewRecorder()
	s.handleNodes(response, httptest.NewRequest(http.MethodPost, "/api/v1/nodes", strings.NewReader(`{"id":"new","runId":"run","group":"workers"}`)))
	if response.Code != 503 || response.Header().Get(model.AgentAdmissionRetryHeader) != "not-created" || response.Header().Get("Retry-After") == "" || len(s.processes) != 0 {
		t.Fatalf("unsafe admission signal: %d %s", response.Code, response.Body.String())
	}
}

func TestCanceledTelemetryRequestDoesNotAcquireAFreeDecoder(t *testing.T) {
	s := &Server{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s.acquireTelemetryDecoder(ctx) || s.telemetryDecoders.Load() != 0 {
		t.Fatal("canceled request acquired a decoder")
	}
}

func TestTelemetryRecoveryPreservesTailReferencedByDurableAck(t *testing.T) {
	dir := t.TempDir()
	spool, err := readTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.append([]model.TraceEvent{{EventID: "one"}, {EventID: "two"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.acknowledge(2); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(spool.directory, spool.active.name)
	size := spool.active.size - 1
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
	if _, err := readTelemetrySpool(dir); err == nil {
		t.Fatal("acknowledged tail corruption was hidden")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != size {
		t.Fatal("recovery deleted evidence referenced by a durable ACK")
	}
}
