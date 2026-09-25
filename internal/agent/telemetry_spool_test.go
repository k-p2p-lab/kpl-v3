package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetrySpoolRecoversUnacknowledgedSuffixAndTermination(t *testing.T) {
	directory := t.TempDir()
	spool, _, err := openTelemetrySpool(directory)
	if err != nil {
		t.Fatal(err)
	}
	events := []model.TraceEvent{{RunID: "run", EventID: "one"}, {RunID: "run", EventID: "two"}, {RunID: "other", EventID: "three"}}
	if err = spool.append(events); err != nil {
		t.Fatal(err)
	}
	if err = spool.acknowledge(events[:1]); err != nil {
		t.Fatal(err)
	}
	restarted, pending, err := openTelemetrySpool(directory)
	if err != nil || len(pending) != 2 || pending[0].EventID != "two" {
		t.Fatalf("replay: %+v %v", pending, err)
	}
	agent := &Server{spool: restarted, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	agent.queueTermination(model.TraceEvent{RunID: "run", NodeID: "peer", EventID: "termination", Type: "measurement_terminated", Timestamp: time.Now()})
	restored, pending, err := openTelemetrySpool(directory)
	if err != nil || len(pending) != 3 {
		t.Fatalf("termination not durable: %+v %v", pending, err)
	}
	if err = restored.acknowledge(pending); err != nil {
		t.Fatal(err)
	}
	_, pending, err = openTelemetrySpool(directory)
	if err != nil || len(pending) != 0 {
		t.Fatalf("acknowledged events replayed: %+v %v", pending, err)
	}
}
func TestTelemetrySpoolRefusesAcknowledgmentOnDiskFailure(t *testing.T) {
	dir := t.TempDir()
	blocked := filepath.Join(dir, "not-directory")
	if err := os.WriteFile(blocked, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	s := &Server{spool: &telemetrySpool{directory: blocked, refs: map[string][]telemetryRef{}}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{EventID: "must-retry"}}}) {
		t.Fatal("unpersisted events acknowledged")
	}
	if len(s.events) != 0 {
		t.Fatal("failed persistence changed queue")
	}
}
func TestTelemetrySpoolRetriesDeliveryAcrossRestart(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer api.Close()
	dir := t.TempDir()
	spool, _, err := openTelemetrySpool(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{spool: spool, config: Config{ID: "agent", ControllerURL: api.URL}, client: api.Client(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", EventID: "retained"}}}) {
		t.Fatal("enqueue")
	}
	s.flushEvents(context.Background())
	_, pending, err := openTelemetrySpool(dir)
	if err != nil || len(pending) != 1 || pending[0].EventID != "retained" {
		t.Fatalf("failed delivery lost evidence: %+v %v", pending, err)
	}
}
func TestRunDrainWaitsForOnlyMatchingInFlightEvidence(t *testing.T) {
	s := &Server{eventsInFlight: 1, inFlightRuns: map[string]int{"other": 1}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := s.drainRunEvents(context.Background(), "finished"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := s.drainRunEvents(ctx, "other"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("in-flight evidence considered delivered: %v", err)
	}
}
