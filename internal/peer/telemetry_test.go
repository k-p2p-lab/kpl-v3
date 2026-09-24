package peer

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetryEventIdentitySurvivesQueueAndResend(t *testing.T) {
	received := make(chan model.TraceEvent, 2)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.EventBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for _, event := range batch.Events {
			received <- event
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()
	tel := newTelemetry(model.Node{ID: "node", RunID: "run", Group: "receivers"}, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	input := model.TraceEvent{Type: "duplicate", MessageID: "message", Timestamp: time.Now().UTC()}
	tel.emit(input)
	first := <-tel.events
	if first.Group != "receivers" || first.EventID == "" || first.NodeID != "node" || first.RunID != "run" || first.SessionID == "" || first.Sequence != 1 {
		t.Fatalf("source event has no identity: %+v", first)
	}
	tel.flush(context.Background(), []model.TraceEvent{first})
	tel.flush(context.Background(), []model.TraceEvent{first})
	if firstWire, retryWire := <-received, <-received; !reflect.DeepEqual(firstWire, retryWire) || firstWire.EventID != first.EventID {
		t.Fatalf("resend changed event identity: %+v %+v", firstWire, retryWire)
	}
	tel.emit(input)
	if next := <-tel.events; next.EventID == first.EventID || next.SessionID != first.SessionID || next.Sequence != 2 {
		t.Fatal("two source observations were given the same event ID")
	}
	tel.dropped.Store(7)
	tel.mu.Lock()
	tel.reportDropsLocked(false)
	tel.mu.Unlock()
	drop := <-tel.events
	if drop.EventID == "" || drop.EventID == first.EventID || drop.Timestamp.IsZero() || drop.RunID != "run" || drop.NodeID != "node" {
		t.Fatalf("drop report lacks source identity: %+v", drop)
	}
}

func TestTelemetryRetriesOrderedBatchAndDrainsQueueOnShutdown(t *testing.T) {
	var attempts atomic.Int32
	var mu sync.Mutex
	var bodies [][]byte
	var accepted []model.TraceEvent
	firstAttempt := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var batch model.EventBatch
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		mu.Unlock()
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			close(firstAttempt)
			return
		}
		mu.Lock()
		accepted = append(accepted, batch.Events...)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer endpoint.Close()
	tel := newTelemetry(model.Node{ID: "node", RunID: "run"}, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	tel.retryInterval = time.Millisecond
	tel.shutdownTimeout = 2 * time.Second
	for i := 0; i < 310; i++ {
		tel.emit(model.TraceEvent{Type: "deliver"})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- tel.run(ctx) }()
	select {
	case <-firstAttempt:
	case <-time.After(3 * time.Second):
		t.Fatal("no first batch")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) < 3 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatalf("failed batch changed on retry (%d attempts)", len(bodies))
	}
	if len(accepted) != 310 {
		t.Fatalf("accepted %d of 310 events", len(accepted))
	}
	for i, event := range accepted {
		if event.Sequence != uint64(i+1) || event.SessionID == "" || event.EventID == "" {
			t.Fatalf("source order/identity lost at %d: %+v", i, event)
		}
	}
}

func TestTelemetryDrainHasFiniteFailureBudget(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer endpoint.Close()
	tel := newTelemetry(model.Node{}, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	tel.shutdownTimeout = 50 * time.Millisecond
	tel.retryInterval = time.Millisecond
	tel.emit(model.TraceEvent{Type: "deliver"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := tel.run(ctx); err == nil {
		t.Fatal("failed final drain was reported as complete")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("drain exceeded budget: %s", elapsed)
	}
}

func TestTelemetryOverflowReservesFinalStopAndMakesSequenceGapVisible(t *testing.T) {
	tel := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 6)}
	tel.startMeasurement([]string{"topic"})
	for i := 0; i < 10; i++ {
		tel.emit(model.TraceEvent{Type: "deliver"})
	}
	tel.stopMeasurement()
	var events []model.TraceEvent
	for len(tel.events) > 0 {
		events = append(events, <-tel.events)
	}
	if len(events) != 5 || events[3].Type != "telemetry_drop" || events[3].Fields["count"] != uint64(8) || events[4].Type != "measurement_stop" {
		t.Fatalf("final control events missing: %+v", events)
	}
	if events[2].Sequence != 3 || events[3].Sequence != 12 || events[4].Sequence != 13 {
		t.Fatalf("lost events do not leave a source sequence gap: %+v", events)
	}
	for i := 0; i < 20; i++ {
		tel.checkpoint()
	}
	if len(tel.events) != 0 {
		t.Fatal("checkpoint emitted after stop")
	}
}

func TestMeasurementCheckpointCannotPassPendingReceipt(t *testing.T) {
	tel := &telemetry{events: make(chan model.TraceEvent, 20)}
	tel.startMeasurement([]string{"topic"})
	builderEntered, releaseBuilder := make(chan struct{}), make(chan struct{})
	observed, checkpointed := make(chan struct{}), make(chan struct{})
	go func() {
		tel.emitObserved(func(reading controllerClockReading) (model.TraceEvent, bool) {
			close(builderEntered)
			<-releaseBuilder
			return model.TraceEvent{Type: "deliver", Timestamp: reading.timestamp}, true
		})
		close(observed)
	}()
	<-builderEntered
	go func() { tel.checkpoint(); close(checkpointed) }()
	close(releaseBuilder)
	<-observed
	<-checkpointed
	tel.stopMeasurement()
	events := make([]model.TraceEvent, 4)
	for i := range events {
		events[i] = <-tel.events
	}
	for i, kind := range []string{"measurement_start", "deliver", "measurement_checkpoint", "measurement_stop"} {
		if events[i].Type != kind || events[i].Sequence != uint64(i+1) {
			t.Fatalf("unexpected sequence: %+v", events)
		}
		if i > 0 && !events[i].Timestamp.After(events[i-1].Timestamp) {
			t.Fatalf("receipt checkpoint timestamps out of order: %+v", events)
		}
	}
}

func TestTelemetryConcurrentEmissionsKeepSourceSequenceInQueueOrder(t *testing.T) {
	tel := &telemetry{events: make(chan model.TraceEvent, 1003)}
	var producers sync.WaitGroup
	for i := 0; i < 10; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				tel.emit(model.TraceEvent{Type: "deliver"})
			}
		}()
	}
	producers.Wait()
	if len(tel.events) != 1000 {
		t.Fatalf("expected 1000 admitted events, got %d", len(tel.events))
	}
	for i := 0; i < 1000; i++ {
		if event := <-tel.events; event.Sequence != uint64(i+1) {
			t.Fatalf("source sequence reordered at %d: %+v", i, event)
		}
	}
}
