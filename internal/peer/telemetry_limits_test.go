package peer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetryByteBoundedDrainRetriesOnlyUnacknowledgedPrefix(t *testing.T) {
	var attempts atomic.Int32
	received := make(chan []string, 8)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 10<<20))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		var batch model.EventBatch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		ids := []string{}
		for _, event := range batch.Events {
			ids = append(ids, event.EventID)
		}
		received <- ids
		if attempt == 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer endpoint.Close()
	tel := newTelemetry(model.Node{ID: "node"}, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	tel.shutdownTimeout, tel.retryInterval = 10*time.Second, time.Millisecond
	for _, id := range []string{"first", "second"} {
		tel.emit(model.TraceEvent{EventID: id, Fields: map[string]any{"padding": strings.Repeat("x", 6<<20)}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tel.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 3 {
		t.Fatalf("requests=%d decoded=%d", attempts.Load(), len(received))
	}
	for _, want := range [][]string{{"first"}, {"second"}, {"second"}} {
		if got := <-received; !reflect.DeepEqual(got, want) {
			t.Fatalf("retry order=%v want %v", got, want)
		}
	}
}

func TestPublishRejectsTrailingJSONBeforePublishing(t *testing.T) {
	server, ctx := publicationTestServer(t)
	for _, suffix := range []string{` {}`, ` garbage`, strings.Repeat(" ", 1<<20)} {
		response := httptest.NewRecorder()
		server.handlePublish(response, httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{"topic":"topic-a","payloadSize":32}`+suffix)).WithContext(ctx))
		if response.Code != 400 {
			t.Errorf("invalid body accepted: status=%d", response.Code)
		}
	}
	for len(server.telemetry.events) > 0 {
		if event := <-server.telemetry.events; event.Type == "publish" {
			t.Fatal("invalid request published a message")
		}
	}
}

func TestTelemetryDrainReportsUnencodableEventsAndContinues(t *testing.T) {
	received := make(chan model.TraceEvent, 8)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.EventBatch
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 10<<20)).Decode(&batch); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		for _, event := range batch.Events {
			received <- event
		}
		w.WriteHeader(204)
	}))
	defer endpoint.Close()
	tel := newTelemetry(model.Node{ID: "node", AgentID: "agent"}, endpoint.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	tel.shutdownTimeout, tel.retryInterval = 2*time.Second, time.Millisecond
	tel.emit(model.TraceEvent{EventID: "first"})
	tel.emit(model.TraceEvent{EventID: "oversize", Fields: map[string]any{"padding": strings.Repeat("x", 10<<20)}})
	tel.emit(model.TraceEvent{EventID: "invalid", Fields: map[string]any{"unsupported": make(chan int)}})
	tel.emit(model.TraceEvent{EventID: "last"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tel.run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(received) != 3 {
		t.Fatalf("forwarded events=%d want two valid events and a loss report", len(received))
	}
	first, last, drop := <-received, <-received, <-received
	if first.EventID != "first" || last.EventID != "last" || first.Sequence != 1 || last.Sequence != 4 || drop.Type != "telemetry_drop" || drop.Fields["count"] != float64(2) {
		t.Fatalf("invalid event recovery lost source order or loss accounting: %+v %+v %+v", first, last, drop)
	}
	if first.AgentID != "agent" || last.AgentID != "agent" {
		t.Fatal("source omitted known Agent identity before sizing its batch")
	}
}
