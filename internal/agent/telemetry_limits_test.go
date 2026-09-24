package agent

import (
	"bytes"
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

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetrySplitsByEncodedBytesAndRetriesInOrder(t *testing.T) {
	var attempts atomic.Int32
	received := make(chan []string, 4)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
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
			if event.AgentID != "agent" || batch.AgentID != "agent" {
				t.Error("forwarded identity was not normalized")
			}
			ids = append(ids, event.EventID)
		}
		received <- ids
		if attempts.Load() == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer controller.Close()
	s := &Server{config: Config{ID: "agent", ControllerURL: controller.URL}, client: controller.Client(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, id := range []string{"first", "second"} {
		data, err := json.Marshal(model.EventBatch{Events: []model.TraceEvent{{EventID: id, Fields: map[string]any{"padding": strings.Repeat("x", 6<<20)}}}})
		if err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", bytes.NewReader(data)))
		if response.Code != 204 {
			t.Fatalf("valid input: %d %s", response.Code, response.Body.String())
		}
	}
	for range 3 {
		s.flushEvents(context.Background())
	}
	if len(s.events) != 0 || s.eventsInFlight != 0 || len(received) != 3 {
		t.Fatalf("acknowledged events stuck behind oversized batch: queued=%d inFlight=%d requests=%d decoded=%d", len(s.events), s.eventsInFlight, attempts.Load(), len(received))
	}
	for _, want := range [][]string{{"first"}, {"first"}, {"second"}} {
		if got := <-received; !reflect.DeepEqual(got, want) {
			t.Fatalf("retry order=%v want %v", got, want)
		}
	}
}

func TestTelemetryRejectsUnforwardableEventBeforeAcknowledging(t *testing.T) {
	// Incoming JSON fits, but canonical JSON escapes every '<' as six bytes.
	body := `{"events":[{"fields":{"padding":"` + strings.Repeat("<", 2<<20) + `"}}]}`
	s := &Server{config: Config{ID: "agent"}}
	response := httptest.NewRecorder()
	s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", strings.NewReader(body)))
	if response.Code != http.StatusRequestEntityTooLarge || len(s.events) != 0 {
		t.Fatalf("unforwardable event acknowledged: status=%d queued=%d", response.Code, len(s.events))
	}
}

func TestTelemetryRejectsTrailingJSONBeforeAdmission(t *testing.T) {
	for _, suffix := range []string{` {}`, ` garbage`, strings.Repeat(" ", 10<<20)} {
		s := &Server{}
		response := httptest.NewRecorder()
		s.handleTelemetry(response, httptest.NewRequest(http.MethodPost, "/api/v1/telemetry", strings.NewReader(`{"events":[{"eventId":"must-not-admit"}]}`+suffix)))
		if response.Code != 400 || len(s.events) != 0 {
			t.Fatalf("invalid body admitted: status=%d queued=%d", response.Code, len(s.events))
		}
	}
}
