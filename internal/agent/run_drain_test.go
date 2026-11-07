package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRunDrainPendingResponsePreservesUnacknowledgedEvidence(t *testing.T) {
	for _, durable := range []bool{false, true} {
		name := "memory"
		if durable {
			name = "disk"
		}
		t.Run(name, func(t *testing.T) {
			s := &Server{config: Config{ControllerURL: "http://controller"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			directory := t.TempDir()
			if durable {
				var err error
				s.spool, _, err = openTelemetrySpool(directory)
				if err != nil {
					t.Fatal(err)
				}
			}
			fail := true
			s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				w := httptest.NewRecorder()
				if fail {
					w.WriteHeader(http.StatusServiceUnavailable)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
				return w.Result(), nil
			})}
			if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", EventID: "pending"}}}) {
				t.Fatal("enqueue")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r := httptest.NewRequest(http.MethodPost, "/api/v1/runs/run/drain?timeout=1ms", nil).WithContext(ctx)
			w := httptest.NewRecorder()
			s.handleRunAction(w, r)
			var pending model.RunDrainError
			if w.Code != 503 || json.Unmarshal(w.Body.Bytes(), &pending) != nil || pending.Code != model.RunTelemetryPendingCode || pending.RemainingEvents != 1 || w.Header().Get("Retry-After") != "1" {
				t.Fatalf("pending response: %d %s", w.Code, w.Body)
			}
			if !strings.Contains(pending.Error, "run logs not fully delivered (1 remaining): context deadline exceeded") {
				t.Fatalf("legacy error changed: %s", pending.Error)
			}
			if durable {
				_, retained, err := openTelemetrySpool(directory)
				if err != nil || len(retained) != 1 || retained[0].EventID != "pending" {
					t.Fatalf("failed request lost durable event: %+v %v", retained, err)
				}
			}
			fail = false
			w = httptest.NewRecorder()
			s.handleRunAction(w, r)
			if w.Code != 204 {
				t.Fatalf("retry did not finish: %d %s", w.Code, w.Body)
			}
		})
	}
}

func TestRunDrainRejectsInvalidTimeoutAndMethod(t *testing.T) {
	s := &Server{}
	for _, value := range []string{"0", "-1s", "bad", "2m1ns"} {
		w := httptest.NewRecorder()
		s.handleRunAction(w, httptest.NewRequest(http.MethodPost, "/api/v1/runs/run/drain?timeout="+value, nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("accepted timeout %q: %d", value, w.Code)
		}
	}
	w := httptest.NewRecorder()
	s.handleRunAction(w, httptest.NewRequest(http.MethodGet, "/api/v1/runs/run/drain", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatal(w.Code)
	}
	for _, query := range []string{"", "?timeout=2m"} {
		w = httptest.NewRecorder()
		s.handleRunAction(w, httptest.NewRequest(http.MethodPost, "/api/v1/runs/run/drain"+query, nil))
		if w.Code != 204 {
			t.Fatal(w.Code)
		}
	}
}

func TestTelemetryDeliveryHasSeparateTimeoutAndRespectsParent(t *testing.T) {
	for _, durable := range []bool{false, true} {
		s := &Server{config: Config{ControllerURL: "http://controller"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		if durable {
			var err error
			s.spool, _, err = openTelemetrySpool(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
		}
		var deadlines []time.Duration
		s.client = &http.Client{Timeout: 10 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			deadline, ok := r.Context().Deadline()
			if !ok {
				t.Error("unbounded delivery")
			}
			deadlines = append(deadlines, time.Until(deadline))
			return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader(""))}, nil
		})}
		for _, short := range []bool{false, true} {
			ctx := context.Background()
			if short {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", EventID: "event"}}}) {
				t.Fatal("enqueue")
			}
			s.flushEvents(ctx)
		}
		if len(deadlines) != 2 || deadlines[0] < 25*time.Second || deadlines[0] > 30*time.Second || deadlines[1] > time.Second || s.client.Timeout != 10*time.Second {
			t.Fatalf("delivery/parent/control deadlines changed: %v, client=%s", deadlines, s.client.Timeout)
		}
	}
}

func TestRunDrainKeepsStorageFailureDistinctFromPending(t *testing.T) {
	want := errors.New("spool sync failed")
	s := &Server{spoolError: want}
	if err := s.drainRunEvents(context.Background(), "run"); !errors.Is(err, want) {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.handleRunAction(w, httptest.NewRequest(http.MethodPost, "/api/v1/runs/run/drain", nil))
	if w.Code != 503 || strings.Contains(w.Body.String(), model.RunTelemetryPendingCode) {
		t.Fatalf("storage failure treated as pending: %d %s", w.Code, w.Body)
	}
}

func TestRunDrainCompletesAfterSlowControllerAcknowledgment(t *testing.T) {
	for _, durable := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			directory := t.TempDir()
			s := &Server{config: Config{ControllerURL: "http://controller"}, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if durable {
				var err error
				s.spool, _, err = openTelemetrySpool(directory)
				if err != nil {
					t.Fatal(err)
				}
			}
			attempts := 0
			s.client = &http.Client{Timeout: 10 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				var batch model.EventBatch
				if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch.Events) != 1 || batch.Events[0].EventID != "slow-commit" {
					t.Errorf("evidence changed: %+v %v", batch, err)
				}
				select {
				case <-time.After(6 * time.Second):
					return &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader(""))}, nil
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			})}
			if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", EventID: "slow-commit"}}}) {
				t.Fatal("enqueue")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			if err := s.drainRunEvents(ctx, "run"); err != nil || attempts != 1 {
				t.Fatalf("slow acknowledged delivery failed/replayed: attempts=%d err=%v", attempts, err)
			}
			if durable {
				_, events, err := openTelemetrySpool(directory)
				if err != nil || len(events) != 0 {
					t.Fatalf("confirmed delivery replayed after restart: %+v %v", events, err)
				}
			}
		})
	}
}
