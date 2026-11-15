package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type telemetryCommitBodyProbe struct {
	io.ReadCloser
	remaining int64
	decoded   chan<- struct{}
	once      sync.Once
}

func (p *telemetryCommitBodyProbe) Read(b []byte) (int, error) {
	n, err := p.ReadCloser.Read(b)
	p.remaining -= int64(n)
	if p.remaining <= 0 {
		p.once.Do(func() { p.decoded <- struct{}{} })
	}
	return n, err
}

type telemetryCommitResponse struct {
	http.ResponseWriter
	status int
}

func (w *telemetryCommitResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func telemetryCancellationRun(t *testing.T, s *Server, id string) model.Experiment {
	t.Helper()
	run, _ := resultFixture(t, s, id, "running", time.Now().UTC())
	run.DataState = "collecting"
	s.state.mu.Lock()
	s.state.experiments[id] = run
	s.state.mu.Unlock()
	return run
}

func telemetryCancellationWait(t *testing.T, message string, ready func() bool) {
	t.Helper()
	limit := time.NewTimer(2 * time.Second)
	defer limit.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-limit.C:
			t.Fatal(message)
		case <-tick.C:
		}
	}
}

// Only requests that have not acquired persistence ownership may stop on their
// HTTP context. A timed-out sender retains the same IDs and can retry without
// consuming both decoded-request slots or marking valid saved data incomplete.
func TestTelemetryCanceledCommitWaitReleasesAdmissionWithoutChangingRun(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "canceled-telemetry-admission"
	before := telemetryCancellationRun(t, s, id)
	revision := s.state.resultsRevision.Load()
	watcher, unsubscribe := s.state.subscribe()
	defer unsubscribe()
	source, err := os.OpenRoot(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	beforeSource, err := sourceRevisionAt(source, id)
	if err != nil {
		t.Fatal(err)
	}
	decoded := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	finished := make(chan int, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Queued") == "" {
			s.handleEventBatch(w, r)
			return
		}
		r.Body = &telemetryCommitBodyProbe{ReadCloser: r.Body, remaining: r.ContentLength, decoded: decoded}
		context.AfterFunc(r.Context(), func() { canceled <- struct{}{} })
		reply := &telemetryCommitResponse{ResponseWriter: w}
		defer func() { finished <- reply.status }()
		s.handleEventBatch(reply, r)
	}))
	s.state.persistMu.Lock()
	var unlock sync.Once
	defer func() { unlock.Do(s.state.persistMu.Unlock); server.Close() }()
	payloads := make([][]byte, 2)
	clients := make(chan error, 2)
	for i := range 2 {
		event := journalTestEvent(id, uint64(i+1))
		event.EventID = fmt.Sprintf("retained-%d", i)
		payloads[i], _ = json.Marshal(model.EventBatch{AgentID: "sender", Events: []model.TraceEvent{event}})
		go func(data []byte) {
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(data))
			req.Header.Set("X-Test-Queued", "yes")
			resp, err := http.DefaultClient.Do(req)
			if resp != nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			clients <- err
		}(payloads[i])
	}
	for range 2 {
		select {
		case <-decoded:
		case <-time.After(2 * time.Second):
			t.Fatal("valid request was not decoded inside admission")
		}
	}
	for range 2 {
		if err := <-clients; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("client did not retain its unacknowledged batch: %v", err)
		}
	}
	for range 2 {
		select {
		case <-canceled:
		case <-time.After(2 * time.Second):
			t.Fatal("server did not observe HTTP cancellation")
		}
	}
	for range 2 {
		select {
		case status := <-finished:
			if status == http.StatusNoContent {
				t.Fatal("canceled pre-commit batch was acknowledged")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled persistence waiter retained a decoder slot")
		}
	}
	if len(s.telemetrySlots) != 0 || s.telemetryWaiters.Load() != 0 {
		t.Fatal("canceled persistence waiter retained admission")
	}
	s.state.mu.RLock()
	after := s.state.experiments[id]
	changed := len(s.state.summaryDirty) != 0 || len(s.state.failedRunWrites) != 0 || len(s.state.events) != 0
	s.state.mu.RUnlock()
	if !reflect.DeepEqual(before, after) || changed || s.state.resultsRevision.Load() != revision {
		t.Fatalf("uncommitted cancellation changed valid scientific state: before=%+v after=%+v dirty=%v", before, after, changed)
	}
	select {
	case <-watcher:
		t.Fatal("uncommitted cancellation notified saved-state watchers")
	default:
	}
	afterSource, err := sourceRevisionAt(source, id)
	if err != nil || afterSource != beforeSource {
		t.Fatalf("uncommitted cancellation changed source revision: %s -> %s (%v)", beforeSource, afterSource, err)
	}
	for _, name := range []string{"events.jsonl", journalIndexFile, journalPendingFile} {
		if _, err := source.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pre-commit cancellation created %s: %v", name, err)
		}
	}
	// This valid empty batch needs no disk ownership and proves that admission
	// is available before the unrelated persistence owner releases its lock.
	response, err := http.Post(server.URL, "application/json", strings.NewReader(`{"events":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("available decoder rejected a valid request: %d", response.StatusCode)
	}
	unlock.Do(s.state.persistMu.Unlock)
	var retries sync.WaitGroup
	retryErrors := make(chan error, 4)
	for range 2 {
		for _, data := range payloads {
			retries.Add(1)
			go func(data []byte) {
				defer retries.Done()
				response, err := http.Post(server.URL, "application/json", bytes.NewReader(data))
				if err == nil {
					io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if response.StatusCode != http.StatusNoContent {
						err = fmt.Errorf("retry status %d", response.StatusCode)
					}
				}
				retryErrors <- err
			}(data)
		}
	}
	retries.Wait()
	close(retryErrors)
	for err := range retryErrors {
		if err != nil {
			t.Fatal(err)
		}
	}
	events := readJournalEvents(t, s.config.DataDir, id)
	if len(events) != 2 {
		t.Fatalf("concurrent replay lost or duplicated identities: %+v", events)
	}
	identities := map[string]bool{}
	for _, event := range events {
		identities[event.EventID] = true
	}
	if !identities["retained-0"] || !identities["retained-1"] {
		t.Fatalf("replay changed event identities: %+v", events)
	}
}

// A real bbolt file lock pauses the already-owned commit. HTTP cancellation
// must neither abandon that durable operation nor replace a real disk failure
// with the request's cancellation error.
func TestTelemetryCancellationAfterPersistenceOwnershipPreservesCommit(t *testing.T) {
	for _, diskFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit-completes", true: "real-io-error"}[diskFailure], func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
			const id = "owned-telemetry-commit"
			before := telemetryCancellationRun(t, s, id)
			root, db, err := openJournalIndex(filepath.Join(s.config.DataDir, currentRunsDirectory, id))
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			defer db.Close()
			if err := initializeEmptyJournalIndex(root, db, id); err != nil {
				t.Fatal(err)
			}
			decoded := make(chan struct{}, 1)
			canceled := make(chan struct{}, 1)
			finished := make(chan int, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Test-Owned") == "" {
					s.handleEventBatch(w, r)
					return
				}
				r.Body = &telemetryCommitBodyProbe{ReadCloser: r.Body, remaining: r.ContentLength, decoded: decoded}
				context.AfterFunc(r.Context(), func() { canceled <- struct{}{} })
				reply := &telemetryCommitResponse{ResponseWriter: w}
				defer func() { finished <- reply.status }()
				s.handleEventBatch(reply, r)
			}))
			defer server.Close()
			event := journalTestEvent(id, 1)
			event.EventID = "owned-id"
			event.Type = "measurement_terminated"
			payload, _ := json.Marshal(model.EventBatch{AgentID: "sender", Events: []model.TraceEvent{event}})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := make(chan error, 1)
			go func() {
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(payload))
				request.Header.Set("X-Test-Owned", "yes")
				response, err := http.DefaultClient.Do(request)
				if response != nil {
					io.Copy(io.Discard, response.Body)
					response.Body.Close()
				}
				client <- err
			}()
			select {
			case <-decoded:
			case <-time.After(2 * time.Second):
				t.Fatal("valid body was not read")
			}
			telemetryCancellationWait(t, "handler did not acquire persistence before bbolt wait", func() bool {
				if s.state.persistMu.TryLock() {
					s.state.persistMu.Unlock()
					return false
				}
				return true
			})
			cancel()
			if err := <-client; !errors.Is(err, context.Canceled) {
				t.Fatalf("client did not cancel: %v", err)
			}
			select {
			case <-canceled:
			case <-time.After(2 * time.Second):
				t.Fatal("server context was not canceled")
			}
			select {
			case status := <-finished:
				t.Fatalf("owned commit returned before the real file lock resolved: %d", status)
			default:
			}
			if !diskFailure {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			var status int
			select {
			case status = <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("owned file operation did not finish")
			}
			if diskFailure {
				if status != http.StatusInternalServerError {
					t.Fatalf("real file lock timeout status=%d", status)
				}
			} else if status != http.StatusNoContent {
				t.Fatalf("owned commit canceled after start: %d", status)
			}
			telemetryCancellationWait(t, "completed commit retained admission", func() bool { return len(s.telemetrySlots) == 0 })
			s.state.mu.RLock()
			after := s.state.experiments[id]
			s.state.mu.RUnlock()
			if diskFailure {
				if after.DataState != "incomplete" || !strings.Contains(after.IntegrityError, "timeout") || strings.Contains(after.IntegrityError, "context canceled") {
					t.Fatalf("actual I/O error was hidden by request cancellation: %+v", after)
				}
				if _, err := root.Stat("events.jsonl"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed pre-intent I/O wrote raw events: %v", err)
				}
				return
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("successful owned commit changed scientific integrity: before=%+v after=%+v", before, after)
			}
			committed := readJournalEvents(t, s.config.DataDir, id)
			if len(committed) != 1 || committed[0].Fields["timestampBasis"] != "controller-receipt-upper-bound-v1" {
				t.Fatalf("owned commit omitted its termination timestamp evidence: %+v", committed)
			}
			response, err := http.Post(server.URL, "application/json", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusNoContent {
				t.Fatalf("lost-ACK replay status=%d", response.StatusCode)
			}
			events := readJournalEvents(t, s.config.DataDir, id)
			if len(events) != 1 || events[0].EventID != event.EventID || !reflect.DeepEqual(events, committed) {
				t.Fatalf("lost ACK after owned commit changed identity/count: %+v", events)
			}
		})
	}
}
