package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTelemetryPersistenceWaiterPrecedesLaterMetadataWriters(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const active = "metadata-active-run"
	const late = "late-final-drain"
	telemetryCancellationRun(t, s, active)
	run, _ := resultFixture(t, s, late, "completed", time.Now().UTC().Add(-time.Minute))
	s.state.mu.Lock()
	s.state.experiments[late] = run
	s.state.mu.Unlock()
	var entered atomic.Int32
	observed := make(chan int32, 1)
	lookup := s.state.retiredLookup
	s.state.retiredLookup = func(id string) (bool, error) {
		if id == late {
			select {
			case observed <- entered.Load():
			default:
			}
		}
		return lookup(id)
	}
	server := httptest.NewServer(http.HandlerFunc(s.handleEventBatch))
	defer server.Close()
	var writers sync.WaitGroup
	write := func() {
		defer writers.Done()
		s.updateExperiment(active, func(run *model.Experiment) { run.Phase++; entered.Add(1) })
	}
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		if locked {
			s.state.persistMu.Unlock()
		}
		writers.Wait()
	}()
	first := make(chan struct{})
	writers.Add(1)
	go func() { close(first); write() }()
	<-first
	time.Sleep(5 * time.Millisecond)
	s.state.persistMu.Unlock()
	s.state.persistMu.Lock()
	runtime.Gosched()
	event := journalTestEvent(late, 1)
	event.EventID = "late-tail-identity"
	event.Type = "measurement_terminated"
	data, _ := json.Marshal(model.EventBatch{AgentID: "late-agent", Events: []model.TraceEvent{event}})
	response := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(data))
		reply, err := http.DefaultClient.Do(request)
		if reply != nil {
			io.Copy(io.Discard, reply.Body)
			reply.Body.Close()
			if reply.StatusCode != 204 {
				err = fmt.Errorf("late telemetry status %d", reply.StatusCode)
			}
		}
		response <- err
	}()
	stack := make([]byte, 2<<20)
	telemetryCancellationWait(t, "actual late HTTP batch did not wait for persistence", func() bool {
		if len(s.telemetrySlots) < 1 {
			return false
		}
		n := runtime.Stack(stack, true)
		return bytes.Contains(stack[:n], []byte("(*state).appendJournalEventsContext"))
	})
	const later = 128
	started := make(chan struct{}, later)
	for range later {
		writers.Add(1)
		go func() { started <- struct{}{}; write() }()
	}
	for range later {
		<-started
	}
	runtime.Gosched()
	began := time.Now()
	s.state.persistMu.Unlock()
	locked = false
	err := <-response
	writers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	prior := <-observed
	events := readJournalEvents(t, s.config.DataDir, late)
	if len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("late final evidence changed: %+v", events)
	}
	t.Logf("late commit after %d/%d metadata writers, elapsed=%s, all writers=%d", prior, later+1, time.Since(began), entered.Load())
	if prior > 1 {
		t.Fatalf("earlier late-telemetry persistence waiter was passed by %d later actual metadata commits", prior-1)
	}
}

func TestSteadyMetadataWritesAllowEarlierFinalTelemetry(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const active = "steady-metadata-active"
	const late = "steady-late-tail"
	telemetryCancellationRun(t, s, active)
	run, _ := resultFixture(t, s, late, "completed", time.Now().UTC().Add(-time.Minute))
	s.state.mu.Lock()
	s.state.experiments[late] = run
	s.state.mu.Unlock()
	var entered atomic.Int32
	var released atomic.Int64
	var stopping atomic.Bool
	type progress struct {
		count   int32
		latency time.Duration
		active  bool
	}
	observed := make(chan progress, 2)
	lookup := s.state.retiredLookup
	s.state.retiredLookup = func(id string) (bool, error) {
		if id == late {
			observed <- progress{entered.Load(), time.Duration(time.Now().UnixNano() - released.Load()), !stopping.Load()}
		}
		return lookup(id)
	}
	server := httptest.NewServer(http.HandlerFunc(s.handleEventBatch))
	defer server.Close()
	var writers sync.WaitGroup
	write := func() { s.updateExperiment(active, func(run *model.Experiment) { run.Phase++; entered.Add(1) }) }
	stop := make(chan struct{})
	var stopOnce sync.Once
	finish := func() { stopOnce.Do(func() { stopping.Store(true); close(stop) }) }
	s.state.persistMu.Lock()
	locked := true
	defer func() {
		finish()
		if locked {
			s.state.persistMu.Unlock()
		}
		writers.Wait()
	}()
	first := make(chan struct{})
	writers.Add(1)
	go func() { defer writers.Done(); close(first); write() }()
	<-first
	time.Sleep(5 * time.Millisecond)
	s.state.persistMu.Unlock()
	s.state.persistMu.Lock()
	runtime.Gosched()
	results := make(chan error, 2)
	for i := range 2 {
		event := journalTestEvent(late, uint64(i+1))
		event.EventID = fmt.Sprintf("late-steady-%d", i)
		event.Type = "measurement_terminated"
		data, _ := json.Marshal(model.EventBatch{AgentID: "late-agent", Events: []model.TraceEvent{event}})
		go func(data []byte) {
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(data))
			reply, err := http.DefaultClient.Do(request)
			if reply != nil {
				io.Copy(io.Discard, reply.Body)
				reply.Body.Close()
				if reply.StatusCode != 204 {
					err = fmt.Errorf("late tail status=%d", reply.StatusCode)
				}
			}
			results <- err
		}(data)
	}
	// On a single P, polling every goroutine while the clients are still
	// connecting can starve the HTTP handlers that the test is waiting for.
	stack := make([]byte, 2<<20)
	telemetryCancellationWait(t, "actual two late HTTP batches did not wait for persistence", func() bool {
		if len(s.telemetrySlots) < 2 {
			return false
		}
		n := runtime.Stack(stack, true)
		return bytes.Count(stack[:n], []byte("(*state).appendJournalEventsContext")) >= 2
	})
	const concurrent = 32
	started := make(chan struct{}, concurrent)
	for range concurrent {
		writers.Add(1)
		go func() {
			defer writers.Done()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				default:
					write()
				}
			}
		}()
	}
	for range concurrent {
		<-started
	}
	runtime.Gosched()
	released.Store(time.Now().UnixNano())
	s.state.persistMu.Unlock()
	locked = false
	timer := time.AfterFunc(2*time.Second, finish)
	defer timer.Stop()
	time.Sleep(100 * time.Millisecond)
	slotsAtProbe := len(s.telemetrySlots)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewBufferString(`{"events":[]}`))
	probe, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	probeCode := probe.StatusCode
	probe.Body.Close()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	finish()
	writers.Wait()
	var commits []progress
	for range 2 {
		commits = append(commits, <-observed)
	}
	events := readJournalEvents(t, s.config.DataDir, late)
	if len(events) != 2 {
		t.Fatalf("stress lost or duplicated final telemetry: %d", len(events))
	}
	t.Logf("probe status=%d slots_at_100ms=%d durable_metadata_writes=%d commits=%+v", probeCode, slotsAtProbe, entered.Load(), commits)
	if probeCode != 204 {
		t.Fatalf("continuous actual metadata writers starved earlier final telemetry and forced admission status=%d", probeCode)
	}
}
