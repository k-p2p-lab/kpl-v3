package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestLiveWindowIndexMatchesFullIndexWithLateAndMixedEvidence(t *testing.T) {
	events := windowEvents(windowStart("receiver", "r", 0, "topic"), windowDeliveredEvent("receiver", "r", 2, 12), windowEvent("receiver", "r", 5, "measurement_checkpoint", 22))
	duplicate := windowEvent("receiver", "r", 3, "duplicate", 13)
	duplicate.MessageID, duplicate.Topic = "message", "topic"
	events = append(events, duplicate, windowDeliveredEvent("receiver", "r", 4, 11), cohortPublish("legacy", "topic", []string{"b"}), cohortDelivery("legacy", "topic", "b", 5))
	// Include a second publication event for one message and an anonymous one.
	second := windowPublish()
	second.Sequence = 8
	second.EventID = "second-publication"
	anonymous := cohortPublish("", "topic", nil)
	anonymous.EventID = "anonymous"
	events = append(events, second, anonymous)
	for seed := int64(0); seed < 12; seed++ {
		live, full := newLiveRunMetricAccumulator(), newRunMetricAccumulator()
		order := rand.New(rand.NewSource(seed)).Perm(len(events))
		for _, index := range order {
			event := events[index]
			live.observe(event)
			full.observe(event)
			if live.observe(event) || full.observe(event) {
				t.Fatal("retry was counted")
			}
			// Check transitions from legacy to window, unknown coverage and late gaps.
			now := windowTestEpoch.Add(30 * time.Second)
			got, samples := live.summarize("run", now)
			want, wantSamples := full.summarize("run", now)
			if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(samples, wantSamples) {
				t.Fatalf("seed=%d event=%s: compact summary changed evidence", seed, event.Type)
			}
		}
		for _, message := range live.messages {
			if !message.published || message.deliveries != nil || message.duplicates != nil || message.targets != nil {
				t.Fatal("live window retained redundant receiver index")
			}
		}
		if len(full.messages[messageMetricKey{"topic", "message"}].deliveries) == 0 {
			t.Fatal("offline detail was removed")
		}
	}
}

func TestLiveWindowRetentionAcrossThousandReceivers(t *testing.T) {
	live, full := newLiveRunMetricAccumulator(), newRunMetricAccumulator()
	observe := func(e model.TraceEvent) { live.observe(e); full.observe(e) }
	for _, e := range windowEvents() {
		observe(e)
	}
	for i := 0; i < 1000; i++ {
		node := fmt.Sprint(i)
		observe(windowStart(node, node, 0, "topic"))
		observe(windowDeliveredEvent(node, node, 2, 12))
		observe(windowEvent(node, node, 3, "measurement_checkpoint", 22))
	}
	now := windowTestEpoch.Add(time.Minute)
	metrics := live.liveSummary("run", now)
	want, samples := full.summarize("run", now)
	if !reflect.DeepEqual(metrics, want) || metrics.EligibleDeliveries != 1000 {
		t.Fatal("live counts changed")
	}
	if _, histograms := live.livePrometheusSummary("run", now); !reflect.DeepEqual(histograms, propagationHistograms(samples)) {
		t.Fatal("histograms changed")
	}
	if len(live.messages) != 1 || len(live.messages[messageMetricKey{"topic", "message"}].deliveries) != 0 || len(live.window.receipts[messageMetricKey{"topic", "message"}]) != 1000 {
		t.Fatal("receipt retention is incorrect")
	}
}

func TestSlowRunReadersDoNotHoldInventoryOrPersistence(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	a := newLiveRunMetricAccumulator()
	s.state.runMetrics["run"] = a
	s.state.experiments["run"] = model.Experiment{ID: "run", State: "running", StartedAt: time.Now()}
	a.dataMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			a.dataMu.Unlock()
		}
	}()
	var tasks sync.WaitGroup
	tasks.Add(3)
	go func() { defer tasks.Done(); s.state.dashboardSnapshot() }()
	go func() { defer tasks.Done(); _, _ = s.state.metrics.registry.Gather() }()
	go func() {
		defer tasks.Done()
		_ = s.state.appendRunEvents("run", []model.TraceEvent{cohortPublish("message", "topic", []string{"b"})})
	}()
	// Let all three paths reach the held run lock.
	time.Sleep(30 * time.Millisecond)
	control := make(chan struct{})
	go func() {
		s.state.persistMu.Lock()
		s.state.mu.Lock()
		s.state.agents["agent"] = model.Agent{ID: "agent", State: model.AgentOnline}
		s.state.mu.Unlock()
		s.state.persistMu.Unlock()
		close(control)
	}()
	select {
	case <-control:
	case <-time.After(time.Second):
		t.Fatal("metric reader or writer blocked experiment control")
	}
	a.dataMu.Unlock()
	unlocked = true
	tasks.Wait()
}

func TestConcurrentFirstTelemetryBatchesShareDedupIndex(t *testing.T) {
	s := newState(t.TempDir())
	event := cohortPublish("same", "topic", []string{"b"})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.appendRunEvents("run", []model.TraceEvent{event}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := s.runMetrics["run"].liveSummary("run", time.Now()); got.Published != 1 {
		t.Fatalf("published=%d", got.Published)
	}
}

type telemetryReadProbe struct {
	io.Reader
	once  sync.Once
	reads *atomic.Int32
}

func (r *telemetryReadProbe) Read(p []byte) (int, error) {
	r.once.Do(func() { r.reads.Add(1) })
	return r.Reader.Read(p)
}

func TestControllerTelemetryRejectsBeforeDecodingAndRetryCommits(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	if !s.acquireTelemetryDecoder(context.Background()) || !s.acquireTelemetryDecoder(context.Background()) {
		t.Fatal("admission failed")
	}
	s.telemetryWaiters.Store(telemetryAdmissionWaiters)
	batch := model.EventBatch{Events: []model.TraceEvent{cohortPublish("one", "topic", []string{"b"})}}
	data, _ := json.Marshal(batch)
	var reads atomic.Int32
	body := &telemetryReadProbe{Reader: bytes.NewReader(data), reads: &reads}
	response := httptest.NewRecorder()
	s.handleEventBatch(response, httptest.NewRequest("POST", "/api/v1/events/batch", body))
	if response.Code != 503 || response.Header().Get("Retry-After") != "1" || reads.Load() != 0 {
		t.Fatalf("busy request decoded or acknowledged: %d / %d reads", response.Code, reads.Load())
	}
	s.telemetryWaiters.Store(0)
	s.releaseTelemetryDecoder()
	s.releaseTelemetryDecoder()
	for i := 0; i < 2; i++ {
		response = httptest.NewRecorder()
		s.handleEventBatch(response, httptest.NewRequest("POST", "/api/v1/events/batch", bytes.NewReader(data)))
		if response.Code != 204 {
			t.Fatal(response.Body.String())
		}
	}
	if s.state.runMetrics["run"].liveSummary("run", time.Now()).Published != 1 {
		t.Fatal("retry counted twice")
	}
	if len(s.telemetrySlots) != 0 || s.telemetryRejected.Load() != 1 {
		t.Fatal("admission slot leaked")
	}
}

func TestControllerTelemetryWaitingRequestsCancelWithoutReading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		s.acquireTelemetryDecoder(context.Background())
		s.acquireTelemetryDecoder(context.Background())
		defer s.releaseTelemetryDecoder()
		defer s.releaseTelemetryDecoder()
		ctx, cancel := context.WithCancel(context.Background())
		var reads atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				body := &telemetryReadProbe{Reader: strings.NewReader(`{"events":[]}`), reads: &reads}
				response := httptest.NewRecorder()
				s.handleEventBatch(response, httptest.NewRequest("POST", "/api/v1/events/batch", body).WithContext(ctx))
				if response.Code != 503 {
					t.Errorf("canceled request status=%d", response.Code)
				}
			}()
		}
		synctest.Wait()
		if s.telemetryWaiters.Load() != 64 {
			t.Fatal("waiters were not bounded before decode")
		}
		cancel()
		wg.Wait()
		if reads.Load() != 0 || s.telemetryWaiters.Load() != 0 {
			t.Fatal("canceled waiter retained a body or admission slot")
		}
	})
}

func TestSlowSnapshotKeepsStreamAliveAndReleasesCanceledWatchers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(ServerConfig{DataDir: t.TempDir()}, nil)
		// A shared producer is pending; each reconnect must join this one flight.
		flight := &dashboardSnapshotFlight{done: make(chan struct{})}
		s.dashboardFlight = flight
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.handleStream(response, httptest.NewRequest("GET", "/api/v1/stream?view=dashboard", nil).WithContext(ctx))
			}()
			synctest.Wait()
			time.Sleep(16 * time.Second)
			synctest.Wait()
			if !strings.Contains(response.Body.String(), "event: heartbeat") {
				t.Fatal("slow snapshot stopped heartbeat")
			}
			cancel()
			<-done
			if len(s.state.watchers) != 0 || s.dashboardFlight != flight {
				t.Fatal("reconnect leaked watcher or forked producer")
			}
		}
		close(flight.done)
		s.dashboardFlight = nil
	})
}

func TestSnapshotViewsDoNotBlockEachOtherAndWaitIsCancelable(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	s.snapshotFlight = &rawSnapshotFlight{done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := s.dashboardStreamSnapshotContext(ctx, nil); err != nil {
		t.Fatalf("raw snapshot blocked dashboard: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := s.streamSnapshotAtContext(ctx, nil); done <- err }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reader: %v", err)
	}
	close(s.snapshotFlight.done)
}

func TestMetricScrapesAreBounded(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	a := newRunMetricAccumulator()
	s.state.runMetrics["run"] = a
	a.dataMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			a.dataMu.Unlock()
		}
	}()
	handler := s.routes(context.Background())
	var wg sync.WaitGroup
	statuses := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			statuses <- response.Code
		}()
	}
	for i := 0; i < 6; i++ {
		select {
		case status := <-statuses:
			if status != 503 {
				t.Fatalf("unbounded scrape: %d", status)
			}
		case <-time.After(time.Second):
			t.Fatal("excess scrape waited for metrics")
		}
	}
	a.dataMu.Unlock()
	unlocked = true
	wg.Wait()
	for i := 0; i < 2; i++ {
		if status := <-statuses; status != 200 {
			t.Fatalf("admitted scrape: %d", status)
		}
	}
}

func TestTelemetryWithoutRunRemainsAccepted(t *testing.T) {
	s := newState(t.TempDir())
	event := model.TraceEvent{NodeID: "peer", EventID: "unscoped", Type: "add_peer"}
	if err := s.appendEvents(model.EventBatch{Events: []model.TraceEvent{event}}); err != nil {
		t.Fatal(err)
	}
	if len(s.events) != 1 || !s.runMetrics[""].hasEvent(event) {
		t.Fatal("unscoped telemetry lost")
	}
}
