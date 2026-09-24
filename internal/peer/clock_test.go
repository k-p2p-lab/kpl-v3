package peer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestEstimateControllerClockRemovesWallClockOffset(t *testing.T) {
	const offset = 3 * time.Second
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "time": time.Now().UTC().Add(offset)})
	}))
	defer server.Close()
	estimate, err := estimateControllerClock(context.Background(), server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if delta := estimate.offset - offset; delta < -20*time.Millisecond || delta > 20*time.Millisecond {
		t.Fatalf("estimated offset %s, want %s within 20ms", estimate.offset, offset)
	}
	if estimate.uncertainty < 0 || estimate.uncertainty > 20*time.Millisecond {
		t.Fatalf("unexpected minimum-RTT uncertainty %s", estimate.uncertainty)
	}
}

func TestEstimateControllerClockRequiresAValidHealthResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if _, err := estimateControllerClockWithRetry(context.Background(), server.URL, server.Client(), time.Second, time.Millisecond); err == nil {
		t.Fatal("invalid Controller time response was accepted")
	}
}

func TestEstimateControllerClockRetriesTransientHealthFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) <= 2 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "time": time.Now().UTC()})
	}))
	defer server.Close()
	started := time.Now()
	if _, err := estimateControllerClockWithRetry(context.Background(), server.URL, server.Client(), time.Second, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != controllerClockProbes || time.Since(started) < 15*time.Millisecond {
		t.Fatalf("transient failure was not retried with spacing: attempts=%d elapsed=%s", attempts.Load(), time.Since(started))
	}
}

func TestEstimateControllerClockRetryHonorsCancellation(t *testing.T) {
	first := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(first) })
		http.Error(w, "starting", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := estimateControllerClockWithRetry(ctx, server.URL, server.Client(), 5*time.Second, time.Second)
		done <- err
	}()
	select {
	case <-first:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("clock synchronization did not issue its first probe")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled synchronization returned %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("clock retry did not stop on cancellation")
	}
}

func TestSynchronizedTelemetryUsesAndReportsControllerClock(t *testing.T) {
	tel := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 1)}
	tel.acceptClockEstimate(controllerClockEstimate{offset: 2 * time.Second, uncertainty: 4 * time.Millisecond}, time.Now())
	before := time.Now().UTC().Add(2 * time.Second)
	tel.emit(model.TraceEvent{Type: "deliver"})
	after := time.Now().UTC().Add(2 * time.Second)
	event := <-tel.events
	if event.Timestamp.Before(before) || event.Timestamp.After(after) {
		t.Fatalf("event timestamp %s is outside synchronized interval [%s, %s]", event.Timestamp, before, after)
	}
	if event.Fields["clockBasis"] != controllerClockBasis || event.Fields["clockOffsetMs"] != float64(2000) || event.Fields["clockUncertaintyMs"] != float64(4) {
		t.Fatalf("clock provenance missing from event: %+v", event.Fields)
	}
}

func TestClockSnapshotIsAtomicAndExpiresAtAgeBoundary(t *testing.T) {
	tel := &telemetry{}
	base := time.Now()
	first := controllerClockEstimate{offset: time.Hour, uncertainty: 11 * time.Millisecond}
	second := controllerClockEstimate{offset: 2 * time.Hour, uncertainty: 22 * time.Millisecond}
	tel.acceptClockEstimate(first, base)

	for _, test := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"before", base.Add(-time.Nanosecond), false},
		{"inside", base.Add(controllerClockMaxAge - time.Nanosecond), true},
		{"boundary", base.Add(controllerClockMaxAge), true},
		{"expired", base.Add(controllerClockMaxAge + time.Nanosecond), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			offset, uncertainty, synchronized := tel.clockSnapshotAt(test.at)
			if offset != first.offset || uncertainty != first.uncertainty || synchronized != test.want {
				t.Fatalf("snapshot=(%s,%s,%t), want=(%s,%s,%t)", offset, uncertainty, synchronized, first.offset, first.uncertainty, test.want)
			}
		})
	}

	var invalid atomic.Bool
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; i < 10000; i++ {
			tel.acceptClockEstimate(first, base)
			tel.acceptClockEstimate(second, base)
		}
	}()
	for i := 0; i < 10000; i++ {
		offset, uncertainty, synchronized := tel.clockSnapshotAt(base.Add(time.Second))
		if !synchronized || offset == first.offset && uncertainty != first.uncertainty || offset == second.offset && uncertainty != second.uncertainty || offset != first.offset && offset != second.offset {
			invalid.Store(true)
			break
		}
	}
	writers.Wait()
	if invalid.Load() {
		t.Fatal("clock reader observed values from different refresh generations")
	}
}

func TestStaleClockOmitsTrustMetadata(t *testing.T) {
	tel := &telemetry{node: model.Node{ID: "node", RunID: "run"}, events: make(chan model.TraceEvent, 2)}
	tel.acceptClockEstimate(controllerClockEstimate{offset: 2 * time.Second, uncertainty: 4 * time.Millisecond}, time.Now().Add(-controllerClockMaxAge-time.Second))
	tel.emit(model.TraceEvent{Type: "deliver"})
	event := <-tel.events
	if event.Fields != nil {
		if _, exists := event.Fields["clockBasis"]; exists {
			t.Fatalf("stale measurement retained synchronized provenance: %+v", event.Fields)
		}
	}

	server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run"}}, telemetry: tel}
	stale := tel.clockReading()
	message, err := server.preparePublicationWithClock(model.PublishRequest{PayloadSize: 1}, stale)
	if err != nil {
		t.Fatal(err)
	}
	var payload envelope
	if err := json.Unmarshal(message.wire, &payload); err != nil {
		t.Fatal(err)
	}
	if stale.synchronized || payload.ClockBasis != "" || payload.ClockUncertaintyNS != 0 {
		t.Fatalf("stale clock was asserted in envelope: reading=%+v payload=%+v", stale, payload)
	}
}

func TestPublicationTimestampAndUncertaintyUseOneClockSnapshot(t *testing.T) {
	base := time.Now()
	tel := &telemetry{}
	tel.acceptClockEstimate(controllerClockEstimate{offset: time.Hour, uncertainty: 11 * time.Millisecond}, base)
	reading := tel.clockReadingAt(base.Add(time.Second))
	tel.acceptClockEstimate(controllerClockEstimate{offset: 2 * time.Hour, uncertainty: 22 * time.Millisecond}, base.Add(time.Second))
	server := &Server{config: model.PeerProcessConfig{Node: model.Node{ID: "node", RunID: "run"}}, telemetry: tel}
	message, err := server.preparePublicationWithClock(model.PublishRequest{PayloadSize: 1}, reading)
	if err != nil {
		t.Fatal(err)
	}
	var payload envelope
	if err := json.Unmarshal(message.wire, &payload); err != nil {
		t.Fatal(err)
	}
	if message.sentAt != reading.timestamp || payload.SentAt != reading.timestamp.UnixNano() || payload.ClockUncertaintyNS != int64(11*time.Millisecond) {
		t.Fatalf("publication mixed clock snapshots: reading=%+v message=%+v payload=%+v", reading, message, payload)
	}
}

func TestClockSyncLoopUsesStateSpecificIntervalsAndStops(t *testing.T) {
	tel := newTelemetry(model.Node{}, "", "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	calls := make(chan time.Time, 3)
	var attempt atomic.Int32
	synchronize := func(context.Context, string) error {
		n := attempt.Add(1)
		calls <- time.Now()
		switch n {
		case 1:
			tel.acceptClockEstimate(controllerClockEstimate{offset: time.Second, uncertainty: time.Millisecond}, time.Now())
		case 2:
			return errors.New("temporary failure")
		case 3:
			tel.acceptClockEstimate(controllerClockEstimate{offset: -2 * time.Second, uncertainty: 2 * time.Millisecond}, time.Now())
		}
		return nil
	}
	go func() {
		defer close(done)
		tel.clockSyncLoopWithIntervals(ctx, "controller", 5*time.Millisecond, 25*time.Millisecond, synchronize)
	}()
	nextCall := func() time.Time {
		t.Helper()
		select {
		case at := <-calls:
			return at
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatal("clock maintenance did not run on schedule")
			return time.Time{}
		}
	}
	first, second, third := nextCall(), nextCall(), nextCall()
	if second.Sub(first) < 15*time.Millisecond || third.Sub(second) < 15*time.Millisecond {
		cancel()
		<-done
		t.Fatalf("synchronized refresh used the unsynchronized cadence: %s, %s", second.Sub(first), third.Sub(second))
	}
	offset, uncertainty, synchronized := tel.clockSnapshot()
	if !synchronized || offset != -2*time.Second || uncertainty != 2*time.Millisecond {
		cancel()
		<-done
		t.Fatalf("periodic refresh did not publish the new sample: %s %s %t", offset, uncertainty, synchronized)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("clock synchronization loop leaked after cancellation")
	}
}

func TestServerRunContinuesWithoutInitialClockSync(t *testing.T) {
	ready := make(chan struct{})
	var readyOnce sync.Once
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/health":
			http.Error(w, "starting", http.StatusServiceUnavailable)
		case "/api/v1/bootstrap":
			_, _ = io.WriteString(w, "[]")
		case "/api/v1/nodes/node/status":
			readyOnce.Do(func() { close(ready) })
			w.WriteHeader(http.StatusAccepted)
		case "/api/v1/telemetry":
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer endpoint.Close()
	port, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	apiAddress := port.Addr().String()
	_ = port.Close()
	disabled := false
	config := model.PeerProcessConfig{
		Node: model.Node{ID: "node", RunID: "run", Role: "boot"}, AgentURL: endpoint.URL, ControllerURL: endpoint.URL,
		APListen:   apiAddress,
		NodeConfig: model.NodeConfig{Kademlia: model.KademliaConfig{Enabled: &disabled}, GossipSub: model.GossipSubConfig{Enabled: &disabled}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := newRunTestServer(t, config)
	defer server.Close()
	server.telemetry.shutdownTimeout = time.Second
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	select {
	case <-ready:
	case <-time.After(4 * time.Second):
		cancel()
		t.Fatal("peer did not reach Ready after clock fallback")
	}
	if _, _, synchronized := server.telemetry.clockSnapshot(); synchronized {
		cancel()
		t.Fatal("failed initial synchronization was marked synchronized")
	}
	// The status handler signals when it receives the request; allow the client
	// to consume the response before cancellation reaches that request context.
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful clock fallback returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("peer did not stop after clock fallback")
	}
}
