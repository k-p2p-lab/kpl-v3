package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

type telemetry struct {
	node               model.Node
	agentURL           string
	token              string
	client             *http.Client
	logger             *slog.Logger
	events             chan model.TraceEvent
	dropped            atomic.Uint64
	mu                 sync.Mutex // Source sequence, receipt timestamps, and queue admission.
	sessionID          string
	sequence           uint64
	observedAt         time.Time
	measurementStarted bool
	measurementStopped bool
	measurementDone    chan struct{}
	sealed             bool
	shutdownTimeout    time.Duration
	retryInterval      time.Duration
	clock              atomic.Pointer[controllerClockSample]
}

const telemetryShutdownTimeout = 20 * time.Second

func newTelemetry(node model.Node, agentURL, token string, logger *slog.Logger) *telemetry {
	if logger == nil {
		logger = slog.Default()
	}
	return &telemetry{
		node:     node,
		agentURL: strings.TrimRight(agentURL, "/"),
		token:    token,
		client:   &http.Client{Timeout: 5 * time.Second},
		logger:   logger,
		events:   make(chan model.TraceEvent, 10000),
	}
}

func (t *telemetry) emit(event model.TraceEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.sealed {
		t.reportDropsLocked(false)
	}
	t.enqueueLocked(event, false)
}

func (t *telemetry) emitWithClock(event model.TraceEvent, reading controllerClockReading) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.sealed {
		t.reportDropsLocked(false)
	}
	t.enqueueLockedWithClock(event, false, reading)
}

// Receipt timestamps are created inside the same critical section as a
// checkpoint. A checkpoint therefore never certifies a delivery whose earlier
// receipt timestamp has been assigned but whose event is still awaiting enqueue.
func (t *telemetry) emitObserved(build func(controllerClockReading) (model.TraceEvent, bool)) {
	t.emitObservedPriority(build, false)
}

func (t *telemetry) emitObservedPriority(build func(controllerClockReading) (model.TraceEvent, bool), priority bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	reading := t.observationClockLocked()
	if event, ok := build(reading); ok {
		if !t.sealed {
			t.reportDropsLocked(false)
		}
		t.enqueueLockedWithClock(event, priority, reading)
	}
}

func (t *telemetry) observationClockLocked() controllerClockReading {
	reading := t.clockReading()
	if !reading.timestamp.After(t.observedAt) {
		reading.timestamp = t.observedAt.Add(time.Nanosecond)
	}
	t.observedAt = reading.timestamp
	return reading
}

func (t *telemetry) now() time.Time {
	return t.clockReading().timestamp
}

func (t *telemetry) adjustTimestamp(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	reading := t.clockReading()
	if reading.offset == 0 {
		return value.UTC()
	}
	return value.UTC().Add(reading.offset)
}

func (t *telemetry) synchronizeClock(ctx context.Context, controllerURL string) error {
	estimate, err := estimateControllerClock(ctx, controllerURL, t.client)
	if err != nil {
		return err
	}
	t.acceptClockEstimate(estimate, time.Now())
	return nil
}

func (t *telemetry) clockSnapshot() (offset, uncertainty time.Duration, synchronized bool) {
	reading := t.clockReading()
	return reading.offset, reading.uncertainty, reading.synchronized
}

func (t *telemetry) clockSnapshotAt(now time.Time) (offset, uncertainty time.Duration, synchronized bool) {
	reading := t.clockReadingAt(now)
	return reading.offset, reading.uncertainty, reading.synchronized
}

func (t *telemetry) clockReading() controllerClockReading {
	return t.clockReadingAt(time.Now())
}

func (t *telemetry) clockReadingAt(now time.Time) controllerClockReading {
	reading := controllerClockReading{timestamp: now.UTC()}
	sample := t.clock.Load()
	if sample == nil {
		return reading
	}
	reading.offset = sample.offset
	reading.uncertainty = sample.uncertainty
	// Retain the last offset for timestamp continuity, but stop asserting that
	// it is synchronized once its age is outside the bounded validity window.
	reading.timestamp = reading.timestamp.Add(sample.offset)
	age := now.Sub(sample.sampledAt)
	reading.synchronized = age >= 0 && age <= controllerClockMaxAge
	return reading
}

func (t *telemetry) acceptClockEstimate(estimate controllerClockEstimate, sampledAt time.Time) {
	t.clock.Store(&controllerClockSample{offset: estimate.offset, uncertainty: estimate.uncertainty, sampledAt: sampledAt})
}

type clockSynchronizer func(context.Context, string) error

func (t *telemetry) clockSyncLoop(ctx context.Context, controllerURL string) {
	t.clockSyncLoopWithIntervals(ctx, controllerURL, controllerClockUnsyncedInterval, controllerClockResyncInterval, t.synchronizeClock)
}

func (t *telemetry) clockSyncLoopWithIntervals(ctx context.Context, controllerURL string, unsyncedInterval, syncedInterval time.Duration, synchronize clockSynchronizer) {
	for {
		_, _, synchronized := t.clockSnapshot()
		interval := unsyncedInterval
		if synchronized {
			interval = syncedInterval
		}
		if interval <= 0 {
			interval = time.Nanosecond
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := synchronize(ctx, controllerURL); err != nil && ctx.Err() == nil && t.logger != nil {
			t.logger.Warn("synchronize peer clock", "error", err)
		}
	}
}

// Leave three slots for the final drop notice, stop checkpoint and bandwidth
// counters when the ordinary queue is saturated.
func (t *telemetry) queueLimit(priority bool) int {
	limit := cap(t.events)
	if !priority && limit >= 4 {
		limit -= 3
	}
	return limit
}

func (t *telemetry) enqueueLocked(event model.TraceEvent, priority bool) {
	t.enqueueLockedWithClock(event, priority, t.clockReading())
}

func (t *telemetry) enqueueLockedWithClock(event model.TraceEvent, priority bool, reading controllerClockReading) {
	event = t.identifyLockedWithClock(event, reading)
	if t.sealed || (cap(t.events) > 0 && len(t.events) >= t.queueLimit(priority)) {
		t.dropped.Add(1)
		return
	}
	select {
	case t.events <- event:
	default:
		t.dropped.Add(1)
	}
}

func (t *telemetry) identifyLocked(event model.TraceEvent) model.TraceEvent {
	return t.identifyLockedWithClock(event, t.clockReading())
}

func (t *telemetry) identifyLockedWithClock(event model.TraceEvent, reading controllerClockReading) model.TraceEvent {
	if t.sessionID == "" {
		t.sessionID = rand.Text()
	}
	t.sequence++
	event.SessionID = t.sessionID
	event.Sequence = t.sequence
	event.RunID = t.node.RunID
	event.NodeID = t.node.ID
	event.Group = t.node.Group
	event.AgentID = t.node.AgentID
	if event.EventID == "" {
		// Assigned at the source, so Agent queue retries retain the same ID.
		event.EventID = rand.Text()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = reading.timestamp
	}
	if reading.synchronized &&
		(event.Type == "measurement_start" || event.Type == "measurement_checkpoint" || event.Type == "measurement_stop" ||
			event.Type == "publish" || event.Type == "deliver" || event.Type == "duplicate" || event.Type == "bandwidth" ||
			event.Type == "rpc_metadata" || event.Type == "pubsub_reject" || event.Type == "graft" || event.Type == "prune" || event.Type == "join" || event.Type == "leave" || event.Fields["rpcMetadataVersion"] == 1) {
		if event.Fields == nil {
			event.Fields = make(map[string]any)
		}
		event.Fields["clockBasis"] = controllerClockBasis
		event.Fields["clockOffsetMs"] = float64(reading.offset) / float64(time.Millisecond)
		event.Fields["clockUncertaintyMs"] = float64(reading.uncertainty) / float64(time.Millisecond)
	}
	return event
}

func (t *telemetry) reportDropsLocked(priority bool) {
	if t.dropped.Load() == 0 || len(t.events) >= t.queueLimit(priority) {
		return
	}
	count := t.dropped.Swap(0)
	event := t.identifyLocked(model.TraceEvent{Type: "telemetry_drop", Fields: map[string]any{"count": count}})
	// All producers hold mu, and the consumer can only free capacity.
	t.events <- event
}

func (t *telemetry) startMeasurement(topics []string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.measurementStarted || t.measurementStopped {
		return false
	}
	t.measurementStarted = true
	t.measurementDone = make(chan struct{})
	t.reportDropsLocked(false)
	reading := t.observationClockLocked()
	t.enqueueLockedWithClock(model.TraceEvent{Type: "measurement_start", Timestamp: reading.timestamp, Fields: map[string]any{"subscribedTopics": append([]string{}, topics...)}}, false, reading)
	return true
}

func (t *telemetry) checkpoint() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.measurementStarted && !t.measurementStopped {
		t.reportDropsLocked(false)
		reading := t.observationClockLocked()
		t.enqueueLockedWithClock(model.TraceEvent{Type: "measurement_checkpoint", Timestamp: reading.timestamp}, false, reading)
	}
}

func (t *telemetry) stopMeasurement() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.measurementStopped {
		return
	}
	t.measurementStopped = true
	if t.measurementStarted {
		t.reportDropsLocked(true)
		reading := t.observationClockLocked()
		t.enqueueLockedWithClock(model.TraceEvent{Type: "measurement_stop", Timestamp: reading.timestamp}, true, reading)
		close(t.measurementDone)
	}
}

func (t *telemetry) measurementLoop(ctx context.Context) {
	t.mu.Lock()
	done := t.measurementDone
	t.mu.Unlock()
	if done == nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			t.stopMeasurement()
			return
		case <-ticker.C:
			t.checkpoint()
		}
	}
}

func (t *telemetry) run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	timeout := t.shutdownTimeout
	if timeout <= 0 {
		timeout = telemetryShutdownTimeout
	}
	retry := t.retryInterval
	if retry <= 0 {
		retry = 250 * time.Millisecond
	}
	var drainCtx context.Context
	batch := make([]model.TraceEvent, 0, 250)
	flush := false
	for {
		if ctx.Err() != nil && drainCtx == nil {
			var drainCancel context.CancelFunc
			drainCtx, drainCancel = context.WithTimeout(context.Background(), timeout)
			defer drainCancel() // This transition happens once; keep cancellation with its context.
			t.mu.Lock()
			t.sealed = true
			t.mu.Unlock()
		}
		activeCtx := ctx
		if drainCtx != nil {
			activeCtx = drainCtx
			if err := drainCtx.Err(); err != nil {
				return fmt.Errorf("drain peer telemetry (%d queued, %d pending): %w", len(t.events), len(batch), err)
			}
		}
		if len(batch) > 0 && (flush || len(batch) == cap(batch) || drainCtx != nil) {
			count, err := t.flush(activeCtx, batch)
			if err != nil {
				var encodingErr *telemetryEncodingError
				if errors.As(err, &encodingErr) {
					// This source event cannot be sent in any request. Retain
					// its sequence gap and report the loss instead of blocking
					// every later event behind a permanent encoding failure.
					count = 1
					t.dropped.Add(1)
					if t.logger != nil {
						t.logger.Error("drop unencodable telemetry event", "eventId", batch[0].EventID, "error", err)
					}
				} else {
					if t.logger != nil {
						t.logger.Warn("retry peer telemetry", "events", len(batch), "error", err)
					}
					wait := time.NewTimer(retry)
					select {
					case <-activeCtx.Done():
					case <-wait.C:
					}
					wait.Stop()
					flush = true
					continue // Keep this bounded batch, preserving source order and IDs.
				}
			}
			// A byte-limited request can accept only part of this batch. Keep
			// the suffix without replaying an already acknowledged prefix.
			copy(batch, batch[count:])
			clear(batch[len(batch)-count:])
			batch = batch[:len(batch)-count]
			flush = len(batch) > 0
			if flush {
				continue
			}
		}
		t.mu.Lock()
		t.reportDropsLocked(drainCtx != nil)
		t.mu.Unlock()
		if drainCtx != nil {
			for len(batch) < cap(batch) {
				select {
				case event := <-t.events:
					batch = append(batch, event)
				default:
					if len(batch) == 0 {
						return nil
					}
					flush = true
				}
				if flush {
					break
				}
			}
			continue
		}
		select {
		case <-ctx.Done():
		case event := <-t.events:
			batch = append(batch, event)
		case <-ticker.C:
			flush = true
		}
	}
}

type telemetryEncodingError struct{ err error }

func (e *telemetryEncodingError) Error() string { return "encode telemetry: " + e.err.Error() }
func (e *telemetryEncodingError) Unwrap() error { return e.err }

func (t *telemetry) flush(ctx context.Context, events []model.TraceEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	data, count, err := model.MarshalEventBatchPrefix(t.node.AgentID, events)
	if err != nil {
		return 0, &telemetryEncodingError{err: err}
	}
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(flushCtx, http.MethodPost, t.agentURL+"/api/v1/telemetry", bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return 0, fmt.Errorf("agent returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
	return count, nil
}

func (t *telemetry) reportNode(ctx context.Context, node model.Node) error {
	data, err := json.Marshal(node)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.agentURL+"/api/v1/nodes/"+node.ID+"/status", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.token != "" {
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("agent returned %s: %s", resp.Status, strings.TrimSpace(string(message)))
	}
	return nil
}
