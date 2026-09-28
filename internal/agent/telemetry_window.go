package agent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func telemetryEncodedSize(events []model.TraceEvent) (int64, error) {
	var size int64
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return 0, err
		}
		size += int64(len(data))
	}
	return size, nil
}

// Caller holds eventsMu. Disk records include the currently transmitted prefix.
func (s *Server) pendingEventsLocked() int {
	if s.spool != nil {
		return len(s.spool.records) + len(s.terminations)
	}
	return len(s.events) + s.eventsInFlight + len(s.terminations)
}
func (s *Server) pendingBytesLocked() int64 {
	if s.spool != nil {
		return s.spool.bytes
	}
	return s.eventsBytes + s.inFlightBytes
}

// Enter with eventsMu held; always release it before returning. A failed send
// discards only the decoded window, never its durable records. The next attempt
// reloads the same prefix. Newly received batches stay behind it on disk.
func (s *Server) flushSpoolEventsLocked(ctx context.Context) {
	if len(s.events) == 0 {
		var err error
		s.events, err = s.spool.loadPrefix()
		if err != nil {
			s.spoolError = err
			s.eventsMu.Unlock()
			s.logger.Error("read telemetry window", "error", err)
			return
		}
	}
	if len(s.events) == 0 {
		if len(s.terminations) == 0 {
			s.spoolError = nil
		}
		s.eventsMu.Unlock()
		return
	}
	data, count, err := model.MarshalEventBatchPrefix(s.config.ID, s.events)
	s.events = nil
	if err != nil {
		s.spoolError = err
		s.eventsMu.Unlock()
		s.logger.Error("encode telemetry window", "error", err)
		return
	}
	s.eventsInFlight = count
	s.inFlightBytes = int64(len(data))
	s.eventsMu.Unlock()
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = s.postData(flushCtx, "/api/v1/events/batch", data, nil)
	s.eventsMu.Lock()
	s.eventsInFlight = 0
	s.inFlightBytes = 0
	if err == nil {
		_, err = s.spool.acknowledge(count)
		s.spoolError = err
	}
	more := len(s.spool.records) > 0
	s.eventsMu.Unlock()
	if err != nil {
		s.logger.Warn("telemetry flush failed", "events", count, "error", err)
		return
	}
	// Drain immediately while the Controller keeps up, with one request in flight.
	if more {
		select {
		case s.flushNow <- struct{}{}:
		default:
		}
	}
}
