package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	s.events = nil
	data, count, err := s.spool.marshalPrefix(s.config.ID)
	if err != nil {
		s.spoolError = err
		s.eventsMu.Unlock()
		s.logger.Error("read telemetry window", "error", err)
		return
	}
	if count == 0 {
		if len(s.terminations) == 0 {
			s.spoolError = nil
		}
		s.eventsMu.Unlock()
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

// Copy the already-normalized JSON directly from disk. Re-decoding every event
// into interface maps and encoding it again multiplies heap churn under load.
// The byte budget is unchanged, but small events can fill it instead of being
// cut off at 250 records and forcing extra Controller fsync/round trips.
func (spool *telemetrySpool) marshalPrefix(agentID string) ([]byte, int, error) {
	if spool.cleanup != nil {
		if err := spool.removeSegment(spool.cleanup); err != nil {
			return nil, 0, err
		}
		spool.cleanup = nil
	}
	agent, err := json.Marshal(agentID)
	if err != nil {
		return nil, 0, err
	}
	header := append([]byte(`{"agentId":`), agent...)
	header = append(header, `,"events":[`...)
	count, size := 0, 0
	for _, record := range spool.records {
		if count > 0 && (count >= telemetryWindowEvents || size+record.size > telemetryWindowBytes) {
			break
		}
		if len(header)+size+record.size+count+2 > model.MaxEventBatchBytes {
			if count == 0 {
				return nil, 0, fmt.Errorf("encoded telemetry event exceeds %d-byte batch limit", model.MaxEventBatchBytes)
			}
			break
		}
		size += record.size
		count++
	}
	if count == 0 {
		return nil, 0, nil
	}
	data := make([]byte, 0, len(header)+size+count+2)
	data = append(data, header...)
	var f *os.File
	var segment *telemetrySegment
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	for i, record := range spool.records[:count] {
		if record.segment != segment {
			if f != nil {
				_ = f.Close()
			}
			f, err = os.Open(filepath.Join(spool.directory, record.segment.name))
			if err != nil {
				return nil, 0, err
			}
			segment = record.segment
		}
		if i > 0 {
			data = append(data, ',')
		}
		start := len(data)
		data = data[:start+record.size]
		if _, err := io.ReadFull(io.NewSectionReader(f, record.offset, int64(record.size)), data[start:]); err != nil {
			return nil, 0, err
		}
		if !json.Valid(data[start:]) {
			return nil, 0, fmt.Errorf("invalid telemetry JSON in %s", record.segment.name)
		}
	}
	return append(data, ']', '}'), count, nil
}

const telemetryAdmissionWaiters = 64
const telemetryAdmissionWait = time.Second

func (s *Server) acquireTelemetryDecoder(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	s.telemetryGateOnce.Do(func() { s.telemetrySlots = make(chan struct{}, 2) })
	select {
	case s.telemetrySlots <- struct{}{}:
		if ctx.Err() != nil {
			<-s.telemetrySlots
			return false
		}
		s.telemetryDecoders.Add(1)
		return true
	default:
	}
	if s.telemetryWaiters.Add(1) > telemetryAdmissionWaiters {
		s.telemetryWaiters.Add(-1)
		return false
	}
	defer s.telemetryWaiters.Add(-1)
	timer := time.NewTimer(telemetryAdmissionWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	case s.telemetrySlots <- struct{}{}:
		if ctx.Err() != nil {
			<-s.telemetrySlots
			return false
		}
		s.telemetryDecoders.Add(1)
		return true
	}
}
func (s *Server) releaseTelemetryDecoder() {
	s.telemetryDecoders.Add(-1)
	<-s.telemetrySlots
}
