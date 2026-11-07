package agent

import (
	"context"
	"fmt"
	"time"
)

type runLogsPendingError struct {
	remaining int
	cause     error
}

func (e *runLogsPendingError) Error() string {
	return fmt.Sprintf("run logs not fully delivered (%d remaining): %v", e.remaining, e.cause)
}
func (e *runLogsPendingError) Unwrap() error { return e.cause }

// Other experiments may keep producing telemetry. Wait only for this run's
// queued and in-flight evidence, after its producers have stopped.
func (s *Server) drainRunEvents(ctx context.Context, id string) error {
	for {
		s.eventsMu.Lock()
		remaining := s.inFlightRuns[id]
		for _, event := range s.events {
			if event.RunID == id {
				remaining++
			}
		}
		if s.spool != nil {
			remaining = 0
			for _, record := range s.spool.records {
				if record.runID == id {
					remaining++
				}
			}
		}
		for _, event := range s.terminations {
			if event.RunID == id {
				remaining++
			}
		}
		s.eventsMu.Unlock()
		if remaining == 0 {
			s.eventsMu.Lock()
			err := s.spoolError
			s.eventsMu.Unlock()
			return err
		}
		if err := ctx.Err(); err != nil {
			return &runLogsPendingError{remaining: remaining, cause: err}
		}
		s.flushEvents(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
}
