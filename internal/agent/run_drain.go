package agent

import (
	"context"
	"fmt"
	"time"
)

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
			return fmt.Errorf("run logs not fully delivered (%d remaining): %w", remaining, err)
		}
		s.flushEvents(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
}
