package controller

import (
	"context"
	"encoding/json"
	"time"
)

type rawSnapshotFlight struct {
	done chan struct{}
	data []byte
	at   time.Time
	err  error
}

type dashboardSnapshotFlight struct {
	done  chan struct{}
	frame *dashboardFrame
	err   error
}

// At most one producer per view outlives a disconnected reader. Readers wait
// without holding a mutex and leave immediately on cancellation; reconnects
// join the same producer instead of piling up blocked snapshot computations.
func (s *Server) streamSnapshotAtContext(ctx context.Context, keepAlive func() error) ([]byte, time.Time, error) {
retry:
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, err
	}
	s.snapshotMu.Lock()
	if reset := s.snapshotResetDone; reset != nil {
		s.snapshotMu.Unlock()
		if err := waitSnapshotFlight(ctx, reset, keepAlive); err != nil {
			return nil, time.Time{}, err
		}
		goto retry
	}
	if s.snapshotData != nil && time.Since(s.snapshotAt) < snapshotInterval {
		data, at := s.snapshotData, s.snapshotAt
		s.snapshotMu.Unlock()
		return data, at, nil
	}
	flight := s.snapshotFlight
	if flight == nil {
		epoch := s.snapshotEpoch
		flight = &rawSnapshotFlight{done: make(chan struct{})}
		s.snapshotFlight = flight
		go func() {
			// Timestamp the start so notifications during computation stay pending.
			flight.at = time.Now()
			flight.data, flight.err = json.Marshal(s.state.snapshot())
			s.snapshotMu.Lock()
			if flight.err == nil && s.snapshotEpoch == epoch {
				s.snapshotData, s.snapshotAt = flight.data, flight.at
			}
			if s.snapshotFlight == flight {
				s.snapshotFlight = nil
			}
			close(flight.done)
			s.snapshotMu.Unlock()
		}()
	}
	s.snapshotMu.Unlock()
	if err := waitSnapshotFlight(ctx, flight.done, keepAlive); err != nil {
		return nil, time.Time{}, err
	}
	return flight.data, flight.at, flight.err
}

func (s *Server) dashboardStreamSnapshotContext(ctx context.Context, keepAlive func() error) (*dashboardFrame, error) {
	return s.dashboardStreamSnapshotForTopology(ctx, keepAlive, true)
}

func (s *Server) dashboardStreamSnapshotForTopology(ctx context.Context, keepAlive func() error, includeTopology bool) (*dashboardFrame, error) {
retry:
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.snapshotMu.Lock()
	if reset := s.snapshotResetDone; reset != nil {
		s.snapshotMu.Unlock()
		if err := waitSnapshotFlight(ctx, reset, keepAlive); err != nil {
			return nil, err
		}
		goto retry
	}
	frame, flight := s.dashboardFrame, s.dashboardFlight
	if !includeTopology {
		frame, flight = s.dashboardSummaryFrame, s.dashboardSummaryFlight
	}
	if frame != nil && time.Since(frame.at) < snapshotInterval {
		s.snapshotMu.Unlock()
		return frame, nil
	}
	if flight == nil {
		epoch := s.snapshotEpoch
		flight = &dashboardSnapshotFlight{done: make(chan struct{})}
		if includeTopology {
			s.dashboardFlight = flight
		} else {
			s.dashboardSummaryFlight = flight
		}
		go func() {
			at := time.Now()
			flight.frame, flight.err = newDashboardFrame(s.state.snapshotForTopologyView(true, includeTopology), at)
			s.snapshotMu.Lock()
			if includeTopology {
				if flight.err == nil && s.snapshotEpoch == epoch {
					s.dashboardFrame = flight.frame
				}
				if s.dashboardFlight == flight {
					s.dashboardFlight = nil
				}
			} else {
				if flight.err == nil && s.snapshotEpoch == epoch {
					s.dashboardSummaryFrame = flight.frame
				}
				if s.dashboardSummaryFlight == flight {
					s.dashboardSummaryFlight = nil
				}
			}
			close(flight.done)
			s.snapshotMu.Unlock()
		}()
	}
	s.snapshotMu.Unlock()
	if err := waitSnapshotFlight(ctx, flight.done, keepAlive); err != nil {
		return nil, err
	}
	return flight.frame, flight.err
}

func waitSnapshotFlight(ctx context.Context, done <-chan struct{}, keepAlive func() error) error {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return ctx.Err()
		case <-ticker.C:
			if keepAlive != nil {
				if err := keepAlive(); err != nil {
					return err
				}
			}
		}
	}
}
