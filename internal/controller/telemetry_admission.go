package controller

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Bound decoded batches through durable commit, including requests whose
// clients have timed out. Reject before reading their bodies; Agents keep the
// unacknowledged events in their spool and retry with the same identities.
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
		return true
	}
}
func (s *Server) releaseTelemetryDecoder() {
	<-s.telemetrySlots
}

func (s *Server) registerTelemetryMetrics() {
	s.telemetryGateOnce.Do(func() { s.telemetrySlots = make(chan struct{}, 2) })
	s.state.metrics.registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "kpl_controller_telemetry_inflight_requests", Help: "Telemetry requests decoding or awaiting durable commit (maximum two)."}, func() float64 { return float64(len(s.telemetrySlots)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "kpl_controller_telemetry_waiting_requests", Help: "Telemetry requests waiting for admission without decoded bodies."}, func() float64 { return float64(s.telemetryWaiters.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "kpl_controller_telemetry_rejected_total", Help: "Telemetry requests rejected before decoding; unacknowledged batches remain at the sender."}, func() float64 { return float64(s.telemetryRejected.Load()) }),
	)
}
