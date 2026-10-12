package controller

import (
	"context"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestCanceledSleepRejectsImmediateAndOverdueWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, duration := range []time.Duration{-time.Second, 0, time.Nanosecond, time.Second} {
		if err := sleepContext(ctx, duration); err != context.Canceled {
			t.Fatalf("canceled wait %s: %v", duration, err)
		}
	}
}

func TestCanceledWaitPhaseDoesNotComplete(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := &Server{}
	if err := s.runPhase(ctx, "run", 1, scenario.Phase{Action: "wait", Duration: "0s"}, nil, nil, 0); err != context.Canceled {
		t.Fatalf("canceled immediate phase completed: %v", err)
	}
}

func TestActiveSleepAllowsImmediateAndElapsedWaits(t *testing.T) {
	for _, duration := range []time.Duration{-time.Second, 0, time.Nanosecond} {
		if err := sleepContext(t.Context(), duration); err != nil {
			t.Fatalf("active wait %s: %v", duration, err)
		}
	}
}
