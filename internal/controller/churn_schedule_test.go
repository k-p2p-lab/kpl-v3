package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

func TestSequentialScheduleDoesNotAccumulateRequestDuration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const count = 100
		delays := make([]time.Duration, count)
		for i := range delays {
			delays[i] = 250 * time.Millisecond
		}
		start := time.Now()
		var actual []time.Duration
		err := runOperations(context.Background(), count, false, 0, delays, false, func(ctx context.Context, i int) error {
			actual = append(actual, time.Since(start))
			// Simulate increasing admission costs during a long churn run.
			time.Sleep(time.Duration(i*2) * time.Millisecond)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(actual) != count {
			t.Fatalf("dispatched %d operations, want %d", len(actual), count)
		}
		for i, at := range actual {
			if want := time.Duration(i) * 250 * time.Millisecond; at != want {
				t.Fatalf("operation %d starts at %s, want %s; admission latency accumulated into the schedule", i, at, want)
			}
		}
	})
}

func TestSequentialScheduleKeepsLateOperationsAndRecoversOriginalTimeline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var actual []time.Duration
		err := runOperations(context.Background(), 5, false, 0, []time.Duration{time.Second, time.Second, time.Second, time.Second}, false, func(ctx context.Context, i int) error {
			actual = append(actual, time.Since(start))
			if i == 0 {
				time.Sleep(2500 * time.Millisecond)
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			return nil
		})
		want := []time.Duration{0, 2500 * time.Millisecond, 2600 * time.Millisecond, 3 * time.Second, 4 * time.Second}
		if err != nil || !reflect.DeepEqual(actual, want) {
			t.Fatalf("late dispatch: %v, want %v, error=%v", actual, want, err)
		}
	})
}

func TestSequentialScheduleStopsWaitingOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		count := 0
		err := runOperations(ctx, 3, false, 0, []time.Duration{time.Second, time.Second}, false, func(context.Context, int) error { count++; return nil })
		if count != 1 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dispatched after cancellation: count=%d err=%v", count, err)
		}
	})
}
