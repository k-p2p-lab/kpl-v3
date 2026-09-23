package controller

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

func TestParallelOperationsPropagateCancellation(t *testing.T) {
	for _, failure := range []error{context.Canceled, fmt.Errorf("dispatch canceled: %w", context.Canceled), errors.New("dispatch failed")} {
		err := runOperations(context.Background(), 20, true, 2, nil, false, func(context.Context, int) error { return failure })
		if !errors.Is(err, failure) {
			t.Errorf("operation failure %v became %v", failure, err)
		}
	}
}

func TestParallelOperationsStopQueuedWorkOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	err := runOperations(ctx, 100, true, 1, nil, true, func(context.Context, int) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("canceled batch made %d calls, returned %v; want one call and cancellation", calls, err)
	}
}

func TestParallelOperationsBoundWaitingGoroutines(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprint(delayed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const count, parallelism = 2048, 4
				delays := make([]time.Duration, count)
				for i := range delays {
					delays[i] = time.Hour
				}
				baseline := runtime.NumGoroutine()
				release := make(chan struct{})
				done := make(chan error, 1)
				go func() {
					done <- runOperations(context.Background(), count, true, parallelism, delays, delayed, func(context.Context, int) error {
						<-release
						return nil
					})
				}()
				synctest.Wait()
				added := runtime.NumGoroutine() - baseline
				t.Logf("%d operations, parallelism %d: %d additional waiting goroutines", count, parallelism, added)
				if added > parallelism+16 {
					t.Errorf("%d waiting goroutines for parallelism %d", added, parallelism)
				}
				close(release)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestParallelPublicationDelaysRemainBatchOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var indices []int
		var times []time.Duration
		err := runOperations(context.Background(), 3, true, 1, []time.Duration{3 * time.Second, 0, time.Second}, true, func(_ context.Context, index int) error {
			indices = append(indices, index)
			times = append(times, time.Since(start))
			time.Sleep(2 * time.Second)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for i, index := range []int{1, 2, 0} {
			if indices[i] != index || times[i] != time.Duration(i)*2*time.Second {
				t.Fatalf("dispatch indices/times = %v/%v; want [1 2 0]/[0s 2s 4s]", indices, times)
			}
		}
	})
}
