package controller

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Keep one execution lane available for telemetry on multi-CPU Controllers.
// The existing shared analysis slot still bounds full event indexes in memory.
func analysisParallelism() int {
	return min(4, max(1, runtime.GOMAXPROCS(0)-1))
}

// Each index has one owner. Wait for every worker before returning so callers
// can safely release their inputs, including on cancellation or a worker error.
func parallelAnalysis(ctx context.Context, count, workers int, work func(context.Context, int) error) error {
	workers = min(count, max(1, workers))
	if workers <= 1 {
		for index := 0; index < count; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := work(ctx, index); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next atomic.Int64
	var running sync.WaitGroup
	for range workers {
		running.Add(1)
		go func() {
			defer running.Done()
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				if err := work(ctx, index); err != nil {
					cancel(err)
					return
				}
			}
		}()
	}
	running.Wait()
	return context.Cause(ctx)
}
