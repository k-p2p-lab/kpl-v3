package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
)

const (
	analysisDecodeBatchBytes = 256 << 10
	analysisDecodeBatchRows  = 256
	analysisDecodeQueueBytes = 8 << 20
	analysisDecodeMaxLine    = 16 << 20
)

// Decode can occupy both CPUs of a small analysis worker. The caller owns
// reads, progress and reduction; decoder workers never touch shared metrics.
func analysisDecodeParallelism() int {
	return max(1, runtime.GOMAXPROCS(0))
}

type analysisDecodeBatch[T any] struct {
	data      []byte
	ends      []int
	firstLine int
	values    []T
	decoded   int
	err       error
	done      chan struct{}
}

func (batch *analysisDecodeBatch[T]) decode(ctx context.Context) {
	defer close(batch.done)
	batch.values = make([]T, len(batch.ends))
	start := 0
	for index, end := range batch.ends {
		if err := ctx.Err(); err != nil {
			batch.err = err
			return
		}
		// Each element starts at its zero value: reusing a decoded map or
		// struct would retain fields omitted from the next JSON record.
		if err := json.Unmarshal(batch.data[start:end], &batch.values[index]); err != nil {
			batch.err = err
			return
		}
		batch.decoded++
		start = end
	}
}

func scanDecodedAnalysisFile[T any](ctx context.Context, file resultFile, consume func(T) error) error {
	return scanDecodedAnalysisFileWithWorkers(ctx, file, 0, consume)
}

// A zero worker count selects the production policy. Explicit counts exercise
// the same parser/reducer with serial and parallel execution in regression tests.
func scanDecodedAnalysisFileWithWorkers[T any](ctx context.Context, file resultFile, workers int, consume func(T) error) error {
	if workers <= 0 {
		workers = analysisDecodeParallelism()
		if file.size < analysisDecodeBatchBytes {
			workers = 1
		}
	}
	workers = max(1, workers)
	if workers == 1 {
		return scanAnalysisFile(ctx, file, func(data []byte) error {
			var value T
			if err := json.Unmarshal(data, &value); err != nil {
				return err
			}
			return consume(value)
		})
	}
	if file.file == nil && file.readAt == nil && len(file.parts) == 0 {
		return nil
	}

	workCtx, cancel := context.WithCancel(ctx)
	jobs := make(chan *analysisDecodeBatch[T], workers*2)
	var running sync.WaitGroup
	for range workers {
		running.Add(1)
		go func() {
			defer running.Done()
			for {
				select {
				case <-workCtx.Done():
					return
				case batch, ok := <-jobs:
					if !ok {
						return
					}
					batch.decode(workCtx)
				}
			}
		}()
	}
	defer func() {
		cancel()
		close(jobs)
		// No decoder may retain a source batch after this scan returns,
		// including on a malformed record or consumer cancellation.
		running.Wait()
	}()

	pending := make([]*analysisDecodeBatch[T], 0, workers*2)
	pendingBytes := 0
	drain := func() error {
		batch := pending[0]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-batch.done:
		}
		for index := range batch.decoded {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := consume(batch.values[index]); err != nil {
				return fmt.Errorf("%s line %d: %w", file.name, batch.firstLine+index, err)
			}
			// Release nested maps and slices before the next input batch.
			var zero T
			batch.values[index] = zero
		}
		if batch.err != nil {
			return fmt.Errorf("%s line %d: %w", file.name, batch.firstLine+batch.decoded, batch.err)
		}
		pendingBytes -= len(batch.data)
		copy(pending, pending[1:])
		pending[len(pending)-1] = nil
		pending = pending[:len(pending)-1]
		return nil
	}
	dispatch := func(batch *analysisDecodeBatch[T]) error {
		// Bound queued rows as well as bytes. One individually valid long
		// record can exceed the byte budget, and is admitted alone.
		for len(pending) > 0 && (len(pending) >= workers*2 || pendingBytes+len(batch.data) > analysisDecodeQueueBytes) {
			if err := drain(); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobs <- batch:
		}
		pending = append(pending, batch)
		pendingBytes += len(batch.data)
		return nil
	}

	// Keep source reads and progress callbacks on the caller. In particular,
	// the remote worker's progress callback is deliberately not concurrent.
	reportAnalysisProgress(ctx, file.name, 0)
	reader := analysisProgressReader{Reader: file.reader(), report: func(n int64) { reportAnalysisProgress(ctx, file.name, n) }}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(analysisScanBuffer(file.size), analysisDecodeMaxLine)
	line := 0
	var batch *analysisDecodeBatch[T]
	for scanner.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return err
		}
		data := scanner.Bytes()
		if batch != nil && (len(batch.data)+len(data) > analysisDecodeBatchBytes || len(batch.ends) >= analysisDecodeBatchRows) {
			if err := dispatch(batch); err != nil {
				return err
			}
			batch = nil
		}
		if batch == nil {
			batch = &analysisDecodeBatch[T]{data: make([]byte, 0, max(analysisDecodeBatchBytes, len(data))), ends: make([]int, 0, analysisDecodeBatchRows), firstLine: line, done: make(chan struct{})}
		}
		// Scanner.Bytes is only valid until the next Scan. Copy each record
		// into its batch's contiguous buffer before transferring ownership.
		batch.data = append(batch.data, data...)
		batch.ends = append(batch.ends, len(batch.data))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if batch != nil {
		if err := dispatch(batch); err != nil {
			return err
		}
	}
	// Earlier JSON/consumer failures take precedence over a later reader or
	// token-size error, matching the original sequential scan.
	for len(pending) > 0 {
		if err := drain(); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", file.name, err)
	}
	return ctx.Err()
}
