package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type analysisPrefetchFixture struct {
	input analysisProcessInput
	files [][][]byte
}

func newAnalysisPrefetchFixture() analysisPrefetchFixture {
	fixture := analysisPrefetchFixture{input: analysisProcessInput{RunID: "first"}}
	for snapshot := range 2 {
		descriptor := workerSnapshotDescriptor{}
		var files [][]byte
		for index, name := range []string{"events.jsonl", "observations.jsonl", "scenario.yaml"} {
			size := 6*workerSourceChunk + 317
			if name == "scenario.yaml" {
				size = 8192
			}
			pattern := []byte(fmt.Sprintf("snapshot-%d-file-%d-0123456789abcdefghijklmnopqrstuvwxyz\n", snapshot, index))
			data := bytes.Repeat(pattern, size/len(pattern)+1)[:size]
			files = append(files, data)
			descriptor.Files = append(descriptor.Files, workerSourceDescriptor{Name: name, Size: int64(len(data))})
		}
		fixture.input.Snapshots = append(fixture.input.Snapshots, descriptor)
		fixture.files = append(fixture.files, files)
	}
	return fixture
}

func (fixture analysisPrefetchFixture) fetch(_ context.Context, request workerSourceRead) (workerSourceBytes, error) {
	length, eof, err := remoteAnalysisReadBoundary(fixture.input, request)
	if err != nil || length == 0 {
		return workerSourceBytes{EOF: eof}, err
	}
	data := fixture.files[request.Snapshot][request.File]
	return workerSourceBytes{Data: append([]byte(nil), data[request.Offset:request.Offset+int64(length)]...), EOF: eof}, nil
}

func assertAnalysisPrefetchBytes(t *testing.T, fixture analysisPrefetchFixture, request workerSourceRead, got workerSourceBytes, err error) {
	t.Helper()
	want, expectedErr := fixture.fetch(t.Context(), request)
	if err != nil || expectedErr != nil || got.EOF != want.EOF || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("prefetch changed captured bytes for %+v: got=%d bytes eof=%t error=%v, want=%d bytes eof=%t error=%v", request, len(got.Data), got.EOF, err, len(want.Data), want.EOF, expectedErr)
	}
}

func waitAnalysisPrefetchRequest(t *testing.T, calls <-chan workerSourceRead) workerSourceRead {
	t.Helper()
	select {
	case request := <-calls:
		return request
	case <-time.After(5 * time.Second):
		t.Fatal("prefetch did not request the next source block")
		return workerSourceRead{}
	}
}

func TestAnalysisPrefetchPullsAheadWithoutBlockingBufferedReads(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	prefetch := newAnalysisPrefetch(ctx, fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		if request.Offset == workerSourceChunk {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return workerSourceBytes{}, ctx.Err()
			}
		}
		return fixture.fetch(ctx, request)
	})
	defer prefetch.close()
	defer releaseOnce.Do(func() { close(release) })
	first := workerSourceRead{Length: 64 << 10}
	got, err := prefetch.read(ctx, first)
	assertAnalysisPrefetchBytes(t, fixture, first, got, err)
	select {
	case <-started:
		// The child requested only 64 KiB, but the next network block is
		// already being fetched while it could be decoding that first input.
	case <-time.After(5 * time.Second):
		t.Fatal("source transfer did not overlap consumer calculation")
	}
	second := workerSourceRead{Offset: 64 << 10, Length: 64 << 10}
	type outcome struct {
		value workerSourceBytes
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := prefetch.read(ctx, second)
		done <- outcome{value, err}
	}()
	select {
	case result := <-done:
		assertAnalysisPrefetchBytes(t, fixture, second, result.value, result.err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("available bytes waited for a future network block")
	}
	releaseOnce.Do(func() { close(release) })
}

func TestAnalysisPrefetchQueueAppliesByteBackpressure(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	calls := make(chan workerSourceRead, 16)
	prefetch := newAnalysisPrefetch(t.Context(), fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		calls <- request
		return fixture.fetch(ctx, request)
	})
	defer prefetch.close()
	first := workerSourceRead{Length: 64 << 10}
	got, err := prefetch.read(t.Context(), first)
	assertAnalysisPrefetchBytes(t, fixture, first, got, err)
	for _, offset := range []int64{0, workerSourceChunk} {
		request := waitAnalysisPrefetchRequest(t, calls)
		if request.Offset != offset || request.Length != workerSourceChunk {
			t.Fatalf("unexpected ahead block %+v; want offset=%d length=%d", request, offset, workerSourceChunk)
		}
	}
	select {
	case request := <-calls:
		t.Fatalf("two-MiB queue fetched beyond its byte budget: %+v", request)
	case <-time.After(20 * time.Millisecond):
	}
	// Finishing the retained front block creates exactly one block of room.
	remaining := workerSourceRead{Offset: 64 << 10, Length: workerSourceChunk - 64<<10}
	got, err = prefetch.read(t.Context(), remaining)
	assertAnalysisPrefetchBytes(t, fixture, remaining, got, err)
	request := waitAnalysisPrefetchRequest(t, calls)
	if request.Offset != 2*workerSourceChunk || request.Length != workerSourceChunk {
		t.Fatalf("releasing one block did not resume the producer: %+v", request)
	}
	// close must also wake a producer waiting for room in a full queue.
	done := make(chan struct{})
	go func() { prefetch.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("full prefetch queue prevented cleanup")
	}
}

func TestAnalysisPrefetchReadsAcrossNetworkBlockBoundaries(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	prefetch := newAnalysisPrefetch(t.Context(), fixture.input, 2*workerSourceChunk, fixture.fetch)
	defer prefetch.close()
	request := workerSourceRead{Length: 64 << 10}
	got, err := prefetch.read(t.Context(), request)
	assertAnalysisPrefetchBytes(t, fixture, request, got, err)
	for offset := int64(64 << 10); offset < int64(len(fixture.files[0][0])); offset += workerSourceChunk {
		// The child may request up to one MiB at an offset inside the
		// retained front block. Its response then spans two network blocks.
		request := workerSourceRead{Offset: offset, Length: workerSourceChunk}
		got, err := prefetch.read(t.Context(), request)
		assertAnalysisPrefetchBytes(t, fixture, request, got, err)
	}
}

func TestAnalysisPrefetchRejectsInvalidSourceBlocks(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	for _, corruption := range []string{"truncated", "oversized", "wrong-eof"} {
		t.Run(corruption, func(t *testing.T) {
			prefetch := newAnalysisPrefetch(t.Context(), fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
				block, err := fixture.fetch(ctx, request)
				if err != nil {
					return block, err
				}
				switch corruption {
				case "truncated":
					block.Data = block.Data[:len(block.Data)-1]
				case "oversized":
					block.Data = append(block.Data, 'x')
				case "wrong-eof":
					block.EOF = !block.EOF
				}
				return block, nil
			})
			defer prefetch.close()
			if _, err := prefetch.read(t.Context(), workerSourceRead{Length: 64 << 10}); err == nil || !strings.Contains(err.Error(), "invalid remote analysis source block") {
				t.Fatalf("corrupt %s block was accepted: %v", corruption, err)
			}
		})
	}
}

func TestAnalysisPrefetchSamplesSeeksAndFileChangesKeepCapturedBytes(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	calls := make(chan workerSourceRead, 64)
	prefetch := newAnalysisPrefetch(t.Context(), fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		calls <- request
		return fixture.fetch(ctx, request)
	})
	defer prefetch.close()
	// Source-hash samples retain exact small-read behavior.
	sample := workerSourceRead{Offset: 100, Length: 4096}
	got, err := prefetch.read(t.Context(), sample)
	assertAnalysisPrefetchBytes(t, fixture, sample, got, err)
	if request := waitAnalysisPrefetchRequest(t, calls); request != sample {
		t.Fatalf("small random sample caused speculative transfer: %+v", request)
	}
	select {
	case request := <-calls:
		t.Fatalf("small sample started a prefetch stream: %+v", request)
	case <-time.After(20 * time.Millisecond):
	}
	for _, request := range []workerSourceRead{
		{Length: 64 << 10},
		{Offset: 2*workerSourceChunk + 77, Length: 64 << 10},
		{Offset: 123, Length: 64 << 10},
		{File: 1, Offset: workerSourceChunk + 231, Length: 64 << 10},
		{Snapshot: 1, File: 1, Offset: 41, Length: 64 << 10},
		{Snapshot: 1, File: 2, Offset: 17, Length: 512},
		{Offset: int64(len(fixture.files[0][0])) - 91, Length: 64 << 10},
		{Offset: int64(len(fixture.files[0][0])), Length: 64 << 10},
	} {
		got, err := prefetch.read(t.Context(), request)
		assertAnalysisPrefetchBytes(t, fixture, request, got, err)
	}
}

func TestAnalysisPrefetchRetainsFutureErrorUntilItsOffset(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	failure := errors.New("NAS read failed on second block")
	failed := make(chan struct{})
	prefetch := newAnalysisPrefetch(t.Context(), fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		if request.Offset == workerSourceChunk {
			close(failed)
			return workerSourceBytes{}, failure
		}
		return fixture.fetch(ctx, request)
	})
	defer prefetch.close()
	first := workerSourceRead{Length: 64 << 10}
	got, err := prefetch.read(t.Context(), first)
	assertAnalysisPrefetchBytes(t, fixture, first, got, err)
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("producer did not encounter the future failure")
	}
	buffered := workerSourceRead{Offset: 64 << 10, Length: 64 << 10}
	got, err = prefetch.read(t.Context(), buffered)
	assertAnalysisPrefetchBytes(t, fixture, buffered, got, err)
	remaining := workerSourceRead{Offset: 128 << 10, Length: workerSourceChunk - 128<<10}
	got, err = prefetch.read(t.Context(), remaining)
	assertAnalysisPrefetchBytes(t, fixture, remaining, got, err)
	if _, err := prefetch.read(t.Context(), workerSourceRead{Offset: workerSourceChunk, Length: 64 << 10}); !errors.Is(err, failure) {
		t.Fatalf("future source error was lost: %v", err)
	}
}

func TestAnalysisPrefetchCancellationJoinsInflightSourceRead(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	var active atomic.Int32
	prefetch := newAnalysisPrefetch(ctx, fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		active.Add(1)
		defer active.Add(-1)
		if request.Offset == workerSourceChunk {
			close(started)
			<-ctx.Done()
			return workerSourceBytes{}, ctx.Err()
		}
		return fixture.fetch(ctx, request)
	})
	defer func() { cancel(); prefetch.close() }()
	first := workerSourceRead{Length: 64 << 10}
	got, err := prefetch.read(ctx, first)
	assertAnalysisPrefetchBytes(t, fixture, first, got, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("inflight source read did not start")
	}
	cancel()
	done := make(chan struct{})
	go func() { prefetch.close(); close(done) }()
	select {
	case <-done:
		if active.Load() != 0 {
			t.Fatal("prefetch cleanup returned with an active source read")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled source read was not joined")
	}
	if _, err := prefetch.read(ctx, workerSourceRead{Length: 64 << 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reader accepted more source work: %v", err)
	}
}

func TestAnalysisPrefetchFileSwitchDrainsInflightReplyWithoutCancelingJob(t *testing.T) {
	fixture := newAnalysisPrefetchFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var oldReadDone atomic.Bool
	prefetch := newAnalysisPrefetch(ctx, fixture.input, 2*workerSourceChunk, func(ctx context.Context, request workerSourceRead) (workerSourceBytes, error) {
		if request.File == 0 && request.Offset == workerSourceChunk {
			close(started)
			select {
			case <-release:
				oldReadDone.Store(true)
			case <-ctx.Done():
				return workerSourceBytes{}, errors.New("inflight reply abandoned by stream reset")
			}
		}
		if request.File == 1 && !oldReadDone.Load() {
			return workerSourceBytes{}, errors.New("file switched before the old network reply was drained")
		}
		return fixture.fetch(ctx, request)
	})
	defer prefetch.close()
	defer releaseOnce.Do(func() { close(release) })
	if _, err := prefetch.read(ctx, workerSourceRead{Length: 64 << 10}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("future read did not start")
	}
	request := workerSourceRead{File: 1, Length: 64 << 10}
	type outcome struct {
		value workerSourceBytes
		err   error
	}
	done := make(chan outcome, 1)
	go func() { value, err := prefetch.read(ctx, request); done <- outcome{value, err} }()
	select {
	case result := <-done:
		t.Fatalf("file switch completed before the inflight reply drained: %v", result.err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		assertAnalysisPrefetchBytes(t, fixture, request, result.value, result.err)
		if ctx.Err() != nil {
			t.Fatalf("file switch canceled the whole analysis: %v", ctx.Err())
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("file switch did not resume after draining the old reply")
	}
}
