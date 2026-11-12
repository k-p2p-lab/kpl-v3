package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ReaderAt has no context cancellation hook. Its explicit gate verifies that
// cancellation retains the source until an already-entered storage read ends.
type controllerPrefetchProbe struct {
	data      []byte
	maxEnd    atomic.Int64
	active    atomic.Int32
	watchEnd  int64
	watch     chan struct{}
	watchOnce sync.Once
	blockAt   int64
	blocked   chan struct{}
	blockOnce sync.Once
	release   chan struct{}
}

func (probe *controllerPrefetchProbe) ReadAt(p []byte, offset int64) (int, error) {
	probe.active.Add(1)
	defer probe.active.Add(-1)
	if probe.blocked != nil && offset == probe.blockAt {
		probe.blockOnce.Do(func() { close(probe.blocked) })
		<-probe.release
	}
	if offset >= int64(len(probe.data)) {
		return 0, io.EOF
	}
	n := copy(p, probe.data[offset:])
	end := offset + int64(n)
	for old := probe.maxEnd.Load(); end > old; old = probe.maxEnd.Load() {
		if probe.maxEnd.CompareAndSwap(old, end) {
			break
		}
	}
	if probe.watch != nil && end >= probe.watchEnd {
		probe.watchOnce.Do(func() { close(probe.watch) })
	}
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func controllerPrefetchSnapshot(probe *controllerPrefetchProbe) (*resultSnapshot, analysisProcessInput) {
	at := time.Unix(1000, 0).UTC()
	snapshot := &resultSnapshot{result: savedResult{ID: "controller-prefetch-run", State: "completed", StartedAt: at, FinishedAt: at.Add(time.Minute)},
		exportedAt: at.Add(time.Hour), files: []resultFile{{name: "events.jsonl", size: int64(len(probe.data)), readAt: probe}}}
	input := analysisProcessInput{RunID: snapshot.result.ID, AnalysisID: "controller-prefetch-analysis", Snapshots: []workerSnapshotDescriptor{describeWorkerSnapshot(snapshot)}}
	return snapshot, input
}

type controllerPrefetchOutcome struct {
	artifacts *workerAnalysisArtifacts
	result    analysisProcessResult
	err       error
}

func TestControllerAnalysisPrefetchUsesDefaultAndConfiguredQueueDuringCalculation(t *testing.T) {
	for _, config := range []struct {
		name string
		mib  uint64
	}{
		{name: "default-64-MiB"},
		{name: "configured-1-MiB", mib: 1},
	} {
		t.Run(config.name, func(t *testing.T) {
			s, commands := isolatedAnalysisServer(t)
			s.config.AnalysisPrefetchMiB = config.mib
			line := []byte("{\"runId\":\"unrelated\"}\n")
			data := bytes.Repeat(line, 4*workerSourceChunk/len(line)+1)
			data = data[:len(data)-len(data)%len(line)]
			watchEnd := int64(workerSourceChunk)
			if config.mib == 0 {
				watchEnd = int64(len(data))
			}
			probe := &controllerPrefetchProbe{data: data, watchEnd: watchEnd, watch: make(chan struct{})}
			snapshot, input := controllerPrefetchSnapshot(probe)
			defer snapshot.close()
			// Simulate an archived capture's existing streaming lease. The
			// source adapter must release it before per-block I/O admission.
			s.archiveReadSlots <- struct{}{}
			var releaseCapture sync.Once
			snapshot.releaseArchiveRead = func() { releaseCapture.Do(func() { <-s.archiveReadSlots }) }
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			progressBlocked, releaseProgress := make(chan struct{}), make(chan struct{})
			var progressOnce, releaseOnce sync.Once
			var consumed atomic.Int64
			outcomes := make(chan controllerPrefetchOutcome, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				artifacts, result, err := s.computeAnalysisProcess(ctx, input, []*resultSnapshot{snapshot}, root, func(progress analysisProcessProgress) {
					if progress.Bytes != 0 {
						progressOnce.Do(func() {
							consumed.Store(progress.Bytes)
							close(progressBlocked)
							select {
							case <-releaseProgress:
							case <-ctx.Done():
							}
						})
					}
				}, nil)
				outcomes <- controllerPrefetchOutcome{artifacts, result, err}
			}()
			defer func() {
				cancel()
				releaseOnce.Do(func() { close(releaseProgress) })
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("local analysis did not finish source cleanup")
				}
				select {
				case outcome := <-outcomes:
					if outcome.artifacts != nil {
						outcome.artifacts.discard()
					}
				default:
				}
			}()
			select {
			case <-progressBlocked:
			case <-ctx.Done():
				t.Fatal("analysis child did not reach its first input block")
			}
			select {
			case <-probe.watch:
			case <-ctx.Done():
				t.Fatalf("Controller read only %d bytes while its child waited; want ahead queue through %d", probe.maxEnd.Load(), watchEnd)
			}
			// A larger child read can consume one or more source blocks before
			// this progress callback. Measure the unread queue relative to that
			// reported consumption, rather than assuming a 64-KiB scanner read.
			for probe.maxEnd.Load() <= consumed.Load() {
				select {
				case <-ctx.Done():
					t.Fatal("local source transfer did not run ahead of consumed input")
				case <-time.After(time.Millisecond):
				}
			}
			// The child is blocked after its first progress report, so queued
			// input cannot be consumed to create any additional source credit.
			time.Sleep(20 * time.Millisecond)
			budget := config.mib
			if budget == 0 {
				budget = defaultAnalysisPrefetchMiB
			}
			maximum := min(int64(len(data)), consumed.Load()+(int64(budget)<<20))
			if end := probe.maxEnd.Load(); end > maximum || config.mib == 0 && end != int64(len(data)) {
				t.Fatalf("local queue read through %d after consuming %d; budget=%d MiB maximum=%d", end, consumed.Load(), budget, maximum)
			}
			if probe.active.Load() != 0 || len(s.archiveReadSlots) != 0 {
				t.Fatalf("idle local prefetch retained storage admission: reads=%d slots=%d", probe.active.Load(), len(s.archiveReadSlots))
			}
			releaseOnce.Do(func() { close(releaseProgress) })
			select {
			case outcome := <-outcomes:
				if outcome.err != nil {
					t.Fatal(outcome.err)
				}
				defer outcome.artifacts.discard()
				artifact, err := root.ReadFile(outcome.artifacts.sinks["full"].staged.temp)
				if err != nil {
					t.Fatal(err)
				}
				var analysis resultAnalysis
				if err := json.Unmarshal(artifact, &analysis); err != nil || analysis.EventBytes != int64(len(data)) || analysis.EventCount != 0 {
					t.Fatalf("prefetch changed the captured analysis boundary: error=%v bytes=%d events=%d", err, analysis.EventBytes, analysis.EventCount)
				}
			case <-ctx.Done():
				t.Fatal("local prefetched analysis did not complete")
			}
			started := commands()
			if len(started) != 1 || started[0].ProcessState == nil || len(s.archiveReadSlots) != 0 {
				t.Fatal("local analysis retained its child or storage read slots")
			}
		})
	}
}

func TestControllerAnalysisCancellationJoinsEnteredPrefetchRead(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	s.config.AnalysisPrefetchMiB = 2
	line := []byte("{\"runId\":\"unrelated\"}\n")
	probe := &controllerPrefetchProbe{data: bytes.Repeat(line, 4*workerSourceChunk/len(line)+1), blockAt: workerSourceChunk, blocked: make(chan struct{}), release: make(chan struct{})}
	snapshot, input := controllerPrefetchSnapshot(probe)
	defer snapshot.close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var releaseOnce sync.Once
	outcomes := make(chan controllerPrefetchOutcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		artifacts, result, err := s.computeAnalysisProcess(ctx, input, []*resultSnapshot{snapshot}, root, nil, nil)
		outcomes <- controllerPrefetchOutcome{artifacts, result, err}
	}()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(probe.release) })
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("canceled local analysis did not release its blocked source")
		}
		select {
		case outcome := <-outcomes:
			if outcome.artifacts != nil {
				outcome.artifacts.discard()
			}
		default:
		}
	}()
	select {
	case <-probe.blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("local prefetch did not enter the next storage read")
	}
	cancel()
	select {
	case outcome := <-outcomes:
		t.Fatalf("local analysis returned while a pinned storage read remained active: %v", outcome.err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(probe.release) })
	select {
	case outcome := <-outcomes:
		if !errors.Is(outcome.err, context.Canceled) || outcome.artifacts != nil {
			t.Fatalf("canceled local prefetch returned artifacts or lost cancellation: %+v", outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local prefetch did not finish after the entered read returned")
	}
	started := commands()
	if probe.active.Load() != 0 || len(s.archiveReadSlots) != 0 || len(started) != 1 || started[0].ProcessState == nil {
		t.Fatalf("canceled local analysis retained resources: reads=%d slots=%d processes=%d", probe.active.Load(), len(s.archiveReadSlots), len(started))
	}
}

func TestControllerAnalysisRejectsInvalidPrefetchBeforeStartingChild(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	s.config.AnalysisPrefetchMiB = maxAnalysisPrefetchMiB + 1
	probe := &controllerPrefetchProbe{data: []byte("{\"runId\":\"unrelated\"}\n")}
	snapshot, input := controllerPrefetchSnapshot(probe)
	defer snapshot.close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	artifacts, _, err := s.computeAnalysisProcess(t.Context(), input, []*resultSnapshot{snapshot}, root, nil, nil)
	if artifacts != nil {
		defer artifacts.discard()
	}
	if err == nil || !strings.Contains(err.Error(), "prefetch") || artifacts != nil || len(commands()) != 0 || probe.maxEnd.Load() != 0 || len(s.archiveReadSlots) != 0 {
		t.Fatalf("invalid Controller prefetch setting started source work: error=%v artifacts=%t processes=%d bytes=%d slots=%d", err, artifacts != nil, len(commands()), probe.maxEnd.Load(), len(s.archiveReadSlots))
	}
}
