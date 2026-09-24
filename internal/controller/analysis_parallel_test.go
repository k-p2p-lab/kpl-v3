package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// Distinct sparse mesh snapshots exercise the expensive all-source graph metrics.
func graphAnalysisFixture(count, nodes int) []analysisObservation {
	observations := make([]analysisObservation, count)
	for i := range observations {
		raw := analysisGraph{Protocol: "gossipsub", Nodes: make([]string, nodes), Groups: make([]string, nodes)}
		for node := range raw.Nodes {
			raw.Nodes[node] = fmt.Sprintf("peer-%d", node)
			raw.Groups[node] = "worker"
			for _, offset := range []int{1, 3, 7} {
				raw.Edges = append(raw.Edges, [2]int{node, (node + offset) % nodes})
			}
		}
		raw.Edges = append(raw.Edges, [2]int{i % nodes, (i + nodes/2) % nodes})
		observations[i] = analysisObservation{RunID: "run", At: time.Unix(int64(i*5), 0).UTC(), Graphs: []analysisGraph{raw}, Groups: []analysisGroup{
			{Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: nodes}}},
			{Group: "worker", Layers: []analysisLayer{{Protocol: "gossipsub", Nodes: nodes}}},
		}}
	}
	return observations
}

func BenchmarkAnalysisGraphs(b *testing.B) {
	for _, workers := range []int{1, 4} {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				observations := graphAnalysisFixture(24, 200)
				b.StartTimer()
				if err := enrichAnalysisGraphsWithWorkers(context.Background(), observations, workers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestParallelAnalysisBoundsWorkersAndOwnsEachIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan int, 12)
	release := make(chan struct{})
	done := make(chan error, 1)
	values := make([]int, 12)
	var active, peak atomic.Int32
	go func() {
		done <- parallelAnalysis(ctx, len(values), 3, func(ctx context.Context, index int) error {
			now := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); now > old; old = peak.Load() {
				if peak.CompareAndSwap(old, now) {
					break
				}
			}
			started <- index
			select {
			case <-release:
				values[index]++
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	for range 3 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers did not overlap")
		}
	}
	if active.Load() != 3 {
		t.Fatalf("expected three active workers, got %d", active.Load())
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if peak.Load() != 3 || active.Load() != 0 {
		t.Fatalf("worker bound or join failed: peak=%d active=%d", peak.Load(), active.Load())
	}
	for index, count := range values {
		if count != 1 {
			t.Fatalf("index %d processed %d times", index, count)
		}
	}
}

func TestParallelAnalysisCancelsSiblingsAndRetainsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{}, 2)
	failure := errors.New("invalid graph input")
	var active atomic.Int32
	err := parallelAnalysis(ctx, 100, 3, func(ctx context.Context, index int) error {
		active.Add(1)
		defer active.Add(-1)
		if index == 0 {
			for range 2 {
				select {
				case <-started:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return failure
		}
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, failure) || active.Load() != 0 {
		t.Fatalf("failure=%v, active workers=%d", err, active.Load())
	}
	cancel()
	for _, workers := range []int{1, 4} {
		err = parallelAnalysis(ctx, 10, workers, func(context.Context, int) error {
			t.Error("started work after cancellation")
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation with %d workers: %v", workers, err)
		}
	}
}

func TestAnalysisParallelismReservesCapacityAndIsBounded(t *testing.T) {
	previous := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(previous)
	for _, check := range []struct{ cpus, workers int }{{1, 1}, {2, 1}, {3, 2}, {4, 3}, {8, 4}, {32, 4}} {
		runtime.GOMAXPROCS(check.cpus)
		if got := analysisParallelism(); got != check.workers {
			t.Fatalf("%d CPUs: got %d workers, want %d", check.cpus, got, check.workers)
		}
	}
}

func TestAnalysisGraphCacheSharesConcurrentResultsAndBoundsEntries(t *testing.T) {
	var cache analysisGraphCache
	raw := graphAnalysisFixture(1, 64)[0].Graphs[0]
	results := make([]graphStatistics, 16)
	if err := parallelAnalysis(context.Background(), len(results), 4, func(ctx context.Context, index int) error {
		var err error
		results[index], err = cache.calculate(ctx, raw)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries) != 1 {
		t.Fatalf("duplicate graphs added %d entries", len(cache.entries))
	}
	for _, result := range results {
		if result.Values["node_count"] != results[0].Values["node_count"] {
			t.Fatal("duplicate graph calculations did not share their result")
		}
	}
	if err := parallelAnalysis(context.Background(), 300, 4, func(ctx context.Context, index int) error {
		_, err := cache.calculate(ctx, analysisGraph{Nodes: []string{fmt.Sprint(index)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries) != 256 {
		t.Fatalf("cache exceeded the retained graph limit: %d", len(cache.entries))
	}
}

func TestParallelGraphMetricsPreserveSerialValuesOrderAndProgress(t *testing.T) {
	serial := graphAnalysisFixture(20, 24)
	// Include another layer, duplicate snapshots and legacy degree-only data.
	for index := range serial {
		transport := serial[index].Graphs[0]
		transport.Protocol = "transport"
		serial[index].Graphs = append(serial[index].Graphs, transport)
		for group := range serial[index].Groups {
			serial[index].Groups[group].Layers = append(serial[index].Groups[group].Layers, analysisLayer{Protocol: "transport"})
		}
	}
	serial[7].Graphs = serial[0].Graphs
	serial[8].Graphs = nil
	serial[8].Groups[0].Layers[0].Degrees = []analysisPoint{{1, 4}, {2, 2}}
	data, err := json.Marshal(serial)
	if err != nil {
		t.Fatal(err)
	}
	var parallel []analysisObservation
	if err := json.Unmarshal(data, &parallel); err != nil {
		t.Fatal(err)
	}
	if err := enrichAnalysisGraphsWithWorkers(context.Background(), serial, 1); err != nil {
		t.Fatal(err)
	}
	// The deliberately unsynchronized callback verifies that the worker pool
	// serializes reports; the race detector checks concurrent callback access.
	completed := -1
	report := analysisProgress(func(phase string, bytes int64) {
		var count, total int
		if _, err := fmt.Sscanf(phase, "graph metrics %d/%d", &count, &total); err != nil || count != completed+1 || total != len(parallel) || bytes != 0 {
			t.Errorf("non-monotonic graph progress: %q, after %d", phase, completed)
		}
		completed = count
	})
	ctx := context.WithValue(context.Background(), analysisProgressKey{}, report)
	if err := enrichAnalysisGraphsWithWorkers(ctx, parallel, 4); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(serial, parallel) || completed != len(parallel) {
		t.Fatal("parallel graph metrics changed values, ordering or progress")
	}
	for _, observation := range parallel {
		if observation.Graphs != nil {
			t.Fatal("raw graphs retained in completed analysis")
		}
	}
}

func TestParallelGraphMetricsPropagateErrorsAndCancellation(t *testing.T) {
	observations := graphAnalysisFixture(12, 24)
	observations[0].Graphs[0].Edges = [][2]int{{0, 99}}
	if err := enrichAnalysisGraphsWithWorkers(context.Background(), observations, 4); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("graph failure was lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	report := analysisProgress(func(phase string, bytes int64) {
		if phase == "graph metrics 1/12" {
			cancel()
		}
	})
	ctx = context.WithValue(ctx, analysisProgressKey{}, report)
	defer cancel()
	if err := enrichAnalysisGraphsWithWorkers(ctx, graphAnalysisFixture(12, 24), 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight cancellation: %v", err)
	}
}
