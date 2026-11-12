package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func decodedAnalysisTestFile(name, text string) resultFile {
	return resultFile{name: name, size: int64(len(text)), readAt: strings.NewReader(text)}
}

func serialDecodedAnalysisTestScan[T any](ctx context.Context, file resultFile, consume func(T) error) error {
	return scanAnalysisFile(ctx, file, func(data []byte) error {
		var value T
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		return consume(value)
	})
}

func TestDecodedAnalysisPreservesSerialValuesAndOrder(t *testing.T) {
	type row struct {
		Index   int            `json:"index"`
		Payload string         `json:"payload"`
		Fields  map[string]int `json:"fields"`
	}
	var text bytes.Buffer
	encoder := json.NewEncoder(&text)
	for i := range 6000 {
		// Large and small rows cross both Scanner and decoder-buffer boundaries.
		padding := 17
		if i%53 == 0 {
			padding = 16 << 10
		}
		if i%17 == 0 {
			// Omitted fields must start from zero instead of retaining the
			// preceding row's map or payload when decoder storage is reused.
			if err := encoder.Encode(map[string]int{"index": i}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := encoder.Encode(row{Index: i, Payload: strings.Repeat("x", padding), Fields: map[string]int{"value": i}}); err != nil {
			t.Fatal(err)
		}
	}
	file := decodedAnalysisTestFile("events.jsonl", text.String())
	var serial []row
	if err := serialDecodedAnalysisTestScan(t.Context(), file, func(value row) error {
		serial = append(serial, value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			var decoded []row
			if err := scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, func(value row) error {
				decoded = append(decoded, value)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(serial, decoded) {
				t.Fatalf("%d decode workers changed %d source rows or their order", workers, len(serial))
			}
		})
	}
}

func TestDecodedAnalysisRetainsEarliestSourceError(t *testing.T) {
	type row struct {
		Index int `json:"index"`
	}
	failure := errors.New("consumer rejected row")
	for _, check := range []struct {
		name, text string
		failAt     int
	}{
		{name: "invalid-json", text: "{\"index\":1}\ninvalid\n{\"index\":3}\ninvalid\n"},
		{name: "invalid-type", text: "{\"index\":1}\n{\"index\":\"bad\"}\ninvalid\n"},
		{name: "consumer-before-parser", text: "{\"index\":1}\ninvalid\n", failAt: 1},
		{name: "blank-line", text: "{\"index\":1}\n\n{\"index\":3}\n"},
		{name: "trailing-json", text: "{\"index\":1}\n{\"index\":2} {}\n"},
		{name: "last-row-without-newline", text: "{\"index\":1}\n{\"index\":2}"},
	} {
		t.Run(check.name, func(t *testing.T) {
			file := decodedAnalysisTestFile("events.jsonl", check.text)
			run := func(workers int) ([]row, error) {
				var values []row
				consume := func(value row) error {
					if value.Index == check.failAt {
						return failure
					}
					values = append(values, value)
					return nil
				}
				err := scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, consume)
				return values, err
			}
			// Store the baseline only after the callback has run.
			var expected []row
			expectedErr := serialDecodedAnalysisTestScan(t.Context(), file, func(value row) error {
				if value.Index == check.failAt {
					return failure
				}
				expected = append(expected, value)
				return nil
			})
			for _, workers := range []int{1, 4} {
				values, err := run(workers)
				if fmt.Sprint(err) != fmt.Sprint(expectedErr) || !reflect.DeepEqual(values, expected) || errors.Is(err, failure) != errors.Is(expectedErr, failure) {
					t.Fatalf("%d workers: values=%v error=%v, want values=%v error=%v", workers, values, err, expected, expectedErr)
				}
			}
		})
	}
}

func TestDecodedAnalysisRetainsEarliestCrossBatchError(t *testing.T) {
	type row struct {
		Index int `json:"index"`
	}
	padding := strings.Repeat("x", 1<<20)
	input := fmt.Sprintf("{\"index\":1,\"padding\":%q}\n{\"index\":\"bad\",\"padding\":%q}\n{\"index\":3,\"padding\":%q}\ninvalid\n", padding, padding, padding)
	file := decodedAnalysisTestFile("events.jsonl", input)
	for _, consumerFails := range []bool{false, true} {
		failure := errors.New("reject first record")
		var expected []row
		baselineErr := serialDecodedAnalysisTestScan(t.Context(), file, func(value row) error {
			if consumerFails {
				return failure
			}
			expected = append(expected, value)
			return nil
		})
		for _, workers := range []int{2, 4} {
			var got []row
			err := scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, func(value row) error {
				if consumerFails {
					return failure
				}
				got = append(got, value)
				return nil
			})
			if fmt.Sprint(err) != fmt.Sprint(baselineErr) || errors.Is(err, failure) != errors.Is(baselineErr, failure) || !reflect.DeepEqual(got, expected) {
				t.Fatalf("%d workers, consumerFails=%t: error ordering changed across batches: values=%v error=%v, want values=%v error=%v", workers, consumerFails, got, err, expected, baselineErr)
			}
		}
	}
}

func TestDecodedAnalysisRetainsScannerLimitAndEarlierConsumerFailure(t *testing.T) {
	file := decodedAnalysisTestFile("observations.jsonl", "{\"index\":1}\n{\"index\":2,\"padding\":\""+strings.Repeat("x", 16<<20)+"\"}\n")
	type row struct {
		Index int `json:"index"`
	}
	for _, workers := range []int{1, 4} {
		var calls int
		err := scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, func(value row) error { calls++; return nil })
		if err == nil || !strings.Contains(err.Error(), "read observations.jsonl: bufio.Scanner: token too long") || calls != 1 {
			t.Fatalf("%d workers: scanner limit error=%v, callbacks=%d", workers, err, calls)
		}
		failure := errors.New("reject first observation")
		err = scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, func(value row) error { return failure })
		if !errors.Is(err, failure) || !strings.Contains(err.Error(), "observations.jsonl line 1") {
			t.Fatalf("%d workers: later oversized input replaced earlier consumer failure: %v", workers, err)
		}
	}
}

type decodedAnalysisFailingReader struct {
	data []byte
	err  error
}

func (reader decodedAnalysisFailingReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset >= int64(len(reader.data)) {
		return 0, reader.err
	}
	n := copy(p, reader.data[offset:])
	if n < len(p) {
		return n, reader.err
	}
	return n, nil
}

func TestDecodedAnalysisPreservesSourceReadErrors(t *testing.T) {
	type row struct {
		Index int `json:"index"`
	}
	var input bytes.Buffer
	for i := range 3000 {
		fmt.Fprintf(&input, "{\"index\":%d,\"padding\":%q}\n", i, strings.Repeat("x", 128))
	}
	sourceFailure := fmt.Errorf("source disconnected: %w", io.ErrUnexpectedEOF)
	file := resultFile{name: "events.jsonl", size: int64(input.Len() + 1024), readAt: decodedAnalysisFailingReader{data: input.Bytes(), err: sourceFailure}}
	for _, failAt := range []int{-1, 17} {
		consumerFailure := errors.New("invalid metric")
		var expected []row
		baselineErr := serialDecodedAnalysisTestScan(t.Context(), file, func(value row) error {
			if value.Index == failAt {
				return consumerFailure
			}
			expected = append(expected, value)
			return nil
		})
		for _, workers := range []int{1, 4} {
			var got []row
			err := scanDecodedAnalysisFileWithWorkers(t.Context(), file, workers, func(value row) error {
				if value.Index == failAt {
					return consumerFailure
				}
				got = append(got, value)
				return nil
			})
			if fmt.Sprint(err) != fmt.Sprint(baselineErr) || errors.Is(err, sourceFailure) != errors.Is(baselineErr, sourceFailure) || !reflect.DeepEqual(got, expected) {
				t.Fatalf("%d workers, failAt=%d: read/callback outcome changed: rows=%d error=%v, want rows=%d error=%v", workers, failAt, len(got), err, len(expected), baselineErr)
			}
		}
	}
}

type decodedAnalysisTestGate struct {
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
}

var decodedAnalysisTestGates sync.Map

type decodedAnalysisBlockingRow struct {
	Token string `json:"token"`
}

func (row *decodedAnalysisBlockingRow) UnmarshalJSON(data []byte) error {
	type plain decodedAnalysisBlockingRow
	if err := json.Unmarshal(data, (*plain)(row)); err != nil {
		return err
	}
	value, ok := decodedAnalysisTestGates.Load(row.Token)
	if !ok {
		return errors.New("missing test decode gate")
	}
	gate := value.(*decodedAnalysisTestGate)
	gate.active.Add(1)
	defer gate.active.Add(-1)
	close(gate.started)
	<-gate.release
	return nil
}

func TestDecodedAnalysisUsesConfiguredDecoderConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gates := make([]*decodedAnalysisTestGate, 6)
	var text bytes.Buffer
	want := make([]string, len(gates))
	for index := range want {
		want[index] = fmt.Sprintf("%s/%d", t.Name(), index)
	}
	for i, token := range want {
		gate := &decodedAnalysisTestGate{started: make(chan struct{}), release: make(chan struct{})}
		gates[i] = gate
		decodedAnalysisTestGates.Store(token, gate)
		defer decodedAnalysisTestGates.Delete(token)
		defer close(gate.release)
		// Large independent JSON records ensure both decoders receive work even
		// when the implementation batches smaller lines to reduce overhead.
		if err := json.NewEncoder(&text).Encode(map[string]string{"token": token, "padding": strings.Repeat("x", 1<<20)}); err != nil {
			t.Fatal(err)
		}
	}
	file := decodedAnalysisTestFile("events.jsonl", text.String())
	done := make(chan error, 1)
	var got []string
	go func() {
		done <- scanDecodedAnalysisFileWithWorkers(ctx, file, len(gates), func(value decodedAnalysisBlockingRow) error {
			got = append(got, value.Token)
			return nil
		})
	}()
	for _, gate := range gates {
		select {
		case <-gate.started:
		case <-time.After(5 * time.Second):
			t.Fatal("JSON decoders did not overlap")
		}
	}
	// Defer owns each close. Six blocked decoders prove configured concurrency
	// exceeds the old four-worker cap, independently of physical CPU timing.
	// Cancellation exercises cleanup of every outstanding batch.
	cancel()
	for _, gate := range gates {
		gate.release <- struct{}{}
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || len(got) != 0 {
			t.Fatalf("concurrent cancellation: error=%v callbacks=%v", err, got)
		}
		for _, gate := range gates {
			if gate.active.Load() != 0 {
				t.Fatal("scan returned before all concurrent decoders stopped")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent decoders were not reclaimed")
	}
}

func TestDecodedAnalysisCancellationWaitsForDecoderCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gate := &decodedAnalysisTestGate{started: make(chan struct{}), release: make(chan struct{})}
	token := t.Name()
	decodedAnalysisTestGates.Store(token, gate)
	defer decodedAnalysisTestGates.Delete(token)
	var release sync.Once
	defer release.Do(func() { close(gate.release) })
	data, err := json.Marshal(decodedAnalysisBlockingRow{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	file := decodedAnalysisTestFile("events.jsonl", string(data)+"\n")
	done := make(chan error, 1)
	var calls atomic.Int32
	go func() {
		done <- scanDecodedAnalysisFileWithWorkers(ctx, file, 4, func(value decodedAnalysisBlockingRow) error {
			calls.Add(1)
			return nil
		})
	}()
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("decoder did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("decoder still owns its input but scan already returned: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(gate.release) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || gate.active.Load() != 0 || calls.Load() != 0 {
			t.Fatalf("cancel error=%v active decoders=%d callbacks=%d", err, gate.active.Load(), calls.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled decoder was not reclaimed")
	}
}

func TestDecodedAnalysisFullResultMatchesSerialOrder(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	at := time.Unix(1000, 0).UTC()
	resultFixture(t, s, "run", "completed", at)
	first := bandwidthEvent("s", 1, 10, 100, 200, false)
	last := bandwidthEvent("s", 3, 40, 400, 800, true)
	events := []model.TraceEvent{cohortPublish("message", "topic", []string{"receiver"}), cohortDelivery("message", "topic", "receiver", 20), first, last, first, bandwidthEvent("s", 2, 20, 200, 400, false)}
	for i := range 5000 {
		events = append(events, model.TraceEvent{RunID: "run", NodeID: "node", EventID: fmt.Sprint(i), Type: "other", Timestamp: at.Add(time.Duration(i) * time.Second), Fields: map[string]any{"padding": strings.Repeat("x", 128)}})
	}
	analysisWriteLines(t, s, "run", "events.jsonl", events)
	observations := make([]analysisObservation, 3000)
	for i := range observations {
		observation := analysisObservation{RunID: "run", At: at.Add(time.Duration(i) * 5 * time.Second), Groups: []analysisGroup{{Group: "worker", Layers: []analysisLayer{}}}}
		if i%7 != 1 {
			observation.Groups[0].ScoreComponents = map[string]model.ScoreStatistic{"p1": {Count: 2, Mean: float64(i%5 - 2), Min: -2, Max: 2}}
		}
		if i == 29 {
			observation.At = at.Add(time.Second)
		}
		observations[i] = observation
	}
	analysisWriteLines(t, s, "run", "observations.jsonl", observations)
	snapshot, err := s.captureResultFiles("run", false)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.close()
	snapshot.exportedAt = at.Add(24 * time.Hour)
	serial, err := analyzeResultWithWorkers(t.Context(), snapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := analyzeResultWithWorkers(t.Context(), snapshot, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(serial, parallel) {
		t.Fatal("parallel JSON decode changed the complete analysis result")
	}
	if parallel.Metrics.Bandwidth == nil || parallel.Metrics.Bandwidth.SupersededSamples != 1 || len(parallel.ScoreTimeline) == 0 || parallel.ObservationCount != len(observations) || parallel.Metrics.LatencySamples != 1 {
		t.Fatalf("fixture did not exercise bandwidth ordering, scores and message delivery: metrics=%+v scores=%d observations=%d", parallel.Metrics, len(parallel.ScoreTimeline), parallel.ObservationCount)
	}
}
