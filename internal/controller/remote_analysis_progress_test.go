package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRemoteAnalysisProgressDoesNotBlockChildAndPreservesSourceCounts(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var sent []analysisProcessProgress
	progress := newRemoteAnalysisProgress(t.Context(), func(ctx context.Context, update analysisProcessProgress) error {
		if len(sent) == 0 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		sent = append(sent, update)
		return nil
	}, nil)
	defer func() { once.Do(func() { close(release) }); _ = progress.close() }()
	first := analysisProcessProgress{Snapshot: 0, Phase: "events.jsonl", Bytes: 10}
	if err := progress.enqueue(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, started, "first remote progress exchange")
	for _, update := range []analysisProcessProgress{
		{Snapshot: 0, Phase: "events.jsonl", Bytes: 20},
		{Snapshot: 0, Phase: "events.jsonl", Bytes: 30},
		{Snapshot: 0, Phase: "graph metrics 1/3"},
		{Snapshot: 0, Phase: "graph metrics 2/3"},
		{Snapshot: 1, Phase: "events.jsonl", Bytes: 100},
	} {
		// The first network exchange is still held. Enqueuing child updates
		// must finish without its acknowledgement or the next source block.
		if err := progress.enqueue(t.Context(), update); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- progress.close() }()
	select {
	case err := <-done:
		t.Fatalf("progress closed before its final acknowledgement: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued progress was not drained")
	}
	want := []analysisProcessProgress{first,
		{Snapshot: 0, Phase: "events.jsonl", Bytes: 50},
		{Snapshot: 0, Phase: "graph metrics 2/3"},
		{Snapshot: 1, Phase: "events.jsonl", Bytes: 100}}
	if !reflect.DeepEqual(sent, want) {
		t.Fatalf("progress changed source byte counts or phase order: got=%+v want=%+v", sent, want)
	}
}

func TestRemoteAnalysisProgressQueueIsBounded(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var calls atomic.Int32
	progress := newRemoteAnalysisProgress(t.Context(), func(ctx context.Context, update analysisProcessProgress) error {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}, nil)
	defer func() { once.Do(func() { close(release) }); _ = progress.close() }()
	if err := progress.enqueue(t.Context(), analysisProcessProgress{Phase: "first", Bytes: 1}); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, started, "held progress acknowledgement")
	for snapshot := range remoteAnalysisProgressQueueLimit {
		if err := progress.enqueue(t.Context(), analysisProcessProgress{Snapshot: snapshot, Phase: "events.jsonl", Bytes: 1}); err != nil {
			t.Fatalf("progress reached its configured bound too soon: %v", err)
		}
	}
	if err := progress.enqueue(t.Context(), analysisProcessProgress{Snapshot: remoteAnalysisProgressQueueLimit, Phase: "events.jsonl", Bytes: 1}); err == nil || !strings.Contains(err.Error(), "queue limit") {
		t.Fatalf("unbounded progress backlog accepted: %v", err)
	}
	once.Do(func() { close(release) })
	if err := progress.close(); err != nil || calls.Load() != remoteAnalysisProgressQueueLimit+1 {
		t.Fatalf("bounded queue did not drain its accepted updates: calls=%d error=%v", calls.Load(), err)
	}
}

func TestRemoteAnalysisProgressFailureCancelsCalculationAndRemainsVisible(t *testing.T) {
	failure := errors.New("progress transport unavailable")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var failed atomic.Int32
	progress := newRemoteAnalysisProgress(ctx, func(context.Context, analysisProcessProgress) error { return failure }, func() { failed.Add(1); cancel() })
	if err := progress.enqueue(t.Context(), analysisProcessProgress{Phase: "events.jsonl", Bytes: 1}); err != nil {
		t.Fatal(err)
	}
	if err := progress.close(); !errors.Is(err, failure) || ctx.Err() == nil || failed.Load() != 1 {
		t.Fatalf("sender failure did not stop and preserve calculation failure: error=%v canceled=%v failures=%d", err, ctx.Err(), failed.Load())
	}
	if err := progress.enqueue(t.Context(), analysisProcessProgress{Phase: "events.jsonl", Bytes: 1}); !errors.Is(err, failure) {
		t.Fatalf("failed sender accepted more progress: %v", err)
	}
}

func TestRemoteAnalysisProgressCancellationJoinsInflightExchange(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	var active atomic.Int32
	progress := newRemoteAnalysisProgress(ctx, func(ctx context.Context, update analysisProcessProgress) error {
		active.Add(1)
		defer active.Add(-1)
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	if err := progress.enqueue(t.Context(), analysisProcessProgress{Phase: "events.jsonl", Bytes: 1}); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, started, "cancelable progress exchange")
	cancel()
	if err := progress.close(); err != nil || active.Load() != 0 {
		t.Fatalf("progress cancellation did not join the sender cleanly: error=%v active=%d", err, active.Load())
	}
}

func TestAgentAnalysisFailureDoesNotWaitForSpeculativeIOOrProgressAcknowledgement(t *testing.T) {
	s, commands := isolatedAnalysisServer(t)
	worker := newRemoteAnalysisWorker(remoteTestToken, s.logger, s.workerCommand)
	finished := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == remoteAnalysisPath {
			defer close(finished)
		}
		worker.ServeHTTP(w, r)
	}))
	defer endpoint.Close()
	u, err := remoteAnalysisURL(endpoint.URL)
	if err != nil {
		t.Fatal(err)
	}
	connection, _, err := websocket.DefaultDialer.DialContext(t.Context(), u.String(), http.Header{"Authorization": {"Bearer " + remoteTestToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	socket := openRemoteAnalysisSocket(ctx, connection, nil)
	defer socket.close()
	input := analysisProcessInput{RunID: "bad-input", AnalysisID: "bad-input-analysis", CPUs: 1,
		Snapshots: []workerSnapshotDescriptor{{Result: savedResult{ID: "bad-input", State: "completed"}, Files: []workerSourceDescriptor{{Name: "events.jsonl", Size: 3 * workerSourceChunk}}}}}
	if err := socket.send("start", remoteAnalysisHello{Protocol: remoteAnalysisProtocol, AnalysisVersion: currentAnalysisVersion, PrefetchMiB: 2, Build: model.BuildDetails().BinaryHash, Input: input}, nil); err != nil {
		t.Fatal(err)
	}
	first := make([]byte, workerSourceChunk)
	copy(first, "not-json\n")
	delivered := false
	for {
		frame, err := socket.receive(ctx)
		if err != nil {
			t.Fatalf("analysis failed to terminate while a future reply was deliberately withheld: %v", err)
		}
		switch frame.Method {
		case "analysis.progress":
			var update analysisProcessProgress
			if err := json.Unmarshal(frame.Body, &update); err != nil {
				t.Fatal(err)
			}
			if update.Bytes == 0 {
				if err := socket.send("reply", nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			// Positive-byte progress remains unacknowledged. It must not
			// prevent the child parsing bytes already delivered to it.
		case "source.read":
			var read workerSourceRead
			if err := json.Unmarshal(frame.Body, &read); err != nil {
				t.Fatal(err)
			}
			if read.Offset == 0 {
				if delivered || read.Length != workerSourceChunk {
					t.Fatalf("unexpected first source read: %+v", read)
				}
				delivered = true
				if err := socket.send("reply", false, first); err != nil {
					t.Fatal(err)
				}
			}
			// Leave all speculative future source replies unanswered too.
		case "error":
			var message string
			if json.Unmarshal(frame.Body, &message) != nil || !delivered || !strings.Contains(message, "events.jsonl line 1") {
				t.Fatalf("original parser failure was lost: delivered=%t message=%q", delivered, message)
			}
			queueTestAwait(t, finished, "Agent terminal child cleanup")
			if processes := commands(); len(processes) != 1 || processes[0].ProcessState == nil {
				t.Fatal("Agent released its calculation before reaping the failed child")
			}
			if err := probeRemoteAnalysisIdle(t.Context(), endpoint.URL, remoteTestToken); err != nil {
				t.Fatalf("failed calculation retained the Agent slot: %v", err)
			}
			return
		default:
			t.Fatalf("unexpected Agent frame before parser failure: %s", frame.Method)
		}
	}
}
