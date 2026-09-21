package controller

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// HTTP/2 expires a response's write deadline even between writes. The 10-second
// deadline must protect a write only, not terminate an idle 15-second heartbeat.
func TestStreamHTTP2SurvivesIdleWriteDeadline(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server := httptest.NewUnstartedServer(s.apiTestHandler(context.Background()))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/v1/stream?view=dashboard", nil)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 {
		t.Fatalf("test needs HTTP/2, got %s", response.Proto)
	}
	scanner := bufio.NewScanner(response.Body)
	seenSnapshot := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "event: snapshot" {
			seenSnapshot = true
		}
		if seenSnapshot && (strings.HasPrefix(line, ": keep-alive") || line == "event: heartbeat") {
			return
		}
	}
	t.Fatalf("idle stream closed before its first heartbeat: %v", scanner.Err())
}

type streamDeadlineWriter struct {
	*httptest.ResponseRecorder
	mu                   sync.Mutex
	deadlines            []time.Time
	blocked, interrupted chan struct{}
	once                 sync.Once
	flushError           bool
}

func (w *streamDeadlineWriter) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, at)
	w.mu.Unlock()
	if w.interrupted != nil && !at.IsZero() && !at.After(time.Now()) {
		w.once.Do(func() { close(w.interrupted) })
	}
	return nil
}
func (w *streamDeadlineWriter) WriteString(value string) (int, error) { return w.Write([]byte(value)) }
func (w *streamDeadlineWriter) Write(data []byte) (int, error) {
	if w.blocked != nil {
		close(w.blocked)
		<-w.interrupted
		return 0, context.DeadlineExceeded
	}
	return w.ResponseRecorder.Write(data)
}
func (w *streamDeadlineWriter) FlushError() error {
	if w.flushError {
		return errors.New("flush failed")
	}
	w.ResponseRecorder.Flush()
	return nil
}

func TestStreamWriteClearsDeadlineOnFlushAndFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		w := &streamDeadlineWriter{ResponseRecorder: httptest.NewRecorder(), flushError: failed}
		err := writeStreamEvent(context.Background(), w, "heartbeat", []byte("{}"))
		if (err != nil) != failed {
			t.Fatalf("flush failure=%v, error=%v", failed, err)
		}
		if len(w.deadlines) != 2 || w.deadlines[0].IsZero() || !w.deadlines[1].IsZero() {
			t.Fatalf("write deadline not scoped to the write: %v", w.deadlines)
		}
		if w.Body.String() != "event: heartbeat\ndata: {}\n\n" {
			t.Fatal("incorrect heartbeat framing")
		}
	}
}

func TestStreamWriteCancellationInterruptsSlowClientAndClearsDeadline(t *testing.T) {
	w := &streamDeadlineWriter{ResponseRecorder: httptest.NewRecorder(), blocked: make(chan struct{}), interrupted: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- writeStreamEvent(ctx, w, "snapshot", []byte("{}")) }()
	select {
	case <-w.blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("write did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled write succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not interrupt write")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.deadlines) < 3 || !w.deadlines[len(w.deadlines)-1].IsZero() {
		t.Fatalf("cancellation left an expired deadline: %v", w.deadlines)
	}
}
