package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorkQueueCancelAPIAuthenticatesAndRetainsActiveCleanup(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir(), User: "queue-user", Password: "queue-test-only"}, nil)
	head := queueTestEnqueue(t, s, "analysis")
	queueTestAwait(t, head.entered, "analysis")
	waiting := queueTestEnqueue(t, s, "batch-analysis")
	request := func(method, id string, authenticated bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/v1/work-queue/"+id+"/cancel", nil)
		if authenticated {
			authenticateRequest(t, s, r)
		}
		w := httptest.NewRecorder()
		s.Handler(context.Background()).ServeHTTP(w, r)
		return w
	}
	if w := request(http.MethodPost, waiting.slot.id, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated cancellation: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodGet, waiting.slot.id, true); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method accepted: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodPost, "unknown-work", true); w.Code != http.StatusNotFound {
		t.Fatalf("unknown ID accepted: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodPost, waiting.slot.id, true); w.Code != http.StatusAccepted {
		t.Fatalf("queued cancellation failed: %d %s", w.Code, w.Body)
	}
	<-waiting.done
	if w := request(http.MethodPost, waiting.slot.id, true); w.Code != http.StatusAccepted {
		t.Fatalf("duplicate cancellation unsafe: %d %s", w.Code, w.Body)
	}
	if w := request(http.MethodPost, head.slot.id, true); w.Code != http.StatusAccepted {
		t.Fatalf("active cancellation failed: %d %s", w.Code, w.Body)
	}
	queue := s.state.snapshot().WorkQueue
	if len(queue) != 1 || queue[0].ID != head.slot.id || queue[0].Phase != "Stopping" {
		t.Fatalf("API released active work before cleanup: %+v", queue)
	}
	head.once.Do(func() { close(head.release) })
	s.workWorkers.Wait()
}
