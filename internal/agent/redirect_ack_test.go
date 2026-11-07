package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestControllerRedirectNeverAcknowledgesQueuedTelemetry(t *testing.T) {
	for _, status := range []int{302, 303, 307, 308} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/disk=%t", status, disk), func(t *testing.T) {
				var direct atomic.Bool
				var logins atomic.Int32
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/login" {
						logins.Add(1)
						_, _ = io.WriteString(w, "<html>Login required</html>")
						return
					}
					if !direct.Load() {
						http.Redirect(w, r, "/login", status)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer api.Close()
				s := &Server{config: Config{ID: "agent", ControllerURL: api.URL}, client: api.Client(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
				if disk {
					var err error
					s.spool, _, err = openTelemetrySpool(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
				}
				if !s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", EventID: "must-deliver"}}}) {
					t.Fatal("enqueue")
				}
				s.flushEvents(context.Background())
				if pending := s.pendingEventsLocked(); pending != 1 || logins.Load() != 0 {
					t.Fatalf("redirect/login was treated as delivery: pending=%d logins=%d", pending, logins.Load())
				}
				var responseErr *controllerResponseError
				if err := s.postJSON(context.Background(), "/api/v1/agents/heartbeat", model.AgentHeartbeat{}, nil); !errors.As(err, &responseErr) || responseErr.statusCode != status {
					t.Fatalf("redirect status lost: %v", err)
				}
				if s.client.CheckRedirect != nil {
					t.Fatal("changed shared Controller client")
				}
				direct.Store(true)
				s.flushEvents(context.Background())
				if s.pendingEventsLocked() != 0 {
					t.Fatal("direct delivery did not acknowledge the retained event")
				}
			})
		}
	}
}
