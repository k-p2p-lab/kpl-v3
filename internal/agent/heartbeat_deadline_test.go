package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatSerializationDoesNotOutliveCanceledCleanup(t *testing.T) {
	for _, operation := range []string{"heartbeat", "register", "reset"} {
		for _, expires := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/canceled", true: "/deadline"}[expires], func(t *testing.T) {
				s := historyTestServer(t)
				entered := make(chan struct{}, 1)
				release := make(chan struct{})
				var calls atomic.Int32
				controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					select {
					case entered <- struct{}{}:
					default:
					}
					select {
					case <-release:
					case <-r.Context().Done():
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer controller.Close()
				s.config.ControllerURL, s.client = controller.URL, controller.Client()
				firstCtx, stopFirst := context.WithTimeout(t.Context(), 5*time.Second)
				defer stopFirst()
				first := make(chan error, 1)
				go func() { first <- s.heartbeat(firstCtx) }()
				select {
				case <-entered:
				case <-time.After(time.Second):
					close(release)
					t.Fatal("first heartbeat did not reach Controller")
				}
				// The first real HTTP request remains blocked. Cancellation of a
				// cleanup request must not wait for that unrelated request's reply.
				ctx, cancel := context.WithCancel(t.Context())
				wanted := context.Canceled
				if expires {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
					wanted = context.DeadlineExceeded
				} else {
					cancel()
				}
				defer cancel()
				second := make(chan error, 1)
				go func() {
					switch operation {
					case "heartbeat":
						second <- s.heartbeat(ctx)
					case "register":
						second <- s.register(ctx)
					case "reset":
						second <- s.prepareRunReset(ctx, "previous-run")
					}
				}()
				returned := false
				select {
				case err := <-second:
					returned = true
					if !errors.Is(err, wanted) {
						t.Errorf("canceled %s returned %v", operation, err)
					}
				case <-time.After(400 * time.Millisecond):
					t.Errorf("canceled %s waited behind the unrelated heartbeat HTTP request", operation)
				}
				if calls.Load() != 1 {
					t.Errorf("canceled %s sent another Controller request", operation)
				}
				close(release)
				if err := <-first; err != nil {
					t.Fatal(err)
				}
				if !returned {
					if err := <-second; !errors.Is(err, wanted) {
						t.Errorf("released %s lost cancellation: %v", operation, err)
					}
				}
			})
		}
	}
}
