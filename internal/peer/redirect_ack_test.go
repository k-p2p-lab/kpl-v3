package peer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAgentRedirectNeverAcknowledgesPeerTelemetry(t *testing.T) {
	for _, status := range []int{302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
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
			node := model.Node{ID: "peer", RunID: "run", AgentID: "agent"}
			telemetry := newTelemetry(node, api.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
			events := []model.TraceEvent{{EventID: "must-deliver", RunID: "run", NodeID: "peer"}}
			if count, err := telemetry.flush(context.Background(), events); err == nil || count != 0 || logins.Load() != 0 {
				t.Fatalf("redirect/login acknowledged source events: count=%d logins=%d err=%v", count, logins.Load(), err)
			}
			if err := telemetry.reportNode(context.Background(), node); err == nil {
				t.Fatal("redirect/login acknowledged Peer status")
			}
			if telemetry.client.CheckRedirect != nil {
				t.Fatal("changed shared clock/telemetry client")
			}
			direct.Store(true)
			if count, err := telemetry.flush(context.Background(), events); err != nil || count != 1 {
				t.Fatalf("retained source event could not be delivered: count=%d err=%v", count, err)
			}
		})
	}
}
