package controller

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestWorkerRecoveryFetchesTerminalInventoryOnce(t *testing.T) {
	f := newControllerWorkerFixture(t)
	const id = "recovered-inventory"
	run, _ := resultFixture(t, f.server, id, "failed", time.Now().Add(-time.Minute))
	f.server.state.experiments[id] = run
	var full, identity atomic.Int32
	f.server.client.Transport = discoveryTransport(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/api/v1/status" {
			if request.URL.Query().Get("view") == "agent" {
				identity.Add(1)
			} else {
				full.Add(1)
			}
		}
		return http.DefaultTransport.RoundTrip(request)
	})
	if err := f.server.cleanupBeforeResume(t.Context(), []string{id}, scenario.Scenario{JobShutdownTimeout: "5s"}); err != nil {
		t.Fatal(err)
	}
	if identity.Load() != 1 || full.Load() != 1 {
		t.Fatalf("recovery duplicated terminal inventory: compact identity=%d full inventory=%d", identity.Load(), full.Load())
	}
	if err := f.server.verifyRetiredRuntime(id); err != nil {
		t.Fatalf("recovery skipped authoritative cleanup evidence: %v", err)
	}
}
