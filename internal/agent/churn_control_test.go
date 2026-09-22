package agent

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/k-p2p-lab/v3/internal/model"
)

func TestSlowTelemetryDoesNotDelayAgentHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(Config{ID: "agent", DockerImage: "kpl:test", DockerNetwork: "peers", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080", DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var heartbeats []time.Duration
		flushes := 0
		s.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/api/v1/agents/heartbeat":
				heartbeats = append(heartbeats, time.Since(start))
			case "/api/v1/events/batch":
				flushes++
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
		})
		s.enqueueEvents(model.EventBatch{Events: []model.TraceEvent{{RunID: "run", NodeID: "peer", Type: "test"}}})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); s.controlLoop(ctx) }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if flushes < 2 {
			t.Fatal("telemetry stalls were not exercised")
		}
		if len(heartbeats) != 10 {
			t.Fatalf("slow event forwarding suppressed heartbeat: %v", heartbeats)
		}
		for i, at := range heartbeats {
			if want := time.Duration(i+1) * 2 * time.Second; at != want {
				t.Fatalf("heartbeat at %s, want %s", at, want)
			}
		}
	})
}
