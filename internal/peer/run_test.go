package peer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Lifecycle tests provide a host without P2P listeners after the container startup boundary.
// Actual startup always resolves its overlay address and prepares its namespace.
func newRunTestServer(t *testing.T, config model.PeerProcessConfig) *Server {
	t.Helper()
	config.NodeConfig = config.NodeConfig.WithDefaults()
	if err := config.NodeConfig.Validate(); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &Server{
		config:     config,
		host:       newConfigTestHost(t),
		topics:     make(map[string]*pubsub.Topic),
		peerScores: make(map[string]float64),
		telemetry:  newTelemetry(config.Node, config.AgentURL, config.Token, logger),
		logger:     logger,
		startedAt:  time.Now().UTC(),
	}
}

func TestServerRunCancelsPendingHTTPPublication(t *testing.T) {
	ready := make(chan struct{})
	var readyOnce sync.Once
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/health":
			_ = json.NewEncoder(w).Encode(map[string]any{"time": time.Now().UTC()})
		case "/api/v1/bootstrap":
			_, _ = io.WriteString(w, "[]")
		default:
			_, _ = io.Copy(io.Discard, r.Body)
			if strings.HasSuffix(r.URL.Path, "/status") {
				readyOnce.Do(func() { close(ready) })
			}
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	defer endpoint.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	server := newRunTestServer(t, model.PeerProcessConfig{
		Node:     model.Node{ID: "node", RunID: "run", Role: "boot"},
		AgentURL: endpoint.URL, ControllerURL: endpoint.URL, APListen: address,
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("Peer did not finish shutdown")
		}
	}()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("Peer did not become ready")
	}
	if err := server.publishGate.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer server.publishGate.release()
	written := make(chan struct{})
	var writtenOnce sync.Once
	requestCtx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { writtenOnce.Do(func() { close(written) }) },
	})
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://"+address+"/publish", strings.NewReader(`{"payloadSize":32}`))
	if err != nil {
		t.Fatal(err)
	}
	status := make(chan int, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 7 * time.Second}).Do(request)
		if err != nil {
			status <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		status <- resp.StatusCode
	}()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("publication request was not sent")
	}
	select {
	case code := <-status:
		t.Fatalf("publication bypassed the held gate: status %d", code)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case code := <-status:
		if code != http.StatusGatewayTimeout {
			t.Fatalf("canceled publication returned status %d", code)
		}
	case <-time.After(time.Second):
		t.Fatal("Peer cancellation did not release its pending HTTP publication")
	}
}
