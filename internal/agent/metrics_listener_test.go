package agent

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestValidateMetricsEndpoint(t *testing.T) {
	tests := []struct {
		name, listen, url string
		wantErr           bool
	}{
		{name: "disabled"},
		{name: "HTTP", listen: ":9091", url: "http://worker.example:9091/metrics"},
		{name: "HTTPS IPv6", listen: "[::]:9091", url: "https://[2001:db8::1]:9091/metrics"},
		{name: "listen only", listen: ":9091", wantErr: true},
		{name: "URL only", url: "http://worker.example:9091/metrics", wantErr: true},
		{name: "surrounding whitespace", listen: ":9091", url: " http://worker.example:9091/metrics", wantErr: true},
		{name: "relative", listen: ":9091", url: "/metrics", wantErr: true},
		{name: "wrong scheme", listen: ":9091", url: "ftp://worker.example:9091/metrics", wantErr: true},
		{name: "userinfo", listen: ":9091", url: "http://user@worker.example:9091/metrics", wantErr: true},
		{name: "localhost", listen: ":9091", url: "http://localhost:9091/metrics", wantErr: true},
		{name: "loopback IPv4", listen: ":9091", url: "http://127.0.0.1:9091/metrics", wantErr: true},
		{name: "unspecified IPv4", listen: ":9091", url: "http://0.0.0.0:9091/metrics", wantErr: true},
		{name: "loopback IPv6", listen: ":9091", url: "http://[::1]:9091/metrics", wantErr: true},
		{name: "unspecified IPv6", listen: ":9091", url: "http://[::]:9091/metrics", wantErr: true},
		{name: "wrong path", listen: ":9091", url: "http://worker.example:9091/", wantErr: true},
		{name: "trailing slash", listen: ":9091", url: "http://worker.example:9091/metrics/", wantErr: true},
		{name: "encoded path", listen: ":9091", url: "http://worker.example:9091/%6detrics", wantErr: true},
		{name: "query", listen: ":9091", url: "http://worker.example:9091/metrics?format=text", wantErr: true},
		{name: "empty query", listen: ":9091", url: "http://worker.example:9091/metrics?", wantErr: true},
		{name: "fragment", listen: ":9091", url: "http://worker.example:9091/metrics#target", wantErr: true},
		{name: "empty fragment", listen: ":9091", url: "http://worker.example:9091/metrics#", wantErr: true},
		{name: "zero port", listen: ":9091", url: "http://worker.example:0/metrics", wantErr: true},
		{name: "large port", listen: ":9091", url: "http://worker.example:65536/metrics", wantErr: true},
		{name: "empty port", listen: ":9091", url: "http://worker.example:/metrics", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMetricsEndpoint(test.listen, test.url)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateMetricsEndpoint(%q, %q) error=%v, wantErr=%t", test.listen, test.url, err, test.wantErr)
			}
		})
	}
}

func TestNewRequiresCompleteMetricsEndpoint(t *testing.T) {
	_, err := New(Config{
		DockerImage: "registry.example:5000/kpl-v3:test", DockerNetwork: "kpl-v3-peers",
		ID: "agent", AdvertiseURL: "http://agent:8090", ControllerURL: "http://controller:8080",
		MetricsListen: ":9091",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "configured together") {
		t.Fatalf("incomplete metrics configuration was accepted: %v", err)
	}
}

func TestMetricsOnlyHandlerExposesNoControlRoutes(t *testing.T) {
	s := &Server{config: Config{ID: "agent", Capacity: 4, Token: "secret"}, processes: map[string]*process{}}
	handler := s.metricsOnlyHandler()

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `kpl_local_capacity{agent_id="agent"} 4`) {
		t.Fatalf("GET /metrics status=%d body=%s", response.Code, response.Body)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodHead, "/metrics", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("HEAD /metrics status=%d", response.Code)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST /metrics status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}

	for _, path := range []string{"/", "/metrics/", "/api/v1/health", "/api/v1/status", "/api/v1/nodes"} {
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("GET %s status=%d, want 404", path, response.Code)
		}
	}
}

func TestSnapshotAdvertisesMetricsURL(t *testing.T) {
	s := &Server{
		config:    Config{ID: "agent", Name: "Agent", AdvertiseURL: "http://agent:8090/", MetricsURL: "https://worker.example:9091/metrics"},
		startedAt: time.Now(), processes: map[string]*process{},
	}
	if got := s.snapshot().Agent.MetricsURL; got != s.config.MetricsURL {
		t.Fatalf("snapshot metrics URL=%q, want %q", got, s.config.MetricsURL)
	}
}

func TestAgentServesAndGracefullyStopsMetricsEndpoint(t *testing.T) {
	registered := make(chan model.Agent, 1)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		if strings.HasSuffix(r.URL.Path, "/register") {
			var reported model.Agent
			if err := json.NewDecoder(r.Body).Decode(&reported); err != nil {
				t.Errorf("decode registration: %v", err)
			} else {
				select {
				case registered <- reported:
				default:
				}
			}
		} else {
			_, _ = io.Copy(io.Discard, r.Body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()

	// Keep both sockets bound until Serve takes ownership; releasing a chosen
	// port before Agent startup allows the kernel to assign it again.
	controlListener := listenMetricsTestTCP(t)
	metricsListener := listenMetricsTestTCP(t)
	controlAddress := controlListener.Addr().String()
	metricsAddress := metricsListener.Addr().String()
	_, metricsPort, err := net.SplitHostPort(metricsAddress)
	if err != nil {
		t.Fatal(err)
	}
	metricsURL := "http://192.0.2.9:" + metricsPort + "/metrics"
	scrapeURL := "http://" + metricsAddress + "/metrics"
	s, _ := newContainerTestServer(t, nil)
	s.config.Listen = controlAddress
	s.config.ControllerURL = controller.URL
	s.config.MetricsListen = metricsAddress
	s.config.MetricsURL = metricsURL
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.serve(ctx, controlListener, metricsListener) }()
	defer cancel()

	select {
	case report := <-registered:
		if report.MetricsURL != metricsURL {
			t.Fatalf("registered metrics URL=%q, want %q", report.MetricsURL, metricsURL)
		}
	case err := <-done:
		t.Fatalf("Agent stopped before registration: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Agent did not register")
	}

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(scrapeURL)
	if err != nil {
		t.Fatalf("scrape separate metrics endpoint: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics endpoint status=%d", response.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("graceful Agent shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Agent did not stop with its metrics listener")
	}
	if _, err := client.Get(scrapeURL); err == nil {
		t.Fatal("metrics listener remained reachable after shutdown")
	}
}

func TestAgentRunRejectsOccupiedMetricsAddress(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s, err := New(Config{
		DockerImage: "registry.example:5000/kpl-v3:test", DockerNetwork: "kpl-v3-peers",
		ID: "agent", Listen: "127.0.0.1:0", AdvertiseURL: "http://agent:8090",
		ControllerURL: "http://controller:8080", MetricsListen: occupied.Addr().String(),
		MetricsURL: "http://worker.example:9091/metrics", DataDir: t.TempDir(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "metrics endpoint") {
		t.Fatalf("occupied metrics address error=%v", err)
	}
}

func listenMetricsTestTCP(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}
