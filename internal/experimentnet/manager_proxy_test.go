package experimentnet

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerGatewayHealthProxyProcessFixture(t *testing.T) {
	if os.Getenv("KPL_TEST_GATEWAY_HEALTH_PROXY") != "1" {
		return
	}
	m, err := NewManager(ManagerConfig{Stack: "test", PeerNetwork: "test-peers", ControlNetwork: "test-control", ControllerURL: "http://controller:8080", Token: "fixture-only-token", SelfContainer: "manager", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer m.client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := m.gatewayReady(ctx, os.Getenv("KPL_TEST_GATEWAY_HEALTH_URL")); err != nil {
		t.Fatal(err)
	}
}

func TestManagerGatewayHealthConnectsDirectlyDespiteProxyEnvironment(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var localIP net.IP
	for _, address := range addresses {
		if network, ok := address.(*net.IPNet); ok && network.IP.To4() != nil && network.IP.IsPrivate() && !network.IP.IsLoopback() {
			localIP = network.IP
			break
		}
	}
	if localIP == nil {
		t.Skip("no local private IPv4 address for a self-contained proxy test")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(localIP.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	var directRequests, proxyRequests atomic.Int32
	var proxyAuthenticated atomic.Bool
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		directRequests.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-only-token" || r.URL.Path != "/health" {
			t.Error("gateway health request lost its authentication or path")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	target.Listener = listener
	target.Start()
	defer target.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyRequests.Add(1)
		proxyAuthenticated.Store(r.Header.Get("Authorization") == "Bearer fixture-only-token")
		// A proxy's successful response must never prove gateway readiness.
		w.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestManagerGatewayHealthProxyProcessFixture$")
	// A fresh process also isolates ProxyFromEnvironment's cached settings.
	command.Env = []string{"KPL_TEST_GATEWAY_HEALTH_PROXY=1", "KPL_TEST_GATEWAY_HEALTH_URL=" + target.URL,
		"HTTP_PROXY=" + proxy.URL, "HTTPS_PROXY=" + proxy.URL, "NO_PROXY=", "GORACE=atexit_sleep_ms=0"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("gateway health process failed: %v: %s", err, strings.TrimSpace(string(output)))
	}
	if directRequests.Load() != 1 || proxyRequests.Load() != 0 || proxyAuthenticated.Load() {
		t.Fatalf("gateway health did not use its direct private connection: gateway=%d proxy=%d proxyReceivedToken=%t", directRequests.Load(), proxyRequests.Load(), proxyAuthenticated.Load())
	}
}
