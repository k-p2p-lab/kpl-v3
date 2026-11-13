package experimentnet

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestManagerAcknowledgedNetworkWithStoppedGatewayWaitsWithoutRestartingIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, fake, request := managerFixture(t)
		first, err := m.prepare(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		// Retain the acknowledged container and network, but make its listener
		// unavailable as it would be after the standalone gateway exits.
		healthyTransport := m.client.Transport
		m.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("gateway listener is unavailable")
		})
		mutations := len(fake.mutations)
		started := time.Now()
		if _, err := m.prepare(t.Context(), request); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "gateway not ready") {
			t.Fatalf("unavailable acknowledged gateway was not reported: %v", err)
		}
		if elapsed := time.Since(started); elapsed != 20*time.Second {
			t.Fatalf("gateway readiness did not use its bounded wait: %v", elapsed)
		}
		if len(fake.mutations) != mutations {
			t.Fatal("acknowledgment replay unexpectedly mutated the active network")
		}
		m.client.Transport = healthyTransport
		request.Epoch = "new-attempt"
		request.RequestedAt = time.Now().UTC()
		result, err := m.prepare(t.Context(), request)
		if err != nil || result.NetworkID == "" || result.NetworkID == first.NetworkID {
			t.Fatalf("new attempt did not replace the stopped gateway and network: result=%+v err=%v", result, err)
		}
	})
}

func TestManagerAbsentOverlayBypassesIncompletePreparationUsingSavedAllocation(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.failStart = true
	if _, err := m.prepare(t.Context(), request); err == nil {
		t.Fatal("gateway start failure was ignored")
	}
	oldID := fake.network.ID
	if _, err := m.prepare(t.Context(), request); err == nil || !strings.Contains(err.Error(), "previous preparation is incomplete") {
		t.Fatalf("remaining overlay did not retain incomplete state: %v", err)
	}
	// Model an externally confirmed gateway removal followed by overlay removal.
	// Docker would reject removal while the gateway remains attached.
	fake.gateway, fake.network, fake.failStart = false, nil, false
	result, err := m.prepare(t.Context(), request)
	if err != nil || result.NetworkID == "" || result.NetworkID == oldID {
		t.Fatalf("absent overlay did not recover from saved allocation: result=%+v err=%v", result, err)
	}
	if fake.network.IPAM.Config[0].Subnet != "10.11.0.0/16" || fake.network.IPAM.Config[0].Gateway != "10.11.0.1" {
		t.Fatal("recovery did not preserve the saved allocation")
	}
}

func TestGatewayLivenessDoesNotAssertUpstreamReachability(t *testing.T) {
	handler, err := GatewayHandler(GatewayConfig{Token: "test-key", ControllerURL: "http://controller:8080", Subnet: "10.11.0.0/16"}, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("control upstream is unavailable")
	}))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, tc := range []struct {
		path string
		want int
	}{{"/health", http.StatusNoContent}, {"/controller/api/v1/health", http.StatusBadGateway}} {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer test-key")
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != tc.want {
			t.Fatalf("%s returned %d, want %d", tc.path, response.StatusCode, tc.want)
		}
	}
}
