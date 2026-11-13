package experimentnet

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayRoutesOnlyConfiguredControlAndPeerAPI(t *testing.T) {
	config := GatewayConfig{Token: "secret", ControllerURL: "http://controller:8080", Agents: map[string]string{"agent-one": "http://agent:8090"}, Subnet: "10.11.0.0/16"}
	var destination string
	var payload string
	handler, err := GatewayHandler(config, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		destination = r.URL.String()
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			payload = string(data)
		}
		return &http.Response{StatusCode: http.StatusAccepted, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("upstream"))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path, token, want string
		status            int
	}{
		{"/peers/10.11.1.2/topology", "secret", "http://10.11.1.2:18000/topology", 202},
		{"/agents/agent-one/api/v1/telemetry", "secret", "http://agent:8090/api/v1/telemetry", 202},
		{"/controller/api/v1/discovery", "secret", "http://controller:8080/api/v1/discovery", 202},
		{"/peers/127.0.0.1/health", "secret", "", 404},
		{"/peers/10.12.0.1/health", "secret", "", 404},
		{"/peers/10.11.1.2:1234/health", "secret", "", 404},
		{"/agents/unknown/api/v1/telemetry", "secret", "", 404},
		{"/controller/api/v1/experiments", "secret", "", 404},
		{"/agents/agent-one/api/v1/network/fence", "secret", "", 404},
		{"/controller/api/v1/discovery", "wrong", "", 401},
	} {
		destination, payload = "", ""
		r := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader("payload"))
		r.Header.Set("Authorization", "Bearer "+test.token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != test.status || destination != test.want {
			t.Fatalf("%s status=%d destination=%s", test.path, w.Code, destination)
		}
		if test.status == 202 && payload != "payload" {
			t.Fatal("forwarded body changed")
		}
	}
	// Clock health is intentionally public, matching the existing Controller API.
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/controller/api/v1/health", nil))
	if w.Code != 202 || destination != "http://controller:8080/api/v1/health" {
		t.Fatalf("clock endpoint: %d %s", w.Code, destination)
	}
}

func TestManagerAuthenticationPrecedesAnyDockerAccess(t *testing.T) {
	m, fake, _ := managerFixture(t)
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/network/prepare", strings.NewReader(`{}`)))
	if w.Code != http.StatusUnauthorized || len(fake.calls) != 0 {
		t.Fatalf("status=%d calls=%v", w.Code, fake.calls)
	}
}

func TestGatewayAttackRegistryIsAuthenticatedAndReadOnly(t *testing.T) {
	const token = "test-gateway-registry-key"
	forwarded := 0
	handler, err := GatewayHandler(GatewayConfig{Token: token, ControllerURL: "http://controller:8080", Subnet: "10.11.0.0/16"}, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		forwarded++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/attack-targets" || r.Header.Get("Authorization") != "Bearer "+token || r.URL.Query().Get("generation") != "7" {
			t.Errorf("unexpected upstream registry request: %s %s", r.Method, r.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[]`))}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method, path, token string
		status              int
	}{
		{http.MethodGet, "/controller/api/v1/attack-targets?runId=run&requesterNodeId=attacker&generation=7", token, http.StatusOK},
		{http.MethodGet, "/controller/api/v1/attack-targets", "", http.StatusUnauthorized},
		{http.MethodGet, "/controller/api/v1/attack-targets", "wrong", http.StatusUnauthorized},
		{http.MethodPost, "/controller/api/v1/attack-targets", token, http.StatusNotFound},
		{http.MethodGet, "/controller/api/v1/attack-targets/other", token, http.StatusNotFound},
		{http.MethodGet, "/controller/api/v1/experiments", token, http.StatusNotFound},
	} {
		r := httptest.NewRequest(test.method, test.path, nil)
		if test.token != "" {
			r.Header.Set("Authorization", "Bearer "+test.token)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != test.status {
			t.Errorf("%s %s: status=%d want %d", test.method, test.path, w.Code, test.status)
		}
	}
	if forwarded != 1 {
		t.Fatalf("gateway forwarded %d requests; only the authenticated registry GET may pass", forwarded)
	}
}
