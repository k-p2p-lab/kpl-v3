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
