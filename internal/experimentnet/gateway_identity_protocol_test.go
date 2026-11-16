package experimentnet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestGatewayIdentityInspectionWithDockerCLIAndIsolatedEngine(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	m, _, request := managerFixture(t)
	gatewayID := strings.Repeat("d", 64)
	const marker = "fixture-environment-must-not-appear-in-inspection"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/_ping" {
			w.Header().Set("API-Version", "1.44")
			w.Header().Set("OSType", "linux")
			_, _ = w.Write([]byte("OK"))
			return
		}
		var object map[string]any
		switch {
		case strings.HasSuffix(r.URL.Path, "/containers/manager-task/json"):
			object = map[string]any{"Id": "manager-task", "NetworkSettings": map[string]any{"Networks": map[string]gatewayNetworkEndpoint{
				"kpl-control": {IPAddress: "10.90.0.3", NetworkID: "control-network"},
			}}}
		case strings.HasSuffix(r.URL.Path, "/containers/"+gatewayID+"/json"):
			object = map[string]any{"Id": gatewayID, "Name": "/kpl-network-gateway", "State": map[string]any{"Running": true, "Pid": 5252},
				"Config": map[string]any{"Env": []string{marker}, "Labels": map[string]string{
					"io.kpl.network-gateway": "kpl", "com.docker.stack.namespace": "kpl", "io.kpl.execution": request.Epoch,
				}}, "NetworkSettings": map[string]any{"Networks": map[string]gatewayNetworkEndpoint{
					"kpl-control": {IPAddress: "10.90.0.2", NetworkID: "control-network"},
					"kpl-peers":   {IPAddress: "10.11.0.2", NetworkID: "fresh-network-1"},
				}}}
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "unsupported isolated fixture endpoint"})
			return
		}
		_ = json.NewEncoder(w).Encode(object)
	}))
	defer server.Close()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "tcp://"+strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("DOCKER_API_VERSION", "1.44")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		output, err := dockerCommand(ctx, input, args...)
		if strings.Contains(string(output), marker) {
			return nil, fmt.Errorf("inspection included container environment")
		}
		return output, err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := m.inspectGateway(ctx, request, gatewayID, "fresh-network-1", "10.11.0.0/16")
	if err != nil || result.NetworkID != "fresh-network-1" || result.GatewayURL != "http://10.90.0.2:18081" || result.PeerGatewayURL != "http://10.11.0.2:18081" {
		t.Fatalf("actual Docker inspection did not prove the expected gateway: result=%+v err=%v", result, err)
	}
}
