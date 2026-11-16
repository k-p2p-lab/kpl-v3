package experimentnet

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerAcknowledgedReplayPreservesGatewayWithDockerCLIRetiringNameView(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	m, fake, request := managerFixture(t)
	first, err := m.prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := json.Marshal(fake.network)
	if err != nil {
		t.Fatal(err)
	}
	old := fixtureNetwork(t)
	retiring, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var nameLists atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.44")
			w.Header().Set("OSType", "linux")
			_, _ = w.Write([]byte("OK"))
		case strings.HasSuffix(r.URL.Path, "/networks"):
			nameLists.Add(1)
			_ = json.NewEncoder(w).Encode([]map[string]string{{"Id": old.ID, "Name": old.Name, "Driver": old.Driver, "Scope": old.Scope}})
		case strings.HasSuffix(r.URL.Path, "/networks/"+old.ID):
			_, _ = w.Write(retiring)
		case strings.HasSuffix(r.URL.Path, "/networks/"+first.NetworkID):
			_, _ = w.Write(fresh)
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "unsupported isolated fixture endpoint"})
		}
	}))
	defer server.Close()
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("DOCKER_HOST", "tcp://"+strings.TrimPrefix(server.URL, "http://"))
	t.Setenv("DOCKER_API_VERSION", "1.44")
	t.Setenv("DOCKER_TLS_VERIFY", "")
	t.Setenv("DOCKER_CERT_PATH", "")
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		if len(args) > 1 && args[0] == "network" && (args[1] == "ls" || args[1] == "inspect") {
			return dockerCommand(ctx, input, args...)
		}
		return original(ctx, input, args...)
	}
	mutations := len(fake.mutations)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := m.prepare(ctx, request)
	if err != nil || result != first || len(fake.mutations) != mutations || nameLists.Load() != 0 {
		t.Fatalf("acknowledged retry consulted a retiring name or changed Docker: result=%+v err=%v mutations=%d nameLists=%d", result, err, len(fake.mutations)-mutations, nameLists.Load())
	}
}
