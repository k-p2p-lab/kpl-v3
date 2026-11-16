package experimentnet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the actual Docker CLI against an isolated Engine protocol fixture.
// A creation error can follow a successful mutation, and the subsequent name
// views may regress to the retiring overlay. The captured new ID remains valid.
func TestManagerAmbiguousCreateUsesCapturedNetworkIdentityWithDockerCLI(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	for _, view := range []string{"absent", "retiring", "different-epoch", "changed-ID"} {
		t.Run(view, func(t *testing.T) {
			m, _, request := managerFixture(t)
			oldID, newID := strings.Repeat("b", 64), strings.Repeat("c", 64)
			var lists, creates, inspected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/_ping":
					w.Header().Set("API-Version", "1.44")
					w.Header().Set("OSType", "linux")
					_, _ = w.Write([]byte("OK"))
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/networks"):
					switch lists.Add(1) {
					case 1:
						_, _ = w.Write([]byte("[]"))
					case 2:
						_ = json.NewEncoder(w).Encode([]map[string]string{{"Id": newID, "Name": "kpl-peers", "Driver": "overlay", "Scope": "swarm"}})
					default:
						if view == "different-epoch" || view == "changed-ID" {
							_ = json.NewEncoder(w).Encode([]map[string]string{{"Id": newID, "Name": "kpl-peers", "Driver": "overlay", "Scope": "swarm"}})
						} else if view == "retiring" {
							_ = json.NewEncoder(w).Encode([]map[string]string{{"Id": oldID, "Name": "kpl-peers", "Driver": "overlay", "Scope": "swarm"}})
						} else {
							_, _ = w.Write([]byte("[]"))
						}
					}
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/networks/create"):
					creates.Add(1)
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{"message": "creation completed but response failed"})
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/networks/"+newID):
					inspected.Add(1)
					network := fixtureNetwork(t)
					network.ID = newID
					network.Labels["io.kpl.execution"] = request.Epoch
					if view == "different-epoch" {
						network.Labels["io.kpl.execution"] = "another-attempt"
					} else if view == "changed-ID" {
						network.ID = strings.Repeat("d", 64)
					}
					_ = json.NewEncoder(w).Encode(network)
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/services"):
					_, _ = w.Write([]byte("[]"))
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
			m.command = dockerCommand
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			id, err := m.createFreshNetwork(ctx, request, networkAllocation{Subnet: "10.11.0.0/16", Gateway: "10.11.0.1"}, oldID)
			if creates.Load() != 1 || inspected.Load() != 1 {
				t.Fatalf("captured new overlay was not reconciled: id=%q creates=%d inspected=%d lists=%d err=%v", id, creates.Load(), inspected.Load(), lists.Load(), err)
			}
			if view == "different-epoch" || view == "changed-ID" {
				if err == nil || id != "" || !strings.Contains(err.Error(), "identity") {
					t.Fatalf("unverified new overlay was accepted: id=%q err=%v", id, err)
				}
			} else if err != nil || id != newID {
				t.Fatalf("captured new overlay was not reconciled: id=%q err=%v", id, err)
			}
		})
	}
}
