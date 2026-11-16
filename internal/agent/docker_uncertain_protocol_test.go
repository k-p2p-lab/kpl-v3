package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// Exercise the installed Docker CLI's inspect template and error protocol.
// Its config is empty and its explicit host is this isolated HTTP server;
// neither the user's Docker settings nor a real daemon are consulted.
func TestDockerUncertainCreateInstalledCLIProtocol(t *testing.T) {
	binary, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI is not installed")
	}
	for _, present := range []bool{false, true} {
		name := "absent"
		if present {
			name = "observed"
		}
		t.Run(name, func(t *testing.T) {
			node := model.Node{ID: "protocol-peer", AgentID: "protocol-agent", RunID: "protocol-run", Generation: 9}
			attempt, err := peerContainerName(node)
			if err != nil {
				t.Fatal(err)
			}
			var recorded atomic.Bool
			var removals atomic.Int32
			engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				if component, rest, ok := strings.Cut(strings.TrimPrefix(path, "/"), "/"); ok && strings.HasPrefix(component, "v") {
					path = "/" + rest
				}
				w.Header().Set("Content-Type", "application/json")
				switch {
				case path == "/_ping":
					w.Header().Set("API-Version", "1.45")
					_, _ = w.Write([]byte("OK"))
				case r.Method == http.MethodGet && path == "/containers/"+attempt+"/json":
					if !present {
						w.WriteHeader(http.StatusNotFound)
						_ = json.NewEncoder(w).Encode(map[string]string{"message": "No such container: " + attempt})
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"Id": fakeContainerID, "Name": "/" + attempt, "Config": map[string]any{"Labels": map[string]string{"io.kpl.managed": "true", "io.kpl.network": "peers", "io.kpl.agent": node.AgentID, "io.kpl.run": node.RunID, "io.kpl.node": node.ID, "io.kpl.generation": "9"}}})
				case r.Method == http.MethodDelete && path == "/containers/"+fakeContainerID:
					if !recorded.Load() || r.URL.Query().Get("force") != "1" {
						t.Error("CLI removal preceded recorded immutable identity or lost force mode")
					}
					removals.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected isolated Engine request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer engine.Close()
			config := t.TempDir()
			d := &dockerRuntime{binary: binary, network: "peers"}
			d.commandContext = func(ctx context.Context, binary string, args ...string) *exec.Cmd {
				prefix := []string{"--config", config, "--host", "tcp://" + engine.Listener.Addr().String(), "--tls=false"}
				cmd := exec.CommandContext(ctx, binary, append(prefix, args...)...)
				for _, entry := range os.Environ() {
					key, _, _ := strings.Cut(entry, "=")
					if !strings.HasPrefix(strings.ToUpper(key), "DOCKER_") {
						cmd.Env = append(cmd.Env, entry)
					}
				}
				cmd.Env = append(cmd.Env, "HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY=", "NO_PROXY=*")
				return cmd
			}
			id, err := d.removeUncertainCreate(t.Context(), attempt, node, func(id string) error {
				if id != fakeContainerID {
					t.Errorf("CLI changed observed immutable identity: %s", id)
				}
				recorded.Store(true)
				return nil
			})
			if present {
				if err != nil || id != fakeContainerID || !recorded.Load() || removals.Load() != 1 {
					t.Fatalf("installed CLI failed positive recovery: id=%s err=%v recorded=%t removals=%d", id, err, recorded.Load(), removals.Load())
				}
			} else if !errors.Is(err, errDockerCreateUncertain) || id != "" || recorded.Load() || removals.Load() != 0 {
				t.Fatalf("installed CLI absence invented cleanup proof: id=%s err=%v recorded=%t removals=%d", id, err, recorded.Load(), removals.Load())
			}
		})
	}
}
