package master

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSwarmImageResolutionWithDockerCLIAndIsolatedEngineProtocol(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	digest := "sha256:" + strings.Repeat("c", 64)
	image := "registry.example.test/lab@" + digest
	var paths []string
	var pathsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathsMu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		pathsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.44")
			w.Header().Set("OSType", "linux")
			_, _ = w.Write([]byte("OK"))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/images/create"):
			for _, status := range []string{"Pulling from lab", "Digest: " + digest, "Status: Image is up to date"} {
				_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
			}
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/images/") && strings.HasSuffix(r.URL.Path, "/json"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Id": "sha256:" + strings.Repeat("d", 64), "RepoDigests": []string{image},
				"Config": map[string]any{"Labels": map[string]string{"io.kpl.cli.layout": cliImageLayout}},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "unsupported isolated fixture endpoint"})
		}
	}))
	defer server.Close()
	s, _, output := newSwarmFixture(t, "deploy")
	s.a.Runner = ExecRunner{}
	s.a.Environ = []string{
		"PATH=" + os.Getenv("PATH"),
		"DOCKER_CONFIG=" + t.TempDir(),
		"DOCKER_HOST=tcp://" + strings.TrimPrefix(server.URL, "http://"),
		"DOCKER_API_VERSION=1.44",
	}
	s.timeout, s.pullTimeout = 5*time.Second, 5*time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := s.resolveImage(ctx); err != nil {
		pathsMu.Lock()
		defer pathsMu.Unlock()
		t.Fatalf("actual Docker CLI did not satisfy pull/inspect protocol: %v\noutput=%s\nrequests=%v", err, output, paths)
	}
	if s.config["KPL_IMAGE"] != image {
		t.Fatalf("wrong exact resolved reference: %s", s.config["KPL_IMAGE"])
	}
	pathsMu.Lock()
	defer pathsMu.Unlock()
	var operations []string
	for _, path := range paths {
		if !strings.HasSuffix(path, " /_ping") {
			operations = append(operations, path)
		}
	}
	if len(operations) != 2 || !strings.Contains(operations[0], "/images/create") || !strings.Contains(operations[1], "/images/") {
		t.Fatalf("unexpected fixture requests: %v", paths)
	}
}

func TestSwarmNetworkRecreationRetriesDockerDaemonNameConflict(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is not installed")
	}
	newID := strings.Repeat("c", 64)
	var creates atomic.Int32
	var created atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("API-Version", "1.44")
			w.Header().Set("OSType", "linux")
			_, _ = w.Write([]byte("OK"))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/networks"):
			if !created.Load() {
				_, _ = w.Write([]byte("[]"))
			} else {
				_ = json.NewEncoder(w).Encode([]map[string]string{{"Id": newID, "Name": "kpl-peers", "Driver": "overlay", "Scope": "swarm"}})
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/networks/create"):
			if creates.Add(1) == 1 {
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": "network with name kpl-peers already exists"})
				return
			}
			created.Store(true)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": newID, "Warning": ""})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "unsupported isolated fixture endpoint"})
		}
	}))
	defer server.Close()
	s, _, output := newSwarmFixture(t, "reset-peer-network")
	s.a.Runner = ExecRunner{}
	s.a.Environ = []string{
		"PATH=" + os.Getenv("PATH"), "DOCKER_CONFIG=" + t.TempDir(),
		"DOCKER_HOST=tcp://" + strings.TrimPrefix(server.URL, "http://"), "DOCKER_API_VERSION=1.44",
	}
	s.timeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	id, err := s.createResetPeerNetwork(ctx, []string{"network", "create", "--driver", "overlay", "--attachable", s.network}, strings.Repeat("b", 64))
	if err != nil || id != newID || creates.Load() != 2 {
		t.Fatalf("transient Engine name conflict was not retried: id=%s creates=%d err=%v\noutput=%s", id, creates.Load(), err, output)
	}
}
