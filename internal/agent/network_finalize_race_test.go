//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

// A fence file is atomically replaced during finalization. Keep an earlier
// read open across that replacement to model a delayed disk reader without
// sleeps or scheduler-dependent retries. No Docker daemon is involved.
func TestNetworkFenceCannotRestoreBindingAfterVerifiedRunReset(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	s.config.AutoResetNetwork = true
	s.config.RunWorkerFactory = agentRunProcessFactory()
	s.startupReconciled = true
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer controller.Close()
	s.config.ControllerURL = controller.URL
	const run = "finalizing-network-run"
	path := s.history.path("fences", run)
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- s.fenceExperimentNetwork(t.Context(), model.ExperimentNetworkRequest{RunID: run, Epoch: "late-network-attempt", RequestedAt: time.Now().UTC()})
	}()
	writer, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Write([]byte("0\n")); err != nil {
		t.Fatal(err)
	}
	// The reader owns the old inode and waits for its EOF. The path now holds
	// the ordinary regular file that finishRunWorker will replace durably.
	if err := os.Rename(path, filepath.Join(t.TempDir(), "old-fence-read")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.finishRunWorker(t.Context(), run); err != nil {
		t.Fatalf("Run did not finish before the earlier network read resumed: %v", err)
	}
	proof, err := s.proveRunReset(run)
	if err != nil || proof.NetworkBound {
		t.Fatalf("fixture did not establish a verified empty network baseline: %+v, %v", proof, err)
	}
	// Fences from other recovered Runs can evict this finalized Run from the
	// bounded hot cache. The durable MAX fence still applies after eviction;
	// checking just the cache cannot validate the earlier disk snapshot.
	for i := 0; i < runFenceCacheLimit+1; i++ {
		s.stopRunGeneration(fmt.Sprintf("zz-retired-network-run-%03d", i), ^uint64(0))
	}
	if err := s.reclaimHistory(); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	_, cached := s.runFences[run]
	s.mu.RUnlock()
	if cached {
		t.Fatal("fixture did not evict the finalized Run from the hot fence cache")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("late network fence restored a binding for an already reset Run")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("late network fence did not finish after its old disk read resumed")
	}
	if proof, err := s.proveRunReset(run); err != nil || proof.NetworkBound {
		t.Fatalf("late rejected network request dirtied the verified baseline: %+v, %v", proof, err)
	}
	fresh := model.ExperimentNetworkRequest{RunID: "fresh-network-run", Epoch: "fresh-network-attempt", RequestedAt: time.Now().UTC().Add(time.Second)}
	if err := s.fenceExperimentNetwork(t.Context(), fresh); err != nil {
		t.Fatalf("rejected old network request blocked a fresh Run: %v", err)
	}
	s.client = &http.Client{Transport: networkTestTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	network := model.ExperimentNetwork{RunID: fresh.RunID, Epoch: fresh.Epoch, NetworkID: "fresh-network-id", NetworkName: s.docker.network, GatewayURL: "http://10.90.0.2:18081", PeerGatewayURL: "http://10.11.0.2:18081"}
	if err := s.activateExperimentNetwork(t.Context(), network); err != nil {
		t.Fatalf("fresh Run could not activate its own network: %v", err)
	}
	s.mu.RLock()
	active := s.experimentNetwork
	s.mu.RUnlock()
	if active == nil || active.RunID != fresh.RunID || active.Epoch != fresh.Epoch {
		t.Fatal("fresh Run lost its network binding")
	}
}

func TestCanceledNetworkFenceDoesNotReplaceCurrentBinding(t *testing.T) {
	for _, processCapable := range []bool{false, true} {
		t.Run(fmt.Sprintf("process-capable=%t", processCapable), func(t *testing.T) {
			s, logPath := newContainerTestServer(t, nil)
			s.config.AutoResetNetwork = true
			s.startupReconciled = true
			if processCapable {
				s.config.RunWorkerFactory = agentRunProcessFactory()
				// Cancellation takes precedence over another Run's pending
				// cleanup and avoids starting its admission/history checks.
				s.events = []model.TraceEvent{{RunID: "other-pending-run"}}
				defer func() { s.events = nil }()
			}
			active := &model.ExperimentNetwork{RunID: "current-run", Epoch: "current-attempt"}
			s.experimentNetwork = active
			request := model.ExperimentNetworkRequest{RunID: "canceled-run", Epoch: "canceled-attempt", RequestedAt: time.Now().UTC()}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := s.fenceExperimentNetwork(ctx, request); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled network fence did not preserve cancellation: %v", err)
			}
			if s.experimentNetwork != active || s.networkRequest.RunID != "" {
				t.Fatal("canceled network request replaced the current network binding")
			}
			if len(dockerCalls(t, logPath)) != 0 {
				t.Fatal("canceled network request contacted Docker")
			}
		})
	}
}
