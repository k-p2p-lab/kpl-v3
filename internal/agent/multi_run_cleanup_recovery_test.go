package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRecoveredRunDrainRequiresOtherRecoveredPeersFencedFirst(t *testing.T) {
	s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.RunWorkerFactory = agentRunProcessFactory()
	var heartbeats atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/api/v1/agents/heartbeat" {
			heartbeats.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(controller.Close)
	s.config.ControllerURL = controller.URL
	const previous, active = "older-recovered-run", "later-recovered-run"
	if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "later-peer", RunID: active, Group: "workers"}); err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, dockerLog, "wait")
	s.mu.RLock()
	config := s.processes["later-peer"].configPath
	s.mu.RUnlock()
	if config == "" {
		t.Fatal("active recovered Peer did not retain its owned config")
	}
	// Automatic-network Agents also retain this binding across the earlier
	// historical Run's drain. A per-Run cleanup must not erase another owner.
	s.mu.Lock()
	s.networkRequest = model.ExperimentNetworkRequest{RunID: active, Epoch: "recovered-network", RequestedAt: time.Now().UTC()}
	s.experimentNetwork = &model.ExperimentNetwork{RunID: active, Epoch: "recovered-network"}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	err := s.finishRunWorker(ctx, previous)
	cancel()
	var pending *runWorkerPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("earlier Run reset did not retain the other recovered Peer barrier: %v", err)
	}
	before := heartbeats.Load()
	t.Logf("earlier Run drain remained pending with another active recovered Peer; heartbeat attempts=%d in 250ms", before)
	// A full worker-backed heartbeat sends both a compact lease and inventory.
	if before > 8 {
		t.Fatalf("blocked Run reset repeatedly flooded Controller heartbeats: %d attempts in 250ms", before)
	}
	s.mu.RLock()
	live := !s.processes["later-peer"].exited
	s.mu.RUnlock()
	if !live {
		t.Fatal("draining an unrelated earlier Run stopped the later Peer")
	}
	// The Controller must fence every recovered Run before asking the first
	// one for a full reset. A per-Run drain must not kill another Run itself.
	s.stopRunGeneration(active, ^uint64(0))
	cleanupCtx, cleanupCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cleanupCancel()
	if err := s.waitRunContainers(cleanupCtx, active, ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	if err := s.drainRunEvents(cleanupCtx, active); err != nil {
		t.Fatal(err)
	}
	if err := s.finishRunWorker(cleanupCtx, previous); !errors.As(err, &pending) {
		t.Fatalf("an earlier Run reset incorrectly released another Run's network binding: %v", err)
	}
	if err := s.finishRunWorker(cleanupCtx, active); err != nil {
		t.Fatal(err)
	}
	if err := s.finishRunWorker(cleanupCtx, previous); err != nil {
		t.Fatalf("earlier Run remained blocked after all recovered Peers were fenced: %v", err)
	}
}
