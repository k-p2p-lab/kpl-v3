package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestCreateReplayConcurrentAdmissionStartsOneContainer(t *testing.T) {
	s, log := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	s.config.Capacity = 1
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", Generation: 2, Seed: 17, Lifetime: "1h", AgentStartedAt: s.startedAt}
	var wg sync.WaitGroup
	nodes := make(chan model.Node, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, err := s.createNode(context.Background(), request)
			if err != nil {
				t.Error(err)
				return
			}
			nodes <- node
		}()
	}
	wg.Wait()
	close(nodes)
	var started time.Time
	for node := range nodes {
		if started.IsZero() {
			started = node.StartedAt
		}
		if node.ID != request.ID || !node.StartedAt.Equal(started) {
			t.Fatalf("replay changed Peer identity or lifetime origin: %+v", node)
		}
	}
	waitDockerCall(t, log, "wait")
	creates := 0
	for _, call := range dockerCalls(t, log) {
		if call.Args[0] == "create" {
			creates++
		}
	}
	if creates != 1 || s.snapshot().Agent.ActiveNodes != 1 {
		t.Fatalf("creates=%d occupancy=%d, want one", creates, s.snapshot().Agent.ActiveNodes)
	}
	changed := request
	changed.Seed++
	if _, err := s.createNode(context.Background(), changed); err == nil || !strings.Contains(err.Error(), "different create request") {
		t.Fatalf("changed request accepted: %v", err)
	}
	changed = request
	changed.AgentStartedAt = changed.AgentStartedAt.Add(-time.Second)
	if _, err := s.createNode(context.Background(), changed); err == nil || !strings.Contains(err.Error(), "instance changed") {
		t.Fatalf("old instance accepted: %v", err)
	}
	changed = request
	changed.AgentStartedAt = time.Time{}
	if _, err := s.createNode(context.Background(), changed); !errors.Is(err, errNodeExists) {
		t.Fatalf("legacy duplicate behavior changed: %v", err)
	}
}

func TestCreateReplayRetiredPeerDoesNotRestart(t *testing.T) {
	s, log := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", AgentStartedAt: s.startedAt}
	node, err := s.createNode(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, log, "wait")
	s.stopRunGeneration(request.RunID, request.Generation)
	if err := s.waitRunContainers(t.Context(), request.RunID, request.Generation); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	retired := heartbeatNodeStatus(s.processes[request.ID])
	s.mu.RUnlock()
	if err := s.history.saveNodes([]model.Node{retired}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	delete(s.processes, request.ID)
	s.historyRevision++
	s.mu.Unlock()
	before := len(dockerCalls(t, log))
	replay, err := s.createNode(t.Context(), request)
	if err != nil || replay.State != model.NodeStopped || !replay.StartedAt.Equal(node.StartedAt) {
		t.Fatalf("retired replay=%+v error=%v", replay, err)
	}
	if len(dockerCalls(t, log)) != before || len(s.processes) != 0 {
		t.Fatal("replay recreated a retired Peer")
	}
	request.ID = "late"
	if _, err := s.createNode(t.Context(), request); err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("replay capability bypassed run fence: %v", err)
	}
}

func TestCreateReplayFailedPreparationDoesNotBecomeSuccess(t *testing.T) {
	s, log := newContainerTestServer(t, nil)
	if err := os.WriteFile(filepath.Join(s.config.DataDir, "nodes"), []byte("blocked"), 0600); err != nil {
		t.Fatal(err)
	}
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", AgentStartedAt: s.startedAt}
	if _, err := s.createNode(t.Context(), request); err == nil {
		t.Fatal("expected config failure")
	}
	if _, err := s.createNode(t.Context(), request); err == nil || !strings.Contains(err.Error(), "admission did not complete") {
		t.Fatalf("failed admission replay=%v", err)
	}
	if len(dockerCalls(t, log)) != 0 {
		t.Fatal("failed preparation launched Docker")
	}
}

func TestCreateReplayPendingWaitHonorsRequestCancellation(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "workers", AgentStartedAt: s.startedAt}
	hash, err := s.createRequestHash(request)
	if err != nil {
		t.Fatal(err)
	}
	s.processes[request.ID] = &process{node: model.Node{ID: request.ID, Metadata: map[string]string{"createRequestHash": hash}}, admissionDone: make(chan struct{})}
	t.Cleanup(func() { delete(s.processes, request.ID) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.admitNode(ctx, context.Background(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("pending replay ignored cancellation: %v", err)
	}
}
