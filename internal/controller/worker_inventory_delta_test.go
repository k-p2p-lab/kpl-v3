package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type inventoryDeltaStats struct {
	nodes, bytes int
	full         bool
}

func inventoryDeltaProcess(t *testing.T, server *Server, transform func(*workerInventoryPage)) (func() (workerInventoryTestProof, inventoryDeltaStats), *runWorkerBroker) {
	t.Helper()
	broker := &runWorkerBroker{server: server, runID: "dense-inventory"}
	var stats inventoryDeltaStats
	var statsMu sync.Mutex
	handler := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		reply, err := broker.handle(ctx, method, raw)
		if err != nil || method != "inventory" {
			return reply, err
		}
		wrapped := reply.(workerReply)
		if wrapped.Error != nil {
			return wrapped, nil
		}
		var page workerInventoryPage
		if err := json.Unmarshal(wrapped.Value, &page); err != nil {
			return nil, err
		}
		statsMu.Lock()
		for _, item := range page.Items {
			if bytes.HasPrefix(item, []byte(`{"node":`)) {
				stats.nodes++
			}
		}
		stats.bytes += len(wrapped.Value)
		stats.full = stats.full || page.Full
		statsMu.Unlock()
		if transform != nil {
			transform(&page)
		}
		wrapped.Value, err = json.Marshal(page)
		return wrapped, err
	}
	command := func(string) *exec.Cmd {
		return exec.Command(os.Args[0], "-test.run=^TestControllerInventoryWorkerHelper$", "--", "inventory-helper")
	}
	process, err := workerproc.Start(t.Context(), "controller-inventory-test", struct {
		RunID string `json:"runId"`
	}{broker.runID}, handler, workerproc.Options{Command: command, Stdout: io.Discard, Stderr: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Close(); err != nil {
			t.Error(err)
		}
		if err := broker.close(); err != nil {
			t.Error(err)
		}
	})
	return func() (workerInventoryTestProof, inventoryDeltaStats) {
		statsMu.Lock()
		stats = inventoryDeltaStats{}
		statsMu.Unlock()
		var proof workerInventoryTestProof
		if err := process.Call(t.Context(), "execute", nil, &proof); err != nil {
			t.Fatal(err)
		}
		statsMu.Lock()
		defer statsMu.Unlock()
		return proof, stats
	}, broker
}

func expectedInventoryDigest(broker *runWorkerBroker) string {
	snapshot := broker.inventory()
	sort.Slice(snapshot.Agents, func(i, j int) bool { return snapshot.Agents[i].ID < snapshot.Agents[j].ID })
	sort.Slice(snapshot.Nodes, func(i, j int) bool { return snapshot.Nodes[i].ID < snapshot.Nodes[j].ID })
	return inventoryDigest(snapshot)
}

func TestWorkerInventoryDeltaPreservesDenseFieldsAndMetadataOnlyUpdates(t *testing.T) {
	server := inventoryTestServer(t, 20)
	poll, broker := inventoryDeltaProcess(t, server, nil)
	first, stats := poll()
	if first.Error != "" || first.Digest != expectedInventoryDigest(broker) || first.CompleteScores != 20 || !stats.full || stats.nodes != 20 {
		t.Fatalf("initial inventory: %+v %+v", first, stats)
	}
	for range 3 {
		proof, stats := poll()
		if proof.Error != "" || proof.Digest != first.Digest || proof.Revision != first.Revision || stats.full || stats.nodes != 0 || stats.bytes > 64<<10 {
			t.Fatalf("unchanged inventory was retransmitted or lost: %+v %+v", proof, stats)
		}
	}
	server.state.mu.Lock()
	node := server.state.nodes["dense-00"]
	node.Metadata = map[string]string{"revision": "metadata-only"}
	node.Profile = "updated-profile"
	node.PeerScores = maps.Clone(node.PeerScores)
	node.PeerScores["new-score-with-the-same-timestamp"] = 42
	server.state.setNodeLocked(node)
	server.state.mu.Unlock()
	proof, stats := poll()
	if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || proof.Revision <= first.Revision || stats.full || stats.nodes != 1 {
		t.Fatalf("metadata/score update was lost: %+v %+v", proof, stats)
	}
}

func TestWorkerInventoryDeltaResynchronizesAfterDeletionAndUnindexedFixtureChanges(t *testing.T) {
	server := inventoryTestServer(t, 3)
	poll, broker := inventoryDeltaProcess(t, server, nil)
	if proof, _ := poll(); proof.Error != "" {
		t.Fatal(proof.Error)
	}
	changes := []func(){
		func() { server.state.deleteNodeLocked("dense-00") },
		func() {
			server.state.nodes["inserted"] = model.Node{ID: "inserted", RunID: broker.runID, State: model.NodeStopped}
			server.state.nodeReportTimes["inserted"] = time.Now().UTC()
		},
		func() { delete(server.state.nodes, "dense-01"); delete(server.state.nodeReportTimes, "dense-01") },
		func() {
			delete(server.state.nodes, "inserted")
			delete(server.state.nodeReportTimes, "inserted")
			server.state.nodes["replacement"] = model.Node{ID: "replacement", RunID: broker.runID, State: model.NodeStopped}
		},
	}
	for i, change := range changes {
		server.state.mu.Lock()
		change()
		server.state.mu.Unlock()
		proof, stats := poll()
		if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || !stats.full {
			t.Fatalf("change %d retained stale worker state: %+v %+v", i, proof, stats)
		}
	}
}

func TestWorkerInventoryDeltaFailedPageKeepsStateAndCursorUntilRetry(t *testing.T) {
	server := inventoryTestServer(t, 20)
	var tamper atomic.Bool
	poll, broker := inventoryDeltaProcess(t, server, func(page *workerInventoryPage) {
		if tamper.Load() && page.Next > len(page.Items) {
			page.Revision++
		}
	})
	first, _ := poll()
	if first.Error != "" {
		t.Fatal(first.Error)
	}
	server.state.mu.Lock()
	for _, node := range server.state.nodes {
		node.Metadata = map[string]string{"revision": "new"}
		server.state.setNodeLocked(node)
	}
	server.state.mu.Unlock()
	tamper.Store(true)
	failed, _ := poll()
	if !strings.Contains(failed.Error, "inventory revision changed") || failed.Digest != first.Digest || failed.Revision != first.Revision {
		t.Fatalf("partial delta became authoritative: first=%+v failed=%+v", first, failed)
	}
	tamper.Store(false)
	proof, stats := poll()
	if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || proof.Revision <= first.Revision || stats.full || stats.nodes != 20 {
		t.Fatalf("failed delta did not retry completely: %+v %+v", proof, stats)
	}
}

func TestWorkerInventoryDeltaKeepsFiniteCaptureDuringConcurrentChanges(t *testing.T) {
	server := inventoryTestServer(t, 20)
	var mutate atomic.Bool
	poll, broker := inventoryDeltaProcess(t, server, func(page *workerInventoryPage) {
		if len(page.Items) == 0 || !mutate.Swap(false) {
			return
		}
		server.state.mu.Lock()
		defer server.state.mu.Unlock()
		for _, node := range server.state.nodes {
			node.Metadata = map[string]string{"revision": "after capture"}
			server.state.setNodeLocked(node)
		}
	})
	if proof, _ := poll(); proof.Error != "" {
		t.Fatal(proof.Error)
	}
	server.state.mu.Lock()
	for _, node := range server.state.nodes {
		node.Metadata = map[string]string{"revision": "before capture"}
		server.state.setNodeLocked(node)
	}
	server.state.mu.Unlock()
	expected := expectedInventoryDigest(broker)
	mutate.Store(true)
	proof, _ := poll()
	if proof.Error != "" || proof.Digest != expected {
		t.Fatalf("finite delta snapshot changed between pages: %+v", proof)
	}
	proof, stats := poll()
	if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || stats.nodes != 20 || stats.full {
		t.Fatalf("concurrent changes were lost after cursor advance: %+v %+v", proof, stats)
	}
}

func TestWorkerInventoryDeltaNeverRevivesTerminalHeartbeat(t *testing.T) {
	server := inventoryTestServer(t, 1)
	poll, broker := inventoryDeltaProcess(t, server, nil)
	if proof, _ := poll(); proof.Error != "" {
		t.Fatal(proof.Error)
	}
	server.state.mu.Lock()
	node := server.state.nodes["dense-00"]
	node.State = model.NodeStopped
	node.Metadata = map[string]string{"cleanupComplete": "true"}
	server.state.setNodeLocked(node)
	agent := server.state.agents["agent"]
	server.state.mu.Unlock()
	if proof, _ := poll(); proof.Error != "" {
		t.Fatal(proof.Error)
	}
	node.State = model.NodeReady
	node.LastSeen = node.LastSeen.Add(time.Second)
	if err := server.state.heartbeat(model.AgentHeartbeat{Agent: agent, Nodes: []model.Node{node}, Partial: true}); err != nil {
		t.Fatal(err)
	}
	proof, stats := poll()
	if proof.Error != "" || proof.Digest != expectedInventoryDigest(broker) || stats.nodes != 0 {
		t.Fatalf("terminal lifecycle changed: %+v %+v", proof, stats)
	}
	server.state.mu.RLock()
	state := server.state.nodes[node.ID].State
	server.state.mu.RUnlock()
	if state != model.NodeStopped {
		t.Fatal(fmt.Sprintf("terminal Peer revived as %s", state))
	}
}
