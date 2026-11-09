package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/workerproc"
)

type workerInventoryTestProof struct {
	Digest         string
	Nodes          int
	CompleteScores int
	Error          string
	Preserved      bool
	Revision       uint64
}

func inventoryDigest(snapshot workerInventory) string {
	encoded, _ := json.Marshal(snapshot)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}
func TestControllerInventoryWorkerHelper(t *testing.T) {
	if len(os.Args) < 2 || os.Args[len(os.Args)-1] != "inventory-helper" {
		return
	}
	factory := func(ctx context.Context, kind string, raw json.RawMessage, peer *workerproc.Peer) (workerproc.Handler, error) {
		server := &Server{state: &state{nodes: map[string]model.Node{"sentinel": {ID: "sentinel"}}, experiments: make(map[string]model.Experiment)}}
		client := &runWorkerClient{peer: peer, runID: peer.RunID(), ctx: ctx, server: server}
		return func(callCtx context.Context, method string, input json.RawMessage) (any, error) {
			if method != "execute" {
				return nil, fmt.Errorf("invalid test operation")
			}
			syncErr := client.syncInventory(callCtx)
			snapshot := workerInventory{Experiment: server.state.experiments[peer.RunID()], ReportTimes: server.state.nodeReportTimes}
			for _, agent := range server.state.agents {
				snapshot.Agents = append(snapshot.Agents, agent)
			}
			for _, node := range server.state.nodes {
				snapshot.Nodes = append(snapshot.Nodes, node)
			}
			sort.Slice(snapshot.Agents, func(i, j int) bool { return snapshot.Agents[i].ID < snapshot.Agents[j].ID })
			sort.Slice(snapshot.Nodes, func(i, j int) bool { return snapshot.Nodes[i].ID < snapshot.Nodes[j].ID })
			proof := workerInventoryTestProof{Digest: inventoryDigest(snapshot), Nodes: len(snapshot.Nodes), Revision: client.inventoryRevision}
			if syncErr != nil {
				_, proof.Preserved = server.state.nodes["sentinel"]
				proof.Error = syncErr.Error()
			}
			for _, node := range snapshot.Nodes {
				if len(node.MeshPeers) == 16 && len(node.PeerScores) == 900 && node.ScoreSample != nil && node.ScoreSample.Valid() {
					proof.CompleteScores++
				}
			}
			return proof, nil
		}, nil
	}
	if err := workerproc.Serve(context.Background(), "controller-inventory-test", factory); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}
func inventoryTestServer(t *testing.T, nodes int) *Server {
	t.Helper()
	s := New(ServerConfig{DataDir: t.TempDir(), RunWorkers: true}, nil)
	const id = "dense-inventory"
	run, _ := resultFixture(t, s, id, "running", time.Now().UTC())
	s.state.experiments[id] = run
	at := time.Now().UTC()
	s.state.agents["agent"] = model.Agent{ID: "agent", StartedAt: at, LastSeen: at, RunDrain: true, RunProcesses: true, State: "online"}
	peers := make([]string, 900)
	scores := make(map[string]float64, 900)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer-%059d", i)
		scores[peers[i]] = float64(i)
	}
	mesh := make(map[string][]string, 16)
	for i := range 16 {
		mesh[fmt.Sprintf("topic-%d", i)] = peers
	}
	for i := range nodes {
		sample := &model.PeerScoreSample{ObservedAt: at, Sequence: 1, IntervalSeconds: 1, Components: make(map[string]model.ScoreStatistic)}
		for _, key := range model.ScoreComponentKeys {
			sample.Components[key] = model.ScoreStatistic{Count: 900}
		}
		node := model.Node{ID: fmt.Sprintf("dense-%02d", i), RunID: id, AgentID: "agent", State: model.NodeReady, LastSeen: at, MeshPeers: mesh, PeerScores: scores, ScoreSample: sample, Metadata: map[string]string{"revision": "before"}}
		s.state.setNodeLocked(node)
		s.state.nodeReportTimes[node.ID] = at
	}
	return s
}
func runInventoryTestProcess(t *testing.T, s *Server, changeAfterFirstPage bool) (workerInventoryTestProof, int, int) {
	t.Helper()
	broker := &runWorkerBroker{server: s, runID: "dense-inventory"}
	defer broker.close()
	pages, maxPage := 0, 0
	handler := func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		reply, err := broker.handle(ctx, method, raw)
		if method == "inventory" && err == nil {
			wrapped := reply.(workerReply)
			if wrapped.Error == nil && len(wrapped.Value) > 0 {
				var page workerInventoryPage
				if err := json.Unmarshal(wrapped.Value, &page); err != nil {
					return nil, err
				}
				if len(page.Items) > 0 {
					pages++
					maxPage = max(maxPage, len(wrapped.Value))
				}
				if changeAfterFirstPage && pages == 1 {
					// Heartbeat replaces immutable Node values while a telemetry revision
					// also advances. Neither may invalidate this finite borrowed snapshot.
					s.state.mu.Lock()
					for _, node := range s.state.nodes {
						node.Metadata = map[string]string{"revision": "after"}
						node.LastSeen = node.LastSeen.Add(time.Second)
						s.state.setNodeLocked(node)
					}
					s.state.mu.Unlock()
					s.state.resultsRevision.Add(1)
					s.state.notify()
				}
			}
		}
		return reply, err
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
	defer process.Close()
	var proof workerInventoryTestProof
	if err := process.Call(t.Context(), "execute", nil, &proof); err != nil {
		t.Fatal(err)
	}
	if err := process.Close(); err != nil {
		t.Fatal(err)
	}
	broker.mu.Lock()
	retained := broker.inventoryLease != nil
	broker.mu.Unlock()
	if retained {
		t.Fatal("completed/failed inventory retained its borrowed snapshot")
	}
	return proof, pages, maxPage
}
func TestWorkerInventoryPagesDenseSnapshotAcrossConcurrentChanges(t *testing.T) {
	s := inventoryTestServer(t, 20)
	broker := &runWorkerBroker{server: s, runID: "dense-inventory"}
	expected := broker.inventory()
	sort.Slice(expected.Agents, func(i, j int) bool { return expected.Agents[i].ID < expected.Agents[j].ID })
	sort.Slice(expected.Nodes, func(i, j int) bool { return expected.Nodes[i].ID < expected.Nodes[j].ID })
	encoded, _ := json.Marshal(expected)
	if len(encoded) <= workerproc.MaxMessageBytes {
		t.Fatalf("dense fixture did not exceed private frame: %d", len(encoded))
	}
	digest := inventoryDigest(expected)
	proof, pages, maxPage := runInventoryTestProcess(t, s, true)
	if proof.Error != "" || proof.Digest != digest || proof.Nodes != 20 || proof.CompleteScores != 20 {
		t.Fatalf("dense inventory was truncated or mixed revisions: %+v", proof)
	}
	if pages < 2 || maxPage >= workerInventoryFrameBytes {
		t.Fatalf("inventory was not byte-paged: pages=%d max=%d", pages, maxPage)
	}
	t.Logf("complete inventory=%d bytes, pages=%d, largest encoded page=%d, scores preserved=%d", len(encoded), pages, maxPage, proof.CompleteScores)
}
func TestWorkerInventoryAcceptsSingleTenMiBNode(t *testing.T) {
	s := inventoryTestServer(t, 1)
	node := s.state.nodes["dense-00"]
	node.MeshPeers = nil
	node.PeerScores = nil
	node.ScoreSample = nil
	node.Metadata = map[string]string{"large": strings.Repeat("x", 10<<20)}
	s.state.setNodeLocked(node)
	expected := (&runWorkerBroker{server: s, runID: "dense-inventory"}).inventory()
	proof, pages, maxPage := runInventoryTestProcess(t, s, false)
	if proof.Error != "" || proof.Digest != inventoryDigest(expected) || proof.Nodes != 1 {
		t.Fatalf("legal large Node failed or lost fields: %+v", proof)
	}
	if pages < 2 || maxPage <= workerInventoryPageBytes || maxPage >= workerInventoryFrameBytes {
		t.Fatalf("single-record frame budget incorrect: pages=%d max=%d", pages, maxPage)
	}
}
func TestWorkerInventoryRejectsOversizeRecordWithoutPartialState(t *testing.T) {
	s := inventoryTestServer(t, 1)
	node := s.state.nodes["dense-00"]
	node.MeshPeers = nil
	node.PeerScores = nil
	node.ScoreSample = nil
	node.Metadata = map[string]string{"large": strings.Repeat("x", 17<<20)}
	s.state.setNodeLocked(node)
	proof, _, _ := runInventoryTestProcess(t, s, false)
	if !proof.Preserved || !strings.Contains(proof.Error, "inventory record exceeds") {
		t.Fatalf("partial inventory became authoritative: %+v", proof)
	}
}
func TestWorkerInventoryLeaseRejectsForeignCursorAndReleasesOnClose(t *testing.T) {
	s := inventoryTestServer(t, 5)
	broker := &runWorkerBroker{server: s, runID: "dense-inventory"}
	first, err := broker.inventoryPage(t.Context(), workerInventoryRequest{})
	if err != nil || first.Done {
		t.Fatalf("first page: %+v %v", first, err)
	}
	if _, err := broker.inventoryPage(t.Context(), workerInventoryRequest{Lease: first.Lease, Next: first.Next + 1}); err == nil {
		t.Fatal("foreign page cursor was accepted")
	}
	if _, err := broker.inventoryPage(t.Context(), workerInventoryRequest{Lease: "foreign", Next: first.Next}); err == nil {
		t.Fatal("foreign inventory lease was accepted")
	}
	broker.close()
	if broker.inventoryLease != nil {
		t.Fatal("closed Run retained inventory references")
	}
	if _, err := broker.inventoryPage(t.Context(), workerInventoryRequest{Lease: first.Lease, Next: first.Next}); err == nil {
		t.Fatal("late page reused a closed Run lease")
	}
}

func TestWorkerInventoryAbandonedLeaseExpires(t *testing.T) {
	s := inventoryTestServer(t, 5)
	broker := &runWorkerBroker{server: s, runID: "dense-inventory"}
	defer broker.close()
	first, err := broker.inventoryPage(t.Context(), workerInventoryRequest{})
	if err != nil || first.Done {
		t.Fatalf("first page: %+v %v", first, err)
	}
	broker.mu.Lock()
	broker.inventoryLease.timer.Reset(time.Millisecond)
	broker.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		broker.mu.Lock()
		released := broker.inventoryLease == nil
		broker.mu.Unlock()
		if released {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("abandoned inventory references were never released")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := broker.inventoryPage(t.Context(), workerInventoryRequest{Lease: first.Lease, Next: first.Next}); err == nil {
		t.Fatal("expired inventory snapshot was resurrected")
	}
}
