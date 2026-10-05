package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func topologyFormationFixture(t *testing.T, n int, handler http.HandlerFunc) (*Server, scenario.Phase) {
	t.Helper()
	api := httptest.NewServer(handler)
	t.Cleanup(api.Close)
	s := New(ServerConfig{DataDir: t.TempDir(), Token: "topology-test"}, nil)
	s.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%04d", i)
		node := meshFreezeNode(id)
		hash, err := multihash.Sum([]byte(id), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatal(err)
		}
		node.PeerID = peer.ID(hash).String()
		node.Addresses = []string{fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 20000+i)}
		s.state.nodes[id] = node
	}
	p := 0.05
	return s, scenario.Phase{Action: "topology", Name: "ER domain", Group: "workers", Count: n, Topic: "topic", Topology: &model.TopologyConfig{Model: "er", P: &p}}
}

func topologyTestCommand(t *testing.T, r *http.Request) (string, model.TopologyRequest, model.TopologyResponse) {
	t.Helper()
	var request model.TopologyRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Error(err)
	}
	if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer topology-test" || request.RunID != "run" || request.Generation != 2 {
		t.Errorf("incorrect topology request scope or authentication: %+v", request)
	}
	nodeID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"), "/topology")
	hash, _ := multihash.Sum([]byte(nodeID), multihash.SHA2_256, -1)
	response := model.TopologyResponse{NodeID: nodeID, PeerID: peer.ID(hash).String(), TopologyID: request.TopologyID, Stage: request.Stage, Topic: request.Topic, Frozen: request.Stage == "apply", Neighbors: []string{}}
	for _, neighbor := range request.Neighbors {
		response.Neighbors = append(response.Neighbors, neighbor.PeerID)
	}
	slices.Sort(response.Neighbors)
	return nodeID, request, response
}

func TestTopologyFormationER500PreparesEntireDomainBeforeApplying(t *testing.T) {
	const n = 500
	var mu sync.Mutex
	prepared := make(map[string]model.TopologyRequest)
	applied := make(map[string]bool)
	var inFlight, peak atomic.Int64
	s, phase := topologyFormationFixture(t, n, func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		for before := peak.Load(); current > before && !peak.CompareAndSwap(before, current); before = peak.Load() {
		}
		nodeID, request, response := topologyTestCommand(t, r)
		mu.Lock()
		if request.Stage == "prepare" {
			prepared[nodeID] = request
		} else {
			if len(prepared) != n || prepared[nodeID].TopologyID != request.TopologyID {
				t.Error("apply started before all peers prepared the same plan")
			}
			if applied[nodeID] {
				t.Error("duplicate apply")
			}
			applied[nodeID] = true
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(response)
	})
	// A ready node outside the selected domain must never receive a command.
	other := s.state.nodes["node-0000"]
	other.ID, other.Group = "other", "outside"
	s.state.nodes[other.ID] = other
	if err := s.runTopology(context.Background(), "run", 2, phase, 1001); err != nil {
		t.Fatal(err)
	}
	if len(applied) != n || peak.Load() > 16 {
		t.Fatalf("applied=%d concurrency=%d", len(applied), peak.Load())
	}
	adjacency := make(map[string]map[string]bool, n)
	edges := 0
	var planID string
	for id, request := range prepared {
		if planID != "" && planID != request.TopologyID {
			t.Fatal("one domain received different topology IDs")
		}
		planID = request.TopologyID
		pid := s.state.nodes[id].PeerID
		adjacency[pid] = make(map[string]bool)
		for _, neighbor := range request.Neighbors {
			if adjacency[pid][neighbor.PeerID] || neighbor.PeerID == pid {
				t.Fatal("duplicate or self edge reached dispatch")
			}
			adjacency[pid][neighbor.PeerID] = true
			edges++
		}
	}
	if edges/2 < 5000 || edges/2 > 7500 {
		t.Fatalf("unexpected ER(500, .05) edge count: %d", edges/2)
	}
	for pid, neighbors := range adjacency {
		for neighbor := range neighbors {
			if !adjacency[neighbor][pid] {
				t.Fatal("asymmetric graph reached participants")
			}
		}
	}
	for _, event := range s.state.recentEvents() {
		if event.Type == "topology_assignment" {
			t.Fatal("full adjacency was retained in the live event feed")
		}
	}
	file, err := os.Open(filepath.Join(s.config.DataDir, currentRunsDirectory, "run", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	assignments, stages := 0, 0
	for scanner.Scan() {
		var event model.TraceEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == "topology_assignment" {
			assignments++
			if event.Fields["evidence"] != "planned" || event.Fields["topologyId"] != planID {
				t.Fatal("intended adjacency was recorded as an observed link")
			}
		}
		if event.Type == "topology_stage" {
			stages++
		}
	}
	if err := scanner.Err(); err != nil || assignments != n || stages != 2 {
		t.Fatalf("stored assignments=%d stages=%d error=%v", assignments, stages, err)
	}
}

func TestTopologyFormationPreflightAndPrepareFailuresDoNotApply(t *testing.T) {
	for _, testCase := range []string{"count", "duplicate-peer", "address", "unready", "prepare-error", "bad-ack", "trailing-ack"} {
		t.Run(testCase, func(t *testing.T) {
			var prepare, apply atomic.Int64
			s, phase := topologyFormationFixture(t, 3, func(w http.ResponseWriter, r *http.Request) {
				_, request, response := topologyTestCommand(t, r)
				if request.Stage == "apply" {
					apply.Add(1)
				} else {
					prepare.Add(1)
				}
				if testCase == "prepare-error" {
					http.Error(w, "not ready", http.StatusServiceUnavailable)
					return
				}
				if testCase == "bad-ack" {
					response.TopologyID = "another-plan"
				}
				_ = json.NewEncoder(w).Encode(response)
				if testCase == "trailing-ack" {
					_, _ = w.Write([]byte("{}"))
				}
			})
			node := s.state.nodes["node-0001"]
			switch testCase {
			case "count":
				phase.Count = 4
			case "duplicate-peer":
				node.PeerID = s.state.nodes["node-0000"].PeerID
			case "address":
				node.Addresses = []string{"invalid"}
			case "unready":
				node.State = model.NodeStarting
			}
			s.state.nodes[node.ID] = node
			err := s.runTopology(context.Background(), "run", 2, phase, 1001)
			if err == nil {
				t.Fatal("invalid topology formation succeeded")
			}
			if testCase == "prepare-error" || strings.Contains(testCase, "ack") {
				if !strings.Contains(err.Error(), "apply was not started") || strings.Contains(err.Error(), "may have applied") {
					t.Fatalf("prepare failure incorrectly described mesh application: %v", err)
				}
			}
			if apply.Load() != 0 {
				t.Fatal("preparation failure still applied a mesh")
			}
			if !strings.Contains(testCase, "ack") && testCase != "prepare-error" && prepare.Load() != 0 {
				t.Fatal("invalid input dispatched a command")
			}
		})
	}
}

func TestTopologyFormationCancellationStopsPrepare(t *testing.T) {
	started := make(chan struct{}, 1)
	var applied atomic.Bool
	s, phase := topologyFormationFixture(t, 1, func(w http.ResponseWriter, r *http.Request) {
		_, request, _ := topologyTestCommand(t, r)
		applied.Store(request.Stage == "apply")
		started <- struct{}{}
		<-r.Context().Done()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.runTopology(ctx, "run", 2, phase, 1001) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("prepare did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || applied.Load() {
			t.Fatalf("cancellation did not stop preparation: %v", err)
		}
		if !strings.Contains(err.Error(), "apply was not started") {
			t.Fatalf("cancelled preparation incorrectly described mesh application: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation retained topology workers")
	}
}

func TestTopologyFormationRejectsApplyFailureAndChangedIncarnation(t *testing.T) {
	for _, failure := range []string{"apply-error", "apply-neighbor-mismatch", "generation-change"} {
		t.Run(failure, func(t *testing.T) {
			var server *Server
			var apply atomic.Int64
			var phase scenario.Phase
			server, phase = topologyFormationFixture(t, 1, func(w http.ResponseWriter, r *http.Request) {
				nodeID, request, response := topologyTestCommand(t, r)
				if request.Stage == "apply" {
					apply.Add(1)
					if failure == "apply-error" {
						http.Error(w, "application failed", http.StatusConflict)
						return
					}
					if failure == "apply-neighbor-mismatch" {
						response.Neighbors = []string{response.PeerID}
					}
				} else if failure == "generation-change" {
					server.state.mu.Lock()
					node := server.state.nodes[nodeID]
					node.Generation++
					server.state.nodes[nodeID] = node
					server.state.mu.Unlock()
				}
				_ = json.NewEncoder(w).Encode(response)
			})
			err := server.runTopology(context.Background(), "run", 2, phase, 1001)
			if err == nil || !strings.Contains(err.Error(), "acknowledged by 0/1") {
				t.Fatalf("invalid application reported success: %v", err)
			}
			if failure == "generation-change" && apply.Load() != 0 {
				t.Fatal("a replacement incarnation received the prepared plan")
			}
			if failure == "generation-change" {
				if !strings.Contains(err.Error(), "apply was not started") || strings.Contains(err.Error(), "may have applied") {
					t.Fatalf("prepare failure incorrectly described mesh application: %v", err)
				}
			} else if !strings.Contains(err.Error(), "applied meshes remain frozen and unacknowledged requests may have applied") {
				t.Fatalf("apply failure omitted possible partial application: %v", err)
			}
			for _, event := range server.state.recentEvents() {
				if event.Type == "topology_stage" && event.Fields["stage"] == "apply" && event.Fields["error"] == nil {
					t.Fatal("a failed application was persisted as successful")
				}
			}
		})
	}
}
