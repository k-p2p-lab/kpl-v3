package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestOperationContinueRecordsFailuresAndAttemptsRemainingNodes(t *testing.T) {
	for _, action := range []string{"publish", "leave"} {
		for _, parallel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parallel=%t", action, parallel), func(t *testing.T) {
				var mu sync.Mutex
				var attempts []string
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					attempts = append(attempts, r.URL.Path)
					mu.Unlock()
					if strings.Contains(r.URL.Path, "node-a") {
						http.Error(w, "peer expired", http.StatusNotFound)
						return
					}
					w.WriteHeader(http.StatusAccepted)
				}))
				defer api.Close()
				server := New(ServerConfig{DataDir: t.TempDir()}, nil)
				server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
				for _, id := range []string{"node-a", "node-b"} {
					server.state.nodes[id] = model.Node{ID: id, RunID: "run", Group: "workers", AgentID: "agent", Type: "full", State: model.NodeReady}
				}
				phase := scenario.Phase{Name: "churn", Job: "job-1", Action: action, Group: "workers", Count: 2, OnError: "continue", Parallel: parallel, PayloadSize: 8, Interval: scenario.Distribution{Model: "fixed", Value: "1ms"}}
				var err error
				if action == "publish" {
					err = server.runPublish(context.Background(), "run", phase, rand.New(rand.NewSource(1)))
				} else {
					err = server.runLeave(context.Background(), "run", phase, rand.New(rand.NewSource(1)))
				}
				if err != nil || len(attempts) != 2 {
					t.Fatalf("error = %v, attempts = %v", err, attempts)
				}
				if len(server.state.events) != 1 {
					t.Fatalf("events = %+v", server.state.events)
				}
				event := server.state.events[0]
				if event.Type != "phase-operation-failed" || event.NodeID != "node-a" || event.RunID != "run" || event.Fields["action"] != action || event.Fields["job"] != "job-1" {
					t.Fatalf("failure event = %+v", event)
				}
				data, err := os.ReadFile(filepath.Join(server.config.DataDir, currentRunsDirectory, "run", "events.jsonl"))
				if err != nil || !strings.Contains(string(data), "peer expired") {
					t.Fatalf("persisted error = %v, events = %s", err, data)
				}
			})
		}
	}
}

func TestPublishDefaultPolicyFails(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "peer expired", http.StatusNotFound)
	}))
	defer api.Close()
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
	server.state.nodes["node"] = model.Node{ID: "node", RunID: "run", Group: "workers", AgentID: "agent", Type: "full", State: model.NodeReady}
	err := server.runPublish(context.Background(), "run", scenario.Phase{Group: "workers", Count: 1}, rand.New(rand.NewSource(1)))
	if err == nil || !strings.Contains(err.Error(), "peer expired") {
		t.Fatalf("publish error = %v", err)
	}
}

func TestContinueWithoutCandidatesAndCancellation(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	for _, action := range []string{"publish", "leave"} {
		phase := scenario.Phase{Action: action, Group: "workers", Count: 1, OnError: "continue"}
		run := server.runPublish
		if action == "leave" {
			run = server.runLeave
		}
		if err := run(context.Background(), "run", phase, rand.New(rand.NewSource(1))); err != nil {
			t.Fatalf("empty %s: %v", action, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := run(ctx, "run", phase, rand.New(rand.NewSource(1))); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled %s error = %v", action, err)
		}
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
			err := server.phaseOperationError(context.Background(), "run", phase, model.Node{ID: "node"}, "", fmt.Errorf("request failed: %w", cause))
			if err != nil {
				t.Fatalf("%s failed on an individual request error %v: %v", action, cause, err)
			}
		}
		deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer deadlineCancel()
		if err := run(deadlineCtx, "run", phase, rand.New(rand.NewSource(1))); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s swallowed phase deadline: %v", action, err)
		}
	}
}

func TestWildcardPublishUsesRawPayloadAndPerTopicTargets(t *testing.T) {
	var requests []model.PublishRequest
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request model.PublishRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		requests = append(requests, request)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer api.Close()
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
	server.state.nodes["publisher"] = model.Node{ID: "publisher", RunID: "run", Group: "workers", AgentID: "agent", Type: "publisher", State: model.NodeReady, Metadata: map[string]string{"topicsJSON": `["z,priority","a","a"]`}}
	server.state.nodes["subscriber"] = model.Node{ID: "subscriber", RunID: "run", AgentID: "agent", Type: "subscriber", State: model.NodeReady, Metadata: map[string]string{"topicsJSON": `["z,priority"]`}}
	phase := scenario.Phase{Action: "publish", Group: "workers", Count: 1, Topic: "*", PayloadSize: 32, PayloadEncoding: "raw", OnError: "continue"}
	if err := server.runPublish(context.Background(), "run", phase, rand.New(rand.NewSource(1))); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 || requests[0].Topic != "*" || !reflect.DeepEqual(requests[0].TargetNodesByTopic, map[string]int{"a": 1, "z,priority": 2}) {
		t.Fatalf("requests = %+v", requests)
	}
	for _, request := range requests {
		if request.PayloadEncoding != "raw" || request.PayloadSize != 32 {
			t.Fatalf("payload changed: %+v", request)
		}
	}
}

func TestContinuePreservesPhaseAfterIndividualRequestTimeout(t *testing.T) {
	for _, action := range []string{"publish", "leave"} {
		t.Run(action, func(t *testing.T) {
			release := make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-release
			}))
			defer api.Close()
			defer close(release)
			server := New(ServerConfig{DataDir: t.TempDir()}, nil)
			server.client.Timeout = 10 * time.Millisecond
			server.state.agents["agent"] = model.Agent{ID: "agent", URL: api.URL, State: model.AgentOnline}
			for _, id := range []string{"node-a", "node-b"} {
				server.state.nodes[id] = model.Node{ID: id, RunID: "run", Group: "workers", AgentID: "agent", Type: "full", State: model.NodeReady}
			}
			phase := scenario.Phase{Action: action, Group: "workers", Count: 2, OnError: "continue"}
			run := server.runPublish
			if action == "leave" {
				run = server.runLeave
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := run(ctx, "run", phase, rand.New(rand.NewSource(1))); err != nil {
				t.Fatal(err)
			}
			if len(server.state.events) != 2 || ctx.Err() != nil {
				t.Fatalf("failure events = %d, phase error = %v", len(server.state.events), ctx.Err())
			}
		})
	}
}

func TestJoinPlacementAndSerialBatchDispatch(t *testing.T) {
	run := func(t *testing.T, placement, agentID string, reverse bool) []string {
		t.Helper()
		server := New(ServerConfig{DataDir: t.TempDir()}, nil)
		var assigned []string
		var nodeIDs []string
		ids := []string{"agent-a", "agent-b", "agent-c"}
		if reverse {
			ids[0], ids[2] = ids[2], ids[0]
		}
		for _, id := range ids {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request model.CreateNodeRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				assigned = append(assigned, id)
				nodeIDs = append(nodeIDs, request.ID)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(api.Close)
			server.state.agents[id] = model.Agent{ID: id, URL: api.URL, State: model.AgentOnline}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		phase := scenario.Phase{Group: "workers", Count: 12, Placement: placement, AgentID: agentID, Parallel: true, Parallelism: 1, Interval: scenario.Distribution{Model: "fixed", Value: "1h"}}
		if err := server.runJoin(ctx, "run", 1, phase, rand.New(rand.NewSource(42))); err != nil {
			t.Fatal(err)
		}
		for i, nodeID := range nodeIDs {
			if want := fmt.Sprintf("run-workers-%05d", i+1); nodeID != want {
				t.Fatalf("dispatch %d = %q, want %q", i, nodeID, want)
			}
		}
		return assigned
	}
	for _, placement := range []string{"balanced", "random", "single-agent"} {
		t.Run(placement, func(t *testing.T) {
			first := run(t, placement, "", false)
			second := run(t, placement, "", true)
			if !reflect.DeepEqual(first, second) {
				t.Fatalf("placement depends on map order: %v vs %v", first, second)
			}
			unique := map[string]bool{}
			for _, id := range first {
				unique[id] = true
			}
			if placement == "single-agent" && len(unique) != 1 || placement != "single-agent" && len(unique) < 2 {
				t.Fatalf("placement %s assigned %v", placement, first)
			}
		})
	}
	t.Run("explicit agent overrides placement", func(t *testing.T) {
		for _, id := range run(t, "random", "agent-b", false) {
			if id != "agent-b" {
				t.Fatalf("explicit placement escaped to %q", id)
			}
		}
	})
}

func TestPinnedAgentWaitsForCapacityWithoutSpilling(t *testing.T) {
	server := New(ServerConfig{DataDir: t.TempDir()}, nil)
	server.state.agents["full"] = model.Agent{ID: "full", State: model.AgentOnline, Capacity: 1, ActiveNodes: 1}
	server.state.agents["free"] = model.Agent{ID: "free", State: model.AgentOnline, Capacity: 10}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if agent, err := server.acquireAgentWithPlacement(ctx, "node", "full", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquired %+v with error %v", agent, err)
	}
	if len(server.state.reservations) != 0 {
		t.Fatalf("unexpected reservation: %v", server.state.reservations)
	}
}
