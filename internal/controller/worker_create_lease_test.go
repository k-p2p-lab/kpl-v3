package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type brokerCreateFixture struct {
	b        *runWorkerBroker
	owner    model.Agent
	request  model.CreateNodeRequest
	call     workerAgentCall
	calls    atomic.Int32
	log      bytes.Buffer
	endpoint *httptest.Server
	bodies   chan []byte
}

func newBrokerCreateFixture(t *testing.T, response func(*brokerCreateFixture, http.ResponseWriter, *http.Request, model.CreateNodeRequest)) *brokerCreateFixture {
	t.Helper()
	f := &brokerCreateFixture{bodies: make(chan []byte, 20)}
	f.endpoint = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		f.calls.Add(1)
		var request model.CreateNodeRequest
		var raw json.RawMessage
		if json.NewDecoder(r.Body).Decode(&raw) != nil || json.Unmarshal(raw, &request) != nil {
			http.Error(w, "bad request", 400)
			return
		}
		f.bodies <- append([]byte(nil), raw...)
		if response != nil {
			response(f, w, r, request)
			return
		}
		writeJSON(w, http.StatusCreated, model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, AgentID: f.owner.ID, State: model.NodeReady})
	}))
	t.Cleanup(f.endpoint.Close)
	s := New(ServerConfig{DataDir: t.TempDir()}, slog.New(slog.NewJSONHandler(&f.log, nil)))
	f.owner = model.Agent{ID: "agent-one", URL: f.endpoint.URL, State: model.AgentOnline, Capacity: 10, StartedAt: time.Now().Add(-time.Minute).UTC(), RunProcesses: true, RunDrain: true, CreateReplay: true}
	if _, err := s.state.registerAgent(f.owner); err != nil {
		t.Fatal(err)
	}
	s.state.experiments["broker-run"] = model.Experiment{ID: "broker-run", ExecutionID: "execution-one", State: "running"}
	f.b = &runWorkerBroker{server: s, runID: "broker-run", executionID: "execution-one", generation: 1}
	f.request = model.CreateNodeRequest{ID: "broker-run-boot-1", RunID: "broker-run", Generation: 1, Group: "boot", Type: "full", Seed: 42, AgentStartedAt: f.owner.StartedAt}
	reservation, _ := json.Marshal(workerReservation{NodeID: f.request.ID, AgentID: f.owner.ID})
	if _, err := f.b.dispatch(t.Context(), "reserve", reservation); err != nil {
		t.Fatal(err)
	}
	f.setRequest(t, f.request)
	return f
}

func (f *brokerCreateFixture) setRequest(t *testing.T, request model.CreateNodeRequest) {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	f.call = workerAgentCall{BaseURL: f.owner.URL, Method: http.MethodPost, Path: "/api/v1/nodes", Input: raw, Output: true}
}

func (f *brokerCreateFixture) mutateAgent(change func(*model.Agent)) {
	s := f.b.server.state
	s.mu.Lock()
	a := s.agents[f.owner.ID]
	change(&a)
	s.agents[a.ID] = a
	s.mu.Unlock()
}

func (f *brokerCreateFixture) publishCreatedHeartbeat(t *testing.T, request model.CreateNodeRequest) {
	t.Helper()
	a := f.owner
	a.LastSeen = time.Now().UTC()
	node := model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, AgentID: a.ID, State: model.NodeReady, LastSeen: a.LastSeen}
	if err := f.b.server.state.heartbeat(model.AgentHeartbeat{Agent: a, Nodes: []model.Node{node}}); err != nil {
		t.Error(err)
	}
}

func TestWorkerCreateAvailabilityRaceDefersBeforeHTTP(t *testing.T) {
	for _, mode := range []string{"stale-heartbeat", "disabled", "cooldown"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerCreateFixture(t, nil)
			// The child's last inventory and its available() check can still be
			// fresh while the authoritative parent changes immediately afterward.
			switch mode {
			case "stale-heartbeat":
				f.mutateAgent(func(a *model.Agent) { a.LastSeen = time.Now().Add(-agentStaleAfter - time.Second) })
			case "disabled":
				f.mutateAgent(func(a *model.Agent) { a.Disabled = true })
			case "cooldown":
				f.b.server.state.agentAdmissionUntil[f.owner.ID] = time.Now().Add(time.Minute)
			}
			_, err := f.b.dispatchAgent(t.Context(), f.call)
			var deferred *agentAdmissionDeferredError
			if !errors.As(err, &deferred) || f.calls.Load() != 0 {
				t.Fatalf("pre-admission race became fatal or sent HTTP: calls=%d err=%v", f.calls.Load(), err)
			}
			wire := encodeWorkerError(err)
			if wire.Kind != "deferred" || !errors.As(wire.decode(), &deferred) {
				t.Fatalf("deferral was lost across worker IPC: %+v", wire)
			}
			if !strings.Contains(f.log.String(), `"reason":"agent-`) {
				t.Fatalf("missing structured denial reason: %s", f.log.String())
			}
			// Exercise the same explicit release/re-reservation sequence used
			// by createReservedNode for a typed pre-admission deferral.
			raw, _ := json.Marshal(f.request.ID)
			if _, err := f.b.dispatch(t.Context(), "release", raw); err != nil {
				t.Fatal(err)
			}
			f.mutateAgent(func(a *model.Agent) { a.Disabled = false; a.LastSeen = time.Now().UTC() })
			delete(f.b.server.state.agentAdmissionUntil, f.owner.ID)
			reserve, _ := json.Marshal(workerReservation{NodeID: f.request.ID, AgentID: f.owner.ID})
			if _, err := f.b.dispatch(t.Context(), "reserve", reserve); err != nil {
				t.Fatal(err)
			}
			if _, err := f.b.dispatchAgent(t.Context(), f.call); err != nil || f.calls.Load() != 1 {
				t.Fatalf("recovered placement failed: calls=%d err=%v", f.calls.Load(), err)
			}
		})
	}
}

func TestWorkerCreateRejectsInvalidRunReservationAndInstance(t *testing.T) {
	for _, mode := range []string{"foreign-run", "foreign-generation", "no-lease", "no-reservation", "wrong-owner", "wrong-url", "new-agent-instance", "request-instance", "no-process-support", "no-drain-support", "closed", "drained", "fenced", "execution-changed", "not-running", "stop-requested"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerCreateFixture(t, nil)
			s := f.b.server.state
			switch mode {
			case "foreign-run":
				r := f.request
				r.RunID = "other-run"
				f.setRequest(t, r)
			case "foreign-generation":
				r := f.request
				r.Generation++
				f.setRequest(t, r)
			case "no-lease":
				f.b.createLeases = nil
			case "no-reservation":
				delete(s.reservations, f.request.ID)
			case "wrong-owner":
				s.reservations[f.request.ID] = "other-agent"
			case "wrong-url":
				f.call.BaseURL = "http://other-agent.invalid"
			case "new-agent-instance":
				f.mutateAgent(func(a *model.Agent) { a.StartedAt = a.StartedAt.Add(time.Second) })
			case "request-instance":
				r := f.request
				r.AgentStartedAt = r.AgentStartedAt.Add(time.Second)
				f.setRequest(t, r)
			case "no-process-support":
				f.mutateAgent(func(a *model.Agent) { a.RunProcesses = false })
			case "no-drain-support":
				f.mutateAgent(func(a *model.Agent) { a.RunDrain = false })
			case "closed":
				f.b.closed = true
			case "drained":
				f.b.drained = true
			case "fenced":
				f.b.createFenced = true
			case "execution-changed":
				r := s.experiments[f.b.runID]
				r.ExecutionID = "new-execution"
				s.experiments[r.ID] = r
			case "not-running":
				r := s.experiments[f.b.runID]
				r.State = "completed"
				s.experiments[r.ID] = r
			case "stop-requested":
				r := s.experiments[f.b.runID]
				r.StopRequested = true
				s.experiments[r.ID] = r
			}
			_, err := f.b.dispatchAgent(t.Context(), f.call)
			var denied *workerCreateAuthorizationError
			var deferred *agentAdmissionDeferredError
			if !errors.As(err, &denied) || errors.As(err, &deferred) || f.calls.Load() != 0 {
				t.Fatalf("invalid capability was allowed or deferred: calls=%d err=%v", f.calls.Load(), err)
			}
			if mode == "stop-requested" && (!errors.Is(err, context.Canceled) || !errors.Is(encodeWorkerError(err).decode(), context.Canceled)) {
				t.Fatalf("Stop was reported as a scientific failure: %v", err)
			}
		})
	}
}

func TestWorkerCreateReplayRecoversHeartbeatConsumedReservation(t *testing.T) {
	f := newBrokerCreateFixture(t, func(f *brokerCreateFixture, w http.ResponseWriter, r *http.Request, request model.CreateNodeRequest) {
		if f.calls.Load() == 1 {
			f.publishCreatedHeartbeat(t, request)
			dropCreateResponse(t, w)
			return
		}
		writeJSON(w, 201, model.Node{ID: request.ID, RunID: request.RunID, Generation: request.Generation, Group: request.Group, AgentID: f.owner.ID, State: model.NodeReady})
	})
	_, err := f.b.dispatchAgent(t.Context(), f.call)
	var uncertain *uncertainNodeCreateError
	if !errors.As(err, &uncertain) {
		t.Fatalf("lost acknowledgement did not preserve uncertainty: %v", err)
	}
	f.b.server.state.mu.RLock()
	reserved := f.b.server.state.reservations[f.request.ID]
	f.b.server.state.mu.RUnlock()
	if reserved != "" {
		t.Fatal("heartbeat fixture did not consume the reservation")
	}
	output, err := f.b.dispatchAgent(t.Context(), f.call)
	var node model.Node
	if err != nil || json.Unmarshal(output, &node) != nil || node.ID != f.request.ID || f.calls.Load() != 2 {
		t.Fatalf("same-body replay failed: calls=%d output=%s err=%v", f.calls.Load(), output, err)
	}
	first, second := <-f.bodies, <-f.bodies
	if !bytes.Equal(first, second) || !bytes.Equal(first, f.call.Input) {
		t.Fatal("replay changed the original HTTP create bytes")
	}
	created, _ := json.Marshal(workerCreated{Request: f.request, AgentID: f.owner.ID, Node: node, Expected: []time.Time{f.owner.StartedAt}})
	if _, err := f.b.dispatch(t.Context(), "created", created); err != nil {
		t.Fatal(err)
	}
	if len(f.b.createLeases) != 0 {
		t.Fatal("acknowledged creation retained its capability")
	}
}

func TestWorkerCreateReplayNeverUsesInventedOrChangedCapability(t *testing.T) {
	for _, mode := range []string{"never-dispatched", "missing-lease", "changed-body", "new-instance", "foreign-generation", "different-owner", "new-reservation-owner", "terminal-peer", "stopping-peer", "no-replay", "stopped", "drained"} {
		t.Run(mode, func(t *testing.T) {
			f := newBrokerCreateFixture(t, func(f *brokerCreateFixture, w http.ResponseWriter, r *http.Request, request model.CreateNodeRequest) {
				f.publishCreatedHeartbeat(t, request)
				dropCreateResponse(t, w)
			})
			attempted := mode != "never-dispatched" && mode != "missing-lease"
			if attempted {
				_, _ = f.b.dispatchAgent(t.Context(), f.call)
			} else {
				f.publishCreatedHeartbeat(t, f.request)
			}
			s := f.b.server.state
			switch mode {
			case "missing-lease":
				f.b.createLeases = nil
			case "changed-body":
				r := f.request
				r.Seed++
				f.setRequest(t, r)
			case "new-instance":
				f.mutateAgent(func(a *model.Agent) { a.StartedAt = a.StartedAt.Add(time.Second) })
			case "foreign-generation":
				r := f.request
				r.Generation++
				f.setRequest(t, r)
			case "different-owner":
				s.mu.Lock()
				n := s.nodes[f.request.ID]
				n.AgentID = "other-agent"
				s.setNodeLocked(n)
				s.mu.Unlock()
			case "new-reservation-owner":
				s.mu.Lock()
				s.reservations[f.request.ID] = "other-agent"
				s.mu.Unlock()
			case "terminal-peer":
				s.mu.Lock()
				n := s.nodes[f.request.ID]
				n.State = model.NodeStopped
				s.setNodeLocked(n)
				s.mu.Unlock()
			case "stopping-peer":
				s.mu.Lock()
				n := s.nodes[f.request.ID]
				n.State = model.NodeStopping
				s.setNodeLocked(n)
				s.mu.Unlock()
			case "no-replay":
				f.mutateAgent(func(a *model.Agent) { a.CreateReplay = false })
			case "stopped":
				r := s.experiments[f.b.runID]
				r.StopRequested = true
				s.experiments[r.ID] = r
			case "drained":
				f.b.drained = true
			}
			_, err := f.b.dispatchAgent(t.Context(), f.call)
			if err == nil {
				t.Fatal("invented or changed replay capability was accepted")
			}
			want := int32(0)
			if attempted {
				want = 1
				var uncertain *uncertainNodeCreateError
				if !errors.As(err, &uncertain) || encodeWorkerError(err).Kind != "uncertain" {
					t.Fatalf("a post-dispatch denial released uncertainty: %v", err)
				}
			}
			if f.calls.Load() != want {
				t.Fatalf("denied replay reached HTTP: calls=%d want=%d", f.calls.Load(), want)
			}
		})
	}
}

func TestWorkerCreateTerminalFenceBlocksGenerationOverflow(t *testing.T) {
	f := newBrokerCreateFixture(t, nil)
	if _, err := f.b.dispatchAgent(t.Context(), f.call); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(workerGeneration{RunID: f.b.runID, Generation: ^uint64(0)})
	if _, err := f.b.dispatch(t.Context(), "stop-generation", raw); err != nil {
		t.Fatal(err)
	}
	if !f.b.createFenced || len(f.b.createLeases) != 0 {
		t.Fatal("terminal generation fence retained create capabilities")
	}
	if _, err := f.b.dispatchAgent(t.Context(), f.call); err == nil {
		t.Fatal("uint64 generation overflow reopened creation")
	}
	reservation, _ := json.Marshal(workerReservation{NodeID: f.request.ID, AgentID: f.owner.ID})
	if _, err := f.b.dispatch(t.Context(), "reserve", reservation); err == nil {
		t.Fatal("terminal fence admitted a fresh reservation")
	}
	if f.calls.Load() != 1 {
		t.Fatal("terminal fence sent another HTTP create")
	}
}

func TestWorkerCreateLeaseBoundDoesNotEvictPendingAdmission(t *testing.T) {
	f := newBrokerCreateFixture(t, nil)
	first := f.b.createLeases[f.request.ID]
	for i := 1; i < workerCreateLeaseLimit; i++ {
		f.b.createLeases[fmt.Sprintf("reserved-%d", i)] = &workerCreateLease{attempted: true}
	}
	if err := f.b.bindCreateReservation("broker-run-other", f.owner); err == nil {
		t.Fatal("pending create capability count is unbounded")
	}
	if len(f.b.createLeases) != workerCreateLeaseLimit || f.b.createLeases[f.request.ID] != first {
		t.Fatal("lease bound discarded existing pending evidence")
	}
	if _, err := f.b.dispatchAgent(t.Context(), f.call); err != nil {
		t.Fatalf("lease bound invalidated an existing reservation: %v", err)
	}
}

func TestWorkerCreateCancellationAndExplicitRefusalDoNotConsumeReplayEvidence(t *testing.T) {
	f := newBrokerCreateFixture(t, func(f *brokerCreateFixture, w http.ResponseWriter, r *http.Request, request model.CreateNodeRequest) {
		w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
		w.Header().Set("Retry-After", "1")
		writeError(w, 503, "telemetry backlog")
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.b.dispatchAgent(ctx, f.call); !errors.Is(err, context.Canceled) || f.calls.Load() != 0 {
		t.Fatalf("canceled create dispatched HTTP: %v", err)
	}
	_, err := f.b.dispatchAgent(t.Context(), f.call)
	var deferred *agentAdmissionDeferredError
	if !errors.As(err, &deferred) || f.b.createLeases[f.request.ID].attempted {
		t.Fatalf("definite no-create refusal retained possible admission: %v", err)
	}
}
