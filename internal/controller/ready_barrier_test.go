package controller

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestReadyBarrierDiagnosesUnavailableOwners(t *testing.T) {
	for _, test := range []struct {
		name, agentState, wantOwner string
		lastSeen                    time.Time
		registered                  bool
	}{
		{name: "offline", agentState: model.AgentOffline, registered: true, wantOwner: "3/offline/lastSeen=unknown"},
		{name: "stale", agentState: model.AgentOnline, registered: true, lastSeen: time.Now().Add(-time.Minute), wantOwner: "3/stale/lastSeen="},
		{name: "unregistered", wantOwner: "3/unregistered/lastSeen=unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newLifecycleTestController(t)
			seedReadinessBootPeers(s, 7)
			s.state.mu.Lock()
			if test.registered {
				s.state.agents["unavailable"] = model.Agent{ID: "unavailable", State: test.agentState, LastSeen: test.lastSeen, URL: "https://private.invalid/?token=not-for-diagnostics"}
			}
			for i := 7; i < 10; i++ {
				id := fmt.Sprintf("boot-%02d", i)
				s.state.nodes[id] = model.Node{ID: id, RunID: "run", Generation: 1, Group: "boot", Type: "full", AgentID: "unavailable", State: model.NodeReady, Error: "secret remote error"}
			}
			s.state.mu.Unlock()
			phase := scenario.Phase{Group: "boot", NodeType: "full", MinCount: 10, ReadyRatio: 1, Timeout: "10ms"}
			err := s.waitReady(context.Background(), "run", 1, phase)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout cause lost: %v", err)
			}
			for _, want := range []string{`ready barrier timed out for group "boot" in generation 1: 7/10 ready, 0 failed`, "ready-on-unavailable-agent=3", `"unavailable":` + test.wantOwner, `"boot-07":ready/agent-unavailable@"unavailable"`} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("diagnostic %q missing %q", err, want)
				}
			}
			if strings.Contains(err.Error(), "secret remote error") || strings.Contains(err.Error(), "private.invalid") || strings.Contains(err.Error(), "token=") {
				t.Fatalf("remote error or configuration included in diagnostics: %v", err)
			}
		})
	}
}

func TestReadyBarrierDiagnosesStatesAndFiltersCohort(t *testing.T) {
	s := newLifecycleTestController(t)
	seedReadinessBootPeers(s, 7)
	s.state.mu.Lock()
	for i, state := range []string{model.NodeStarting, model.NodeStopping, model.NodeStopped, model.NodeFailed, "future-state"} {
		id := fmt.Sprintf("boot-%02d", i+7)
		s.state.nodes[id] = model.Node{ID: id, RunID: "run", Generation: 1, Group: "boot", Type: "full", AgentID: "available", State: state}
	}
	for _, node := range []model.Node{
		{ID: "different-generation", RunID: "run", Generation: 2, Group: "boot", Type: "full", State: model.NodeFailed},
		{ID: "different-group", RunID: "run", Generation: 1, Group: "workers", Type: "full", State: model.NodeFailed},
		{ID: "different-type", RunID: "run", Generation: 1, Group: "boot", Type: "other", State: model.NodeFailed},
		{ID: "different-run", RunID: "other-run", Generation: 1, Group: "boot", Type: "full", State: model.NodeFailed},
	} {
		s.state.nodes[node.ID] = node
	}
	s.state.mu.Unlock()
	err := s.readyBarrierTimeoutError("run", 1, scenario.Phase{Group: "boot", NodeType: "full"}, context.DeadlineExceeded)
	for _, want := range []string{"7/12 ready, 1 failed", "starting=1, stopping=1, stopped=1, ready-on-unavailable-agent=0, unknown=1", "unavailable owners=[]"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("diagnostic %q missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "different-") || strings.Contains(err.Error(), "future-state") {
		t.Fatalf("diagnostics included filtered peers or an arbitrary state value: %v", err)
	}
	if ready, total, failed := s.readyCount("run", 1, "boot", "full"); ready != 7 || total != 12 || failed != 1 {
		t.Fatalf("readiness counts changed: %d/%d ready, %d failed", ready, total, failed)
	}
}

func TestReadyBarrierDiagnosticsAreBoundedAndDeterministic(t *testing.T) {
	s := newLifecycleTestController(t)
	s.state.mu.Lock()
	for i := 9; i >= 0; i-- {
		id, agentID := fmt.Sprintf("node-%02d", i), fmt.Sprintf("agent-%02d", i)
		s.state.nodes[id] = model.Node{ID: id, RunID: "run", Generation: 1, AgentID: agentID, State: model.NodeStarting}
	}
	s.state.mu.Unlock()
	first := s.readyBarrierTimeoutError("run", 1, scenario.Phase{}, context.DeadlineExceeded).Error()
	for i := 0; i < 10; i++ {
		if got := s.readyBarrierTimeoutError("run", 1, scenario.Phase{}, context.DeadlineExceeded).Error(); got != first {
			t.Fatalf("map iteration changed diagnostic:\n%s\n%s", first, got)
		}
	}
	for _, want := range []string{`"agent-00":1/unregistered`, `"agent-02":1/unregistered`, "+7 more", `"node-00":starting@"agent-00"`, `"node-02":starting@"agent-02"`} {
		if !strings.Contains(first, want) {
			t.Errorf("diagnostic %q missing %q", first, want)
		}
	}
	for _, omitted := range []string{"agent-03", "node-03", "agent-09", "node-09"} {
		if strings.Contains(first, omitted) {
			t.Errorf("diagnostic included a fourth sample: %s", omitted)
		}
	}
	if got := readinessDiagnosticID(strings.Repeat("한", 1000) + "\n\t"); strings.ContainsAny(got, "\n\t") || len([]rune(got)) > 90 || !strings.Contains(got, "...") {
		t.Fatalf("long/control ID was not bounded and quoted: %q", got)
	}
}

func TestReadyBarrierDistinguishesDeferredAdmissionFromExistingPeers(t *testing.T) {
	var calls atomic.Int32
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set(model.AgentAdmissionRetryHeader, "not-created")
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusServiceUnavailable, "Agent telemetry backlog is full; retry Peer admission after delivery recovers")
	}))
	defer endpoint.Close()
	s := newLifecycleTestController(t)
	seedReadinessBootPeers(s, 7)
	if _, err := s.state.registerAgent(model.Agent{ID: "busy", URL: endpoint.URL, Capacity: 3}); err != nil {
		t.Fatal(err)
	}
	for i := 7; i < 10; i++ {
		id := fmt.Sprintf("boot-%02d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		agent, err := s.acquireAgentWithPlacement(ctx, id, "busy", nil)
		if err == nil {
			err = s.createReservedNode(ctx, model.CreateNodeRequest{ID: id, RunID: "run", Generation: 1, Group: "boot", Type: "full"}, agent, "busy", nil)
		}
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deferred create should remain cancellable: %v", err)
		}
	}
	if calls.Load() != 1 || len(s.state.reservations) != 0 || len(s.state.uncertainCreates) != 0 {
		t.Fatalf("deferred creation leaked admission state or ignored cooldown: calls=%d reservations=%d uncertain=%d", calls.Load(), len(s.state.reservations), len(s.state.uncertainCreates))
	}
	phase := scenario.Phase{Group: "boot", NodeType: "full", MinCount: 10, ReadyRatio: 1, Timeout: "10ms"}
	err := s.waitReady(context.Background(), "run", 1, phase)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "7/7 ready, 0 failed") {
		t.Fatalf("pre-admission 503 fabricated registered peers or bypassed minCount: %v", err)
	}
	s.state.mu.Lock()
	for i := 7; i < 10; i++ {
		id := fmt.Sprintf("boot-%02d", i)
		s.state.nodes[id] = model.Node{ID: id, RunID: "run", Generation: 1, Group: "boot", Type: "full", AgentID: "busy", State: model.NodeStarting}
	}
	s.state.mu.Unlock()
	err = s.waitReady(context.Background(), "run", 1, phase)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "7/10 ready, 0 failed; starting=3") {
		t.Fatalf("existing starting peers were mistaken for failed or absent peers: %v", err)
	}
}

func seedReadinessBootPeers(s *Server, count int) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.agents["available"] = model.Agent{ID: "available", State: model.AgentOnline}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("boot-%02d", i)
		s.state.nodes[id] = model.Node{ID: id, RunID: "run", Generation: 1, Group: "boot", Type: "full", AgentID: "available", State: model.NodeReady}
	}
}
