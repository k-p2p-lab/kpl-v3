package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestTopologyCommandTimeoutDiagnosisPreservesTransportCauses(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{context.DeadlineExceeded, true},
		{&url.Error{Op: "Post", URL: "http://agent.test", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}, true},
		{&topologyAgentError{status: http.StatusGatewayTimeout, message: "peer timeout"}, true},
		{&topologyAgentError{status: http.StatusBadGateway, message: "peer returned 409: context deadline exceeded"}, false},
		{context.Canceled, false},
		{nil, false},
	} {
		if got := topologyCommandTimedOut(test.err); got != test.want {
			t.Errorf("%v: timeout=%t, want %t", test.err, got, test.want)
		}
	}
}

func TestTopologyDefaultBudgetAccountsForBoundedDispatch(t *testing.T) {
	for _, test := range []struct {
		count int
		want  time.Duration
	}{{1, 2 * time.Minute}, {16, 2 * time.Minute}, {17, 125 * time.Second}, {500, 275 * time.Second}, {1000, 430 * time.Second}} {
		if got := defaultTopologyPhaseTimeout(test.count); got != test.want {
			t.Errorf("%d peers: got %s, want %s", test.count, got, test.want)
		}
	}
}

func TestTopologyFormation1000RetriesOnlyUnacknowledgedPrepare(t *testing.T) {
	const count = 1000
	var mu sync.Mutex
	prepared := make(map[string]int)
	applied := make(map[string]int)
	var firstPlan string
	var active, peak atomic.Int64
	s, phase := topologyFormationFixture(t, count, func(w http.ResponseWriter, r *http.Request) {
		inFlight := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); inFlight > old && !peak.CompareAndSwap(old, inFlight); old = peak.Load() {
		}
		id, request, response := topologyTestCommand(t, r)
		mu.Lock()
		defer mu.Unlock()
		if firstPlan == "" {
			firstPlan = request.TopologyID
		}
		if request.TopologyID != firstPlan {
			t.Error("retry changed the graph plan")
		}
		if request.Stage == "prepare" {
			prepared[id]++
			if id == "node-0000" && prepared[id] < 3 {
				status := http.StatusServiceUnavailable
				if prepared[id] == 2 {
					status = http.StatusGatewayTimeout
				}
				http.Error(w, "temporary connection readiness failure", status)
				return
			}
		} else {
			if len(prepared) != count || prepared["node-0000"] != 3 {
				t.Error("apply began before every prepare acknowledgement")
			}
			applied[id]++
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	if err := s.runTopology(context.Background(), "run", 2, phase, 1001); err != nil {
		t.Fatal(err)
	}
	if len(applied) != count || peak.Load() > topologyCommandParallelism {
		t.Fatalf("applied=%d peak=%d", len(applied), peak.Load())
	}
	for id, attempts := range prepared {
		want := 1
		if id == "node-0000" {
			want = 3
		}
		if attempts != want || applied[id] != 1 {
			t.Fatalf("%s prepare=%d apply=%d", id, attempts, applied[id])
		}
	}
	for _, event := range s.state.recentEvents() {
		if event.Type == "topology_stage" && event.Fields["stage"] == "prepare" {
			if event.Fields["attempts"] != int64(count+2) || event.Fields["retries"] != int64(2) || event.Fields["timeoutSeconds"] != float64(430) || event.Fields["timeoutScope"] != nil {
				t.Fatalf("incorrect prepare diagnostics: %+v", event.Fields)
			}
			return
		}
	}
	t.Fatal("prepare diagnostics missing")
}

func TestTopologyFormationRetriesTransportTimeoutWithinRequestBudget(t *testing.T) {
	var prepared, applied atomic.Int64
	s, phase := topologyFormationFixture(t, 1, func(w http.ResponseWriter, r *http.Request) {
		_, request, response := topologyTestCommand(t, r)
		if request.Stage == "prepare" {
			prepared.Add(1)
		} else {
			applied.Add(1)
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	var calls atomic.Int64
	s.client = &http.Client{Timeout: time.Millisecond, Transport: discoveryTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) <= model.TopologyProxyTimeout || time.Until(deadline) > model.TopologyRequestTimeout {
			t.Errorf("Controller did not leave time for Agent diagnostics: %v", deadline)
		}
		if calls.Add(1) == 1 {
			return nil, context.DeadlineExceeded
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	if err := s.runTopology(context.Background(), "run", 2, phase, 1001); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 || prepared.Load() != 1 || applied.Load() != 1 || s.client.Timeout != time.Millisecond {
		t.Fatalf("calls=%d prepare=%d apply=%d shared timeout=%s", calls.Load(), prepared.Load(), applied.Load(), s.client.Timeout)
	}
}

func TestTopologyFormationRetryLimitsAndFailureBoundaries(t *testing.T) {
	for _, test := range []struct {
		name  string
		calls int64
		scope string
	}{
		{"exhausted", 3, "command"}, {"explicit-timeout", 1, "phase"},
		{"conflict", 1, ""}, {"permanent-proxy-error", 1, ""},
		{"invalid-ack", 1, ""}, {"changed-generation", 1, ""}, {"canceled", 1, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests, applications atomic.Int64
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var server *Server
			s, phase := topologyFormationFixture(t, 1, func(w http.ResponseWriter, r *http.Request) {
				id, request, response := topologyTestCommand(t, r)
				requests.Add(1)
				if request.Stage == "apply" {
					applications.Add(1)
				}
				switch test.name {
				case "conflict":
					http.Error(w, "conflicting plan", http.StatusConflict)
				case "permanent-proxy-error":
					http.Error(w, "peer returned 409 Conflict", http.StatusBadGateway)
				case "invalid-ack":
					response.TopologyID = "wrong-plan"
					_ = json.NewEncoder(w).Encode(response)
				case "changed-generation":
					server.state.mu.Lock()
					node := server.state.nodes[id]
					node.Generation++
					server.state.nodes[id] = node
					server.state.mu.Unlock()
					http.Error(w, "temporary", http.StatusServiceUnavailable)
				case "canceled":
					cancel()
				default:
					http.Error(w, "waiting for subscriptions", http.StatusGatewayTimeout)
				}
			})
			server = s
			if test.name == "explicit-timeout" {
				phase.Timeout = "100ms"
			}
			started := time.Now()
			err := s.runTopology(ctx, "run", 2, phase, 1001)
			if err == nil || !strings.Contains(err.Error(), "apply was not started") || applications.Load() != 0 || requests.Load() != test.calls {
				t.Fatalf("requests=%d apply=%d error=%v", requests.Load(), applications.Load(), err)
			}
			if test.name == "explicit-timeout" && (!errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second) {
				t.Fatalf("explicit phase deadline was extended: %v", err)
			}
			if test.name == "canceled" && (!errors.Is(err, context.Canceled) || time.Since(started) > time.Second) {
				t.Fatalf("cancellation retained retries: %v", err)
			}
			for _, event := range s.state.recentEvents() {
				if event.Type == "topology_stage" {
					if test.scope == "" && event.Fields["timeoutScope"] != nil || test.scope != "" && event.Fields["timeoutScope"] != test.scope {
						t.Fatalf("incorrect timeout scope: %+v", event.Fields)
					}
				}
			}
		})
	}
}

func TestTopologyFormationDoesNotRetryUnacknowledgedApply(t *testing.T) {
	var applications atomic.Int64
	s, phase := topologyFormationFixture(t, 1, func(w http.ResponseWriter, r *http.Request) {
		_, request, response := topologyTestCommand(t, r)
		if request.Stage == "apply" {
			applications.Add(1)
			http.Error(w, "application response unavailable", http.StatusGatewayTimeout)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	err := s.runTopology(context.Background(), "run", 2, phase, 1001)
	if err == nil || applications.Load() != 1 || !strings.Contains(err.Error(), "unacknowledged requests may have applied") {
		t.Fatalf("apply attempts=%d error=%v", applications.Load(), err)
	}
}
