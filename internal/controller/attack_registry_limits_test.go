package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackRegistryCapacityNeverReturnsPartialSelection(t *testing.T) {
	for _, limit := range []string{"identities", "encoded-bytes"} {
		t.Run(limit, func(t *testing.T) {
			s := New(ServerConfig{DataDir: t.TempDir()}, nil)
			node := model.Node{ID: "requester", RunID: "run", Generation: 1, Role: "worker", Type: "full", State: model.NodeReady, PeerID: "transport"}
			s.state.nodes[node.ID] = node
			if limit == "identities" {
				for i := 0; i < 50000; i++ {
					target := node
					target.ID = fmt.Sprintf("target-%05d", i)
					s.state.nodes[target.ID] = target
				}
			} else {
				node.Addresses = []string{strings.Repeat("a", 8<<20)}
				s.state.nodes[node.ID] = node
			}
			request := func() *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				s.handleAttackTargets(w, httptest.NewRequest(http.MethodGet, "/api/v1/attack-targets?runId=run&requesterNodeId=requester&generation=1", nil))
				return w
			}
			first := request()
			if first.Code != http.StatusServiceUnavailable {
				t.Fatalf("oversized registry returned %d, want unavailable", first.Code)
			}
			if len(s.attackTargets.raw) != 0 || !s.attackTargets.failed {
				t.Fatal("capacity failure retained a partial registry response")
			}
			// A cached failure must remain unavailable, and a later successful
			// refresh must replace it rather than leaving failure sticky.
			node.Addresses = nil
			s.state.nodes = map[string]model.Node{node.ID: node}
			// Pin freshness explicitly: marshaling the oversized fixture can
			// already consume the real one-second cache TTL under race.
			s.attackTargets.observedAt = time.Now().Add(time.Hour)
			if cached := request(); cached.Code != http.StatusServiceUnavailable {
				t.Fatal("failed cache unexpectedly exposed a selection before refresh")
			}
			s.attackTargets.observedAt = time.Now().Add(-2 * time.Second)
			recovered := request()
			if recovered.Code != http.StatusOK || s.attackTargets.failed {
				t.Fatalf("registry did not recover after capacity became available: %d", recovered.Code)
			}
			var targets []model.AttackTarget
			if err := json.Unmarshal(recovered.Body.Bytes(), &targets); err != nil || len(targets) != 1 || targets[0].NodeID != "requester" {
				t.Fatalf("recovered registry changed identity: %v %+v", err, targets)
			}
		})
	}
}

func TestAttackRegistryCachedSnapshotCannotAuthorizeTerminalRequester(t *testing.T) {
	s := New(ServerConfig{DataDir: t.TempDir()}, nil)
	requester := model.Node{ID: "requester", RunID: "run", Role: "worker", Type: "full", State: model.NodeReady, PeerID: "transport"}
	s.state.nodes[requester.ID] = requester
	request := func() int {
		w := httptest.NewRecorder()
		s.handleAttackTargets(w, httptest.NewRequest(http.MethodGet, "/api/v1/attack-targets?runId=run&requesterNodeId=requester&generation=0", nil))
		return w.Code
	}
	if request() != http.StatusOK {
		t.Fatal("initial live requester could not fetch registry")
	}
	for _, state := range []string{model.NodeStopped, model.NodeFailed} {
		requester.State = state
		s.state.nodes[requester.ID] = requester
		if got := request(); got != http.StatusConflict {
			t.Fatalf("%s requester was authorized by a cached registry: %d", state, got)
		}
	}
}
