package peer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func newMeshFreezeTestServer(t *testing.T, enabled bool) *Server {
	t.Helper()
	s := newRunTestServer(t, model.PeerProcessConfig{
		Node: model.Node{ID: "node", RunID: "run", Generation: 2}, Token: "test-token",
		NodeConfig: model.NodeConfig{GossipSub: model.GossipSubConfig{MeshFreeze: &enabled}},
	})
	ctx, cancel := context.WithCancel(t.Context())
	options, err := gossipSubOptions(s.config.NodeConfig.GossipSub, discardEventTracer{})
	if err != nil {
		t.Fatal(err)
	}
	s.pubsub, err = pubsub.NewGossipSub(ctx, s.host, options...)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	return s
}

func TestMeshFreezeHTTPValidatesScopeAndConfiguration(t *testing.T) {
	for _, test := range []struct {
		name, method, nodeID, body, token string
		enabled                           bool
		want                              int
	}{
		{"disabled", "POST", "node", `{"runId":"run","generation":2}`, "test-token", false, 409},
		{"recycled IP", "POST", "previous-node", `{"runId":"run","generation":2}`, "test-token", true, 409},
		{"missing target", "POST", "", `{"runId":"run","generation":2}`, "test-token", true, 409},
		{"old run", "POST", "node", `{"runId":"old","generation":2}`, "test-token", true, 409},
		{"old generation", "POST", "node", `{"runId":"run","generation":1}`, "test-token", true, 409},
		{"trailing JSON", "POST", "node", `{"runId":"run","generation":2}{}`, "test-token", true, 400},
		{"unknown field", "POST", "node", `{"runId":"run","generation":2,"enabled":true}`, "test-token", true, 400},
		{"unauthorized", "POST", "node", `{"runId":"run","generation":2}`, "wrong-token", true, 401},
		{"method", "GET", "node", `{"runId":"run","generation":2}`, "test-token", true, 405},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newMeshFreezeTestServer(t, test.enabled)
			r := httptest.NewRequest(test.method, "/mesh-freeze", strings.NewReader(test.body))
			r.Header.Set("X-KPL-Node-ID", test.nodeID)
			r.Header.Set("Authorization", "Bearer "+test.token)
			w := httptest.NewRecorder()
			s.handler().ServeHTTP(w, r)
			if w.Code != test.want || s.meshFrozen.Load() {
				t.Fatalf("status=%d body=%s frozen=%v", w.Code, w.Body, s.meshFrozen.Load())
			}
		})
	}
}

func TestMeshFreezeHTTPAcknowledgesApplicationAndIsIdempotent(t *testing.T) {
	s := newMeshFreezeTestServer(t, true)
	before := model.Node{Metadata: map[string]string{"meshFrozen": "false"}}
	s.addMeshFreezeStatus(&before)
	if before.Metadata["meshFrozen"] != "false" {
		t.Fatal("capability froze the mesh before a command")
	}
	for i := 0; i < 2; i++ {
		r := httptest.NewRequest(http.MethodPost, "/mesh-freeze", strings.NewReader(`{"runId":"run","generation":2}`))
		r.Header.Set("X-KPL-Node-ID", "node")
		r.Header.Set("Authorization", "Bearer test-token")
		w := httptest.NewRecorder()
		s.handler().ServeHTTP(w, r)
		var response model.MeshFreezeResponse
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || !response.Frozen || response.NodeID != "node" || response.PeerID != s.host.ID().String() {
			t.Fatalf("status=%d body=%s", w.Code, w.Body)
		}
	}
	if !s.meshFrozen.Load() {
		t.Fatal("response preceded application")
	}
	if len(s.telemetry.events) != 1 {
		t.Fatalf("repeated command emitted %d events", len(s.telemetry.events))
	}
	event := <-s.telemetry.events
	if event.Type != "mesh_freeze" || event.Fields["frozen"] != true || event.Timestamp.IsZero() {
		t.Fatalf("missing freeze observation: %+v", event)
	}
	original := map[string]string{"meshFrozen": "false", "keep": "value"}
	node := model.Node{Metadata: original}
	s.addMeshFreezeStatus(&node)
	if original["meshFrozen"] != "false" || node.Metadata["meshFrozen"] != "true" || node.Metadata["keep"] != "value" {
		t.Fatalf("status mutated shared metadata: %v / %v", original, node.Metadata)
	}
}

func TestMeshFreezeCanceledRequestDoesNotApply(t *testing.T) {
	s := newMeshFreezeTestServer(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	cancel()
	r := httptest.NewRequest(http.MethodPost, "/mesh-freeze", strings.NewReader(`{"runId":"run","generation":2}`)).WithContext(ctx)
	r.Header.Set("X-KPL-Node-ID", "node")
	w := httptest.NewRecorder()
	s.handleMeshFreeze(w, r)
	if w.Code < 400 || s.meshFrozen.Load() {
		t.Fatalf("canceled freeze applied: status=%d", w.Code)
	}
}

func TestMeshFreezeStatusRecoversAppliedFreezeAndReportsUsableEdges(t *testing.T) {
	s := newMeshFreezeTestServer(t, true)
	// Model a command whose acknowledgement was lost. Its later status must
	// discover the router state, not rely on a successful HTTP response.
	if err := s.pubsub.FreezeMesh(t.Context()); err != nil {
		t.Fatal(err)
	}
	node := model.Node{MeshPeers: map[string][]string{"old-topic": {"disconnected"}}}
	if err := s.observeMeshFreezeStatus(t.Context(), &node); err != nil {
		t.Fatal(err)
	}
	if node.Metadata["meshFrozen"] != "true" || len(node.MeshPeers) != 0 || node.OverlayObservedAt.IsZero() {
		t.Fatalf("status retained stale live edges or missed freeze: %+v", node)
	}
	if len(s.telemetry.events) != 1 {
		t.Fatal("recovered freeze observation was not recorded")
	}
}
