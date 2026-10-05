package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func meshFreezeTestProcess(apiURL string) *process {
	return &process{apiURL: apiURL, node: model.Node{
		ID: "node", PeerID: "peer", RunID: "run", Generation: 2, State: model.NodeReady,
		Metadata: map[string]string{"pubsubRouter": "gossipsub", "pubsubEnabled": "true", "meshFreezeEnabled": "true"},
	}}
}

func TestMeshFreezeAdmissionRecordsCapabilityWithoutFreezing(t *testing.T) {
	s, _ := newContainerTestServer(t, map[string]string{"HANG": "wait"})
	on := true
	node, err := s.createNode(t.Context(), model.CreateNodeRequest{
		ID: "peer", RunID: "run", Group: "workers", Type: "full",
		Config: model.NodeConfig{GossipSub: model.GossipSubConfig{MeshFreeze: &on}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if node.Metadata["meshFreezeEnabled"] != "true" || node.Metadata["meshFrozen"] == "true" {
		t.Fatalf("incorrect startup freeze state: %v", node.Metadata)
	}
}

func TestMeshFreezeProxyForwardsIdentityAndRecordsAcknowledgement(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mesh-freeze" || r.Header.Get("X-KPL-Node-ID") != "node" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing command route or identity/authentication guard")
		}
		var request model.MeshFreezeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.RunID != "run" || request.Generation != 2 {
			t.Errorf("scope: %+v / %v", request, err)
		}
		_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: "node", PeerID: "peer", Frozen: true})
	}))
	defer peer.Close()
	s := &Server{client: peer.Client(), processes: map[string]*process{"node": meshFreezeTestProcess(peer.URL)}}
	s.config.Token = "test-token"
	r := httptest.NewRequest(http.MethodPost, "/api/v1/nodes/node/mesh-freeze", strings.NewReader(`{"runId":"run","generation":2}`))
	r.Header.Set("Authorization", "Bearer test-token")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || s.processes["node"].node.Metadata["meshFrozen"] != "true" {
		t.Fatalf("freeze status=%d body=%s metadata=%v", w.Code, w.Body, s.processes["node"].node.Metadata)
	}
	// A crossed earlier status cannot thaw a one-way freeze.
	if err := s.updateNode(model.Node{ID: "node", PeerID: "peer", State: model.NodeReady, LastSeen: time.Now(), Metadata: map[string]string{"meshFrozen": "false"}}); err != nil {
		t.Fatal(err)
	}
	if s.processes["node"].node.Metadata["meshFrozen"] != "true" {
		t.Fatal("stale report cleared acknowledged freeze")
	}
}

func TestMeshFreezeProxyRejectsStaleTargetsBeforeNetworkIO(t *testing.T) {
	var calls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer peer.Close()
	for _, name := range []string{"unknown", "stopping", "exited", "wrong run", "wrong generation", "fenced", "global fence", "disabled", "wrong router", "pubsub disabled", "no endpoint"} {
		t.Run(name, func(t *testing.T) {
			proc := meshFreezeTestProcess(peer.URL)
			s := &Server{client: peer.Client(), processes: map[string]*process{"node": proc}}
			request := model.MeshFreezeRequest{RunID: "run", Generation: 2}
			switch name {
			case "unknown":
				delete(s.processes, "node")
			case "stopping":
				proc.node.State = model.NodeStopping
			case "exited":
				proc.exited = true
			case "wrong run":
				request.RunID = "old"
			case "wrong generation":
				request.Generation = 1
			case "fenced":
				s.runFences = map[string]uint64{"run": 2}
			case "global fence":
				s.fencingAll = true
			case "disabled":
				proc.node.Metadata["meshFreezeEnabled"] = "false"
			case "wrong router":
				proc.node.Metadata["pubsubRouter"] = "floodsub"
			case "pubsub disabled":
				proc.node.Metadata["pubsubEnabled"] = "false"
			case "no endpoint":
				proc.apiURL = ""
			}
			if _, err := s.proxyMeshFreeze(t.Context(), "node", request); err == nil {
				t.Fatal("invalid freeze target accepted")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("sent %d invalid commands", calls.Load())
	}
}

func TestMeshFreezeProxyRejectsInvalidAcknowledgements(t *testing.T) {
	for _, body := range []string{
		`{"nodeId":"other","peerId":"peer","frozen":true}`,
		`{"nodeId":"node","peerId":"other","frozen":true}`,
		`{"nodeId":"node","peerId":"peer","frozen":false}`,
		`{"nodeId":"node","peerId":"peer","frozen":true}{}`,
		`{`,
		`{"nodeId":"node","peerId":"peer","frozen":true}` + strings.Repeat(" ", 4096),
	} {
		t.Run(body, func(t *testing.T) {
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer peer.Close()
			s := &Server{client: peer.Client(), processes: map[string]*process{"node": meshFreezeTestProcess(peer.URL)}}
			if _, err := s.proxyMeshFreeze(t.Context(), "node", model.MeshFreezeRequest{RunID: "run", Generation: 2}); err == nil {
				t.Fatal("bad acknowledgement accepted")
			}
			if s.processes["node"].node.Metadata["meshFrozen"] == "true" {
				t.Fatal("bad acknowledgement changed state")
			}
		})
	}
}

func TestMeshFreezeProxyDoesNotHoldStateLockOrReviveStoppedPeer(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: "node", PeerID: "peer", Frozen: true})
	}))
	defer peer.Close()
	defer close(release)
	proc := meshFreezeTestProcess(peer.URL)
	s := &Server{client: peer.Client(), processes: map[string]*process{"node": proc}}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.proxyMeshFreeze(ctx, "node", model.MeshFreezeRequest{RunID: "run", Generation: 2})
		done <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("request not dispatched")
	}
	locked := make(chan struct{})
	go func() {
		s.mu.Lock()
		proc.exited = true
		proc.node.State = model.NodeStopped
		s.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-ctx.Done():
		t.Fatal("network call holds Agent state lock")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled command acknowledged")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled command did not return")
	}
	if proc.node.State != model.NodeStopped || proc.node.Metadata["meshFrozen"] == "true" {
		t.Fatal("late command revived stopped peer")
	}
}
