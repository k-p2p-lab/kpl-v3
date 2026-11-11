package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestRunProcessControlKeepsDirectAndGatewayRoutesInSupervisor(t *testing.T) {
	for _, prefix := range []string{"", "/peers/10.90.0.2"} {
		name := "direct"
		if prefix != "" {
			name = "gateway"
		}
		t.Run(name, func(t *testing.T) {
			s, dockerLog := newContainerTestServer(t, map[string]string{"HANG": "wait"})
			s.config.RunWorkerFactory = agentRunProcessFactory()
			s.config.Token = "fake-parent-peer-control-key"
			s.config.HostRebootToken = "fake-parent-host-control-key"
			workerControllerFixture(t, s)
			wireID := topologyProxyTestID(t)
			called := make(chan string, 8)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+s.config.Token || r.Header.Get("X-KPL-Node-ID") != "peer" {
					t.Error("Peer control lost the supervisor-owned method, credentials or Node identity")
					w.WriteHeader(http.StatusForbidden)
					return
				}
				path := strings.TrimPrefix(r.URL.Path, prefix)
				if r.URL.Path != prefix+path {
					t.Error("Peer control bypassed its gateway route")
					w.WriteHeader(http.StatusNotFound)
					return
				}
				called <- path
				w.Header().Set("Content-Type", "application/json")
				switch path {
				case "/publish":
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusAccepted)
				case "/profile":
					var request model.ProfileUpdateRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					ack := agentProfileAck(t, request)
					ack.NodeID, ack.PeerID = "peer", wireID
					_ = json.NewEncoder(w).Encode(ack)
				case "/topology":
					var request model.TopologyRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					ack := topologyProxyAck(wireID, request.Stage)
					ack.NodeID = "peer"
					_ = json.NewEncoder(w).Encode(ack)
				case "/mesh-freeze":
					_ = json.NewEncoder(w).Encode(model.MeshFreezeResponse{NodeID: "peer", PeerID: wireID, Frozen: true})
				default:
					t.Errorf("unexpected Peer operation route %q", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer endpoint.Close()
			meshFreeze := true
			if _, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "control", Generation: 2, Group: "workers", Type: "full", Config: model.NodeConfig{GossipSub: model.GossipSubConfig{MeshFreeze: &meshFreeze}}}); err != nil {
				t.Fatal(err)
			}
			waitDockerCall(t, dockerLog, "wait")
			s.mu.Lock()
			proc := s.processes["peer"]
			proc.apiURL = endpoint.URL + prefix
			s.mu.Unlock()
			s.syncRunWorkerNode(proc)
			if err := s.updateNode(model.Node{ID: "peer", RunID: "control", State: model.NodeReady, PeerID: wireID, LastSeen: time.Now().UTC()}); err != nil {
				t.Fatal(err)
			}
			if err := s.proxyPublish(t.Context(), "peer", model.PublishRequest{RunID: "control", PayloadSize: 1}); err != nil {
				t.Fatal(err)
			}
			profile := agentProfileRequest(t)
			profile.RunID, profile.Set.Network = "control", nil
			if _, err := s.proxyProfileUpdate(t.Context(), "peer", profile); err != nil {
				t.Fatal(err)
			}
			topology := topologyProxyRequest("apply")
			topology.RunID = "control"
			if _, err := s.proxyTopology(t.Context(), "peer", topology); err != nil {
				t.Fatal(err)
			}
			if _, err := s.proxyMeshFreeze(t.Context(), "peer", model.MeshFreezeRequest{RunID: "control", Generation: 2}); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"/publish", "/profile", "/topology", "/mesh-freeze"} {
				select {
				case got := <-called:
					if got != want {
						t.Fatalf("Peer operation route %q, want %q", got, want)
					}
				default:
					t.Fatalf("Peer operation %q was not dispatched", want)
				}
			}
			worker, err := s.runWorker("control")
			if err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.runFences["control"] = 2
			s.mu.Unlock()
			if err := s.proxyPublish(t.Context(), "peer", model.PublishRequest{RunID: "control", PayloadSize: 1}); err == nil {
				t.Fatal("a stale child bypassed the supervisor Run generation fence")
			}
			s.mu.Lock()
			delete(s.runFences, "control")
			proc.node.State = model.NodeStopping
			s.mu.Unlock()
			body, _ := json.Marshal(model.PublishRequest{RunID: "control", PayloadSize: 1})
			var reply runWorkerReply
			if err := worker.call(t.Context(), "peer.control", runWorkerControl{NodeID: "peer", Action: "publish", Body: body}, &reply); err == nil && reply.Error == "" {
				t.Fatal("a stale child bypassed the supervisor's stopped Peer target")
			}
			if len(called) != 0 {
				t.Fatal("a fenced or stale Peer operation reached the network")
			}
			s.mu.Lock()
			proc.node.State = model.NodeReady
			s.mu.Unlock()
			var page runWorkerSnapshots
			if err := worker.process.Call(t.Context(), "node.snapshots", "", &page); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), s.config.Token) || strings.Contains(string(encoded), s.config.HostRebootToken) {
				t.Fatal("supervisor credentials entered child runtime state")
			}
			if response := drainAgentRunForTest(t, s, "control"); response.Code != http.StatusNoContent {
				t.Fatalf("drain: %d %s", response.Code, response.Body)
			}
		})
	}
}
