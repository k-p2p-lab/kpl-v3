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

func agentProfileRequest(t *testing.T) model.ProfileUpdateRequest {
	t.Helper()
	var patch model.RuntimeProfilePatch
	if err := json.Unmarshal([]byte(`{"gossipsub":{"params":{"d":8}},"network":{"delay":"25ms"}}`), &patch); err != nil {
		t.Fatal(err)
	}
	return model.ProfileUpdateRequest{RunID: "run", Generation: 2, Revision: 1, Stage: "apply", Set: patch}
}

func agentProfileAck(t *testing.T, request model.ProfileUpdateRequest) model.ProfileUpdateResponse {
	t.Helper()
	base, _ := model.BuiltInNodeConfig("full")
	updated, err := request.Set.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	return model.ProfileUpdateResponse{NodeID: "node", PeerID: "peer", Revision: request.Revision, Stage: request.Stage, Effective: request.Set.Snapshot(updated)}
}

func TestProfileProxyForwardsFencesAndRetainsLatestAppliedStatus(t *testing.T) {
	request := agentProfileRequest(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/profile" || r.Header.Get("X-KPL-Node-ID") != "node" || r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing scoped control headers")
		}
		var got model.ProfileUpdateRequest
		_ = json.NewDecoder(r.Body).Decode(&got)
		if got.RunID != request.RunID || got.Generation != request.Generation {
			t.Error("scope changed")
		}
		_ = json.NewEncoder(w).Encode(agentProfileAck(t, got))
	}))
	defer api.Close()
	proc := meshFreezeTestProcess(api.URL)
	proc.node.Metadata["networkMutable"] = "true"
	s := &Server{client: api.Client(), processes: map[string]*process{"node": proc}}
	s.config.Token = "test-token"
	request.Stage = "prepare"
	if _, err := s.proxyProfileUpdate(t.Context(), "node", request); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["runtimeProfile"] != "" {
		t.Fatal("prepare was reported as applied")
	}
	request.Stage = "apply"
	if _, err := s.proxyProfileUpdate(t.Context(), "node", request); err != nil {
		t.Fatal(err)
	}
	if proc.node.Metadata["profileRevision"] != "1" || !strings.Contains(proc.node.Metadata["network"], "25ms") {
		t.Fatalf("missing applied profile: %v", proc.node.Metadata)
	}
	older := map[string]string{"profileRevision": "0", "networkRevision": "0", "network": "{}", "networkPending": "false", "networkAppliedAt": time.Now().Format(time.RFC3339Nano)}
	updateProcessNetwork(proc, older)
	updateProcessProfile(proc, older)
	if !strings.Contains(proc.node.Metadata["network"], "25ms") {
		t.Fatal("old Peer status reverted acknowledged config")
	}
	request.Revision = 2
	ack := agentProfileAck(t, request)
	raw, _ := json.Marshal(ack.Effective)
	updateProcessProfile(proc, map[string]string{"profileRevision": "2", "runtimeProfile": string(raw), "profileAppliedAt": time.Now().Format(time.RFC3339Nano)})
	if proc.node.Metadata["profileRevision"] != "2" {
		t.Fatal("lost acknowledgement not recovered from Peer status")
	}
}

func TestProfileProxyRejectsInvalidAcknowledgementAndStaleGeneration(t *testing.T) {
	for _, kind := range []string{"generation", "fenced", "capability", "identity", "revision", "effective", "oversize", "trailing"} {
		t.Run(kind, func(t *testing.T) {
			request := agentProfileRequest(t)
			response := agentProfileAck(t, request)
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch kind {
				case "identity":
					response.PeerID = "other"
				case "revision":
					response.Revision++
				case "effective":
					wrong := 9
					response.Effective.GossipSub.Params.D = &wrong
				}
				data, _ := json.Marshal(response)
				_, _ = w.Write(data)
				if kind == "oversize" {
					_, _ = w.Write([]byte(strings.Repeat(" ", model.MaxProfileUpdateBytes)))
				}
				if kind == "trailing" {
					_, _ = w.Write([]byte("{}"))
				}
			}))
			defer api.Close()
			proc := meshFreezeTestProcess(api.URL)
			proc.node.Metadata["networkMutable"] = "true"
			s := &Server{client: api.Client(), processes: map[string]*process{"node": proc}}
			switch kind {
			case "generation":
				request.Generation--
			case "fenced":
				s.runFences = map[string]uint64{"run": 2}
			case "capability":
				delete(proc.node.Metadata, "networkMutable")
			}
			if _, err := s.proxyProfileUpdate(t.Context(), "node", request); err == nil {
				t.Fatal("invalid update accepted")
			}
			if proc.node.Metadata["runtimeProfile"] != "" {
				t.Fatal("failed request changed applied metadata")
			}
			if (kind == "generation" || kind == "fenced" || kind == "capability") && calls.Load() != 0 {
				t.Fatal("stale request reached Peer")
			}
		})
	}
}

func TestProfileProxyDoesNotHoldStateLockAcrossNetworkOrRevivePeer(t *testing.T) {
	request := agentProfileRequest(t)
	started, release := make(chan struct{}), make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_ = json.NewEncoder(w).Encode(agentProfileAck(t, request))
	}))
	defer api.Close()
	proc := meshFreezeTestProcess(api.URL)
	proc.node.Metadata["networkMutable"] = "true"
	s := &Server{client: api.Client(), processes: map[string]*process{"node": proc}}
	done := make(chan error, 1)
	go func() { _, err := s.proxyProfileUpdate(t.Context(), "node", request); done <- err }()
	<-started
	s.mu.Lock()
	proc.node.State = model.NodeStopping
	s.mu.Unlock()
	close(release)
	if err := <-done; err == nil || proc.node.Metadata["runtimeProfile"] != "" {
		t.Fatal("late response revived a stopped process")
	}
}

func TestDockerPreparesNetworkCapabilityBeforeFirstImpairment(t *testing.T) {
	d, log := fakeDocker(t, nil)
	node, data := dockerTestConfig(t, false)
	var cfg model.PeerProcessConfig
	_ = json.Unmarshal(data, &cfg)
	cfg.NetworkMutable = true
	data, _ = json.Marshal(cfg)
	if _, _, err := d.create(context.Background(), node, data); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(dockerCalls(t, log)[0].Args, " "), "--cap-add NET_ADMIN") {
		t.Fatal("future shaping lacks namespace capability")
	}
}
