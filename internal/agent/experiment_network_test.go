package agent

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestPreparedPeerUsesImmutableNetworkAndControlGateway(t *testing.T) {
	s, logPath := newContainerTestServer(t, map[string]string{"HANG": "wait", "ATTACHED_NETWORK_ID": "network-id"})
	s.config.AutoResetNetwork = true
	s.experimentNetwork = &model.ExperimentNetwork{RunID: "run", NetworkID: "network-id", GatewayURL: "http://10.90.0.2:18081", PeerGatewayURL: "http://10.11.0.2:18081"}
	node, err := s.createNode(t.Context(), model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "group", PeerNetworkID: "network-id"})
	if err != nil {
		t.Fatal(err)
	}
	waitDockerCall(t, logPath, "wait")
	s.mu.RLock()
	proc := s.processes[node.ID]
	path, endpoint := proc.configPath, proc.apiURL
	s.mu.RUnlock()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config model.PeerProcessConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.ControllerURL != "http://10.11.0.2:18081/controller" || config.AgentURL != "http://10.11.0.2:18081/agents/agent" || !strings.HasPrefix(endpoint, "http://10.90.0.2:18081/peers/") {
		t.Fatalf("gateway routes controller=%s agent=%s api=%s", config.ControllerURL, config.AgentURL, endpoint)
	}
	found := false
	for _, call := range dockerCalls(t, logPath) {
		if len(call.Args) > 0 && call.Args[0] == "create" {
			for i, arg := range call.Args {
				if arg == "--network" {
					found = true
					if call.Args[i+1] != "network-id" {
						t.Fatal("Docker used mutable network name")
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("Peer network attachment missing")
	}
}

type networkTestTransport func(*http.Request) (*http.Response, error)

func (f networkTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNetworkFenceRequiresRemovedPeersAndBlocksLateAdmissions(t *testing.T) {
	s, _ := newContainerTestServer(t, nil)
	s.config.AutoResetNetwork = true
	s.startupReconciled = true
	request := model.ExperimentNetworkRequest{RunID: "run", Epoch: "epoch", RequestedAt: time.Now().UTC()}
	s.processes["old"] = &process{node: model.Node{ID: "old"}, exited: true, cleanupErr: errors.New("removal unconfirmed")}
	if err := s.fenceExperimentNetwork(t.Context(), request); err == nil {
		t.Fatal("unconfirmed removal accepted")
	}
	s.processes = map[string]*process{}
	if err := s.fenceExperimentNetwork(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	create := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "group", PeerNetworkID: "old-network"}
	if _, err := s.createNode(t.Context(), create); err == nil || !strings.Contains(err.Error(), "not been prepared") {
		t.Fatalf("late admission: %v", err)
	}
	stale := request
	stale.Epoch = "previous"
	stale.RequestedAt = stale.RequestedAt.Add(-time.Second)
	if err := s.fenceExperimentNetwork(t.Context(), stale); err == nil {
		t.Fatal("old preparation replaced newer fence")
	}
}

func TestNetworkActivationRequiresMatchingEpochBeforeWorkerAttachment(t *testing.T) {
	s, _ := newContainerTestServer(t, map[string]string{"FAIL": "network"})
	s.config.AutoResetNetwork = true
	s.startupReconciled = true
	s.networkRequest = model.ExperimentNetworkRequest{RunID: "run", Epoch: "epoch", RequestedAt: time.Now().UTC()}
	s.client = &http.Client{Transport: networkTestTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	network := model.ExperimentNetwork{RunID: "run", Epoch: "old-epoch", NetworkID: "new-network", NetworkName: "kpl-v3-peers", GatewayURL: "http://10.90.0.2:18081", PeerGatewayURL: "http://10.11.0.2:18081"}
	if err := s.activateExperimentNetwork(t.Context(), network); err == nil {
		t.Fatal("wrong network epoch accepted")
	}
	network.Epoch = "epoch"
	if err := s.activateExperimentNetwork(t.Context(), network); err != nil {
		t.Fatal(err)
	}
	if s.experimentNetwork == nil || s.experimentNetwork.NetworkID != "new-network" {
		t.Fatal("network not activated")
	}
	request := model.CreateNodeRequest{ID: "peer", RunID: "run", Group: "group", PeerNetworkID: "old-network"}
	if err := s.lockAdmission(t.Context(), request); err == nil {
		s.mu.Unlock()
		t.Fatal("wrong network admitted")
	}
	request.PeerNetworkID = "new-network"
	if err := s.lockAdmission(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	s.mu.Unlock()
	request.RunID = "old-run"
	if err := s.lockAdmission(t.Context(), request); err == nil {
		s.mu.Unlock()
		t.Fatal("wrong run admitted")
	}
}

func TestNetworkFenceChecksDaemonForUntrackedPeers(t *testing.T) {
	s, _ := newContainerTestServer(t, map[string]string{"PS": fakeContainerID})
	s.config.AutoResetNetwork = true
	s.startupReconciled = true
	request := model.ExperimentNetworkRequest{RunID: "run", Epoch: "epoch", RequestedAt: time.Now().UTC()}
	if err := s.fenceExperimentNetwork(t.Context(), request); err == nil || !strings.Contains(err.Error(), "still has Peer containers") {
		t.Fatalf("orphan inventory: %v", err)
	}
}

func TestDockerRejectsUnexpectedExperimentNetworkAttachment(t *testing.T) {
	d, logPath := fakeDocker(t, map[string]string{"ATTACHED_NETWORK_ID": "wrong-network"})
	node := model.Node{ID: "peer", RunID: "run", AgentID: "agent", Metadata: map[string]string{"peerNetworkId": "expected-network"}}
	id, _, err := d.create(t.Context(), node, []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "prepared network ID") || id != "" {
		t.Fatalf("mismatched attachment accepted: id=%s err=%v", id, err)
	}
	calls := dockerCalls(t, logPath)
	if calls[len(calls)-1].Args[0] != "rm" {
		t.Fatal("mismatched Peer was not cleaned up")
	}
}

func TestNetworkFenceRejectsMalformedBodyBeforeClosingAdmission(t *testing.T) {
	for _, suffix := range []string{"{}", "!", strings.Repeat(" ", 64<<10)} {
		s, _ := newContainerTestServer(t, nil)
		s.config.AutoResetNetwork = true
		s.startupReconciled = true
		active := &model.ExperimentNetwork{RunID: "old-run", Epoch: "old-epoch"}
		s.experimentNetwork = active
		body, err := json.Marshal(model.ExperimentNetworkRequest{RunID: "new-run", Epoch: "new-epoch", RequestedAt: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/network/fence", strings.NewReader(string(body)+suffix))
		result := httptest.NewRecorder()
		s.handleExperimentNetwork(result, req)
		if result.Code != http.StatusBadRequest || s.experimentNetwork != active {
			t.Fatalf("status=%d active=%+v", result.Code, s.experimentNetwork)
		}
	}
}
