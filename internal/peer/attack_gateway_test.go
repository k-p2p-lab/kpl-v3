package peer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/experimentnet"
	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackRegistryFetchThroughExperimentGateway(t *testing.T) {
	const token = "test-attack-gateway-key"
	var calls atomic.Int32
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/attack-targets" || r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("registry route lost its method, path or credentials: %s %s", r.Method, r.URL.Path)
			http.Error(w, "wrong registry request", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("runId") != "run" || r.URL.Query().Get("requesterNodeId") != "attacker" || r.URL.Query().Get("generation") != "7" {
			t.Errorf("registry route lost the caller scope: %s", r.URL.RawQuery)
			http.Error(w, "wrong caller scope", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode([]model.AttackTarget{{NodeID: "victim", Group: "victims", PeerID: "victim-transport", Ready: true}})
	}))
	t.Cleanup(controller.Close)
	handler, err := experimentnet.GatewayHandler(experimentnet.GatewayConfig{Token: token, ControllerURL: controller.URL, Subnet: "10.11.0.0/16"}, controller.Client().Transport)
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	t.Cleanup(gateway.Close)

	server := &Server{config: model.PeerProcessConfig{
		Node: model.Node{ID: "attacker", RunID: "run", Generation: 7}, ControllerURL: gateway.URL + "/controller", Token: token,
		NodeConfig: model.NodeConfig{ConnectionAttack: &model.ConnectionAttackConfig{Enabled: attackBool(true), Targets: &model.AttackSelector{Groups: []string{"victims"}}}},
	}}
	runtime := newAttackRuntime(server)
	if !runtime.needsRegistry() {
		t.Fatal("group-selected connection attack did not require the registry")
	}
	if err := runtime.fetchTargets(t.Context()); err != nil {
		t.Fatalf("Peer cannot obtain its attack registry through the experiment gateway: %v", err)
	}
	if selected, known := runtime.matchTransport(&model.AttackSelector{Groups: []string{"victims"}}, "victim-transport"); !selected || !known {
		t.Fatal("forwarded registry did not preserve the group-selected transport")
	}
	if calls.Load() != 1 {
		t.Fatalf("registry request reached Controller %d times, want once", calls.Load())
	}

	// A failed authorization must neither reach the Controller nor replace the
	// last usable registry, even though the Peer knows the gateway address.
	previous := runtime.registry.Load()
	for _, badToken := range []string{"", "wrong-key"} {
		server.config.Token = badToken
		if err := runtime.fetchTargets(t.Context()); err == nil || !strings.Contains(err.Error(), "401") {
			t.Fatalf("unauthenticated registry fetch was accepted: %v", err)
		}
		if runtime.registry.Load() != previous || calls.Load() != 1 {
			t.Fatal("unauthenticated registry request reached upstream or replaced its prior evidence")
		}
	}
}
