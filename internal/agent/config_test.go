package agent

import (
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestResolveCreateNodeConfigAppliesBuiltInPreset(t *testing.T) {
	config, nodeType := resolveCreateNodeConfig(model.CreateNodeRequest{Type: "full", Role: "worker"})
	if nodeType != "full" {
		t.Fatalf("node type = %q, want full", nodeType)
	}
	if config.Libp2p.ConnectionLimit == nil || *config.Libp2p.ConnectionLimit != 55 {
		t.Fatalf("direct REST full node connection limit = %#v, want 55", config.Libp2p.ConnectionLimit)
	}
	if *config.GossipSub.Params.DScore != 3 || *config.GossipSub.Params.MaxIHaveLength != 5500 || config.GossipSub.Params.HeartbeatInitialDelay != "1s" {
		t.Fatalf("direct REST full node missed v2 GossipSub defaults: %+v", config.GossipSub.Params)
	}
}

func TestResolveCreateNodeConfigUsesRoleFallbackAndOverrides(t *testing.T) {
	boot, nodeType := resolveCreateNodeConfig(model.CreateNodeRequest{Role: "boot"})
	if nodeType != "boot" || boot.GossipSub.Enabled == nil || *boot.GossipSub.Enabled {
		t.Fatalf("role=boot fallback did not resolve DHT-only boot preset: type=%q config=%+v", nodeType, boot)
	}

	zero := 0
	full, _ := resolveCreateNodeConfig(model.CreateNodeRequest{
		Type: "full",
		Config: model.NodeConfig{GossipSub: model.GossipSubConfig{
			Params: model.GossipSubParamsConfig{HistoryGossip: &zero},
		}},
	})
	if *full.GossipSub.Params.HistoryGossip != 0 {
		t.Fatalf("inline explicit zero was lost: %+v", full.GossipSub.Params)
	}
}

func TestResolveCreateNodeConfigCanonicalizesWorkerAlias(t *testing.T) {
	requests := []model.CreateNodeRequest{
		{Type: "worker"},
		{Config: model.NodeConfig{Type: "worker"}},
	}
	for _, request := range requests {
		config, nodeType := resolveCreateNodeConfig(request)
		if nodeType != "full" || config.Type != "full" {
			t.Fatalf("worker alias resolved to nodeType=%q config.type=%q, want full", nodeType, config.Type)
		}
	}
}

func TestResolveCreateNodeConfigCustomTypeInheritsWorkerDefaults(t *testing.T) {
	zero := 0
	config, nodeType := resolveCreateNodeConfig(model.CreateNodeRequest{
		Type: "custom-router",
		Config: model.NodeConfig{GossipSub: model.GossipSubConfig{
			Params: model.GossipSubParamsConfig{HistoryGossip: &zero},
		}},
	})
	if nodeType != "custom-router" || config.Type != "custom-router" {
		t.Fatalf("custom type was not preserved: nodeType=%q config.type=%q", nodeType, config.Type)
	}
	if config.Libp2p.ConnectionLimit == nil || *config.Libp2p.ConnectionLimit != 55 {
		t.Fatalf("custom type did not inherit full connection limit: %+v", config.Libp2p.ConnectionLimit)
	}
	if config.GossipSub.Params.DScore == nil || *config.GossipSub.Params.DScore != 3 {
		t.Fatalf("custom type did not inherit v2 worker GossipSub defaults: %+v", config.GossipSub.Params)
	}
	if config.GossipSub.Params.HistoryGossip == nil || *config.GossipSub.Params.HistoryGossip != 0 {
		t.Fatalf("custom type override was not retained: %+v", config.GossipSub.Params)
	}
}

func TestPeerScoreStatusIsRetainedForHeartbeat(t *testing.T) {
	server := &Server{processes: map[string]*process{
		"node": {node: model.Node{ID: "node", State: model.NodeStarting}},
	}}
	if err := server.updateNode(model.Node{
		ID:         "node",
		State:      model.NodeReady,
		PeerScores: map[string]float64{"peer-a": 1.25},
	}); err != nil {
		t.Fatal(err)
	}
	nodes := server.nodes()
	if len(nodes) != 1 || nodes[0].PeerScores["peer-a"] != 1.25 {
		t.Fatalf("peer score was not retained in agent status: %+v", nodes)
	}
}

func TestCreateNodeRejectsInvalidLifetimeBeforeStartingProcess(t *testing.T) {
	server := &Server{}
	_, err := server.createNode(t.Context(), model.CreateNodeRequest{
		ID: "node", RunID: "run", Group: "group", Lifetime: "-1s",
	})
	if err == nil {
		t.Fatal("negative direct-API lifetime was accepted")
	}
}
