package model

import "testing"

func TestMeshFreezeConfigurationRequiresGossipSubAndPreservesFalseOverride(t *testing.T) {
	on, off := true, false
	base := NodeConfig{GossipSub: GossipSubConfig{MeshFreeze: &on}}.WithDefaults()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := base.Merge(NodeConfig{GossipSub: GossipSubConfig{MeshFreeze: &off}}).WithDefaults(); got.GossipSub.MeshFreeze == nil || *got.GossipSub.MeshFreeze {
		t.Fatal("explicit false did not override inherited meshFreeze")
	}
	if got := (NodeConfig{}).WithDefaults(); got.GossipSub.MeshFreeze != nil && *got.GossipSub.MeshFreeze {
		t.Fatal("meshFreeze enabled without opt-in")
	}
	for _, router := range []string{"floodsub", "randomsub"} {
		config := base
		config.GossipSub.Router = router
		if err := config.Validate(); err == nil {
			t.Fatalf("meshFreeze accepted by %s", router)
		}
	}
	disabled := base
	disabled.GossipSub.Enabled = &off
	if err := disabled.Validate(); err == nil {
		t.Fatal("meshFreeze accepted with disabled PubSub")
	}
}
