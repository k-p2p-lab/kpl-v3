package model

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRuntimeProfilePatchPreservesZerosAndUntouchedFields(t *testing.T) {
	base, _ := BuiltInNodeConfig("full")
	base.Network = NetworkConfig{Delay: "30ms", LossPercent: float64Ptr(2)}
	var patch RuntimeProfilePatch
	if err := json.Unmarshal([]byte(`{"gossipsub":{"params":{"d":8,"dOut":0,"gossipFactor":0}},"network":{"delay":"0s","lossPercent":0}}`), &patch); err != nil {
		t.Fatal(err)
	}
	updated, err := patch.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	if *updated.GossipSub.Params.D != 8 || *updated.GossipSub.Params.DOut != 0 || *updated.GossipSub.Params.GossipFactor != 0 || updated.Network.Delay != "0s" || *updated.Network.LossPercent != 0 {
		t.Fatalf("patch lost explicit zeros: %+v", updated)
	}
	if *base.GossipSub.Params.D != 6 || base.Network.Delay != "30ms" || !reflect.DeepEqual(base.Kademlia, updated.Kademlia) || !reflect.DeepEqual(base.Libp2p, updated.Libp2p) {
		t.Fatal("patch mutated the source or an unrelated setting")
	}
	request := ProfileUpdateRequest{Set: patch}
	response := ProfileUpdateResponse{Effective: patch.Snapshot(updated)}
	if err := ValidateProfileAcknowledgement(request, response); err != nil {
		t.Fatal(err)
	}
	wrong := 9
	response.Effective.GossipSub.Params.D = &wrong
	if err := ValidateProfileAcknowledgement(request, response); err == nil {
		t.Fatal("wrong effective D was acknowledged")
	}
}

func TestRuntimeProfilePatchRejectsWrongRouterAndInvalidMergedState(t *testing.T) {
	d := 3
	patch := RuntimeProfilePatch{GossipSub: &RuntimeGossipSubConfig{Params: RuntimeGossipSubParams{D: &d}}}
	for _, kind := range []string{"full", "flood", "dht-only"} {
		base, _ := BuiltInNodeConfig(kind)
		if _, err := patch.Apply(base); err == nil {
			t.Fatalf("invalid change accepted for %s", kind)
		}
	}
	base, _ := BuiltInNodeConfig("full")
	base.Network = NetworkConfig{Delay: "10ms", Jitter: "2ms"}
	if _, err := (RuntimeProfilePatch{Network: &NetworkConfig{Delay: "0s"}}).Apply(base); err == nil {
		t.Fatal("zero delay retained incompatible jitter")
	}
}
