package model

import "testing"

func TestForwardingRuntimePreservesSelectorsAndRejectsWrongAcknowledgements(t *testing.T) {
	base := NodeConfig{Forwarding: &ForwardingConfig{Topics: []string{"blocks"}, Targets: &AttackSelector{Groups: []string{"victims"}}}, GossipSub: GossipSubConfig{Topics: []string{"blocks"}}}.WithDefaults()
	if base.Forwarding.Active() || *base.Forwarding.Probability != 1 {
		t.Fatal("forwarding should default inactive")
	}
	zero := 0.0
	patch := RuntimeProfilePatch{Forwarding: &ProbabilityAttack{Enabled: boolPointer(true), Probability: &zero}}
	updated, err := patch.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.Forwarding.Active() || *updated.Forwarding.Probability != 0 || updated.Forwarding.Targets.Groups[0] != "victims" {
		t.Fatal("runtime patch lost selector or explicit zero")
	}
	request := ProfileUpdateRequest{Set: patch}
	response := ProfileUpdateResponse{Effective: patch.Snapshot(updated)}
	if err := ValidateProfileAcknowledgement(request, response); err != nil {
		t.Fatal(err)
	}
	response.Effective.Forwarding = nil
	if ValidateProfileAcknowledgement(request, response) == nil {
		t.Fatal("missing forwarding ACK accepted")
	}
	response.Effective = patch.Snapshot(updated)
	wrong := *response.Effective.Forwarding
	wrong.Enabled = boolPointer(false)
	response.Effective.Forwarding = &wrong
	if ValidateProfileAcknowledgement(request, response) == nil {
		t.Fatal("wrong forwarding ACK accepted")
	}
	if base.Forwarding.Active() {
		t.Fatal("patch mutated baseline")
	}
}

func TestInvalidMessageValidationBoundsAndActivation(t *testing.T) {
	base := NodeConfig{GossipSub: GossipSubConfig{Topics: []string{"blocks"}}, InvalidMessage: &InvalidMessageConfig{ControlAttackConfig: ControlAttackConfig{Topics: []string{"blocks"}}}}.WithDefaults()
	if *base.InvalidMessage.Enabled || base.InvalidMessage.Mode != "invalid-signature" || *base.InvalidMessage.MessagesPerPeer != 1 || *base.InvalidMessage.PayloadSize != 256 {
		t.Fatal("wrong invalid-message defaults")
	}
	for _, mode := range []string{"invalid-signature", "malformed-protobuf"} {
		c := base.Merge(NodeConfig{InvalidMessage: &InvalidMessageConfig{ControlAttackConfig: ControlAttackConfig{Enabled: boolPointer(true)}, Mode: mode}}).WithDefaults()
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		if c.ValidateAttackRole("boot") == nil {
			t.Fatal("wire attack accepted on boot")
		}
	}
	for _, p := range []*InvalidMessageConfig{{Mode: "corrupt"}, {PayloadSize: intPointer(0)}, {PayloadSize: intPointer(1048577)}, {MessagesPerPeer: intPointer(0)}, {MessagesPerPeer: intPointer(1025)}, {ControlAttackConfig: ControlAttackConfig{Interval: "-1s"}}} {
		if (NodeConfig{InvalidMessage: p}).ValidateAttackSyntax() == nil {
			t.Fatalf("invalid declaration accepted: %+v", p)
		}
	}
	active := base.Merge(NodeConfig{InvalidMessage: &InvalidMessageConfig{ControlAttackConfig: ControlAttackConfig{Enabled: boolPointer(true)}}}).WithDefaults()
	active.GossipSub.Router = "floodsub"
	if active.Validate() == nil {
		t.Fatal("wire attack on unsupported router accepted")
	}
}
