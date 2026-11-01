package model

import (
	"math"
	"testing"
)

func TestAttackDefaultsAndFieldwiseOverrides(t *testing.T) {
	probability := 0.25
	zero := 0.0
	base := NodeConfig{Sink: &ProbabilityAttack{Enabled: boolPointer(true), Probability: &probability}, Censorship: &CensorshipConfig{ProbabilityAttack: ProbabilityAttack{Enabled: boolPointer(true), Probability: &probability}, Topics: []string{"blocks"}}}.Merge(NodeConfig{GossipSub: GossipSubConfig{Topics: []string{"blocks"}}}).WithDefaults()
	changed := base.Merge(NodeConfig{Sink: &ProbabilityAttack{Enabled: boolPointer(false)}, Censorship: &CensorshipConfig{ProbabilityAttack: ProbabilityAttack{Probability: &zero}}}).WithDefaults()
	if changed.Sink.Active() || *changed.Sink.Probability != 0.25 || *changed.Censorship.Probability != 0 || !changed.Censorship.Active() {
		t.Fatalf("lost explicit false/zero or inherited fields: %+v %+v", changed.Sink, changed.Censorship)
	}
	if !base.Sink.Active() || *base.Censorship.Probability != 0.25 {
		t.Fatal("overlay mutated the original profile")
	}
	defaults := NodeConfig{Sink: &ProbabilityAttack{}}.WithDefaults()
	if defaults.Sink.Active() || *defaults.Sink.Probability != 1 {
		t.Fatal("sink must default to disabled with probability one")
	}
	if (NodeConfig{}).WithDefaults().Sink != nil {
		t.Fatal("omitted options should stay omitted")
	}
}
func TestAttackInvalidProbabilityAndSelectors(t *testing.T) {
	for _, p := range []float64{-0.01, 1.01, math.NaN(), math.Inf(1)} {
		if err := (NodeConfig{Sink: &ProbabilityAttack{Probability: &p}}).Validate(); err == nil {
			t.Fatalf("accepted probability %v", p)
		}
	}
	for _, p := range []float64{0, 0.5, 1} {
		if err := (NodeConfig{Sink: &ProbabilityAttack{Enabled: boolPointer(true), Probability: &p}}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (NodeConfig{Censorship: &CensorshipConfig{ProbabilityAttack: ProbabilityAttack{Enabled: boolPointer(true)}}}).Validate(); err == nil {
		t.Fatal("unscoped censorship accepted")
	}
	if err := (&AttackSelector{Groups: []string{"group", "group"}}).Validate(); err == nil {
		t.Fatal("duplicate selector accepted")
	}
	if err := (&AttackSelector{PeerIDs: []string{"invalid"}}).Validate(); err == nil {
		t.Fatal("invalid Peer ID accepted")
	}
	s := &AttackSelector{Groups: []string{"victim"}, NodeIDs: []string{"one", "two"}}
	if !s.Matches("victim", "one", "") || s.Matches("other", "one", "") || s.Matches("victim", "three", "") {
		t.Fatal("selector union/intersection contract failed")
	}
}
func TestAttackRuntimeAcknowledgementAndConstructionSettings(t *testing.T) {
	p := 0.3
	base := NodeConfig{Sink: &ProbabilityAttack{Probability: &p}, Censorship: &CensorshipConfig{Topics: []string{"blocks"}}, GossipSub: GossipSubConfig{Topics: []string{"blocks"}}}.WithDefaults()
	patch := RuntimeProfilePatch{Sink: &ProbabilityAttack{Enabled: boolPointer(true)}, Censorship: &ProbabilityAttack{Enabled: boolPointer(true)}}
	actual, err := patch.Apply(base)
	if err != nil {
		t.Fatal(err)
	}
	if !actual.Sink.Active() || *actual.Sink.Probability != 0.3 || len(actual.Censorship.Topics) != 1 {
		t.Fatal("runtime patch replaced construction settings")
	}
	request := ProfileUpdateRequest{RunID: "run", Revision: 1, Stage: "prepare", Set: patch}
	response := ProfileUpdateResponse{Effective: patch.Snapshot(actual)}
	if err := ValidateProfileAcknowledgement(request, response); err != nil {
		t.Fatal(err)
	}
	bad := response
	value := 0.4
	attack := *response.Effective.Sink
	attack.Probability = &value
	bad.Effective.Sink = &attack
	if ValidateProfileAcknowledgement(request, bad) != nil {
		t.Fatal("unrequested inherited probability may legitimately be reflected")
	}
	request.Set.Sink.Probability = &p
	if ValidateProfileAcknowledgement(request, bad) == nil {
		t.Fatal("wrong requested probability accepted")
	}
	bad = response
	bad.Effective.MessageDelay = &ProbabilityAttack{Enabled: boolPointer(false), Probability: &p}
	if ValidateProfileAcknowledgement(request, bad) == nil {
		t.Fatal("unexpected ACK field accepted")
	}
	if err := (RuntimeProfilePatch{Sink: &ProbabilityAttack{}}).Validate(); err == nil {
		t.Fatal("empty attack patch accepted")
	}
}
func TestAttacksRequireWorkerAndEnabledGossipSub(t *testing.T) {
	c := NodeConfig{Sink: &ProbabilityAttack{Enabled: boolPointer(true)}}.WithDefaults()
	if c.ValidateAttackRole("boot") == nil || c.ValidateAttackRole("worker") != nil {
		t.Fatal("wrong worker role validation")
	}
	c.GossipSub.Enabled = boolPointer(false)
	if c.Validate() == nil {
		t.Fatal("attack without PubSub accepted")
	}
	c.GossipSub.Enabled = boolPointer(true)
	c.GossipSub.Router = "floodsub"
	if c.Validate() == nil {
		t.Fatal("GossipSub attack on FloodSub accepted")
	}
}

func TestAttackRuntimeRequiresGossipEvenForDisabledPatch(t *testing.T) {
	patch := RuntimeProfilePatch{Sink: &ProbabilityAttack{Enabled: boolPointer(false)}}
	for _, base := range []NodeConfig{{GossipSub: GossipSubConfig{Enabled: boolPointer(false)}}, {GossipSub: GossipSubConfig{Router: "floodsub"}}} {
		if _, err := patch.Apply(base); err == nil {
			t.Fatal("unsupported dormant runtime patch passed preflight")
		}
		if base.Sink != nil {
			t.Fatal("failed preflight mutated the input")
		}
	}
}
