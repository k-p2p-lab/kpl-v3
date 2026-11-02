package controller

import (
	"context"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
	"github.com/k-p2p-lab/kpl-v3/internal/scenario"
)

func TestForwardingScheduledStateReachesOnlyMatchingFutureJoins(t *testing.T) {
	enabled, probability := true, 0.5
	profiles := newRunProfileState()
	profiles.revision = 3
	profiles.overrides = []profileOverride{{phase: scenario.Phase{Group: "attackers"}, set: model.RuntimeProfilePatch{Forwarding: &model.ProbabilityAttack{Enabled: &enabled, Probability: &probability}}}}
	ctx := context.WithValue(t.Context(), profileRuntimeKey{}, profiles)
	base := model.NodeConfig{Forwarding: &model.ForwardingConfig{Topics: []string{"blocks"}, Targets: &model.AttackSelector{Groups: []string{"honest"}}}, GossipSub: model.GossipSubConfig{Topics: []string{"blocks"}}}.WithDefaults()
	for _, group := range []string{"attackers", "honest"} {
		request := model.CreateNodeRequest{ID: "future", Group: group, Role: "worker", Config: base}
		updated, release, err := profileCreateRequest(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if updated.Config.Forwarding.Active() != (group == "attackers") || updated.ProfileRevision != 3 {
			t.Fatal("future join inherited wrong policy/revision")
		}
		if updated.Config.Forwarding.Topics[0] != "blocks" || updated.Config.Forwarding.Targets.Groups[0] != "honest" {
			t.Fatal("future join lost construction filters")
		}
	}
	if base.Forwarding.Active() {
		t.Fatal("schedule mutated shared profile template")
	}
}
