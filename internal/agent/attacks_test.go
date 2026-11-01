package agent

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAttackRuntimeMetadataSurvivesOtherProfileChanges(t *testing.T) {
	enabled := true
	probability := 0.3
	proc := &process{node: model.Node{Role: "worker", Metadata: map[string]string{"pubsubEnabled": "true", "pubsubRouter": "gossipsub"}}}
	now := time.Now().UTC()
	storeProfileAcknowledgement(proc, model.ProfileUpdateResponse{Revision: 1, Effective: model.RuntimeProfilePatch{Sink: &model.ProbabilityAttack{Enabled: &enabled, Probability: &probability}}}, now)
	storeProfileAcknowledgement(proc, model.ProfileUpdateResponse{Revision: 2, Effective: model.RuntimeProfilePatch{Network: &model.NetworkConfig{Delay: "1ms"}}}, now)
	var state model.RuntimeProfilePatch
	if err := json.Unmarshal([]byte(proc.node.Metadata["runtimeProfile"]), &state); err != nil {
		t.Fatal(err)
	}
	if state.Sink == nil || !state.Sink.Active() || *state.Sink.Probability != 0.3 || state.Network == nil {
		t.Fatal("network ACK erased attack state")
	}
}
func TestTerminalHeartbeatRetainsAttackAndAuthorEvidence(t *testing.T) {
	proc := &process{node: model.Node{Metadata: map[string]string{"cleanupComplete": "true", "attackOptions": "options", "attackStats": "counters", "attackRevision": "3", "pubsubAuthorId": "virtual-author", "pubsubAuthorMode": "explicit"}}}
	result := heartbeatNodeStatus(proc)
	for _, key := range []string{"attackOptions", "attackStats", "attackRevision", "pubsubAuthorId", "pubsubAuthorMode"} {
		if result.Metadata[key] != proc.node.Metadata[key] {
			t.Fatalf("terminal projection lost %s", key)
		}
	}
}
