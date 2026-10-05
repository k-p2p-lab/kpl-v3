package scenario

import (
	"fmt"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func meshFreezeTestPeerID(t *testing.T) string {
	t.Helper()
	hash, err := multihash.Sum([]byte("mesh-freeze-test-peer"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return peer.ID(hash).String()
}

func TestMeshFreezeV3SelectorsAndDefaults(t *testing.T) {
	peerID := meshFreezeTestPeerID(t)
	for _, selector := range []string{
		"group: workers",
		"nodeIds: [run-workers-00001, run-workers-00002]",
		"peerIds: [" + peerID + "]",
		"group: workers, nodeIds: [run-workers-00001], peerIds: [" + peerID + "], role: worker, type: full",
	} {
		t.Run(selector, func(t *testing.T) {
			spec, err := Parse([]byte("version: 3\nname: freeze\nphases: [{action: mesh-freeze, " + selector + "}]"))
			if err != nil {
				t.Fatal(err)
			}
			phase := spec.Phases[0]
			if phase.Timeout != "30s" || !phase.ShouldAwait() || phase.Repeat != 1 {
				t.Fatalf("unexpected defaults: %+v", phase)
			}
		})
	}
}

func TestMeshFreezeRejectsInvalidSchema(t *testing.T) {
	peerID := meshFreezeTestPeerID(t)
	cases := []struct{ name, fields string }{
		{"missing selector", ""},
		{"empty group", "group: ' '"},
		{"empty node selector", "group: workers, nodeIds: []"},
		{"empty peer selector", "group: workers, peerIds: []"},
		{"empty ID", "nodeIds: ['']"},
		{"space in ID", "nodeIds: ['node id']"},
		{"unicode space in ID", "nodeIds: ['node\u2003id']"},
		{"path in ID", "nodeIds: ['../node']"},
		{"duplicate ID", "nodeIds: [node, node]"},
		{"invalid peer ID", "peerIds: [node]"},
		{"duplicate peer ID", "peerIds: [" + peerID + ", " + peerID + "]"},
		{"zero timeout", "group: workers, timeout: 0s"},
		{"bad timeout", "group: workers, timeout: later"},
		{"background", "group: workers, await: false"},
		{"parallel", "group: workers, parallel: true"},
		{"parallelism", "group: workers, parallelism: 2"},
		{"count", "group: workers, count: 1"},
		{"interval", "group: workers, interval: {model: fixed, value: 1s}"},
		{"topic", "group: workers, topic: topic"},
		{"continue", "group: workers, onError: continue"},
		{"role", "group: workers, role: anything"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte("version: 3\nname: freeze\nphases: [{action: mesh-freeze, " + tc.fields + "}]"))
			if err == nil {
				t.Fatal("invalid mesh-freeze phase was accepted")
			}
		})
	}
	for _, version := range []int{0, 1, 2} {
		_, err := Parse([]byte(fmt.Sprintf("version: %d\nname: freeze\nphases: [{action: mesh-freeze, group: workers}]", version)))
		if err == nil || !strings.Contains(err.Error(), "requires version 3") {
			t.Fatalf("version %d error = %v", version, err)
		}
	}
	for _, version := range []int{1, 2, 3} {
		_, err := Parse([]byte(fmt.Sprintf("version: %d\nname: freeze\nphases: [{action: wait, duration: 1s, nodeIds: [node]}]", version)))
		if err == nil {
			t.Fatalf("version %d accepted nodeIds on wait", version)
		}
	}
}

func TestLegacyScenarioVersionsRemainSupported(t *testing.T) {
	for _, version := range []int{0, 1, 2, 3} {
		spec, err := Parse([]byte(fmt.Sprintf("version: %d\nname: existing\nphases: [{action: wait, duration: 1s}]", version)))
		if err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
		if spec.Version != max(1, version) {
			t.Fatalf("version %d changed to %d", version, spec.Version)
		}
	}
}
