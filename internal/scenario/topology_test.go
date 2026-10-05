package scenario

import (
	"fmt"
	"strings"
	"testing"
)

func TestTopologyV3SelectorsAndDefaults(t *testing.T) {
	peerID := meshFreezeTestPeerID(t)
	for _, selector := range []string{
		"group: workers",
		"nodeIds: [run-workers-00001, run-workers-00002]",
		"peerIds: [" + peerID + "]",
		"group: workers, nodeIds: [run-workers-00001], peerIds: [" + peerID + "], role: worker, type: full, count: 1",
	} {
		t.Run(selector, func(t *testing.T) {
			spec, err := Parse([]byte("version: 3\nname: graph\nphases: [{action: join, group: workers, count: 2, node: {gossipsub: {meshFreeze: true}}}, {action: topology, topic: blocks, topology: {model: er, p: 0, seed: 0}, " + selector + "}]"))
			if err != nil {
				t.Fatal(err)
			}
			phase := spec.Phases[1]
			if phase.Timeout != "2m" || !phase.ShouldAwait() || phase.Repeat != 1 || phase.Job != "" {
				t.Fatalf("unexpected defaults: %+v", phase)
			}
			if phase.Topology.Seed == nil || *phase.Topology.Seed != 0 {
				t.Fatal("explicit zero seed was lost")
			}
			if err := spec.Validate(); err != nil {
				t.Fatalf("validation is not idempotent: %v", err)
			}
		})
	}
}

func TestTopologyRejectsInvalidSchema(t *testing.T) {
	base := "group: workers, topic: blocks, topology: {model: er, p: 0.1}"
	cases := []struct{ name, fields string }{
		{"missing selector", "topic: blocks, topology: {model: er, p: 0.1}"},
		{"missing config", "group: workers, topic: blocks"},
		{"missing topic", "group: workers, topology: {model: er, p: 0.1}"},
		{"blank topic", "group: workers, topic: ' ', topology: {model: er, p: 0.1}"},
		{"wildcard topic", "group: workers, topic: '*', topology: {model: er, p: 0.1}"},
		{"excessive topic", "group: workers, topic: '" + strings.Repeat("x", 1025) + "', topology: {model: er, p: 0.1}"},
		{"empty selector", base + ", nodeIds: []"},
		{"duplicate IDs", base + ", nodeIds: [a, a]"},
		{"invalid peer ID", base + ", peerIds: [invalid]"},
		{"background", base + ", await: false"},
		{"parallel", base + ", parallel: true"},
		{"parallelism", base + ", parallelism: 1"},
		{"repeated", base + ", repeat: 2"},
		{"job", base + ", job: topology-job"},
		{"jobs", base + ", jobs: [join-job]"},
		{"onError", base + ", onError: continue"},
		{"placement", base + ", placement: balanced"},
		{"agent", base + ", agentId: agent"},
		{"profile", base + ", profile: full"},
		{"duration", base + ", duration: 1s"},
		{"message", base + ", message: ignored"},
		{"readyRatio", base + ", readyRatio: 0.5"},
		{"minCount", base + ", minCount: 1"},
		{"payloadSize", base + ", payloadSize: 10"},
		{"payloadEncoding", base + ", payloadEncoding: raw"},
		{"deliveryWindow", base + ", deliveryWindow: 1s"},
		{"interval", base + ", interval: {model: fixed, value: 1s}"},
		{"lifetime", base + ", lifetime: {model: fixed, value: 1s}"},
		{"node override", base + ", node: {type: full}"},
		{"count negative", base + ", count: -1"},
		{"count too large", base + ", count: 10001"},
		{"zero timeout", base + ", timeout: 0s"},
		{"bad timeout", base + ", timeout: later"},
		{"missing parameter", "group: workers, topic: blocks, topology: {model: ws, p: 0.1}"},
		{"unknown parameter", "group: workers, topic: blocks, topology: {model: er, p: 0.1, probability: 0.1}"},
		{"irrelevant parameter", "group: workers, topic: blocks, topology: {model: er, p: 0.1, radius: 0}"},
		{"grid wrong count", "group: workers, count: 7, topic: blocks, topology: {model: grid, rows: 2, columns: 3}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte("version: 3\nname: graph\nphases: [{action: join, group: workers, count: 6, node: {gossipsub: {meshFreeze: true}}}, {action: topology, " + tc.fields + "}]"))
			if err == nil {
				t.Fatal("invalid topology phase was accepted")
			}
		})
	}
	for _, version := range []int{0, 1, 2} {
		_, err := Parse([]byte(fmt.Sprintf("version: %d\nname: graph\nphases: [{action: topology, %s}]", version, base)))
		if err == nil || !strings.Contains(err.Error(), "requires version 3") {
			t.Fatalf("version %d error=%v", version, err)
		}
	}
	if _, err := Parse([]byte("version: 3\nname: graph\nphases: [{action: wait, duration: 1s, topology: {model: er, p: 0.1}}]")); err == nil {
		t.Fatal("topology configuration on another action was accepted")
	}
}

func TestTopologyLatticeExplicitDimensions(t *testing.T) {
	for _, name := range []string{"grid", "triangular"} {
		for _, count := range []string{"", ", count: 6"} {
			data := fmt.Sprintf("version: 3\nname: lattice\nphases: [{action: join, group: workers, count: 6, node: {gossipsub: {meshFreeze: true}}}, {action: topology, group: workers, topic: blocks, topology: {model: %s, rows: 2, columns: 3}%s}]", name, count)
			if _, err := Parse([]byte(data)); err != nil {
				t.Fatalf("%s%s: %v", name, count, err)
			}
		}
	}
}

func TestTopologyRequiresFreezeForMatchingGroupJoins(t *testing.T) {
	const enabled = "{name: enabled, action: join, group: workers, count: 2, profile: frozen}"
	const disabled = "{name: disabled, action: join, group: workers, count: 2}"
	const topology = "{name: graph, action: topology, group: workers, topic: blocks, topology: {model: er, p: 0}}"
	for _, tc := range []struct {
		name, phases, diagnostic string
	}{
		{"profile enables freeze", enabled + "," + topology, ""},
		{"missing freeze", disabled + "," + topology, "matching join phase \"disabled\""},
		{"explicit false", "{action: join, group: workers, count: 1, node: {gossipsub: {meshFreeze: false}}}," + topology, "meshFreeze: true"},
		{"profile overridden false", "{action: join, group: workers, count: 1, profile: frozen, node: {gossipsub: {meshFreeze: false}}}," + topology, "meshFreeze: true"},
		{"inline enables freeze", "{action: join, group: workers, count: 1, node: {gossipsub: {meshFreeze: true}}}," + topology, ""},
		{"mixed group", enabled + "," + disabled + "," + topology, "matching join phase \"disabled\""},
		{"later enabled join cannot mask disabled", disabled + "," + enabled + "," + topology, "matching join phase \"disabled\""},
		{"unrelated group", strings.ReplaceAll(disabled, "workers", "other") + "," + enabled + "," + topology, ""},
		{"role filter excludes boot", "{action: join, group: workers, role: boot, count: 1}," + enabled + "," + strings.Replace(topology, "topic:", "role: worker, topic:", 1), ""},
		{"type filter uses canonical alias", "{action: join, group: workers, type: gossip-only, count: 1}," + enabled + "," + strings.Replace(topology, "topic:", "type: worker, topic:", 1), ""},
		{"filter selects disabled", enabled + ",{name: disabled, action: join, group: workers, role: boot, count: 1}," + strings.Replace(topology, "topic:", "role: boot, topic:", 1), "matching join phase \"disabled\""},
		{"filter matches no join", enabled + "," + strings.Replace(topology, "topic:", "role: boot, topic:", 1), "preceding join"},
		{"unknown group", topology, "preceding join"},
		{"future join does not qualify", topology + "," + enabled, "preceding join"},
		{"later disabled join does not affect topology", enabled + "," + topology + "," + disabled, ""},
		{"background join also checked", strings.Replace(disabled, "count:", "await: false, count:", 1) + "," + topology, "matching join phase \"disabled\""},
		{"background join enabled", strings.Replace(enabled, "count:", "await: false, count:", 1) + "," + topology, ""},
		{"stop-all clears joins", disabled + ",{action: stop-all}," + enabled + "," + topology, ""},
		{"reset clears joins", disabled + ",{action: reset}," + enabled + "," + topology, ""},
		{"stop-all requires new join", enabled + ",{action: stop-all}," + topology, "preceding join"},
		{"leave cannot prove surviving configurations", disabled + ",{action: leave, group: workers, count: 1}," + enabled + "," + topology, "matching join phase \"disabled\""},
		{"ordinary scenario permits disabled freeze", disabled + ",{action: wait, duration: 1s}", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := Parse([]byte("version: 3\nname: graph\nprofiles: {frozen: {gossipsub: {meshFreeze: true}}}\nphases: [" + tc.phases + "]"))
			if tc.diagnostic != "" {
				if err == nil || !strings.Contains(err.Error(), tc.diagnostic) || !strings.Contains(err.Error(), "phase \"graph\"") {
					t.Fatalf("got %v, want graph phase diagnostic containing %q", err, tc.diagnostic)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := spec.Validate(); err != nil {
				t.Fatalf("validation is not idempotent: %v", err)
			}
		})
	}
}

func TestTopologyExplicitIDsDeferJoinResolutionToRuntime(t *testing.T) {
	for _, selector := range []string{"nodeIds: [run-workers-00001]", "peerIds: [" + meshFreezeTestPeerID(t) + "]"} {
		for _, group := range []string{"", "group: workers,"} {
			spec, err := Parse([]byte("version: 3\nname: graph\nphases: [{action: join, group: workers, count: 1}, {action: topology, " + group + selector + ", topic: blocks, topology: {model: er, p: 0}}]"))
			if err != nil {
				t.Fatalf("%s%s: %v", group, selector, err)
			}
			if err := spec.Validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
