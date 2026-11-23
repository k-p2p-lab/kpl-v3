package scenario

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestAdaptiveHopwaveScenarioVersionAndResolution(t *testing.T) {
	const input = `version: 5
name: adaptive
profiles:
  participants:
    gossipsub:
      hopwave: true
      params:
        hopwaveAdaptive:
          retentionWindow: 0s
          payloadRanges:
            - {minBytes: 1024, maxBytes: 10240, factor: 0}
            - {minBytes: 10240, factor: 0.7}
phases:
  - {action: join, group: first, count: 1, profile: participants, node: {gossipsub: {params: {hopwaveAdaptive: {retentionWindow: 60s}}}}}
  - {action: join, group: second, count: 1, profile: participants}
  - {action: join, group: third, count: 1, profile: participants, node: {gossipsub: {params: {hopwaveAdaptive: {retentionWindow: 1m, payloadRanges: []}}}}}
  - {action: join, group: fourth, count: 1, profile: participants, node: {gossipsub: {hopwave: false}}}
  - {action: join, group: fifth, count: 1, profile: participants, node: {gossipsub: {enabled: false}}}
`
	spec, err := Parse([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	for i, phase := range spec.Phases {
		wire, err := json.Marshal(model.PeerProcessConfig{NodeConfig: phase.Node})
		if err != nil {
			t.Fatal(err)
		}
		var peer model.PeerProcessConfig
		if err := json.Unmarshal(wire, &peer); err != nil {
			t.Fatal(err)
		}
		policy := peer.NodeConfig.GossipSub.Params.HopwaveAdaptive
		if i >= 3 {
			if policy != nil {
				t.Fatalf("phase %d: disabled Peer retained inherited policy", i)
			}
			continue
		}
		if policy == nil {
			t.Fatal("peer policy omitted")
		}
		wantWindow := time.Minute
		if i == 1 {
			wantWindow = 0
		}
		if window, err := policy.WindowDuration(); err != nil || window != wantWindow {
			t.Fatalf("phase %d: window=%v err=%v", i, window, err)
		}
		if i == 2 {
			if policy.PayloadRanges == nil || len(policy.PayloadRanges) != 0 {
				t.Fatalf("inherited ranges survived explicit []: %v", policy.PayloadRanges)
			}
		} else if r := policy.PayloadRanges; len(r) != 2 || r[0].MinBytes != 1024 || r[0].MaxBytes == nil || *r[0].MaxBytes != 10240 || r[0].Factor == nil || *r[0].Factor != 0 || r[1].MinBytes != 10240 || r[1].MaxBytes != nil || r[1].Factor == nil || *r[1].Factor != .7 {
			t.Fatalf("phase %d: ranges=%+v", i, r)
		}
	}
	if _, err := Parse([]byte(strings.Replace(input, "version: 5", "version: 6", 1))); err == nil {
		t.Fatal("unexpected future version accepted")
	}
	for version := 1; version <= 4; version++ {
		for _, source := range []string{
			"name: unused\nprofiles: {unused: {gossipsub: {params: {hopwaveAdaptive: {}}}}}\nphases: [{action: wait, duration: 1ms}]",
			"name: inline\nphases: [{action: join, group: g, count: 1, node: {gossipsub: {hopwave: true, params: {hopwaveAdaptive: {}}}}}]",
		} {
			if _, err := Parse([]byte(fmt.Sprintf("version: %d\n%s", version, source))); err == nil || !strings.Contains(err.Error(), "requires version 5") {
				t.Fatalf("v%d: %v", version, err)
			}
		}
	}
	for _, settings := range []string{"hopwave: false", "enabled: false, hopwave: true", "router: randomsub, hopwave: true"} {
		source := fmt.Sprintf("version: 5\nname: disabled\nphases: [{action: join, group: g, count: 1, node: {gossipsub: {%s, params: {hopwaveAdaptive: {retentionWindow: 60s}}}}}]", settings)
		if _, err := Parse([]byte(source)); err == nil || !strings.Contains(err.Error(), "requires enabled GossipSub and hopwave") {
			t.Fatalf("%s: %v", settings, err)
		}
	}
}
