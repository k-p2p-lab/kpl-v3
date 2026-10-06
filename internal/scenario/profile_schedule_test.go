package scenario

import (
	"strings"
	"testing"
)

const profileScheduleExample = `version: 3
name: profile changes
profiles:
  baseline:
    gossipsub: {params: {d: 6}}
phases:
  - {action: join, group: selected, profile: baseline, count: 1}
  - action: schedule
    group: selected
    job: conditions
    await: false
    schedule:
      changes:
        - after: 0s
          set: {gossipsub: {params: {d: 8}}, network: {delay: 50ms}}
        - after: 10s
          set: {gossipsub: {params: {d: 6}}, network: {delay: 0s}}
  - {action: join, group: selected, profile: baseline, count: 1}
  - {action: join, group: unselected, profile: baseline, count: 1}
  - {action: wait-jobs, jobs: [conditions]}
  - {action: stop-all}
  - {action: join, group: selected, profile: baseline, count: 1}
`

func TestProfileScheduleValidationPreparesOnlyMatchingGeneration(t *testing.T) {
	spec, err := Parse([]byte(profileScheduleExample))
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, 2} {
		if !spec.Phases[i].NetworkMutable {
			t.Fatalf("join %d lacks network capability", i)
		}
	}
	for _, i := range []int{3, 6} {
		if spec.Phases[i].NetworkMutable {
			t.Fatalf("join %d acquired unrelated capability", i)
		}
	}
	for _, i := range []int{0, 2, 3, 6} {
		if *spec.Phases[i].Node.GossipSub.Params.D != 6 || spec.Phases[i].Node.Network.Delay != "" {
			t.Fatal("validation changed an immutable profile template")
		}
	}
	if spec.Phases[1].Schedule.Clock() != "phase-start" || spec.Phases[1].Timeout != "2m" {
		t.Fatal("wrong schedule defaults")
	}
}

func TestProfileScheduleRejectsInvalidOrUnsupportedChanges(t *testing.T) {
	for name, replace := range map[string][2]string{
		"old-version":          {"version: 3", "version: 2"},
		"negative-time":        {"after: 0s", "after: -1s"},
		"unordered":            {"after: 10s", "after: 0s"},
		"clock":                {"changes:", "reference: peer-join\n      changes:"},
		"degree-bounds":        {"d: 8", "d: 20"},
		"unsupported-setting":  {"d: 8", "heartbeatInterval: 2s"},
		"network-range":        {"delay: 50ms", "lossPercent: 101"},
		"nested-network-timer": {"delay: 50ms", "schedule: {changes: []}"},
		"missing-selector":     {"    group: selected\n    job:", "    job:"},
		"missing-profile":      {"    group: selected\n    job:", "    profile: nonexistent\n    job:"},
		"repeat":               {"    job: conditions", "    repeat: 2\n    job: conditions"},
		"wrong-action":         {"action: schedule", "action: wait"},
		"empty-set":            {"set: {gossipsub: {params: {d: 8}}, network: {delay: 50ms}}", "set: {}"},
	} {
		t.Run(name, func(t *testing.T) {
			raw := strings.Replace(profileScheduleExample, replace[0], replace[1], 1)
			if raw == profileScheduleExample {
				t.Fatal("replacement missed input")
			}
			if _, err := Parse([]byte(raw)); err == nil {
				t.Fatal("invalid schedule accepted")
			}
		})
	}
}

func TestProfileScheduleProfileSelectorAndLegacyNetworkIsolation(t *testing.T) {
	raw := strings.Replace(profileScheduleExample, "    group: selected\n    job:", "    profile: baseline\n    job:", 1)
	spec, err := Parse([]byte(raw))
	if err != nil || !spec.Phases[3].NetworkMutable {
		t.Fatalf("profile selector: %v", err)
	}
	raw = strings.Replace(profileScheduleExample, "gossipsub: {params: {d: 6}}", "gossipsub: {params: {d: 6}}\n    network: {schedule: {changes: [{after: 1s, set: {delay: 20ms}}]}}", 1)
	if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlapping network timers accepted: %v", err)
	}
	raw = strings.ReplaceAll(raw, ", network: {delay: 50ms}", "")
	raw = strings.ReplaceAll(raw, ", network: {delay: 0s}", "")
	if _, err := Parse([]byte(raw)); err != nil {
		t.Fatalf("gossip-only change interfered with existing network timers: %v", err)
	}
}
