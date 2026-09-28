package scenario

import (
	"os"
	"strings"
	"testing"
)

func TestNetworkProfileAndPhaseOverrides(t *testing.T) {
	scenario, err := Parse([]byte(`
version: 2
name: network-profiles
profiles:
  wan:
    network:
      delay: 80ms
      jitter: 8ms
      lossPercent: 2.5
      duplicatePercent: 1
      rateMbps: 20
phases:
  - action: join
    group: slow
    profile: wan
    count: 2
  - action: join
    group: fast
    profile: wan
    count: 1
    node:
      network:
        delay: 0s
        jitter: 0s
        lossPercent: 0
        duplicatePercent: 0
        rateMbps: 0
`))
	if err != nil {
		t.Fatal(err)
	}
	slow, fast := scenario.Phases[0].Node.Network, scenario.Phases[1].Node.Network
	if slow.Delay != "80ms" || slow.Jitter != "8ms" || *slow.LossPercent != 2.5 || *slow.RateMbps != 20 {
		t.Fatalf("profile impairments lost: %+v", slow)
	}
	if fast.Enabled() || fast.LossPercent == nil || fast.RateMbps == nil {
		t.Fatalf("explicit phase zeros did not disable inherited impairments: %+v", fast)
	}
}

func TestScenarioRejectsInvalidNetworkBeforeExecution(t *testing.T) {
	for _, field := range []string{"lossPercent: 101", "lossPercent: .nan", "delay: -2ms", "jitter: 5ms", "reorderPercent: 10", "rateMbps: -2", "queueLimit: 0"} {
		t.Run(field, func(t *testing.T) {
			_, err := Parse([]byte("name: invalid-network\nphases:\n  - action: join\n    group: peers\n    count: 1\n    node:\n      network:\n        " + field + "\n"))
			if err == nil || !strings.Contains(err.Error(), "network:") {
				t.Fatalf("invalid impairment %q accepted: %v", field, err)
			}
		})
	}
}

func TestNetworkSchedulesParseInProfilesAndJoinOverrides(t *testing.T) {
	spec, err := Parse([]byte(`version: 2
name: scheduled-network
profiles:
  wan:
    network:
      delay: 10ms
      schedule:
        reference: experiment-start
        changes:
          - after: 10m
            set:
              delay: 100ms
phases:
  - action: join
    group: absolute
    count: 1
    profile: wan
  - action: join
    group: relative
    count: 1
    profile: wan
    node:
      network:
        schedule:
          reference: peer-join
          changes:
            - after: 3m
              set:
                delay: 100ms
  - action: join
    group: static
    count: 1
    profile: wan
    node:
      network:
        schedule:
          changes: []
`))
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := spec.Phases[0].Node.Network, spec.Phases[1].Node.Network, spec.Phases[2].Node.Network
	if a.Schedule.Clock() != "experiment-start" || b.Schedule.Clock() != "peer-join" || b.Schedule.Changes[0].After != "3m" || c.Scheduled() || c.Delay != "10ms" {
		t.Fatalf("profile schedule overlay: %+v %+v %+v", a, b, c)
	}
	if _, err := Parse([]byte("name: invalid\nphases:\n  - action: join\n    group: peers\n    count: 1\n    node:\n      network:\n        schedule:\n          clock: peer-join\n")); err == nil {
		t.Fatal("unknown schedule key silently ignored")
	}
}

func TestNetworkScheduleExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/network-schedule.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(data); err != nil {
		t.Fatalf("network schedule example: %v", err)
	}
}
