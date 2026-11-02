package scenario

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAttackOptionsRejectNonJoinNodeBlocks(t *testing.T) {
	for _, phase := range []string{
		"action: wait, duration: 1ms", "action: sleep, duration: 1ms",
		"action: publish, group: workers, count: 1", "action: leave, group: workers, count: 1",
		"action: wait-ready, group: workers", "action: wait-jobs",
		"action: log, message: checkpoint", "action: stop-all", "action: reset",
		"action: mesh-freeze, group: workers",
	} {
		for _, option := range []string{"sink", "censorship", "messageDelay", "connectionAttack", "graftFlood", "ihaveSpam", "iwantWithholding", "forwarding", "invalidMessage"} {
			for _, config := range []string{"{}", "{enabled: false}", "{enabled: true}"} {
				t.Run(phase+"/"+option+"/"+config, func(t *testing.T) {
					raw := fmt.Sprintf("version: 4\nname: misplaced attack\nphases:\n  - {action: join, group: workers, count: 1}\n  - {name: misplaced, %s, node: {%s: %s}}\n", phase, option, config)
					if _, err := Parse([]byte(raw)); err == nil || !strings.Contains(err.Error(), "join") {
						t.Fatalf("non-join attack declaration was not rejected for its location: %v", err)
					}
				})
			}
		}
	}
}

func TestInactiveAttackTemplatesRemainValidWithoutJoins(t *testing.T) {
	for _, option := range []string{"sink", "censorship", "messageDelay", "connectionAttack", "graftFlood", "ihaveSpam", "iwantWithholding", "forwarding", "invalidMessage"} {
		raw := fmt.Sprintf("version: 4\nname: inactive template\nprofiles:\n  optional: {%s: {enabled: false}}\nphases:\n  - {action: wait, duration: 1ms}\n", option)
		if _, err := Parse([]byte(raw)); err != nil {
			t.Fatalf("inactive %s template became invalid: %v", option, err)
		}
	}
}

func TestV4AttackSchemaGateAndExplicitOverrides(t *testing.T) {
	text := `version: 4
name: attack
profiles:
  worker:
    type: worker
    sink: {enabled: true, probability: 0.25}
phases:
  - action: join
    group: workers
    profile: worker
    count: 1
    node: {sink: {enabled: false, probability: 0}}
`
	s, err := Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if s.Phases[0].Node.Sink.Active() || *s.Phases[0].Node.Sink.Probability != 0 {
		t.Fatal("profile override lost explicit values")
	}
	for _, version := range []string{"1", "2", "3"} {
		_, err := Parse([]byte(strings.Replace(text, "version: 4", "version: "+version, 1)))
		if err == nil || !strings.Contains(err.Error(), "version 4") {
			t.Fatalf("old schema accepted attack: %v", err)
		}
	}
	for _, fragment := range []string{"true", "{enabled: true, probability: 30}", "{enabled: false, probabilty: 0.5}"} {
		bad := strings.Replace(text, "{enabled: true, probability: 0.25}", fragment, 1)
		if _, err := Parse([]byte(bad)); err == nil {
			t.Fatalf("accepted malformed sink: %s", fragment)
		}
	}
}
func TestV4ExamplesValidateAndV3ActionsRemainAvailable(t *testing.T) {
	paths, err := filepath.Glob("../../examples/attacks/*.yaml")
	if err != nil || len(paths) < 17 {
		t.Fatalf("missing attack examples: %v %d", err, len(paths))
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Parse(data); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, name := range []string{"scenario-v3-topology.yaml", "scenario-v3-mesh-freeze.yaml", "scenario-v3-schedule.yaml"} {
		data, err := os.ReadFile("../../examples/" + name)
		if err != nil {
			t.Fatal(err)
		}
		data = []byte(strings.Replace(string(data), "version: 3", "version: 4", 1))
		if _, err := Parse(data); err != nil {
			t.Fatalf("v3 feature rejected in v4: %v", err)
		}
	}
}

func TestAttackExamplesUsePublisherCountsAndExplicitRepetition(t *testing.T) {
	paths, err := filepath.Glob("../../examples/attacks/*.yaml")
	if err != nil || len(paths) < 17 {
		t.Fatalf("missing attack examples: %v %d", err, len(paths))
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			cohorts := map[string]int{}
			for _, phase := range spec.Phases {
				if phase.Action == "join" && phase.Node.PublishAllowed() {
					cohorts[phase.Group] += phase.Count * phase.Repeat
				}
				if phase.Action != "publish" {
					continue
				}
				want := 120
				if filepath.Base(path) == "publish-load.yaml" {
					want = 5000
				}
				if phase.Count > cohorts[phase.Group] || phase.Count*phase.Repeat != want {
					t.Fatalf("%s: count selects publishers, not messages: cohort=%d count=%d repeat=%d, want %d attempts", phase.Name, cohorts[phase.Group], phase.Count, phase.Repeat, want)
				}
			}
		})
	}
}

func TestV4RejectsUnknownTargetGroupsAndAttackOnBoot(t *testing.T) {
	text := `version: 4
name: attack
profiles:
  attacker:
    type: worker
    connectionAttack: {enabled: true, targets: {groups: [victims]}}
phases:
  - {action: join, group: attackers, profile: attacker, count: 1}
  - {action: wait, duration: 1s}
  - {action: join, group: victims, count: 1}
`
	if _, err := Parse([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse([]byte(strings.Replace(text, "group: victims", "group: other", 1))); err == nil {
		t.Fatal("unknown target group accepted")
	}
	if _, err := Parse([]byte(strings.Replace(text, "profile: attacker", "profile: attacker, role: boot", 1))); err == nil {
		t.Fatal("attack on boot role accepted")
	}
	text = strings.Replace(text, "- {action: wait, duration: 1s}", "- {action: stop-all}", 1)
	if _, err := Parse([]byte(text)); err == nil {
		t.Fatal("target group in another generation accepted")
	}
}
