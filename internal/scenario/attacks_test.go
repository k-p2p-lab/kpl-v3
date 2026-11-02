package scenario

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
