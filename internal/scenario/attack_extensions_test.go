package scenario

import (
	"fmt"
	"strings"
	"testing"
)

func TestV4AdditionalAttackVersionTargetsAndRuntimeGates(t *testing.T) {
	for _, option := range []string{"forwarding: {enabled: true, probability: 0.5, targets: {groups: [victims]}}", "invalidMessage: {enabled: true, topics: [blocks], targets: {groups: [victims]}}"} {
		text := fmt.Sprintf(`version: 4
name: additional attacks
profiles:
  attacker:
    type: worker
    gossipsub: {topics: [blocks]}
    %s
phases:
  - {action: join, group: attackers, profile: attacker, count: 1}
  - {action: join, group: victims, count: 1}
`, option)
		if _, err := Parse([]byte(text)); err != nil {
			t.Fatal(err)
		}
		for _, v := range []string{"1", "2", "3"} {
			if _, err := Parse([]byte(strings.Replace(text, "version: 4", "version: "+v, 1))); err == nil {
				t.Fatal("legacy schema accepted new attack")
			}
		}
		if _, err := Parse([]byte(strings.Replace(text, "group: victims", "group: wrong", 1))); err == nil {
			t.Fatal("unknown target group accepted")
		}
	}
}
