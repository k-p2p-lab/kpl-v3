package scenario

import (
	"fmt"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

func TestProfileScheduleManySiblingsPreserveGenerationScope(t *testing.T) {
	const count = 4096
	raw := "version: 4\nname: schedule siblings\nphases:\n- {action: stop-all}\n- {action: reset}\n" +
		"- &change {action: schedule, group: selected, nodeIds: [future-peer], schedule: {changes: [{after: 0s, set: {network: {delay: 1ms}}}]}}\n" +
		strings.Repeat("- *change\n", count-1) +
		"- {action: join, group: selected, count: 1}\n- {action: reset}\n- {action: join, group: selected, count: 1}\n"
	if len(raw) >= 1<<20 {
		t.Fatal("fixture exceeds the scenario upload limit")
	}
	spec, err := Parse([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !spec.Phases[count+2].NetworkMutable {
		t.Fatal("schedules did not grant capability to their future join")
	}
	if spec.Phases[count+4].NetworkMutable {
		t.Fatal("schedules crossed a reset into another generation")
	}
	if spec.Phases[count+2].Node.Network.Delay != "" {
		t.Fatal("validation changed the initial network conditions")
	}
}

func BenchmarkProfileScheduleSiblingLookup(b *testing.B) {
	for _, count := range []int{1000, 4000, 16000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			spec := Scenario{Phases: make([]Phase, count+1)}
			for i := 0; i < count; i++ {
				spec.Phases[i] = Phase{Action: "schedule", Group: "selected", NodeIDs: []string{"future-peer"},
					Schedule: &model.ProfileSchedule{Changes: []model.ProfileChange{{After: "0s", Set: model.RuntimeProfilePatch{Network: &model.NetworkConfig{Delay: "1ms"}}}}}}
			}
			spec.Phases[count] = Phase{Action: "join", Group: "selected"}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := spec.validateProfileSchedules(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
