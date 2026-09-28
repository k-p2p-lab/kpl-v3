package model

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/distribution"
)

func TestNetworkScheduleValidationAndFutureCapability(t *testing.T) {
	base := NetworkConfig{Delay: "10ms", Schedule: &NetworkSchedule{Reference: "experiment-start", Changes: []NetworkChange{{After: "10m", Set: NetworkConfig{Delay: "100ms"}}, {After: "20m", Set: NetworkConfig{Delay: "0s"}}}}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	future := NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "3m", Set: NetworkConfig{LossPercent: float64Ptr(100)}}}}}
	if !future.Enabled() || future.Initial().Enabled() {
		t.Fatal("future impairment did not reserve NET_ADMIN")
	}
	cases := []struct {
		name   string
		config NetworkConfig
		want   string
	}{
		{"clock", NetworkConfig{Schedule: &NetworkSchedule{Reference: "wall", Changes: base.Schedule.Changes}}, "reference"},
		{"negative", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "-1s"}}}}, "after"},
		{"missing", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{}}}}, "after"},
		{"unordered", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "2s"}, {After: "1s"}}}}, "strictly increasing"},
		{"duplicate", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "0s"}, {After: "0s"}}}}, "strictly increasing"},
		{"nested", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "1s", Set: NetworkConfig{Schedule: &NetworkSchedule{}}}}}}, "another schedule"},
		{"invalid resulting state", NetworkConfig{Delay: "10ms", Jitter: "1ms", Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "1s", Set: NetworkConfig{Delay: "0s"}}}}}, "jitter requires"},
		{"invalid loss", NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{{After: "1s", Set: NetworkConfig{LossPercent: float64Ptr(101)}}}}}, "lossPercent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.config.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}

func TestNetworkScheduleResolutionIsSeededCumulativeAndImmutable(t *testing.T) {
	dist := &distribution.Distribution{Model: "normal", Mean: "30ms", Sigma: "10ms", Min: "1ms"}
	original := NetworkConfig{DelayDistribution: dist, LossPercent: float64Ptr(2), Schedule: &NetworkSchedule{Changes: []NetworkChange{
		{After: "1s", Set: NetworkConfig{LossPercent: float64Ptr(0)}},
		{After: "2s", Set: NetworkConfig{DelayDistribution: dist}},
		{After: "3s", Set: NetworkConfig{Delay: "100ms"}},
	}}}
	before, _ := json.Marshal(original)
	a, err := original.Resolve(rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := original.Resolve(rand.New(rand.NewSource(42)))
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("unseeded plan: %v", err)
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("resolution changed requested profile")
	}
	if a.Schedule.Reference != "peer-join" || a.Schedule.Changes[0].Set.Delay != a.Delay || *a.Schedule.Changes[0].Set.LossPercent != 0 || a.Schedule.Changes[1].Set.Delay == a.Delay || a.Schedule.Changes[2].Set.Delay != "100ms" {
		t.Fatalf("resolved plan: %+v", a)
	}
	for _, step := range a.Schedule.Changes {
		if step.Set.Schedule != nil || step.Set.DelayDistribution != nil {
			t.Fatal("unresolved step")
		}
	}
	cleared := a.Merge(NetworkConfig{Schedule: &NetworkSchedule{Changes: []NetworkChange{}}})
	if cleared.Scheduled() || cleared.Delay != a.Delay {
		t.Fatal("empty schedule must replace inherited changes and preserve base")
	}
}
