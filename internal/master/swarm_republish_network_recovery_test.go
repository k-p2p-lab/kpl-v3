package master

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// This test uses the real CLI paths with a fake Docker daemon. It records
// whether the control plane can still use Agents while Controller exits.
func TestSwarmRestartKeepsAgentsUntilControllerStops(t *testing.T) {
	for _, test := range []struct {
		name           string
		republish      bool
		failController bool
	}{
		{name: "restart"},
		{name: "republish", republish: true},
		{name: "republish-controller-exit-failure", republish: true, failController: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "restart")
			model := newRepublishLifecycleModel()
			model.base.failStop = test.failController
			var agentStoppedBeforeController bool
			runner.handle = func(ctx context.Context, c Command) (string, error) {
				a := c.Args
				last := a[len(a)-1]
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "ls"}) && !model.base.removed {
					return "controller agent monitor resource grafana networkmanager", nil
				}
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "inspect"}) {
					if component := map[string]string{"networkmanager": "network-manager", "resource": "resource-monitor", "grafana": "grafana"}[last]; component != "" {
						return "kpl_" + component + "|" + swarmApplication, nil
					}
				}
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "ps"}) {
					return strings.NewReplacer("_", "", "-", "").Replace(last) + "task", nil
				}
				if a[0] == "inspect" && slices.Contains(a, "task") {
					if replacement := map[string]string{"kplnetworkmanagertask": "kplnetwork-managertask", "kplresourcemonitortask": "kplresource-monitortask"}[last]; replacement != "" {
						copyCommand := c
						copyCommand.Args = slices.Clone(a)
						copyCommand.Args[len(a)-1] = replacement
						return model.base.handle(ctx, copyCommand)
					}
				}
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "update"}) && last == "kpl_agent" && !model.base.paused["kpl_controller"] {
					agentStoppedBeforeController = true
				}
				if a[0] == "ps" {
					if slices.Contains(a, "label=io.kpl.managed=true") {
						return "", nil
					}
					return "gatewaycontainer", nil
				}
				if a[0] == "inspect" && slices.Contains(a, "container") && strings.Contains(strings.Join(a, " "), ".Config.Labels") {
					return "kpl|kpl|", nil
				}
				if a[0] == "rm" {
					return "", nil
				}
				return model.handle(ctx, c)
			}
			var args []string
			if test.republish {
				args = []string{"--republish"}
			}
			err := s.a.RunSwarm(t.Context(), "restart", args)
			if test.failController {
				if err == nil {
					t.Fatal("republish proceeded without verifying Controller shutdown")
				}
				if model.base.paused["kpl_agent"] || swarmCallIndex(runner.calls, "service", "rm") >= 0 || swarmCallIndex(runner.calls, "build") >= 0 {
					t.Fatal("failed Controller shutdown stopped Agents, removed services, or started publishing")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if agentStoppedBeforeController {
				t.Fatal("Agent was stopped while Controller was still running; Controller shutdown cannot rely on Agent drain")
			}
		})
	}
}
