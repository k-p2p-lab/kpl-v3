package master

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// This test includes all six services and the standalone gateway, unlike the
// older controller/agent/monitor-only fixture. No real Docker is invoked.
// Ordinary restart retains networks and the gateway; republish deliberately
// uses direct teardown and must stop publishing after a removal error.
func TestSwarmRestartAndRepublishGatewayLifecycles(t *testing.T) {
	for _, test := range []struct {
		name            string
		republish       bool
		retainedGateway bool
		blockedShutdown bool
	}{
		{name: "restart"},
		{name: "republish", republish: true},
		{name: "republish-retained-gateway-removal-error", republish: true, retainedGateway: true},
		{name: "restart-waits-for-controller-exit", blockedShutdown: true},
		{name: "republish-waits-for-manager-exit", republish: true, blockedShutdown: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, output := newSwarmFixture(t, "restart")
			model := newRepublishLifecycleModel()
			var agentStoppedBeforeController, gatewayRemoved bool
			var blockedTaskObserved bool
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			terminalChecks := map[string]bool{}
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
					blockedTask, blockedService := "kplcontrollertask", "kpl_controller"
					if test.republish {
						blockedTask, blockedService = "kplnetworkmanagertask", "kpl_network-manager"
					}
					if test.blockedShutdown && last == blockedTask && model.base.paused[blockedService] {
						blockedTaskObserved = true
						cancel()
						return "control|running|container|123|0|ok", nil
					}
					copyCommand := c
					if replacement := map[string]string{"kplnetworkmanagertask": "kplnetwork-managertask", "kplresourcemonitortask": "kplresource-monitortask"}[last]; replacement != "" {
						copyCommand.Args = slices.Clone(a)
						copyCommand.Args[len(a)-1] = replacement
					}
					record, err := model.base.handle(ctx, copyCommand)
					if task, parseErr := parseSwarmTask(record); err == nil && parseErr == nil && task.clean() {
						terminalChecks[last] = true
					}
					return record, err
				}
				if slices.Equal(a[:min(len(a), 2)], []string{"service", "update"}) && last == "kpl_agent" && !model.base.paused["kpl_controller"] {
					agentStoppedBeforeController = true
				}
				if a[0] == "ps" {
					if slices.Contains(a, "label=io.kpl.managed=true") {
						if test.retainedGateway {
							return "remainingpeer", nil
						}
						return "", nil
					}
					return "gatewaycontainer", nil
				}
				if a[0] == "inspect" && slices.Contains(a, "container") && strings.Contains(strings.Join(a, " "), ".Config.Labels") {
					return "kpl|kpl|", nil
				}
				if a[0] == "rm" {
					if !model.base.removed {
						return "", errors.New("gateway removed before service deletion")
					}
					if !terminalChecks["kplagenttask"] || !terminalChecks["kplnetworkmanagertask"] {
						return "", errors.New("gateway removed before clean Agent and Manager task observations")
					}
					gatewayRemoved = true
					return "", nil
				}
				if slices.Equal(a[:min(len(a), 2)], []string{"stack", "rm"}) && test.retainedGateway {
					// Inject a daemon refusal of a control overlay still attached
					// to the gateway; this is not a real-cluster observation.
					return "", errors.New("fixture control network has active gateway endpoint")
				}
				return model.handle(ctx, c)
			}
			var args []string
			if test.republish {
				args = []string{"--republish"}
			}
			err := s.a.RunSwarm(ctx, "restart", args)
			if test.blockedShutdown {
				if !blockedTaskObserved || !errors.Is(err, context.Canceled) || gatewayRemoved || swarmCallIndex(runner.calls, "service", "rm") >= 0 || swarmCallIndex(runner.calls, "build") >= 0 || swarmCallIndex(runner.calls, "stack", "deploy") >= 0 {
					t.Fatalf("still-running task was treated as a clean shutdown: %v", err)
				}
				if !test.republish && model.base.paused["kpl_agent"] {
					t.Fatal("ordinary restart stopped Agents while Controller remained running")
				}
				return
			}
			if test.retainedGateway {
				if err == nil || !strings.Contains(err.Error(), "republish stopped during removal") || !strings.Contains(err.Error(), "active gateway endpoint") {
					t.Fatalf("retained control network error was not returned: %v", err)
				}
				if gatewayRemoved || !strings.Contains(output.String(), "Gateway removal is unverified") || !model.base.removed {
					t.Fatal("unverified gateway was not retained and reported while services were removed")
				}
				for _, forbidden := range [][]string{{"build"}, {"buildx"}, {"push"}, {"pull"}, {"stack", "deploy"}} {
					if swarmCallIndex(runner.calls, forbidden...) >= 0 {
						t.Fatalf("republish continued after stack removal failure: %v", forbidden)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if agentStoppedBeforeController != test.republish || gatewayRemoved != test.republish {
				t.Fatal("restart and republish lost their distinct shutdown/gateway contracts")
			}
			if test.republish {
				previous := -1
				for _, step := range [][]string{{"service", "rm"}, {"rm", "--force", "gatewaycontainer"}, {"stack", "rm"}, {"build"}, {"push"}, {"stack", "deploy"}} {
					index := swarmCallIndex(runner.calls, step...)
					if index <= previous {
						t.Fatalf("republish step is missing or out of order: %v", step)
					}
					previous = index
				}
			} else {
				if !terminalChecks["kplcontrollertask"] {
					t.Fatal("ordinary restart never observed a clean Controller task exit")
				}
				for _, forbidden := range [][]string{{"service", "rm"}, {"stack", "rm"}, {"rm", "--force"}, {"build"}, {"push"}} {
					if swarmCallIndex(runner.calls, forbidden...) >= 0 {
						t.Fatalf("ordinary restart performed teardown or publishing: %v", forbidden)
					}
				}
			}
		})
	}
}
