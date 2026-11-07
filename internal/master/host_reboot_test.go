package master

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/k-p2p-lab/kpl-v3/internal/agent"
	"github.com/k-p2p-lab/kpl-v3/internal/auth"
)

func TestHostRebootConfigurationDefaultsAndLiteralRoundTrip(t *testing.T) {
	config := ConfigDefaults(nil)
	if config["KPL_AGENT_HOST_REBOOT_ENABLED"] != "false" {
		t.Fatal("host reboot must require an explicit opt-in")
	}
	config["KPL_AGENT_HOST_REBOOT_ENABLED"] = "true"
	decoded, legacy, err := parseConfig(encodeConfig(config))
	if err != nil || legacy || decoded["KPL_AGENT_HOST_REBOOT_ENABLED"] != "true" {
		t.Fatalf("explicit host reboot setting was not retained: %v legacy=%t", err, legacy)
	}
}

func TestAgentRoleSeparatesHostManagementFromPeerCredentials(t *testing.T) {
	const user, password = "fake-operator", "fake-test-password-only"
	peerKey := auth.InternalToken(user, password)
	config := authenticatedAgentConfig(user, password, agent.Config{ID: "selected-agent", HostRebootEnabled: true, ControlNodeID: "manager1", Token: "untrusted-input", HostRebootToken: peerKey})
	if config.Token != peerKey || config.HostRebootToken == "" || config.HostRebootToken == config.Token || config.HostRebootToken != auth.HostRebootToken(user, password) {
		t.Fatal("Agent role did not separate host-management authorization from the Peer key")
	}
	if config.ID != "selected-agent" || !config.HostRebootEnabled || config.ControlNodeID != "manager1" {
		t.Fatal("credential derivation changed Agent runtime settings")
	}
	if again := authenticatedAgentConfig(user, password, agent.Config{}); again.Token != config.Token || again.HostRebootToken != config.HostRebootToken {
		t.Fatal("the same configured account did not derive stable runtime credentials")
	}
	if other := authenticatedAgentConfig("another-fake-operator", password, agent.Config{}); other.HostRebootToken == config.HostRebootToken {
		t.Fatal("host-management credentials were not scoped to the configured account")
	}
}

func TestAgentHostRebootEnvironmentAndFlags(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, raw := range []string{"", "false", "true"} {
		t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", raw)
		if err := runAgent(t.Context(), logger, []string{"--host-reboot-enabled=false", "--control-node-id", "manager1", "--help"}); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("valid host reboot setting %q or flags rejected: %v", raw, err)
		}
	}
	for _, raw := range []string{"1", "TRUE", "False", " true", "false ", "yes"} {
		t.Setenv("KPL_AGENT_HOST_REBOOT_ENABLED", raw)
		if err := runAgent(t.Context(), logger, nil); err == nil || !strings.Contains(err.Error(), "KPL_AGENT_HOST_REBOOT_ENABLED") {
			t.Fatalf("invalid host reboot setting %q reached Agent startup: %v", raw, err)
		}
	}
}

func TestSwarmAgentBootstrapHostRebootOptIn(t *testing.T) {
	a, runner := bootstrapFixture(t)
	args, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay)
	if err != nil || slices.Contains(args, "--host-reboot-enabled") || len(runner.calls) != 3 {
		t.Fatalf("default bootstrap changed host reboot behavior: %v %v", args, err)
	}
	a.Environ = append(a.Environ, "KPL_AGENT_HOST_REBOOT_ENABLED=true", "KPL_CONTROL_NODE_ID=manager1")
	runner.calls = nil
	args, err = a.swarmAgentArguments(t.Context(), noBootstrapDelay)
	if err != nil || !slices.Contains(args, "--host-reboot-enabled") || bootstrapValue(args, "--control-node-id") != "manager1" || len(runner.calls) != 3 {
		t.Fatalf("host reboot identity or opt-in was not passed to Agent: %v %v", args, err)
	}
}

func TestSwarmAgentBootstrapRejectsInvalidHostRebootBeforeDocker(t *testing.T) {
	for _, settings := range [][]string{
		{"KPL_AGENT_HOST_REBOOT_ENABLED=true"},
		{"KPL_AGENT_HOST_REBOOT_ENABLED=true", "KPL_CONTROL_NODE_ID=manager/bad"},
		{"KPL_AGENT_HOST_REBOOT_ENABLED=1", "KPL_CONTROL_NODE_ID=manager1"},
		{"KPL_AGENT_HOST_REBOOT_ENABLED=TRUE", "KPL_CONTROL_NODE_ID=manager1"},
	} {
		a, runner := bootstrapFixture(t)
		a.Environ = append(a.Environ, settings...)
		if _, err := a.swarmAgentArguments(t.Context(), noBootstrapDelay); err == nil || len(runner.calls) != 0 {
			t.Fatalf("invalid host reboot settings reached Docker: %v %v", settings, err)
		}
	}
}

func TestHostRebootCompletionIsStaticAndOffersBooleanValues(t *testing.T) {
	a, runner, out := completionFixture(t)
	if got := completionCandidates(t, a, out, "swarm", "configure", "KPL_AGENT_HOST"); !reflect.DeepEqual(got, []string{"KPL_AGENT_HOST_REBOOT_ENABLED="}) {
		t.Fatalf("host reboot setting completion: %q", got)
	}
	if got := completionCandidates(t, a, out, "swarm", "configure", "KPL_AGENT_HOST_REBOOT_ENABLED="); !reflect.DeepEqual(got, []string{"KPL_AGENT_HOST_REBOOT_ENABLED=false", "KPL_AGENT_HOST_REBOOT_ENABLED=true"}) {
		t.Fatalf("host reboot values completion: %q", got)
	}
	if len(runner.calls) != 0 {
		t.Fatal("host reboot completion queried Docker")
	}
}
