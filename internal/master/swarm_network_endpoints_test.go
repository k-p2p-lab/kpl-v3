package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestSwarmNetworkResetRecognizesOnlyExactInternalLB(t *testing.T) {
	endpointID := strings.Repeat("a", 64)
	for _, test := range []struct {
		name, key, endpointName, endpointID string
		count                               int
		extra, inspectError, malformed      bool
		want                                bool
	}{
		{name: "internal-lb", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", endpointID: endpointID, count: 1, want: true},
		{name: "physical-peer", key: strings.Repeat("b", 64), endpointName: "kpl-peers-endpoint", endpointID: endpointID, count: 1},
		{name: "other-network", key: "lb-other-network", endpointName: "other-network-endpoint", endpointID: endpointID, count: 1},
		{name: "prefix-only", key: "lb-kpl-peers-extra", endpointName: "kpl-peers-endpoint", endpointID: endpointID, count: 1},
		{name: "wrong-name", key: "lb-kpl-peers", endpointName: "peer-container", endpointID: endpointID, count: 1},
		{name: "missing-id", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", count: 1},
		{name: "invalid-id", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", endpointID: "invalid", count: 1},
		{name: "uppercase-id", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", endpointID: strings.Repeat("A", 64), count: 1},
		{name: "peer-plus-lb", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", endpointID: endpointID, count: 2, extra: true},
		{name: "peer-attached-between-inspections", key: "lb-kpl-peers", endpointName: "kpl-peers-endpoint", endpointID: endpointID, count: 1, extra: true},
		{name: "inspection-error", count: 1, inspectError: true},
		{name: "malformed-endpoints", count: 1, malformed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "reset-peer-network")
			runner.handle = func(_ context.Context, c Command) (string, error) {
				if !slices.Equal(c.Args[:min(len(c.Args), 2)], []string{"network", "inspect"}) {
					t.Fatalf("endpoint validation mutated Docker: %v", c.Args)
				}
				switch c.Args[3] {
				case swarmResetFormat:
					return fmt.Sprintf("%s|overlay|swarm|true|false|false|false|false||default|0|1|%d|%s|%s||", s.network, test.count, swarmApplication, s.stack), nil
				case swarmResetEndpointsFormat:
					if test.inspectError {
						return "", errors.New("fixture endpoint inspection failed")
					}
					if test.malformed {
						return "{", nil
					}
					endpoints := map[string]map[string]string{test.key: {"Name": test.endpointName, "EndpointID": test.endpointID}}
					if test.extra {
						endpoints["physicalpeer"] = map[string]string{"Name": "leftover-peer", "EndpointID": strings.Repeat("c", 64)}
					}
					data, err := json.Marshal(endpoints)
					return string(data), err
				default:
					t.Fatalf("unexpected inspection: %v", c.Args)
					return "", nil
				}
			}
			valid, err := s.validResetPeerNetwork(t.Context(), "oldnetwork")
			if valid != test.want || (err != nil) != (test.inspectError || test.malformed) {
				t.Fatalf("unsafe endpoint classification: valid=%v err=%v", valid, err)
			}
		})
	}
}

func TestSwarmNetworkResetRechecksInternalLBAndReplacement(t *testing.T) {
	for _, mode := range []string{"unchanged", "peer-before-removal", "inspection-failure-before-removal", "peer-after-creation", "changed-replacement"} {
		t.Run(mode, func(t *testing.T) {
			s, runner, _ := newSwarmFixture(t, "reset-peer-network")
			removed, created := false, false
			endpointChecks := 0
			runner.handle = func(_ context.Context, c Command) (string, error) {
				a := c.Args
				switch {
				case a[0] == "service":
					return "", nil
				case a[0] == "node" && a[1] == "ls":
					return "control", nil
				case a[0] == "node" && a[1] == "inspect":
					return "ready", nil
				case a[0] == "network" && a[1] == "rm":
					if a[2] != "oldnetwork" {
						t.Fatalf("removed unverified identity: %v", a)
					}
					removed = true
					return "", nil
				case a[0] == "network" && a[1] == "create":
					created = true
					return "newnetwork", nil
				case a[0] == "network" && a[1] == "ls":
					if created {
						return "newnetwork", nil
					}
					if removed {
						return "", nil
					}
					return "oldnetwork", nil
				case a[0] == "network" && a[1] == "inspect":
					switch a[3] {
					case "{{.Id}}":
						return "oldnetwork", nil
					case swarmIPAMFormat:
						return `"10.42.0.0/24"|"10.42.0.1"|""|0`, nil
					case swarmResetFormat:
						if created && mode == "changed-replacement" {
							return "foreign network settings", nil
						}
						count := 1
						if created && mode != "peer-after-creation" {
							count = 0
						}
						return fmt.Sprintf("%s|overlay|swarm|true|false|false|false|false||default|0|1|%d|%s|%s||", s.network, count, swarmApplication, s.stack), nil
					case swarmResetEndpointsFormat:
						endpointChecks++
						if endpointChecks == 2 && mode == "inspection-failure-before-removal" {
							return "", errors.New("fixture endpoint inspection failed")
						}
						key, name := "lb-kpl-peers", "kpl-peers-endpoint"
						if endpointChecks == 2 && mode == "peer-before-removal" || created && mode == "peer-after-creation" {
							key, name = "physicalpeer", "leftover-peer"
						}
						return fmt.Sprintf(`{%q:{"Name":%q,"EndpointID":%q}}`, key, name, strings.Repeat("a", 64)), nil
					}
				}
				return "", fmt.Errorf("unexpected Docker command: %v", a)
			}
			err := s.resetPeerNetwork(t.Context())
			if mode == "unchanged" {
				if err != nil || !removed || !created || endpointChecks != 2 {
					t.Fatalf("verified internal LB prevented reset: err=%v removed=%v created=%v endpointChecks=%d", err, removed, created, endpointChecks)
				}
			} else if err == nil {
				t.Fatal("unverified endpoint or replacement was reported successful")
			}
			if strings.HasSuffix(mode, "before-removal") && (removed || created) {
				t.Fatal("network changed after a failed pre-deletion recheck")
			}
			for _, call := range runner.calls {
				if call.Args[0] == "rm" || slices.Equal(call.Args[:min(len(call.Args), 2)], []string{"network", "disconnect"}) {
					t.Fatalf("reset removed an internal sandbox directly: %v", call.Args)
				}
			}
		})
	}
}
