package experimentnet

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagerAcknowledgedReplayRejectsReusedHealthAddressWithoutGateway(t *testing.T) {
	m, fake, request := managerFixture(t)
	first, err := m.prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	// The acknowledged gateway is gone. Another HTTP listener has acquired its
	// former address and returns the same status without proving its identity.
	fake.gateway = false
	delete(fake.network.Containers, "gateway-container")
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer other.Close()
	endpoint, err := url.Parse(other.URL)
	if err != nil {
		t.Fatal(err)
	}
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.URL.Scheme, copy.URL.Host = endpoint.Scheme, endpoint.Host
		return other.Client().Transport.RoundTrip(copy)
	})}
	defer m.client.CloseIdleConnections()
	mutations := len(fake.mutations)
	result, err := m.prepare(t.Context(), request)
	if err == nil || result.NetworkID != "" {
		t.Fatalf("unrelated listener acknowledged a missing gateway: previous network=%q result=%+v err=%v", first.NetworkID, result, err)
	}
	if len(fake.mutations) != mutations {
		t.Fatal("read-only acknowledgment replay mutated the overlay or gateway")
	}
}

func TestManagerAcknowledgedReplayUsesRecordedNetworkInsteadOfRetiringNameView(t *testing.T) {
	for _, view := range []string{"retiring", "absent", "old-and-fresh", "old-fresh-and-third", "third"} {
		t.Run(view, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			retiring := fixtureNetwork(t)
			first, err := m.prepare(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			mutations := len(fake.mutations)
			original, listed := m.command, false
			m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
				if len(args) > 1 && args[0] == "network" {
					if args[1] == "ls" && !listed {
						listed = true
						switch view {
						case "retiring":
							return []byte(retiring.ID), nil
						case "absent":
							return nil, nil
						case "old-and-fresh":
							return []byte(retiring.ID + " " + first.NetworkID), nil
						case "old-fresh-and-third":
							return []byte(retiring.ID + " " + first.NetworkID + " third-network"), nil
						default:
							return []byte("third-network"), nil
						}
					}
					if args[1] == "inspect" && args[2] == retiring.ID {
						return json.Marshal([]networkInfo{*retiring})
					}
				}
				return original(ctx, input, args...)
			}
			result, err := m.prepare(t.Context(), request)
			if err != nil || result != first || len(fake.mutations) != mutations {
				t.Fatalf("retiring name view reset an acknowledged gateway: result=%+v err=%v mutations before=%d after=%d", result, err, mutations, len(fake.mutations))
			}
		})
	}
}

func TestManagerAcknowledgedReplayRejectsChangedEvidenceWithoutMutation(t *testing.T) {
	for _, changed := range []string{"removed-network", "replaced-network", "changed-execution", "corrupt-active", "multiple-active-values", "unreadable-active", "corrupt-active-retiring-name", "missing-active", "wrong-run", "wrong-epoch", "invalid-network-id"} {
		t.Run(changed, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			first, err := m.prepare(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(m.config.DataDir, "active.json")
			switch changed {
			case "removed-network":
				fake.network, fake.gateway = nil, false
			case "replaced-network":
				fake.network.ID = "third-network"
			case "changed-execution":
				fake.network.Labels["io.kpl.execution"] = "another-attempt"
			case "corrupt-active", "corrupt-active-retiring-name":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "multiple-active-values":
				data, err := json.Marshal(first)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, append(data, []byte("{}")...), 0600); err != nil {
					t.Fatal(err)
				}
			case "unreadable-active":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "missing-active":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			default:
				altered := first
				if changed == "wrong-run" {
					altered.RunID = "another-run"
				} else if changed == "wrong-epoch" {
					altered.Epoch = "another-attempt"
				} else {
					altered.NetworkID = "--invalid-id"
				}
				if err := m.persist("active.json", altered); err != nil {
					t.Fatal(err)
				}
			}
			original := m.command
			if changed == "corrupt-active-retiring-name" {
				retiring := fixtureNetwork(t)
				m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
					if len(args) > 1 && args[0] == "network" {
						if args[1] == "ls" {
							return []byte(retiring.ID), nil
						}
						if args[1] == "inspect" && args[2] == retiring.ID {
							return json.Marshal([]networkInfo{*retiring})
						}
					}
					return original(ctx, input, args...)
				}
			}
			mutations := len(fake.mutations)
			result, err := m.prepare(t.Context(), request)
			m.command = original
			if err == nil || result.NetworkID != "" || len(fake.mutations) != mutations {
				t.Fatalf("changed acknowledgment evidence was accepted or mutated: result=%+v err=%v before=%d after=%d", result, err, mutations, len(fake.mutations))
			}
			if changed == "unreadable-active" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			older := request
			request.Epoch = "recovery-attempt"
			request.RequestedAt = request.RequestedAt.Add(time.Second)
			if result, err := m.prepare(t.Context(), request); err != nil || result.NetworkID == "" || result.NetworkID == first.NetworkID {
				t.Fatalf("new attempt did not recover changed acknowledgment evidence: result=%+v err=%v", result, err)
			}
			mutations = len(fake.mutations)
			if _, err := m.prepare(t.Context(), older); err == nil || !strings.Contains(err.Error(), "stale") || len(fake.mutations) != mutations {
				t.Fatalf("stale acknowledged attempt was not fenced: err=%v before=%d after=%d", err, mutations, len(fake.mutations))
			}
		})
	}
}

func TestManagerVerifiesGatewayContainerAndAttachments(t *testing.T) {
	tests := []struct {
		name       string
		change     func(*gatewayInspection)
		valid      bool
		replayOnly bool
	}{
		{name: "unchanged", valid: true},
		{name: "restarted-pid", valid: true, change: func(g *gatewayInspection) { g.PID++ }},
		{name: "replaced-container", change: func(g *gatewayInspection) { g.ID = "another-container" }},
		{name: "wrong-name", change: func(g *gatewayInspection) { g.Name = "/other-gateway" }},
		{name: "stopped", change: func(g *gatewayInspection) { g.Running, g.PID = false, 0 }},
		{name: "missing-pid", change: func(g *gatewayInspection) { g.PID = 0 }},
		{name: "foreign-gateway-owner", change: func(g *gatewayInspection) { g.Labels["io.kpl.network-gateway"] = "other-stack" }},
		{name: "foreign-stack-owner", change: func(g *gatewayInspection) { g.Labels["com.docker.stack.namespace"] = "other-stack" }},
		{name: "foreign-execution", change: func(g *gatewayInspection) { g.Labels["io.kpl.execution"] = "other-attempt" }},
		{name: "different-control-network", change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "10.90.0.2", NetworkID: "other-control-network"}
		}},
		{name: "different-peer-network", change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "10.11.0.2", NetworkID: "other-peer-network"}
		}},
		{name: "missing-control-network", change: func(g *gatewayInspection) { delete(g.Networks, "kpl-control") }},
		{name: "missing-peer-network", change: func(g *gatewayInspection) { delete(g.Networks, "kpl-peers") }},
		{name: "extra-network", change: func(g *gatewayInspection) { g.Networks["other"] = gatewayNetworkEndpoint{NetworkID: "other-network"} }},
		{name: "unspecified-control-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "0.0.0.0", NetworkID: "control-network"}
		}},
		{name: "loopback-control-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "127.0.0.1", NetworkID: "control-network"}
		}},
		{name: "multicast-control-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "224.0.0.1", NetworkID: "control-network"}
		}},
		{name: "unspecified-peer-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "0.0.0.0", NetworkID: g.Networks["kpl-peers"].NetworkID}
		}},
		{name: "loopback-peer-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "127.0.0.1", NetworkID: g.Networks["kpl-peers"].NetworkID}
		}},
		{name: "multicast-peer-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "224.0.0.1", NetworkID: g.Networks["kpl-peers"].NetworkID}
		}},
		{name: "identical-control-and-peer-address", change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "10.11.0.2", NetworkID: "control-network"}
		}},
		{name: "peer-outside-subnet", change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "10.12.0.2", NetworkID: g.Networks["kpl-peers"].NetworkID}
		}},
		{name: "changed-control-address", replayOnly: true, change: func(g *gatewayInspection) {
			g.Networks["kpl-control"] = gatewayNetworkEndpoint{IPAddress: "10.90.0.4", NetworkID: "control-network"}
		}},
		{name: "changed-peer-address", replayOnly: true, change: func(g *gatewayInspection) {
			g.Networks["kpl-peers"] = gatewayNetworkEndpoint{IPAddress: "10.11.0.4", NetworkID: g.Networks["kpl-peers"].NetworkID}
		}},
	}
	for _, replay := range []bool{false, true} {
		stage := "new"
		if replay {
			stage = "replay"
		}
		for _, test := range tests {
			if test.replayOnly && !replay {
				continue
			}
			t.Run(stage+"/"+test.name, func(t *testing.T) {
				m, fake, request := managerFixture(t)
				if replay {
					if _, err := m.prepare(t.Context(), request); err != nil {
						t.Fatal(err)
					}
				}
				mutations := len(fake.mutations)
				original := m.command
				m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
					output, err := original(ctx, input, args...)
					if err != nil || len(args) < 6 || args[4] != gatewayInspectionFormat || test.change == nil {
						return output, err
					}
					var gateway gatewayInspection
					if err := json.Unmarshal(output, &gateway); err != nil {
						return nil, err
					}
					test.change(&gateway)
					return json.Marshal(gateway)
				}
				result, err := m.prepare(t.Context(), request)
				if test.valid {
					if err != nil || result.NetworkID == "" {
						t.Fatalf("current gateway rejected: result=%+v err=%v", result, err)
					}
				} else if err == nil || result.NetworkID != "" {
					t.Fatalf("unverified gateway accepted: result=%+v err=%v", result, err)
				}
				if replay && len(fake.mutations) != mutations {
					t.Fatal("acknowledgment replay mutated gateway or overlay")
				}
			})
		}
	}
}
