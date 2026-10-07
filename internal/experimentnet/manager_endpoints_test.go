package experimentnet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

func TestManagerRepeatedRunsWithDockerLoadBalancerEndpoint(t *testing.T) {
	m, fake, request := managerFixture(t)
	previousID := fake.network.ID
	for i := 0; i < 5; i++ {
		request.RunID, request.Epoch = fmt.Sprintf("run-%d", i), fmt.Sprintf("attempt-%d", i)
		request.RequestedAt = request.RequestedAt.Add(time.Second)
		result, err := m.prepare(t.Context(), request)
		if err != nil || result.NetworkID == "" || result.NetworkID == previousID {
			t.Fatalf("run %d: result=%+v error=%v", i, result, err)
		}
		if _, ok := fake.network.Containers["lb-kpl-peers"]; !ok {
			t.Fatal("fixture did not retain the Docker LB endpoint after gateway attachment")
		}
		previousID = result.NetworkID
	}
	for _, call := range fake.calls {
		if strings.HasPrefix(call, "inspect ") && strings.HasSuffix(call, " lb-kpl-peers") {
			t.Fatalf("Docker LB was inspected as a container: %s", call)
		}
	}
	for _, call := range fake.mutations {
		if call == "rm --force lb-kpl-peers" || strings.HasPrefix(call, "network disconnect ") || strings.HasPrefix(call, "network rm --force ") {
			t.Fatalf("forced network endpoint removal: %s", call)
		}
	}
}

func TestManagerDoesNotMistakeOtherEndpointsForDockerLoadBalancer(t *testing.T) {
	endpointID := strings.Repeat("a", 64)
	for _, tc := range []struct{ name, id, display, endpoint string }{
		{"other-network", "lb-other-network", "other-network-endpoint", endpointID},
		{"prefix-only", "lb-kpl-peers-extra", "kpl-peers-endpoint", endpointID},
		{"wrong-name", "lb-kpl-peers", "peer-container", endpointID},
		{"missing-endpoint-id", "lb-kpl-peers", "kpl-peers-endpoint", ""},
		{"invalid-endpoint-id", "lb-kpl-peers", "kpl-peers-endpoint", "invalid"},
		{"real-container-lb-name", strings.Repeat("b", 64), "kpl-peers-endpoint", endpointID},
		{"orphan-endpoint", "ep-" + endpointID, "kpl-peers-endpoint", endpointID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			raw, err := json.Marshal(map[string]string{"Name": tc.display, "EndpointID": tc.endpoint})
			if err != nil {
				t.Fatal(err)
			}
			fake.network.Containers = map[string]json.RawMessage{tc.id: raw}
			original := m.command
			m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
				if args[0] == "inspect" && args[len(args)-1] == tc.id {
					// Even an existing container with the LB display name must be checked.
					if !strings.Contains(strings.Join(args, " "), "--type container") {
						t.Error("endpoint inspection was not restricted to containers")
					}
					return []byte(""), nil
				}
				return original(ctx, input, args...)
			}
			_, err = m.prepare(t.Context(), request)
			if err == nil || !strings.Contains(err.Error(), tc.id) || !strings.Contains(err.Error(), tc.display) {
				t.Fatalf("endpoint was accepted or not identified: %v", err)
			}
			if len(fake.mutations) != 0 {
				t.Fatalf("unsafe mutation: %v", fake.mutations)
			}
		})
	}
}

func TestManagerEndpointInspectionFailureKeepsIdentityAndDaemonError(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.network.Containers = map[string]json.RawMessage{"missing-container": json.RawMessage(`{"Name":"leftover-peer"}`)}
	_, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "missing-container") || !strings.Contains(err.Error(), "leftover-peer") || !strings.Contains(err.Error(), "No such container") {
		t.Fatalf("lost endpoint diagnosis: %v", err)
	}
	if len(fake.mutations) != 0 {
		t.Fatal("unverified endpoint caused mutation")
	}
}

func TestManagerDockerLoadBalancerStillRequiresUnusedNetwork(t *testing.T) {
	for _, kind := range []string{"service", "remote-attachment"} {
		t.Run(kind, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			fake.network.Containers = map[string]json.RawMessage{"lb-kpl-peers": json.RawMessage(fmt.Sprintf(`{"Name":"kpl-peers-endpoint","EndpointID":%q}`, strings.Repeat("a", 64)))}
			fake.legacyService = kind == "service"
			fake.remoteAttached = kind == "remote-attachment"
			if _, err := m.prepare(t.Context(), request); err == nil {
				t.Fatal("in-use network accepted")
			}
			if len(fake.mutations) != 0 {
				t.Fatalf("changed in-use network: %v", fake.mutations)
			}
		})
	}
}
