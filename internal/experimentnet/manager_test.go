package experimentnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type fakeNetworkDocker struct {
	network                                *networkInfo
	gateway                                bool
	sequence                               int
	calls                                  []string
	mutations                              []string
	remoteAttached, legacyService, offline bool
	failCreate, failStart                  bool
	removing                               bool
	removeLag, createLag, createConflicts  int
	createCalls                            int
}

func fixtureNetwork(t *testing.T) *networkInfo {
	t.Helper()
	var networks []networkInfo
	err := json.Unmarshal([]byte(`[{"Id":"original-network","Name":"kpl-peers","Driver":"overlay","Scope":"swarm","Attachable":true,"IPAM":{"Driver":"default","Config":[{"Subnet":"10.11.0.0/16","Gateway":"10.11.0.1"}]},"Labels":{"io.kpl.application":"kp2plab-v3","io.kpl.stack":"kpl"}}]`), &networks)
	if err != nil {
		t.Fatal(err)
	}
	return &networks[0]
}

func managerFixture(t *testing.T) (*Manager, *fakeNetworkDocker, model.ExperimentNetworkRequest) {
	t.Helper()
	m, err := NewManager(ManagerConfig{Stack: "kpl", PeerNetwork: "kpl-peers", ControlNetwork: "kpl-control", ControllerURL: "http://controller:8080", Token: "test-key", SelfContainer: "manager-task", DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeNetworkDocker{network: fixtureNetwork(t)}
	m.command = fake.command
	m.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("gateway readiness request lacks authentication")
		}
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	request := model.ExperimentNetworkRequest{RunID: "run-one", Epoch: "attempt-one", RequestedAt: time.Now().UTC(), Agents: map[string]string{"agent": "http://agent:8090"}}
	return m, fake, request
}

func (f *fakeNetworkDocker) command(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	call := strings.Join(args, " ")
	f.calls = append(f.calls, call)
	switch args[0] {
	case "info":
		return []byte("true"), nil
	case "node":
		if f.offline {
			return []byte("Ready\nDown"), nil
		}
		return []byte("Ready\nReady"), nil
	case "service":
		if args[1] == "ls" {
			return []byte("controller-service"), nil
		}
		if f.legacyService {
			return []byte(f.network.ID), nil
		}
		return []byte("control-network"), nil
	case "network":
		switch args[1] {
		case "ls":
			if f.removing {
				if f.removeLag > 0 {
					f.removeLag--
				} else {
					f.network, f.removing = nil, false
				}
			}
			if f.network != nil && f.sequence > 0 && !f.removing && f.createLag > 0 {
				f.createLag--
				return nil, nil
			}
			if f.network == nil {
				return nil, nil
			}
			// Docker truncates network IDs even with --quiet unless --no-trunc
			// is requested; inspect and create return complete IDs.
			id := f.network.ID
			if !slices.Contains(args, "--no-trunc") && len(id) > 12 {
				id = id[:12]
			}
			return []byte(id), nil
		case "inspect":
			if f.network == nil {
				return nil, errors.New("network absent")
			}
			return json.Marshal([]networkInfo{*f.network})
		case "rm":
			if f.remoteAttached {
				return nil, errors.New("network has active remote attachments")
			}
			if f.gateway {
				return nil, errors.New("gateway still attached")
			}
			if len(args) != 3 || f.network == nil || args[2] != f.network.ID {
				return nil, errors.New("network was not removed by captured ID")
			}
			f.mutations = append(f.mutations, call)
			if f.removeLag > 0 {
				f.removing = true
			} else {
				f.network = nil
			}
			return nil, nil
		case "create":
			f.createCalls++
			if f.network != nil || f.createConflicts > 0 {
				if f.createConflicts > 0 {
					f.createConflicts--
				}
				return nil, errors.New("Error response from daemon: network with name kpl-peers already exists")
			}
			f.mutations = append(f.mutations, call)
			if f.failCreate {
				return nil, errors.New("daemon creation response unavailable")
			}
			f.sequence++
			var n networkInfo
			n.ID, n.Name, n.Driver, n.Scope, n.Attachable = fmt.Sprintf("fresh-network-%d", f.sequence), "kpl-peers", "overlay", "swarm", true
			n.IPAM.Driver = "default"
			n.Labels = map[string]string{}
			var subnet, gateway string
			for i, a := range args {
				switch a {
				case "--subnet":
					subnet = args[i+1]
				case "--gateway":
					gateway = args[i+1]
				case "--label":
					k, v, _ := strings.Cut(args[i+1], "=")
					n.Labels[k] = v
				}
			}
			encoded := fmt.Sprintf(`{"Config":[{"Subnet":%q,"Gateway":%q}],"Driver":"default"}`, subnet, gateway)
			if err := json.Unmarshal([]byte(encoded), &n.IPAM); err != nil {
				return nil, err
			}
			f.network = &n
			return []byte(n.ID), nil
		case "connect":
			f.mutations = append(f.mutations, call)
			// Attaching the gateway also creates Docker's load-balancer
			// sandbox, which appears in network inspect but is not a container.
			f.network.Containers = map[string]json.RawMessage{
				"gateway-container":    json.RawMessage(`{"Name":"kpl-network-gateway"}`),
				"lb-" + f.network.Name: json.RawMessage(fmt.Sprintf(`{"Name":%q,"EndpointID":%q}`, f.network.Name+"-endpoint", strings.Repeat("a", 64))),
			}
			return nil, nil
		}
	case "ps":
		if f.gateway {
			return []byte("gateway-container"), nil
		}
		return nil, nil
	case "rm":
		f.mutations = append(f.mutations, call)
		f.gateway = false
		if f.network != nil {
			delete(f.network.Containers, "gateway-container")
		}
		return nil, nil
	case "create":
		if f.network == nil {
			return nil, errors.New("gateway created before network")
		}
		f.mutations = append(f.mutations, call)
		f.gateway = true
		return []byte("gateway-container"), nil
	case "cp":
		if input == nil {
			return nil, errors.New("missing gateway config")
		}
		_, err := io.Copy(io.Discard, input)
		return nil, err
	case "start":
		if f.failStart {
			return nil, errors.New("gateway start failed")
		}
		return nil, nil
	case "inspect":
		if strings.Contains(call, "{{.Image}}") {
			return []byte("sha256:test-image"), nil
		}
		if strings.Contains(call, "io.kpl.network-gateway") {
			if args[len(args)-1] == "gateway-container" && f.gateway {
				return []byte("kpl"), nil
			}
			return nil, errors.New("No such container: " + args[len(args)-1])
		}
		return fmt.Appendf(nil, `{"kpl-control":{"IPAddress":"10.90.0.2","NetworkID":"control-network"},"kpl-peers":{"IPAddress":"10.11.0.2","NetworkID":%q}}`, f.network.ID), nil
	}
	return nil, fmt.Errorf("unexpected Docker operation: %s", call)
}

func TestManagerInspectKeepsFullDockerNetworkID(t *testing.T) {
	m, fake, _ := managerFixture(t)
	fake.network.ID = "fsf1dmx3i9q75an49z36jycxd"
	network, err := m.inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if network == nil || network.ID != fake.network.ID {
		t.Fatalf("network identity was not preserved: %+v", network)
	}
	if len(fake.mutations) != 0 {
		t.Fatal("inspection changed Docker state")
	}
}

func TestManagerFreshNetworkEachAttemptAndIdempotentAcknowledgment(t *testing.T) {
	m, fake, request := managerFixture(t)
	first, err := m.prepare(t.Context(), request)
	if err != nil || first.NetworkID == "original-network" || first.NetworkID == "" {
		t.Fatalf("first: %+v %v", first, err)
	}
	mutations := len(fake.mutations)
	repeated, err := m.prepare(t.Context(), request)
	if err != nil || repeated != first || len(fake.mutations) != mutations {
		t.Fatalf("duplicate acknowledgment reset a live network: %+v %v", repeated, err)
	}
	request.RunID, request.Epoch = "run-two", "attempt-two"
	request.RequestedAt = request.RequestedAt.Add(time.Second)
	second, err := m.prepare(t.Context(), request)
	if err != nil || second.NetworkID == first.NetworkID {
		t.Fatalf("second: %+v %v", second, err)
	}
	if fake.network.IPAM.Config[0].Subnet != "10.11.0.0/16" || fake.network.IPAM.Config[0].Gateway != "10.11.0.1" {
		t.Fatal("IPAM changed")
	}
	for _, call := range fake.calls {
		if strings.Contains(call, "network rm --force") || strings.HasPrefix(call, "service update") || strings.Contains(call, "docker.sock") {
			t.Fatalf("unapproved mutation: %s", call)
		}
	}
	mutations = len(fake.mutations)
	request.Epoch = "attempt-one"
	request.RequestedAt = request.RequestedAt.Add(-time.Second)
	if _, err := m.prepare(t.Context(), request); err == nil || len(fake.mutations) != mutations {
		t.Fatal("stale start mutated network")
	}
}

func TestManagerRefusesUnverifiedOrCustomNetworkBeforeMutation(t *testing.T) {
	for _, kind := range []string{"owner", "custom", "legacy-service", "offline", "endpoint", "invalid-gateway"} {
		t.Run(kind, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			switch kind {
			case "owner":
				fake.network.Labels["io.kpl.stack"] = "other"
			case "custom":
				fake.network.Options = map[string]string{"encrypted": ""}
			case "legacy-service":
				fake.legacyService = true
			case "offline":
				fake.offline = true
			case "endpoint":
				fake.network.Containers = map[string]json.RawMessage{"peer-container": json.RawMessage(`{}`)}
			case "invalid-gateway":
				fake.network.IPAM.Config[0].Gateway = "10.11.255.255"
			}
			if kind == "endpoint" {
				original := m.command
				m.command = func(ctx context.Context, r io.Reader, args ...string) ([]byte, error) {
					if args[0] == "inspect" {
						return []byte(""), nil
					}
					return original(ctx, r, args...)
				}
			}
			if _, err := m.prepare(t.Context(), request); err == nil {
				t.Fatal("unsafe network accepted")
			}
			if len(fake.mutations) != 0 {
				t.Fatalf("changed Docker state: %v", fake.mutations)
			}
		})
	}
}

func TestManagerRemoteAttachmentsAndAmbiguousCreationBlockActivation(t *testing.T) {
	m, fake, request := managerFixture(t)
	fake.remoteAttached = true
	if got, err := m.prepare(t.Context(), request); err == nil || got.NetworkID != "" {
		t.Fatalf("remote attachment ignored: %+v %v", got, err)
	}
	if len(fake.mutations) != 0 {
		t.Fatal("created a replacement while old network is in use")
	}
	fake.remoteAttached = false
	fake.failCreate = true
	if got, err := m.prepare(t.Context(), request); err == nil || got.NetworkID != "" {
		t.Fatalf("ambiguous creation accepted: %+v %v", got, err)
	}
	fake.failCreate = false
	request.Epoch = "recovery"
	request.RequestedAt = request.RequestedAt.Add(time.Second)
	got, err := m.prepare(t.Context(), request)
	if err != nil || got.NetworkID == "" {
		t.Fatalf("allocation was not recoverable: %+v %v", got, err)
	}
}

func TestManagerCancellationAndConcurrentPreparationDoNotMutate(t *testing.T) {
	m, fake, request := managerFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := m.prepare(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	m.mu.Lock()
	_, err := m.prepare(t.Context(), request)
	m.mu.Unlock()
	if err == nil || len(fake.calls) > 0 {
		t.Fatal("concurrent preparation reached Docker")
	}
}

func TestManagerRejectsMalformedBodyBeforeDocker(t *testing.T) {
	for _, suffix := range []string{"{}", "!", strings.Repeat(" ", 1<<20)} {
		m, fake, request := managerFixture(t)
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/network/prepare", strings.NewReader(string(body)+suffix))
		req.Header.Set("Authorization", "Bearer test-key")
		result := httptest.NewRecorder()
		m.Handler().ServeHTTP(result, req)
		if result.Code != http.StatusBadRequest || len(fake.calls) != 0 {
			t.Fatalf("status=%d Docker calls=%v", result.Code, fake.calls)
		}
	}
}

func TestManagerNetworkRejectionReportsTheFailedCheckBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*networkInfo)
		want   string
	}{
		{"identity", func(n *networkInfo) { n.ID += "replacement" }, "Peer network identity mismatch:"},
		{"name", func(n *networkInfo) { n.Name = "another-network" }, "Peer network identity mismatch:"},
		{"driver", func(n *networkInfo) { n.Driver = "bridge" }, `driver="bridge"`},
		{"attachable", func(n *networkInfo) { n.Attachable = false }, "attachable=false"},
		{"IPv6", func(n *networkInfo) { n.EnableIPv6 = true }, "ipv6=true"},
		{"IPAM-driver", func(n *networkInfo) { n.IPAM.Driver = "custom" }, `default IPAM with one IPv4 allocation and no custom IPAM options: driver="custom"`},
		{"IPAM-options", func(n *networkInfo) { n.IPAM.Options = map[string]string{"custom": "value"} }, "options=1"},
		{"missing-allocation", func(n *networkInfo) { n.IPAM.Config = nil }, "allocations=0"},
		{"multiple-allocations", func(n *networkInfo) { n.IPAM.Config = append(n.IPAM.Config, n.IPAM.Config[0]) }, "allocations=2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, fake, request := managerFixture(t)
			original := m.command
			m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
				if len(args) >= 2 && args[0] == "network" && args[1] == "inspect" {
					inspected := *fake.network
					tc.change(&inspected)
					return json.Marshal([]networkInfo{inspected})
				}
				return original(ctx, input, args...)
			}
			_, err := m.prepare(t.Context(), request)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("wanted %q, got %v", tc.want, err)
			}
			if len(fake.mutations) != 0 {
				t.Fatalf("invalid network changed Docker state: %v", fake.mutations)
			}
		})
	}
}

func TestManagerRetainsInspectionErrorForNewNetwork(t *testing.T) {
	m, fake, request := managerFixture(t)
	original := m.command
	m.command = func(ctx context.Context, input io.Reader, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "network" && args[1] == "inspect" && fake.sequence > 0 {
			return nil, errors.New("daemon inspect unavailable")
		}
		return original(ctx, input, args...)
	}
	result, err := m.prepare(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "fresh Peer network could not be verified: daemon inspect unavailable") || result.NetworkID != "" {
		t.Fatalf("inspection error was lost: result=%+v error=%v", result, err)
	}
	if fake.gateway {
		t.Fatal("gateway was created for an unverified network")
	}
}
