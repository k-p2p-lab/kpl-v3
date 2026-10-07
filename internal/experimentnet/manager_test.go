package experimentnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
			if f.network == nil {
				return nil, nil
			}
			return []byte(f.network.ID), nil
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
			f.network = nil
			return nil, nil
		case "create":
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
			return []byte("kpl"), nil
		}
		return []byte(fmt.Sprintf(`{"kpl-control":{"IPAddress":"10.90.0.2","NetworkID":"control-network"},"kpl-peers":{"IPAddress":"10.11.0.2","NetworkID":%q}}`, f.network.ID)), nil
	}
	return nil, fmt.Errorf("unexpected Docker operation: %s", call)
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
